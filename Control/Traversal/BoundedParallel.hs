{-# LANGUAGE ScopedTypeVariables #-}
module Control.Traversal.BoundedParallel where

-- import Control.Applicative
import Control.Concurrent
import Control.Concurrent.MVar
import Control.Exception
import Control.Monad
-- import Control.Monad.State.Strict
import Data.Int
import Data.List
import Data.Typeable
import Debug.Trace
import System.IO

data BPOpts =
  BPOpts {
    poParallelism :: !Int -- K
  , poWindow      :: !Int -- W
  , poSpawnThread :: !(IO () -> IO ThreadId) -- usually forkIO or forkOS
  }

dftBPOpts :: BPOpts
dftBPOpts =
  dftBPOpts {
    poParallelism = 2 -- (<=0 getNumCapabilities)
  , poWindow = 4
  , poSpawnThread = forkIO
  }


--
-- Problem is that we want a parallel fold to incrementally generate
-- results from a window at a time.  E.g. generating a list in parallel.
--
-- SPEC:
--  => Exactly K threads processing in parallel.  So we won't be reducing
--     too many (IO a)'s at once.  For example, if each IO is a resource
--     hog, we don't blow out resource use (the whole point of
--     being "bounded")
--
--  => There is a window max W that bounds how far ahead processing threads
--     may work from the last reduction.  E.g. if the reduction step or any
--     given element takes too long, it will cause the other threads to
--     stall.  It follows that W must be >= K.
--
--  => The input (N) can be arbitrarily long (i.e. no O(N) storage).
--     We can use O(K) or O(W) space though.  The expectation is that
--     W can be quite large if one's intermediate results @b@'s are
--     sufficiently small.
--
--  => Exceptions are Deterministic.
--     If any computation of any element raises an exception, that
--     exception is re-raised on the primary thread.  If multiple elements
--     raise an exception, the lowest is re-raised (logically the first in
--     the list).
--
--  => Cleanup.  before returning, all spawned threads have exited.  This
--     includes their
--
--  => Full reduction of the parallel element is the user's respsonsiblity.
--     (as usual, use deepseq or some other means to force each value to NF)
--
--  => The results are deterministic.  The fold is left associative
--     with respect to the list.
--
-- DETERMINE:
--  => Do we force reduction on the calling thread or the
--     TEST: ensure that any exception in the reduction step is properly
--           propagated either way (ensure cleanup too)
--
--  => Arbitrary association might allow for even better parallelism.
--     But this makes the user's algorithm non-deterministic if they violate
--     commutativity.
--
-- NOT SPEC:
--  => The accumulator function should not be the bottleneck.
--      [this would indicate a poor parallel algorithm, consider
--       chunking some of the accumulation in parallel stages]
--
-- bpfold :: ParallelOpts -> (Double -> Int -> IO Double) -> Double -> [IO Int] -> IO Double
bpfold :: BPOpts -> (b -> a -> IO b) -> b -> [IO a] -> IO b
bpfold pos acc b0 a_ios = setup
  where setup = do
          let k :: Int
              k = poParallelism pos

              w :: Int
              w = max k (poWindow pos)

              forkThread :: IO () -> IO ThreadId
              forkThread = poSpawnThread pos

              -- createEmptyMVarList :: Int -> IO [MVar a]
              createEmptyMVarList n = sequence (replicate n newEmptyMVar)

          mv_exited_vars <- createEmptyMVarList k
          mv_output_window <- createEmptyMVarList w -- :: IO [MVar (Either SomeException a)]
          --  inputs :: [(Int, IO a, MVar (Either SomeException a))]
          let inputs = zip3 [0..] a_ios (cycle mv_output_window)
          mv_inputs <- newMVar inputs
          mv_print_lock <- newMVar ()
          tids <-
            forM (zip [0 .. k-1] mv_exited_vars) $ \(tix,mv_exited) ->
              forkThread $ worker tix mv_exited mv_print_lock mv_inputs
          reducer tids mv_exited_vars mv_print_lock inputs


        -- reducer :: [ThreadId] -> [MVar ()] -> MVar () -> [(Int,IO Int,MVar (Either SomeException Int))] -> IO Double
        reducer tids mv_exited mv_print_lock inputs = reducerReduce b0 inputs
          where reducerReduce b [] = return b
                reducerReduce b ((ix,_,mv_a):inputs) = do
                  exa <- takeMVar mv_a
                  case exa of
                    Left (e :: SomeException) -> do
                      reducerPrintLn $ "processing exception " ++ show ix
                      reducerStopWorkers >> throwIO e
                    Right a -> do
                      reducerPrintLn $ "reduced " ++ show ix
                      b1 <- acc b a `onException` reducerStopWorkers
                      reducerReduce b1 inputs

                reducerStopWorkers :: IO ()
                reducerStopWorkers = do
                  reducerPrintLn "stopping workers"
                  mapM_ (\tid -> throwTo tid KillException) tids
                  mapM_ takeMVar mv_exited
                  reducerPrintLn "   workers stopped"

                reducerPrintLn :: String -> IO ()
                reducerPrintLn msg =
                  withMVar mv_print_lock $ \_ ->
                    putStrLn $ "[REDUCER]:  " ++ msg


--        worker :: Int -> MVar () -> MVar () -> MVar [(Int,IO Int,MVar (Either SomeException Int))] -> IO ()
        worker tix mv_exited mv_print_lock mv_input =
            finally
              (catch workerProcessNext topLevelHandler)
              workerSignalExited
          where topLevelHandler :: SomeException -> IO ()
                topLevelHandler e = workerPrintLn ("caught " ++ show e)

                workerProcessNext :: IO ()
                workerProcessNext = do
                  inputs <- takeMVar mv_input
                  case inputs of
                    -- end of the line => exit thread
                    [] -> workerPrintLn "worker exiting naturally"
                    ((a_ix,a_io,mv_out):tail_inputs) -> do
                      workerPrintLn $ "fetching " ++ show a_ix
                      putMVar mv_input tail_inputs -- put the tail back
                      exa <- try a_io
                      case exa of
                        Left e -> do
                          workerPrintLn $ "exception raised " ++ show e
                          putMVar mv_out exa
                          workerPrintLn "worker self-exiting since it raised an exception"
                        Right a -> do
                          workerPrintLn $ "reduced " ++ show a_ix
                          putMVar mv_out exa
                          workerProcessNext

                workerSignalExited = do
                  workerPrintLn "exiting (signaling)"
                  putMVar mv_exited ()

                workerPrintLn :: String -> IO ()
                workerPrintLn msg =
                  withMVar mv_print_lock $ \_ ->
                    putStrLn $ "[WORKER:" ++ show tix ++ "]: " ++ msg

-- IDEAS:
-- Lazy zipping the input list with the (cycled) window
--
--   [a0, a1,     a2, a3,    ... infinite
--   [w0, w1] ++ [w0, w1] ++ ... W mvars to accept (SomeException | a)


data KillException = KillException
  deriving (Show,Typeable)

instance Exception KillException

