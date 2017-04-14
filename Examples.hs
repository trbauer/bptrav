import Control.Traversal.BoundedParallel

import Control.Concurrent

-- Simple example
example1 :: IO ()
example1 = do
  let reduce :: String -> String -> IO String
      reduce pfx sfx = return $ pfx ++ sfx

      genChunk :: Int -> IO String
      genChunk i = do
        let ms = 1000
        threadDelay (500*ms)
        return $ show i ++ "\n"

  r <- bpfold dftBPOpts reduce "" (map genChunk [0..9])
  putStrLn r


{-
-- The following should not leak space
-- illustrates that we lazily process chunks
example2 :: IO ()
example2 = do
  let reduce :: String -> String -> IO String
      reduce pfx sfx = return $ pfx ++ sfx

      genChunk :: Int -> IO String
      genChunk i = do
        let ms = 1000
        threadDelay (500*ms)
        return $ show i ++ "\n"

  r <- bpfold dftBPOpts reduce "" (map genChunk [0..])
  putStrLn r
-}



