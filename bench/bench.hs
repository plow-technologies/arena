{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TupleSections       #-}

module Main where

import           Criterion.Main
import           Data.Semigroup
import           Database.Arena
import           System.Directory

prepDirectory :: IO ()
prepDirectory = do
  createDirectoryIfMissing True "test_data"
  removeDirectoryRecursive "test_data"
  createDirectoryIfMissing True "test_data/journal"
  createDirectoryIfMissing True "test_data/data"

main = defaultMain [
        bgroup "inserts" [ bench "20"    . nfIO $ inserts 20
                         , bench "10000" . nfIO $ inserts 10000]
       ]


startTestArena :: Int -> IO (ArenaDB (Sum Int, Sum Int) Int Int)
startTestArena n = startArena ((1,) . Sum) (getSum . snd) ((succ n <=) . getSum . fst) "test_data"

inserts :: Int -> IO ()
inserts n = do
  prepDirectory
  ad <- startTestArena n
  runArena ad $ mapM_ addData [1..n]
