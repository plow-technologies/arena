{-# LANGUAGE ScopedTypeVariables, TupleSections, OverloadedStrings #-}
module Main where

import System.Mem
import Control.Concurrent

import Control.Monad.Trans
import Data.List
import Data.Semigroup
import System.Directory
import Database.Arena
import qualified Control.Exception as E

assert :: Bool -> String -> IO ()
assert True _ = return ()
assert False err = E.throwIO . E.AssertionFailed $ err

assert' :: MonadIO m => Bool -> String -> m ()
assert' b s = liftIO $ assert b s

prepDirectory :: IO ()
prepDirectory = do
  createDirectoryIfMissing True "test_data"
  removeDirectoryRecursive "test_data"
  createDirectoryIfMissing True "test_data/journal"
  createDirectoryIfMissing True "test_data/data"

main :: IO ()
main = do
  prepDirectory
  let go = runArena ((1,) . Sum) (getSum . snd) ((5 <) . getSum . fst) "test_data"
  as <- go monadTest1
  go $ monadTest2 as

monadTest1 :: Arena (Sum Int, Sum Int) Int Int [(Int, [Int])]
monadTest1 = do
  addData 65
  sm <- (sum . fmap fst) <$> accessData
  assert' (sm == 65) $ "Count was wrong: " ++ show sm
  mapM_ addData [1..14]
  sm <- (sum . fmap fst) <$> accessData
  as <- accessData >>= mapM (\(c, act) -> (c,) <$> liftIO act)
  liftIO $! do putStrLn . show . sort $ as
               assert' (sm == (65 + sum [1..14])) "Larger sum incorrect!"
               performMajorGC
               threadDelay 100000
               performMajorGC
  return as

monadTest2 :: [(Int, [Int])] -> Arena (Sum Int, Sum Int) Int Int ()
monadTest2 as = do
  as' <- accessData >>= mapM (\(c, act) -> (c,) <$> liftIO act)
  liftIO $! do putStrLn . show . sort $ as'
               assert ((sort as) == (sort as')) "doesn't match old data"
