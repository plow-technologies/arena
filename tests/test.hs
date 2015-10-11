{-# LANGUAGE TupleSections #-}
module Main where

import Data.List.NonEmpty
import Data.Semigroup
import System.Directory
import Database.Arena
import qualified Control.Exception as E

assert :: Bool -> String -> IO ()
assert True _ = return ()
assert False err = E.throwIO . E.AssertionFailed $ err

main :: IO ()
main = do
  createDirectoryIfMissing True "test_data"
  removeDirectoryRecursive "test_data"
  createDirectoryIfMissing True "test_data/journal"
  createDirectoryIfMissing True "test_data/data"
  (getState, addVal) <- startArena ((1::Sum Int,) . Sum) (getSum . snd) ((5 <) . getSum . fst) (ArenaLocation "test_data")
  addVal (65::Int)
  sm <- (sum . fmap fst) <$> getState
  assert (sm == 65) "Count was wrong"
  mapM_ addVal [(1::Int)..14]
  sm <- (sum . fmap fst) <$> getState
  as <- getState >>= mapM (\(c, act) -> (c,) <$> act)
  putStrLn . show $ as
  putStrLn . show $ sm
  assert (sm == (65 + sum [1..14])) "Larger sum incorrect!"
  return ()
