{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE OverloadedStrings #-}

module Main where

import           Control.Monad.Trans
import           GHC.Generics
import           Data.Semigroup
import           Data.Bytes.Serial
import           Database.Arena
import           Data.List
import           Data.Map (Map)
import qualified Data.Map as Map
import           Data.Ord

-- | Suppose we have a bunch of (simplified) web server log entries, each containing the source IP,
--   time, and number of bytes served for an HTTP request:
data LogEntry =
  LogEntry {
    leIP          :: String
  , leTimeStamp   :: Time
  , leBytesServed :: Int
  } deriving (Generic, Show)

type Time = Int

-- | We might query this data to find the amount of data we've served over a given time period:
bytesServedInPeriod :: Time -> Time -> [LogEntry] -> Int
bytesServedInPeriod start end les = sum [ bytes | LogEntry _ ts bytes <- les
                                                , ts >= start
                                                , ts <= end ]

-- | To execute this query more more efficiently, we'd like to batch up our entries and store
--   them alongside an index of some salient information about them.  Specifically, we'd like to
--   know how much data we've served, over what time period we served it, and the source IP that
--   made the most requests (the "heaviest hitter").
data LogIndex =
  LogIndex {
    liHeavyHitter :: String
  , liBytesServed :: Int
  , liStartTime   :: Time
  , liEndTime     :: Time
  } deriving (Generic, Show)

-- | Conceptually, our data can now look like @[(LogIndex, [LogEntry])]@
fastBytesServedInPeriod :: Time -> Time -> [(LogIndex, [LogEntry])] -> Int
fastBytesServedInPeriod start end blocks = sum fullyContained + sum partiallyContained
    where fullyContained = [ bs | (LogIndex _ bs liStart liEnd, _) <- blocks
                                , liStart >= start && liEnd <= end ]
          partiallyContained = [ bytesServedInPeriod start end es
                                 | (LogIndex _ bs liStart liEnd, es) <- blocks
                                 , (liStart <= start && start <= liEnd) || (liStart <= end && end <= liEnd)]

-- | To build these indexed blocks <https://en.wikipedia.org/wiki/Online_algorithm online>, we need
--   to keep track of some more information.  In particular, we need to keep count of the log
--   entries for each IP, the total bytes served, the start and end times, and the total count of
--   the log entries as we process them.
data LogSummary =
  LogSummary {
    lsHeavyHitters :: Map String Int
  , lsBytesServed  :: Sum Int
  , lsStartTime    :: Min Time
  , lsEndTime      :: Max Time
  , lsCount        :: Sum Int
  }

-- | By making 'LogSummary' a 'Semigroup', @arena@ can maintain the running summary we require
--   automatically.
instance Semigroup LogSummary where
    LogSummary hh bs st et c <> LogSummary hh' bs' st' et' c' =
       LogSummary (Map.unionWith (+) hh hh') (bs <> bs') (st <> st') (et <> et') (c <> c')

-- | All we need to be able to do is summarize individual 'LogEntry's:
summarizeEntry :: LogEntry -> LogSummary
summarizeEntry (LogEntry ip t bytes) =
  LogSummary (Map.singleton ip 1) (Sum bytes) (Min t) (Max t) (Sum 1)

-- | Of course we need to be able to convert our summaries into 'LogIndex'es:
summaryToIndex :: LogSummary -> LogIndex
summaryToIndex (LogSummary hh (Sum bs) (Min st) (Max et) _) =
    LogIndex heaviestHitter bs st et
  where
    heaviestHitter = fst . maximumBy (comparing snd) . Map.toList $ hh

-- | Now we just need to know when we're done building a block.  Let's assume that 5000 entries per
--   block is a reasonable policy:
blockDone :: LogSummary -> Bool
blockDone ls = lsCount ls == 5000

-- | In order for @arena@ to actually write our data to disk, we need to be able to serialize it.
--   We can just let GHC generics get us the instances we need:
instance Serial LogEntry
instance Serial LogIndex

-- | We can have @arena@ tie all this together for us:
type LogArena a = Arena LogSummary LogIndex LogEntry a

makeLogDB :: ArenaLocation -> IO (ArenaDB LogSummary LogIndex LogEntry)
makeLogDB = startArena summarizeEntry summaryToIndex blockDone

-- | Now we can use @arena@'s 'accessData' function to pull our indexed blocks off disk.  Notice
--   that 'accessData' gives back the slightly different type then above:
--   @[(LogIndex, IO [LogEntry])]@.  This is because @arena@ won't read the actual 'LogEntry's
--   (of which there might be many) unless you specifically force the corresponding IO action.
queryBytesServed :: Time -> Time -> LogArena Int
queryBytesServed start end = do
  blocks <- accessData
  let fullyContained = [ bs | (LogIndex _ bs liStart liEnd, _) <- blocks
                            , liStart >= start && liEnd <= end ]
      partiallyContained = [ bytesServedInPeriod start end <$> es
                              | (LogIndex _ bs liStart liEnd, es) <- blocks
                              , (liStart <= start && start <= liEnd) || (liStart <= end && end <= liEnd)]
  forced <- liftIO . sequence $ partiallyContained
  return $ sum fullyContained + sum forced

-- | Now to use the database, we just add and query data:
addAndQueryData :: LogArena ()
addAndQueryData = do
  mapM_ addData [LogEntry (show $ n `mod` 23) n ((n * n - 1) `mod` 29)
                     | n <- [1..7000]] -- sufficiently random looking data
  46666 <- queryBytesServed 2345 5678
  return ()

-- | Make the database in @/tmp@ and go!
main :: IO ()
main = do
  db <- makeLogDB "/tmp/log_arena"
  runArena db addAndQueryData
