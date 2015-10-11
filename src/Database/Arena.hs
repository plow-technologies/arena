{-# LANGUAGE ScopedTypeVariables #-}
module Database.Arena (
    ArenaLocation(..)
  , startArena
  ) where

import Control.Applicative
import Data.Word
import Data.Either
import Data.Maybe
import Data.List
import Data.Bytes.Serial
import Data.Bytes.Get
import Data.Bytes.Put
import Data.Semigroup
import Data.IORef
import Control.Monad.Trans
import Control.Monad
import Control.Concurrent.MVar
import Data.Digest.CRC32
import System.IO
import System.FilePath
import System.Directory
import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL
import qualified Data.Set as Set
import Safe

newtype ArenaLocation = ArenaLocation FilePath

journalDir :: ArenaLocation -> FilePath
journalDir (ArenaLocation fp) = fp </> "journal"
tempJournal :: ArenaLocation -> FilePath
tempJournal al = journalDir al </> "temp"
dataDir :: ArenaLocation -> FilePath
dataDir (ArenaLocation fp) = fp </> "data"
arenaId2jfl :: ArenaLocation -> ArenaID -> FilePath
arenaId2jfl al i = (journalDir al) </> (show i)
dataFile :: ArenaLocation -> ArenaID -> FilePath
dataFile al i = (dataDir al) </> (addExtension (show i) "data")
dataSummaryFile :: ArenaLocation -> ArenaID -> FilePath
dataSummaryFile al i = (dataDir al) </> (addExtension (show i) "header")

type ArenaID = Word32

data JournalFrame = JF { jLength :: Word32, jHash :: Word32, jData :: BS.ByteString }

mkJournal :: BS.ByteString -> JournalFrame
mkJournal d = JF (fromIntegral . BS.length $ d) (crc32 d) d

instance Serial JournalFrame where
    serialize (JF l h d) = putWord32be l *> putWord32be h *> putByteString d
    deserialize = do
      l <- getWord32be
      h <- getWord32be
      d <- getByteString . fromIntegral $ l
      if h == crc32 d
      then return $ JF l h d
      else fail "Uh, this method shouldn't be in Monad"

data OpenJournal a b = OJ { ojArenaID :: ArenaID, ojHandle :: Handle, ojSummary :: Option b, ojVals :: [a] }

startArena :: forall m1 m2 m3 a b c
           . (MonadIO m1, MonadIO m2, MonadIO m3, Serial a, Serial c, Semigroup b)
           => (a -> b) -> (b -> c) -> (b -> Bool) -> ArenaLocation
           -> m1 (m2 [(c, m2 [a])], a -> m3 ())
startArena summerize finalize arenaFull diskLocation = liftIO $ do
    currJournalM <- internArenas >>= cleanJournal >>= newMVar
    dr <- readAllData >>= newIORef
    return (accessData dr currJournalM, addData dr currJournalM)
  where
    readAllData :: IO [(c, m2 [a])]
    readAllData = do
      ds <- (Set.fromList . mapMaybe (readMay . dropExtension)) <$>
                       (getDirectoryContents . dataDir $ diskLocation)
      forM (Set.toList ds) $ \d -> do
        c <- (runGetL deserialize) <$> (BSL.readFile . dataSummaryFile diskLocation $ d)
        return (c, readDataFl d)
    accessData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> m2 [(c, m2 [a])]
    accessData dr cjM = liftIO $ do
      withMVar cjM $ \(OJ _ _ s as) -> do
        d <- readIORef dr
        return $ (finalize . fromJust . getOption $ s, return . reverse $ as):d
    addData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> a -> m3 ()
    addData dsr ojM a = liftIO . modifyMVar_ ojM $ \(OJ ai h s as) -> do
     BS.hPutStr h . runPutS . serialize . mkJournal . runPutS . serialize $ a
     let s' = s <> (pure . summerize $ a)
     syncHandle h -- This COULD be moved down for a very minor perf boost.
     case arenaFull . fromJust . getOption $ s' of
       False -> return $ OJ ai h s' (a:as)
       True -> do
         hClose h
         internArenaFl ai
         atomicModifyIORef' dsr $ \ods ->
           ((finalize . fromJust . getOption $ s', readDataFl ai):ods, ())
         let ai' = ai + 1
         h' <- openFile (arenaId2jfl diskLocation $ ai') WriteMode
         return $ OJ ai' h' mempty mempty
    syncHandle :: Handle -> IO ()
    syncHandle h = do
      hFlush h
      putStrLn "Wish we could!"
    readDataFl :: MonadIO m => ArenaID -> m [a]
    readDataFl ai = liftIO $ do
      d <- BSL.readFile (dataFile diskLocation ai)
      return $ runGetL (many deserialize) d
    readJournalFl :: FilePath -> IO [a]
    readJournalFl jf = do
      d <- BSL.readFile jf
      return . map (head . rights . pure . runGetS deserialize . jData) . runGetL (many deserialize) $ d
    internArenaFl :: ArenaID -> IO ()
    internArenaFl ai = do
      -- This has a problem if the journal was empty!
      -- We never intern a journal untill we've moved on to another though,
      -- and to move on to another, we have to have had a summary to check for nursery fullness.
      as <- readJournalFl . arenaId2jfl diskLocation $ ai
      withFile (dataFile diskLocation ai) WriteMode $ \h -> do
        mapM_ (BS.hPutStr h . runPutS . serialize) as
        syncHandle h
      withFile (dataSummaryFile diskLocation ai) WriteMode $ \h -> do
        BS.hPutStr h . runPutS . serialize . finalize . alec $ as
        syncHandle h
      removeFile . arenaId2jfl diskLocation $ ai
    internArenas :: IO ArenaID
    internArenas = do
      (js::[ArenaID]) <- mapMaybe readMay <$> (getDirectoryContents . journalDir $ diskLocation)
      case js of
        [] -> return 0
        _ -> do
          let latest = maximum js
          let old = delete latest js
          mapM_ internArenaFl old
          return latest
    cleanJournal :: ArenaID -> IO (OpenJournal a b)
    cleanJournal ai = do
      te <- doesFileExist . tempJournal $ diskLocation
      when te . removeFile . tempJournal $ diskLocation
      je <- doesFileExist . arenaId2jfl diskLocation $ ai
      case je of
        False -> do
         jh <- openFile (arenaId2jfl diskLocation ai) WriteMode
         return $ OJ ai jh (Option Nothing) []
        True -> do
          as <- readJournalFl . arenaId2jfl diskLocation $ ai
          withFile (tempJournal diskLocation) WriteMode $ \h -> do
            mapM_ (BS.hPutStr h . runPutS . serialize . mkJournal . runPutS . serialize) as
            syncHandle h
          renameFile (tempJournal diskLocation) (arenaId2jfl diskLocation ai)
          jh <- openFile (arenaId2jfl diskLocation ai) WriteMode
          return $ OJ ai jh (pure . alec $ as) (reverse as)
    alec :: [a] -> b
    alec = foldl1 (<>) . map summerize
    dfToCIOa :: ArenaID -> IO (c, IO [a])
    dfToCIOa a = do
      return (undefined, readDataFl a)
