{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}

module Database.Arena (
    ArenaLocation(..)
  , startArena
  ) where

import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.Trans
import           Data.Bytes.Get
import           Data.Bytes.Put
import           Data.Bytes.Serial
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BSL
import           Data.Digest.CRC32
import           Data.Either
import           Data.IORef
import           Data.List
import           Data.Maybe
import           Data.Semigroup
import qualified Data.Set                as Set
import           Data.String
import           Data.Word
import           Safe
import           System.Directory
import           System.FilePath
import           System.IO

newtype ArenaLocation = ArenaLocation { getArenaLocation :: FilePath }
  deriving (Eq, Ord, Read, Show, IsString)

journalDir, dataDir, tempJournal :: ArenaLocation -> FilePath
journalDir (ArenaLocation fp) = fp            </> "journal"
dataDir    (ArenaLocation fp) = fp            </> "data"
tempJournal al                = journalDir al </> "temp"

journalFile, dataFile, dataSummaryFile :: ArenaLocation -> ArenaID -> FilePath
journalFile     al i = journalDir al </> show i
dataFile        al i = dataDir    al </> addExtension (show i) "data"
dataSummaryFile al i = dataDir    al </> addExtension (show i) "header"

type ArenaID     = Word32
type JournalHash = Word32

data JournalFrame = JF { jLength :: Word32, jHash :: JournalHash, jData :: BS.ByteString }

mkJournal :: Serial a => a -> JournalFrame
mkJournal a = JF (fromIntegral . BS.length $ d) (crc32 d) d
  where d = runPutS . serialize $ a

instance Serial JournalFrame where
    serialize (JF l h d) = serialize l *> serialize h *> putByteString d
    deserialize = do
      l <- deserialize
      h <- deserialize
      d <- getByteString . fromIntegral $ l
      if h == crc32 d
      then return $ JF l h d
      else fail "Journal Frame failed CRC check---Also, uh, this method shouldn't be in Monad"

data OpenJournal a b = OJ { ojArenaID :: ArenaID, ojHandle :: Handle, ojSummary :: Option b, ojVals :: [a] }

readDataFile :: (MonadIO m, Serial a) => ArenaLocation -> ArenaID -> m [a]
readDataFile l ai = liftIO $ do
  d <- BSL.readFile (dataFile l ai)
  return $ runGetL (many deserialize) d

readJournalFile :: Serial a => ArenaLocation -> ArenaID -> IO [a]
readJournalFile l ai = do
  d <- BSL.readFile (journalFile l ai)
  return . map (head . rights . pure . runGetS deserialize . jData) . runGetL (many deserialize) $ d

startArena :: forall m1 m2 m3 a b c
           . (MonadIO m1, MonadIO m2, MonadIO m3, Serial a, Serial c, Semigroup b)
           => (a -> b) -> (b -> c) -> (b -> Bool) -> ArenaLocation
           -> m1 (m2 [(c, m2 [a])], a -> m3 ())
startArena summarize finalize arenaFull diskLocation = liftIO $ do
    currJournalM <- internArenas >>= cleanJournal >>= newMVar
    dr           <- readAllData >>= newIORef
    return (accessData dr currJournalM, addData dr currJournalM)
  where
    readAllData :: IO [(c, m2 [a])]
    readAllData = do
      ds <- Set.fromList . mapMaybe (readMay . dropExtension) <$>
                       (getDirectoryContents . dataDir $ diskLocation)
      forM (Set.toList ds) $ \d -> do
        c <- runGetL deserialize <$> (BSL.readFile . dataSummaryFile diskLocation $ d)
        return (c, readDataFl d)

    accessData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> m2 [(c, m2 [a])]
    accessData dr cjM = liftIO $ do
      withMVar cjM $ \(OJ _ _ s as) -> do
        d <- readIORef dr
        return $ (finalize . fromJust . getOption $ s, return . reverse $ as):d

    addData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> a -> m3 ()
    addData dsr ojM a = liftIO . modifyMVar_ ojM $ \(OJ ai h s as) -> do
     BS.hPutStr h . runPutS . serialize . mkJournal $ a
     let s' = s <> (pure . summarize $ a)
     syncHandle h -- This COULD be moved down for a very minor perf boost.
     case arenaFull . fromJust . getOption $ s' of
       False -> return $ OJ ai h s' (a:as)
       True -> do
         hClose h
         internArenaFl ai
         atomicModifyIORef' dsr $ \ods ->
           ((finalize . fromJust . getOption $ s', readDataFl ai):ods, ())
         let ai' = ai + 1
         h' <- openFile (journalFile diskLocation $ ai') WriteMode
         return $ OJ ai' h' mempty mempty

    readDataFl :: MonadIO m => ArenaID -> m [a]
    readDataFl = readDataFile diskLocation

    internArenaFl :: ArenaID -> IO ()
    internArenaFl ai = do
      -- This has a problem if the journal was empty!
      -- We never intern a journal until we've moved on to another though,
      -- and to move on to another, we have to have had a summary to check for nursery fullness.
      as <- readJournalFile diskLocation ai
      withFileSync (dataFile diskLocation ai) $ \h ->
        mapM_ (BS.hPutStr h . runPutS . serialize) as
      withFileSync (dataSummaryFile diskLocation ai) $ \h ->
        BS.hPutStr h . runPutS . serialize . finalize . alec $ as
      removeFile . journalFile diskLocation $ ai

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
      je <- doesFileExist . journalFile diskLocation $ ai
      case je of
        False -> do
         jh <- openFile (journalFile diskLocation ai) WriteMode
         return $ OJ ai jh (Option Nothing) []
        True -> do
          as <- readJournalFile diskLocation $ ai
          withFileSync (tempJournal diskLocation) $ \h ->
            mapM_ (BS.hPutStr h . runPutS . serialize . mkJournal) as
          renameFile (tempJournal diskLocation) (journalFile diskLocation ai)
          jh <- openFile (journalFile diskLocation ai) WriteMode
          return $ OJ ai jh (pure . alec $ as) (reverse as)

    alec :: [a] -> b
    alec = foldl1 (<>) . map summarize

syncHandle :: Handle -> IO ()
syncHandle h = do
  hFlush h
  putStrLn "NYI: syncHandle"

withFileSync :: FilePath -> (Handle -> IO r) -> IO r
withFileSync fp f = withFile fp WriteMode go
  where go h = f h <* syncHandle h
