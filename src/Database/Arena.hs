{-# LANGUAGE ScopedTypeVariables #-}
module Database.Arena (
    ArenaLocation(..)
  , startArena
  ) where

import Data.Word
import Data.Bytes.Serial
import Data.Bytes.Get
import Data.Bytes.Put
import Data.Semigroup
import Data.IORef
import Control.Monad.Trans
import Data.Digest.CRC32
import System.FilePath
import System.Directory
import qualified Data.ByteString as BS

newtype ArenaLocation = ArenaLocation FilePath

journalDir :: ArenaLocation -> FilePath
journalDir (ArenaLocation fp) = fp </> "journal"
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
mkJournal d = JF (BS.length d) (crc32 d) d

instance Serial JournalFrame where
    serialize (JF l h d) = putWord32be l *> putWord32be h *> putByteString d
    deserialize = do
      l <- getWord32be
      h <- getWord32be
      d <- getByteString . fromIntegral $ l
      if h == crc32 d
      then return $ JF l h d
      else fail "Uh, this method shouldn't be in Monad"

data OpenJournal b = OJ { ojArenaID :: ArenaID, ojHandle :: Handle, ojSummary :: Option b }

startArena :: (MonadIO m1, MonadIO m2, MonadIO m3, Serial a, Serial c, Semigroup b)
           => (a -> b) -> (b -> c) -> (b -> Bool) -> ArenaLocation
           -> m1 (IORef [(c, m2 [a])], a -> m3 ())
startArena summerize finalize arenaFull diskLocation = do
    currJournalM <- internArenas >>= cleanJournal >>= newMVar
    dr <- readAllData >>= newIORef
    return (dr, addData dr currJournalM)
  where
    addData :: IORef [(c, IO [a])] -> MVar OpenJournal -> a -> IO ()
    addData dsr ojM a = (flip modifyMVar_ ojM) $ \(OJ ai h s) -> do
     BS.hPutStr h . runPutS $ a
     let s' = s <> (pure . summerize $ a)
     syncHandle h -- This COULD be moved down for a very minor perf boost.
     case arenaFull . fromJust . getOption $ s' of
       False -> return $ OJ ai h s'
       True -> do
         hClose h
         internArenaFl ai
         let ai' = ai + 1
         h' <- openFile (arenaId2jfl diskLocation $ ai') WriteMode
         return $ OJ ai' h' mempty
    syncHandle :: Handle -> IO ()
    syncHandle _ = do
      putStrLn "Wish we could!"
    readJournalFl :: FilePath -> IO [a]
    readJournalFl jf = do
      d <- BS.readFile jf
      error "Alec, this is your job."
    internArenaFl :: ArenaID -> IO ()
    internArenaFl ai = do
      -- This has a problem if the journal was empty!
      -- We never intern a journal untill we've moved on to another though,
      -- and to move on to another, we have to have had a summary to check for nursery fullness.
      je <- readJournalFl . arenaId2jfl diskLocation $ ai
      withFile (dataFile diskLocation ai) WriteMode $ \h -> do
        mapM_ (BS.hPutStr h . runPutS) as
        syncHandle h
      withFile (dataSummaryFile diskLocation ai) WriteMode $ \h -> do
        BS.hPutStr h . runPutS . finalize . alec $ as
        syncHandle h
      removeFile . arenaId2jfl diskLocation $ ai
    internArenas :: IO ArenaID
    internArenas = do
      (js::[ArenaID]) <- mapMaybe readMay <$> (getDirectoryContents . journalDir $ diskLocation)
      let latest = maximum js
      let old = delete latest js
      mapM_ internArenaFl old
      return latest
    cleanJournal :: ArenaID -> OpenJournal b
    cleanJournal jf = do
      removeFile . tempJournal $ diskLocation
      as <- readJournalFl . arenaId2jfl diskLocation $ ai
      withFile (tempJournal diskLocation) WriteMode $ \h -> do
        mapM_ (BS.hPutStr h . runPutS . mkJournal) as
        syncHandle h
      renameFile (tempJournal diskLocation) (arenaId2jfl diskLocation ai)
      removeFile ∘ tempJournal $ diskLocation
      jh <- openFile (arenaId2jfl diskLocation ai) WriteMode
      return (jh, alec as)
    alec :: [a] -> b
    alec = foldl1 (<>) . map summarize
    dfToCIOa :: ArenaID -> IO (c, IO [a])
    dfToCIOa a = do
      

