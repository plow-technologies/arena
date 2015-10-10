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

data OpenJournal b = OJ { ojHandle :: Handle, ojSummary :: b }

startArena :: (MonadIO m1, MonadIO m2, MonadIO m3, Serial a, Serial c, Semigroup b)
           => (a -> b) -> (b -> c) -> (b -> Bool) -> ArenaLocation
           -> m1 (IORef [(c, m2 [a])], a -> m3 ())
startArena summerize finalize arenaFull diskLocation = do
    currJournal <- internArenas >>= cleanJournal
    dr <- readAllData >>= newIORef
    return (dr, undefined)
  where
    addData :: IORef [(c, IO [a])] -> MVar FileHandle -> a -> IO ()
    syncHandle :: Handle -> IO ()
    syncHandle _ = do
      putStrLn "Wish we could!"
    readJournalFl :: FilePath -> IO [a]
    readJournalFl jf = do
      d <- BS.readFile jf
      undefined
    internArenaFl :: FilePath -> IO () --This should take an ID?
    internArenaFl jf = do
      je <- readJournalFl jf
      withFile (dataFile diskLocation ) WriteMode $ \h -> do
    internArenas :: IO ArenaID
    internArenas = do
      (js::[ArenaID]) <- mapMaybe readMay <$> (getDirectoryContents . journalDir $ diskLocation)
      let latest = maximum js
      let old = delete latest js
      mapM_ (internArenaFl . arenaId2jfl diskLocation) old
      return latest
    cleanJournal :: ArenaID -> OpenJournal b
    cleanJournal jf = do
      removeFile . tempJournal $ diskLocation
      as <- readJournalFl . arenaId2jfl diskLocation $ jf
      withFile (tempJournal diskLocation) WriteMode $ \h -> do
        mapM_ (BS.hPutStr h . runPutS . mkJournal) as
        syncHandle h
    dfToCIOa :: ArenaID -> IO (c, IO [a])
    dfToCIOa a = do
      

