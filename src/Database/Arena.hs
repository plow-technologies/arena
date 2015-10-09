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
import qualified Data.ByteString as BS

newtype ArenaLocation = ArenaLocation FilePath

journalDir :: ArenaLocation -> FilePath
journalDir (ArenaLocation fp) = fp </> "journal"
dataDir :: ArenaLocation -> FilePath
dataDir (ArenaLocation fp) = fp </> "data"

type ArenaID = Word32

data JournalFrame = JF { jLength :: Word32, jHash :: Word32, jData :: BS.ByteString }

mkJournal :: BS.ByteString -> JournalFrame a
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

startArena :: (MonadIO m1, MonadIO m2, MonadIO m3, Serial a, Serial c, Semigroup b)
           => (a -> b) -> (b -> c) -> (b -> Bool) -> ArenaLocation
           -> m1 (IORef [(c, m2 [a])], a -> m3 ())
startArena summerize finalize arenaFull diskLocation = do
    lastJournalID <- internArenas
    dr <- readAllData >>= newIORef
    return (dr, undefined)
  where
    addData :: IORef [(c, IO [a])] -> MVar FileHandle -> a -> IO ()
    readJournalFl :: FilePath -> IO [a]
    readJournalFl jf = do
      d <- BS.readFile jf
      
    internArenaFl :: FilePath -> IO ()
    internArenaFl jf = do
      je <- readJournalFl jf
      undefined
    cleanJournal jf = do
      je <- readJournalFl jf
      undefined
    dfToCIOa :: ArenaID -> IO (c, IO [a])
    dfToCIOa = undefined
