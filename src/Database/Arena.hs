{-# LANGUAGE CPP #-}
{-# LANGUAGE DeriveGeneric #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Database.Arena
    (
      ArenaLocation(..)

    , ArenaT
    , Arena
    , ArenaDB
    , runArenaT
    , runArena
    , startArena, initArena
    , addData
    , accessData
    ) where

import           Control.Applicative
import           Control.Concurrent.MVar
import           Control.Monad
import           Control.Monad.Reader
import           Control.Monad.Trans
import           Data.Bytes.Get
import           Data.Bytes.Put
import           Data.Bytes.Serial
import qualified Data.ByteString         as BS
import qualified Data.ByteString.Lazy    as BSL
import           Data.Digest.CRC32
import           Data.Either
import           Data.Foldable
import           Data.IORef
import           Data.List
import           Data.Maybe
import           Data.Semigroup
import qualified Data.Set                as Set
import           Data.String
import           Data.Typeable           (cast)
import qualified Data.Vector.Persistent  as PV
import           Data.Word
import           GHC.IO.Device
import           GHC.IO.Exception
import qualified GHC.IO.FD               as FD
import           GHC.IO.Handle
import qualified GHC.IO.Handle.FD        as FD
import           GHC.IO.Handle.Internals
import           GHC.IO.Handle.Types
import           Safe
import           System.Directory
import           System.FilePath
import           System.IO
import           System.IO.Error
import           System.Posix.IO         (handleToFd)
import           System.Posix.Types      (Fd (Fd))
import           System.Posix.Unistd     (fileSynchroniseDataOnly)
import           GHC.Generics            (Generic)
import           Data.Coerce             (coerce)

-- | The base directory for the arena files to be stored under.
newtype ArenaLocation = ArenaLocation { getArenaLocation :: FilePath }
  deriving (Eq, Ord, Read, Show, IsString)

type ArenaID     = Word32
type JournalHash = Word32

data JournalFrame =
    JF {
      jLength :: Word32
      -- ^ The amount of data stored in the frame.
    , jHash :: JournalHash
      -- ^ A hash of the data stored in the frame so we can detect write corruption.
      --   This is sufficient for the entire frame as all other elements are fixed
      --   size and directly impact the data's hash.
    , jData :: BS.ByteString
      -- ^ The user's serialized data.
    }

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

data OpenJournal a b =
  OJ {
    ojArenaID :: ArenaID
    -- ^ The current datablock generation, starting from zero and strictly monotonic.
  , ojHandle :: Handle
    -- ^ The journal file, opened and positioned for writing
  , ojSummary :: Option b
    -- ^ The current summary, if any values have been stored in the journal
  , ojVals :: PV.Vector a
    -- ^ The values currently held in the journal on disk, in a 'PV.Vector' (instead of a list) to
    --   improve GC performance, and provide fast snoc to avoid the use of reverse.
  }

readDataFile :: (MonadIO m, Serial a) => ArenaLocation -> ArenaID -> m [a]
readDataFile l ai = liftIO $ do
  d <- BSL.readFile (dataFile l ai)
  return $ runGetL (many deserialize) d

syncHandle :: Handle -> IO ()
syncHandle h = do
    hFlush h
    unsafeSyncHandle h
  where
    unsafeSyncHandle h@(DuplexHandle {}) =
      ioError (ioeSetErrorString (mkIOError IllegalOperation
                                  "unsafeSyncHandle" (Just h) Nothing)
               "unsafeSyncHandle only works on file descriptors")
    unsafeSyncHandle h@(FileHandle _ m) = do
      withMVar m $ \h_@Handle__{haType=_,..} -> do
        case cast haDevice of
          Nothing -> ioError (ioeSetErrorString (mkIOError IllegalOperation
                                                 "unsafeSyncHandle" (Just h) Nothing)
                              ("handle is not a file descriptor"))
          Just fd -> do
            flushWriteBuffer h_
            fileSynchroniseDataOnly . Fd . FD.fdFD $ fd

withFileSync :: FilePath -> (Handle -> IO r) -> IO r
withFileSync fp f = liftIO $ withFile fp WriteMode go
  where go h = f h <* syncHandle h

data ArenaDB summary finalized d = ArenaDB {
      adSummarize      :: d -> summary
      -- ^ Convert the user's data to an instance of the summary semigroup
    , adFinalize       :: summary -> finalized
      -- ^ Removal of data only used for the merge operation or determining fullness,
      --   changing the summary to a version used purely for the datablock indexing.
    , adArenaFull      :: summary -> Bool
      -- ^ Determine if we've reached a datablock boundary.
    , adArenaLocation  :: ArenaLocation
      -- ^ The location on the filesystem in which we store the data and journal.
    , adDataRef        :: IORef [(finalized, IO [d])]
      -- ^ The list of datablocks, seen as their summary and accessors, for read operations.
    , adCurrentJournal :: MVar (OpenJournal d summary)
      -- ^ The journal for writing new data, in an 'MVar' so we can lock it for consistency when
      --   doing writes or data access.
    }

-- | * @s@: The summary semigroup for the journal data.
--   * @f@: The finalized summary associated with a datablock.
--   * @d@: The user's datatype stored in the arena.
newtype ArenaT s f d m a = ArenaT { unArenaT :: ReaderT (ArenaDB s f d) m a }
    deriving (Functor, Applicative, Monad, MonadTrans, MonadReader (ArenaDB s f d), MonadIO, MonadFail)

runArenaT :: ArenaDB s f d -> ArenaT s f d m a -> m a
runArenaT ad = flip runReaderT ad . unArenaT

type Arena s f d a = ArenaT s f d IO a

runArena :: ArenaDB s f d -> Arena s f d a -> IO a
runArena = runArenaT

summarize :: Monad m => d -> ArenaT s f d m s
summarize d = asks adSummarize <*> pure d

finalize :: Monad m => s -> ArenaT s f d m f
finalize s = asks adFinalize <*> pure s

journalDir, dataDir, tempJournal :: ArenaLocation -> FilePath
journalDir (ArenaLocation fp) = fp            </> "journal"
dataDir    (ArenaLocation fp) = fp            </> "data"
tempJournal al                = journalDir al </> "temp"

journalFile, dataFile, dataSummaryFile :: ArenaLocation -> ArenaID -> FilePath
journalFile     al i = journalDir al </> show i
dataFile        al i = dataDir    al </> addExtension (show i) "data"
dataSummaryFile al i = dataDir    al </> addExtension (show i) "header"

journalDir', dataDir', tempJournal' :: Monad m => ArenaT s f d m FilePath
journalDir'  = asks (journalDir  . adArenaLocation)
dataDir'     = asks (dataDir     . adArenaLocation)
tempJournal' = asks (tempJournal . adArenaLocation)

journalFile', dataFile', dataSummaryFile' :: Monad m => ArenaID -> ArenaT s f d m FilePath
journalFile'     ai = asks (journalFile     . adArenaLocation) <*> pure ai
dataFile'        ai = asks (dataFile        . adArenaLocation) <*> pure ai
dataSummaryFile' ai = asks (dataSummaryFile . adArenaLocation) <*> pure ai

data TheFiles = TheFiles {
      theJournalDir
    , theDataDir
    , theTempJournal
    , theJournalFile
    , theDataFile
    , theDataSummaryFile :: FilePath
    } deriving (Eq, Ord, Read, Show)

theFiles :: Monad m => Maybe ArenaID -> ArenaT s f d m TheFiles
theFiles Nothing   = theFiles (Just $ -1)
theFiles (Just ai) =
  TheFiles <$> journalDir'     <*> dataDir'     <*> tempJournal'
           <*> journalFile' ai <*> dataFile' ai <*> dataSummaryFile' ai

-- | Setup the directory structure for Arena.
--   This is performed implicitly by 'startArena'.
initArena :: ArenaLocation -> IO ()
initArena al = do
  createDirectoryIfMissing True . journalDir $ al
  createDirectoryIfMissing True . dataDir $ al

-- | Launch an 'ArenaDB', using the given summarizing, finalizing, and block policy functions,
--   at the given 'ArenaLocation'.
--
--   __NB:__ Two 'ArenaDB's must not be run concurrently with a shared 'ArenaLocation'.
--   Data loss from the journal is likely to result.
startArena
  :: (Serial d, Serial f, Semigroup s)
    => (d -> s) -> (s -> f) -> (s -> Bool) -> ArenaLocation -> IO (ArenaDB s f d)
startArena adSummarize adFinalize adArenaFull adArenaLocation = do
    initArena adArenaLocation
    runArena fakeConf $ do
      adCurrentJournal <- internArenas >>= cleanJournal >>= liftIO . newMVar
      adDataRef        <- readAllData  >>= liftIO . newIORef
      return ArenaDB {..}
    where fakeConf = ArenaDB { adCurrentJournal = error "uninitialized current journal"
                               , adDataRef        = error "uninitialized data ref"
                               , .. }

readJournalFile :: (Serial d, MonadIO m) => ArenaID -> ArenaT s f d m (PV.Vector d)
readJournalFile ai = do
  d <- journalFile' ai >>= liftIO . BSL.readFile
  return . PV.fromList . map (head . rights . pure . runGetS deserialize . jData) . runGetL (many deserialize) $ d


cleanJournal :: (MonadIO m, Serial d, Semigroup s) => ArenaID -> ArenaT s f d m (OpenJournal d s)
cleanJournal ai = do
  TheFiles {..} <- theFiles (Just ai)
  liftIO $ doesFileExist theTempJournal >>= (`when` removeFile theTempJournal)
  je <- liftIO $ doesFileExist theJournalFile
  case je of
    False -> do
     jh <- liftIO $ openFile theJournalFile WriteMode
     return $ OJ ai jh (Option Nothing) mempty
    True -> do
      as <- readJournalFile ai
      liftIO $ withFileSync theTempJournal $ \h ->
        mapM_ (BS.hPutStr h . runPutS . serialize . mkJournal) as
      liftIO $ renameFile theTempJournal theJournalFile
      jh <- liftIO $ openFile theJournalFile AppendMode
      as' <- if null as
            then return $ Option Nothing
            else Option . Just <$> regenerateSummary as
      return $ OJ ai jh as' as

readArenaIDs :: MonadIO m => FilePath -> m [ArenaID]
readArenaIDs dir = liftIO go
    where go = mapMaybe readMay <$> listDirectory dir

internArenas :: (MonadIO m, Serial d, Serial f, Semigroup s) => ArenaT s f d m ArenaID
internArenas = do
  js <- journalDir' >>= readArenaIDs
  if null js
  then return 0
  else do
    let latest = maximum js
        old    = delete latest js
    mapM_ internArenaFile old
    return latest

regenerateSummary :: (Semigroup s, Monad m) => PV.Vector d -> ArenaT s f d m s
regenerateSummary ds = foldl1 (<>) <$> traverse summarize ds

internArenaFile :: (MonadIO m, Serial f, Serial d, Semigroup s) => ArenaID -> ArenaT s f d m ()
internArenaFile ai = do
  TheFiles {..} <- theFiles (Just ai)
  as  <- readJournalFile ai
  fas <- regenerateSummary as >>= finalize
  liftIO $ do withFileSync theDataFile  $ \h -> mapM_ (BS.hPutStr h . runPutS . serialize) as
              withFileSync theDataSummaryFile $ \h -> BS.hPutStr h . runPutS . serialize $ fas
              removeFile theJournalFile


readDataFile' :: (MonadIO m, Serial d) => ArenaID -> ArenaT s f d m (IO [d])
readDataFile' ai = asks (readDataFile . adArenaLocation) <*> pure ai

readAllData :: (MonadIO m, Serial f, Serial d) => ArenaT s f d m [(f, IO [d])]
readAllData = do
  TheFiles {..} <- theFiles Nothing
  ds <- Set.fromList . mapMaybe (readMay . dropExtension) <$> liftIO (listDirectory theDataDir)
  forM (Set.toList ds) $ \d -> do
                dsf <- dataSummaryFile' d
                c <- runGetL deserialize <$> liftIO (BSL.readFile dsf)
                rdf <- readDataFile' d
                return (c, rdf)

-- | Access an atomic snapshot of the 'ArenaDB' as a list of summaries and accessors
--   for their associated data. One datablock is the journal, and thus does not satisfy
--   the block policy.
--
--   The returned IO actions in the list provide access to a state consistent with the time
--   when the list of accessors was returned, but do not hold more then the journal at that
--   time's contents in memory.
accessData :: MonadIO m => ArenaT s f d m [(f, IO [d])]
accessData = do
  ArenaDB {..} <- ask
  liftIO . withMVar adCurrentJournal $ \(OJ _ _ s as) -> do
               d <- readIORef adDataRef
               return $ if null as
                        then d
                        else (adFinalize . fromJust . getOption $ s, return . toList $ as) : d

-- | Durably insert a piece of data into the 'ArenaDB'.
--   This funtion returns after the data is sync'd to disk.
addData :: (MonadIO m, Serial d, Serial f, Semigroup s) => d -> ArenaT s f d m ()
addData d = do
  conf@ArenaDB {..} <- ask
  liftIO . modifyMVar_ adCurrentJournal $ \(OJ ai h s ds) -> do
              BS.hPutStr h . runPutS . serialize . mkJournal $ d
              let s' = s <> (pure . adSummarize $ d)
              syncHandle h
              case adArenaFull . fromJust . getOption $ s' of
                False -> return $ OJ ai h s' (ds `PV.snoc` d)
                True -> do
                  hClose h
                  let (ArenaT rdr) = internArenaFile ai -- :: Arena s f d ()
                  runReaderT rdr conf
                  atomicModifyIORef' adDataRef $ \ods ->
                    ((adFinalize . fromJust . getOption $ s', readDataFile adArenaLocation ai):ods, ())
                  let ai' = ai + 1
                  h' <- openFile (journalFile adArenaLocation ai') WriteMode
                  return $ OJ ai' h' mempty mempty

#if MIN_VERSION_base(4,16,0)
-- Compat with base 4.16
-- | 'Option' is effectively 'Maybe' with a better instance of
-- 'Monoid', built off of an underlying 'Semigroup' instead of an
-- underlying 'Monoid'.
--
-- Ideally, this type would not exist at all and we would just fix the
-- 'Monoid' instance of 'Maybe'.
--
-- In GHC 8.4 and higher, the 'Monoid' instance for 'Maybe' has been
-- corrected to lift a 'Semigroup' instance instead of a 'Monoid'
-- instance. Consequently, this type is no longer useful. It will be
-- marked deprecated in GHC 8.8 and removed in GHC 8.10.
newtype Option a = Option { getOption :: Maybe a }
  deriving ( Eq       -- ^ @since 4.9.0.0
           , Ord      -- ^ @since 4.9.0.0
           , Show     -- ^ @since 4.9.0.0
           , Read     -- ^ @since 4.9.0.0
           , Generic  -- ^ @since 4.9.0.0
           )

-- | @since 4.9.0.0
instance Functor Option where
  fmap f (Option a) = Option (fmap f a)

-- | @since 4.9.0.0
instance Applicative Option where
  pure a = Option (Just a)
  Option a <*> Option b = Option (a <*> b)
  liftA2 f (Option x) (Option y) = Option (liftA2 f x y)

  Option Nothing  *>  _ = Option Nothing
  _               *>  b = b

-- | @since 4.9.0.0
instance Monad Option where
  Option (Just a) >>= k = k a
  _               >>= _ = Option Nothing
  (>>) = (*>)

-- | @since 4.9.0.0
instance Alternative Option where
  empty = Option Nothing
  Option Nothing <|> b = b
  a <|> _ = a

-- | @since 4.9.0.0
instance MonadPlus Option

-- | @since 4.9.0.0
instance MonadFix Option where
  mfix f = Option (mfix (getOption . f))

-- | @since 4.9.0.0
instance Foldable Option where
  foldMap f (Option (Just m)) = f m
  foldMap _ (Option Nothing)  = mempty

-- | @since 4.9.0.0
instance Traversable Option where
  traverse f (Option (Just a)) = Option . Just <$> f a
  traverse _ (Option Nothing)  = pure (Option Nothing)

-- | Fold an 'Option' case-wise, just like 'maybe'.
option :: b -> (a -> b) -> Option a -> b
option n j (Option m) = maybe n j m

-- | @since 4.9.0.0
instance Semigroup a => Semigroup (Option a) where
  (<>) = coerce ((<>) :: Maybe a -> Maybe a -> Maybe a)
#if !defined(__HADDOCK_VERSION__)
    -- workaround https://github.com/haskell/haddock/issues/680
  stimes _ (Option Nothing) = Option Nothing
  stimes n (Option (Just a)) = case compare n 0 of
    LT -> errorWithoutStackTrace "stimes: Option, negative multiplier"
    EQ -> Option Nothing
    GT -> Option (Just (stimes n a))
#endif

-- | @since 4.9.0.0
instance Semigroup a => Monoid (Option a) where
  mempty = Option Nothing

#endif
