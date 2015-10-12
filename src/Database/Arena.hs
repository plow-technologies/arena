{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}

module Database.Arena where

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
withFileSync fp f = liftIO $ withFile fp WriteMode go
  where go h = f h <* syncHandle h

data ArenaConf summary finalized a = ArenaConf {
      acSummarize :: a -> summary
    , acFinalize  :: summary -> finalized
    , acArenaFull :: summary -> Bool
    , acArenaLocation :: ArenaLocation
    , acDataRef   :: IORef [(finalized, IO [a])]
    , acCurrentJournal :: MVar (OpenJournal a summary)
    }

newtype ArenaT s f d m a = ArenaT { unArenaT :: ReaderT (ArenaConf s f d) m a }
    deriving (Functor, Applicative, Monad, MonadTrans, MonadReader (ArenaConf s f d), MonadIO)

summarize :: Monad m => d -> ArenaT s f d m s
summarize d = asks acSummarize <*> pure d

finalize :: Monad m => s -> ArenaT s f d m f
finalize s = asks acFinalize <*> pure s

-- journalDir, dataDir, tempJournal :: ArenaLocation -> FilePath
-- journalFile, dataFile, dataSummaryFile :: ArenaLocation -> ArenaID -> FilePath

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

initArenaT
  :: (MonadIO m, Serial d, Serial f, Semigroup s) => ArenaT s f d m a -> ArenaT s f d m a
initArenaT at = do
  cj <- internArenas >>= cleanJournal >>= liftIO . newMVar
  dr <- readAllData  >>= liftIO . newIORef
  let extend ac = ac { acCurrentJournal = cj, acDataRef = dr }
  local extend at

runArenaT
 :: (MonadIO m, Serial d, Serial s, Serial f, Semigroup s)
   => (d -> s) -> (s -> f) -> (s -> Bool) -> ArenaLocation -> ArenaT s f d m a -> m a
runArenaT acSummarize acFinalize acArenaFull acArenaLocation at =
    runReaderT (unArenaT $ initArenaT at) ArenaConf {..}
      where acCurrentJournal = error "current journal not initialized"
            acDataRef        = error "data ref not initialized"

type Arena s f d a = ArenaT s f d IO a

runArena
  :: (Serial d, Serial s, Serial f, Semigroup s)
    => (d -> s) -> (s -> f) -> (s -> Bool) -> ArenaLocation -> Arena s f d a -> IO a
runArena = runArenaT

tempJournal' :: Monad m => ArenaT s f d m FilePath
tempJournal' = asks (tempJournal . acArenaLocation)

journalFile' :: Monad m => ArenaID -> ArenaT s f d m FilePath
journalFile' ai = asks (journalFile . acArenaLocation) <*> pure ai

journalDir' :: Monad m => ArenaT s f d m FilePath
journalDir' = asks (journalDir . acArenaLocation)

dataDir' :: Monad m => ArenaT s f d m FilePath
dataDir' = asks (dataDir . acArenaLocation)

dataFile' :: Monad m => ArenaID -> ArenaT s f d m FilePath
dataFile' ai = asks (dataFile . acArenaLocation) <*> pure ai

dataSummaryFile' :: Monad m => ArenaID -> ArenaT s f d m FilePath
dataSummaryFile' ai = asks (dataSummaryFile . acArenaLocation) <*> pure ai

readJournalFile' :: (Serial d, MonadIO m) => ArenaID -> ArenaT s f d m [d]
readJournalFile' ai = do
  d <- journalFile' ai >>= liftIO . BSL.readFile
  return . map (head . rights . pure . runGetS deserialize . jData) . runGetL (many deserialize) $ d


cleanJournal :: (MonadIO m, Serial d, Semigroup s) => ArenaID -> ArenaT s f d m (OpenJournal d s)
cleanJournal ai = do
  TheFiles {..} <- theFiles (Just ai)
  liftIO $ doesFileExist theTempJournal >>= (`when` removeFile theTempJournal)
  je <- liftIO $ doesFileExist theJournalFile
  case je of
    False -> do
     jh <- liftIO $ openFile theJournalFile WriteMode
     return $ OJ ai jh (Option Nothing) []
    True -> do
      as <- readJournalFile' ai
      liftIO $ withFileSync theTempJournal $ \h ->
        mapM_ (BS.hPutStr h . runPutS . serialize . mkJournal) as
      liftIO $ renameFile theTempJournal theJournalFile
      jh <- liftIO $ openFile theJournalFile WriteMode
      as' <- alec' as
      return $ OJ ai jh (pure as') (reverse as)

readArenaIDs :: MonadIO m => FilePath -> m [ArenaID]
readArenaIDs dir = liftIO go
    where go = mapMaybe readMay <$> getDirectoryContents dir

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

alec' :: (Semigroup s, Monad m) => [d] -> ArenaT s f d m s
alec' ds = foldl1 (<>) <$> traverse summarize ds

internArenaFile :: (MonadIO m, Serial f, Serial d, Semigroup s) => ArenaID -> ArenaT s f d m ()
internArenaFile ai = do
  TheFiles {..} <- theFiles (Just ai)
  as  <- readJournalFile' ai
  fas <- alec' as >>= finalize
  liftIO $ do withFileSync theDataFile  $ \h -> mapM_ (BS.hPutStr h . runPutS . serialize) as
              withFileSync theDataSummaryFile $ \h -> BS.hPutStr h . runPutS . serialize $ fas
              removeFile theJournalFile


readDataFile' :: (MonadIO m, Serial d) => ArenaID -> ArenaT s f d m (IO [d])
readDataFile' ai = asks (readDataFile . acArenaLocation) <*> pure ai

readAllData :: (MonadIO m, Serial f, Serial d) => ArenaT s f d m [(f, IO [d])]
readAllData = do
  TheFiles {..} <- theFiles Nothing
  ds <- Set.fromList . mapMaybe (readMay . dropExtension) <$> liftIO (getDirectoryContents theDataDir)
  forM (Set.toList ds) $ \d -> do
                dsf <- dataSummaryFile' d
                c <- runGetL deserialize <$> liftIO (BSL.readFile dsf)
                rdf <- readDataFile' d
                return (c, rdf)

-- accessData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> m2 [(c, m2 [a])]
-- accessData dr cjM = liftIO $ do
--   withMVar cjM $ \(OJ _ _ s as) -> do
--     d <- readIORef dr
--     return $ (finalize . fromJust . getOption $ s, return . reverse $ as):d

accessData :: MonadIO m => ArenaT s f d m [(f, IO [d])]
accessData = do
  ArenaConf {..} <- ask
  liftIO . withMVar acCurrentJournal $ \(OJ _ _ s as) -> do
               d <- readIORef acDataRef
               return $ (acFinalize . fromJust . getOption $ s, return . reverse $ as) : d

addData :: (MonadIO m, Serial d, Serial f, Semigroup s) => d -> ArenaT s f d m ()
addData d = do
  conf@ArenaConf {..} <- ask
  liftIO . modifyMVar_ acCurrentJournal $ \(OJ ai h s ds) -> do
              BS.hPutStr h . runPutS . serialize . mkJournal $ d
              let s' = s <> (pure . acSummarize $ d)
              syncHandle h
              case acArenaFull . fromJust . getOption $ s' of
                False -> return $ OJ ai h s' (d:ds)
                True -> do
                  hClose h
                  let (ArenaT rdr) = internArenaFile ai -- :: Arena s f d ()
                  runReaderT rdr conf
                  atomicModifyIORef' acDataRef $ \ods ->
                    ((acFinalize . fromJust . getOption $ s', readDataFile acArenaLocation ai):ods, ())
                  let ai' = ai + 1
                  h' <- openFile (journalFile acArenaLocation ai') WriteMode
                  return $ OJ ai' h' mempty mempty


-- addData :: IORef [(c, m2 [a])] -> MVar (OpenJournal a b) -> a -> m3 ()
-- addData dsr ojM a = liftIO . modifyMVar_ ojM $ \(OJ ai h s as) -> do
--  BS.hPutStr h . runPutS . serialize . mkJournal $ a
--  let s' = s <> (pure . summarize $ a)
--  syncHandle h -- This COULD be moved down for a very minor perf boost.
--  case arenaFull . fromJust . getOption $ s' of
--    False -> return $ OJ ai h s' (a:as)
--    True -> do
--      hClose h
--      internArenaFl ai
--      atomicModifyIORef' dsr $ \ods ->
--        ((finalize . fromJust . getOption $ s', readDataFl ai):ods, ())
--      let ai' = ai + 1
--      h' <- openFile (journalFile diskLocation $ ai') WriteMode
--      return $ OJ ai' h' mempty mempty
