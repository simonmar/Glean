{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE CPP #-}
module Glean.Database.Env ( withDatabases ) where

#if !MIN_VERSION_base(4,18,0)
import Control.Applicative (liftA2)
#endif
import Control.Concurrent
import Control.Concurrent.Async (async, waitEither, withAsync)
import Control.Exception.Safe
import Control.Monad.Extra
import Data.Default
import qualified Data.HashMap.Strict as HashMap
import Data.List.Split
import Data.Time
import System.Clock (TimeSpec(..))
import System.Environment
import System.Time.Extra (sleep, Seconds, timeout)

import Data.RateLimiterMap
import ServiceData.GlobalStats
import Util.EventBase
import Util.Log
import Util.STM

import qualified Glean.RTS.Foreign.LookupCache as LookupCache
import Glean.Database.Backup (backuper)
import qualified Glean.Database.Catalog as Catalog
import Glean.Database.Config
import Glean.Database.Close
import Glean.Database.Janitor
import Glean.Database.Open
import qualified Glean.Database.Storage as Storage
import Glean.Database.Types
import Glean.Database.Writes
import qualified Glean.ServerConfig.Types as ServerConfig
import Glean.Util.ConfigProvider
import Glean.Util.Observed as Observed
import Glean.Util.Periodic
import Glean.Util.ShardManager (SomeShardManager)
import Glean.Util.ThriftSource as ThriftSource
import Util.Time
import qualified Glean.Util.Warden as Warden
import qualified Glean.Write.Stats as Stats

-- | Initialize an Env for working with Glean databases
withDatabases
  :: ConfigProvider conf
  => EventBaseDataplane
  -> Config
  -> conf
  -> (Env -> IO a)
  -> IO a
withDatabases evb cfg cfgapi act =
  ThriftSource.withValue cfgapi (cfgServerConfig cfg) $ \server_config -> do
  server_cfg <- Observed.get server_config
  schemaLoc <- schemaLocation cfg server_cfg
  logInfo $ "Using schema from " <> showSchemaLocation schemaLoc
  let (schema, updateSchema) = cfgSchemaHook cfg schemaLoc
  ThriftSource.withValue cfgapi schema $ \schema_source -> do
  withDataStore (cfgDataStore cfg) server_cfg $ \catalog storage -> do
    envCatalog <- Catalog.open catalog
    cfgShardManager cfg envCatalog server_config $ \shardManager ->
      bracket
        (initEnv
          evb
          storage
          envCatalog
          shardManager
          cfg
          schema_source
          updateSchema
          server_config)
        closeEnv
        $ \env -> do
            spawnThreads env
            act env

initEnv
  :: Storage.Storage storage
  => EventBaseDataplane
  -> storage
  -> Catalog.Catalog
  -> SomeShardManager
  -> Config
  -> Observed SchemaIndex
  -> Bool
  -> Observed ServerConfig.Config
  -> IO Env
initEnv evb envStorage envCatalog shardManager cfg
  envSchemaSource updateSchema envServerConfig = do
    config <- Observed.get envServerConfig

    envActive <- newTVarIO mempty
    envDeleting <- newTVarIO mempty
    envStats <- Stats.new (TimeSpec 10 0)
    envLookupCacheStats <- LookupCache.newStats
    envWarden <- Warden.create
    envDatabaseJanitor <- newTVarIO Nothing
    envDatabaseJanitorPublishedCounters <- newTVarIO mempty
    envCachedRestorableDBs <- newTVarIO Nothing
    envCachedAvailableDBs <- newTVarIO mempty

    envLoggerRateLimit <-
      newRateLimiterMap (fromIntegral config.config_logging_rate_limit) 600

    envWrites <- newTVarIO HashMap.empty
    envDerivations <- newTVarIO HashMap.empty
    envWriteQueues <- WriteQueues
      <$> newTQueueIO
      <*> newTVarIO 0

    envSchemaUpdateSignal <- newEmptyTMVarIO

    envCompleting <- newTVarIO HashMap.empty

    envCompletingDerived <- newTVarIO HashMap.empty

    envDbSchemaCache <- newMVar HashMap.empty

    debug <- getDebugEnv

    return Env
      { envEventBase = evb
      , envServerLogger = cfgServerLogger cfg
      , envDatabaseLogger = cfgDatabaseLogger cfg
      , envBatchLocationParser = cfgBatchLocationParser cfg
      , envReadOnly = cfgReadOnly cfg
      , envMockWrites = cfgMockWrites cfg
      , envListener = cfgListener cfg
      , envGetCurrentTime = getCurrentTime
      , envUpdateSchema = updateSchema
      , envSchemaId = cfgSchemaId cfg
      , envShardManager = shardManager
      , envBackupBackends = cfgBackupBackends cfg
      , envEnableRecursion =
          if cfgEnableRecursion cfg
          then EnableRecursion
          else DisableRecursion
      , envFilterAvailableDBs = cfgFilterAvailableDBs cfg
      , envTracer = cfgTracer cfg
      , envDebug = cfgDebug cfg <> debug
      , .. }

getDebugEnv :: IO DebugFlags
getDebugEnv = do
  m <- lookupEnv "GLEAN_DEBUG"
  case m of
    Just str -> mconcat <$> mapM add (splitOn "," str)
    Nothing -> return def
  where
  add "tc" = return def { tcDebug = True }
  add "query" = return def { queryDebug = True }
  add other = do
    logWarning $ "Unkonwn GLEAN_DEBUG class: " <> other
    return def

spawnThreads :: Env -> IO ()
spawnThreads env = do
  config <- Observed.get env.envServerConfig

  -- on completion, record the time we last ran the janitor. This is
  -- used by the server to know when to advertise the server as alive.
  let recordJanitorResult result = do
        t <- env.envGetCurrentTime
        atomically $ writeTVar env.envDatabaseJanitor $ Just (t, result)

  case config.config_janitor_period of
    Just secs -> Warden.spawn_ env.envWarden
      $ doPeriodically (seconds (fromIntegral secs))
        -- a conservative timeout in case the janitor deadlocks for
        -- some reason.
      $ do
          let softTimeout = 1 + fromIntegral secs * 20 * 1000 * 1000
                                     -- 20 * config_janitor_period
              hardTimeout = 1 + fromIntegral (secs * 120) -- 6 * soft timeout
          result <- try $ asyncTimeout hardTimeout
                        $ timeout softTimeout
                        $ runDatabaseJanitor env
          case result of
            Right (Just (Just ())) ->
              recordJanitorResult JanitorRunSuccess
            Right (Just Nothing) -> do
              logError "janitor timeout"
              recordJanitorResult JanitorTimeout
            Right Nothing -> do
              logError "janitor stuck"
              recordJanitorResult JanitorStuck
            Left someException -> do
              logError $ "janitor failed: " <> show someException
              if
                | Just e <- fromException someException
                -> recordJanitorResult (JanitorRunFailure e)
                | otherwise
                -> recordJanitorResult
                    (JanitorRunFailure $ OtherJanitorException someException)
    Nothing ->
      recordJanitorResult JanitorDisabled

  Warden.spawn_ env.envWarden $ backuper env

  replicateM_ (fromIntegral config.config_db_writer_threads)
    $ Warden.spawn_ env.envWarden
    $ writerThread env env.envWriteQueues

  when env.envUpdateSchema $ do
    Warden.spawnDaemon env.envWarden "schema updater" $ do
      void $ atomically $ takeTMVar env.envSchemaUpdateSignal
      schemaUpdated env Nothing
    doOnUpdate env.envSchemaSource $
      atomically $ void $ tryPutTMVar env.envSchemaUpdateSignal ()

  -- Disk usage counters
  Warden.spawn_ envWarden $ doPeriodically (seconds 600) $ do
    diskSize <- Storage.getTotalCapacity envStorage
    diskUsed <- Storage.getUsedCapacity envStorage

    whenJust diskSize $ \size ->
        void $ setCounter "glean.db.disk.capacity_bytes" size
    whenJust diskUsed $ \used ->
        void $ setCounter "glean.db.disk.used_bytes" used
    whenJust (liftA2 (,) diskSize diskUsed) $ \(size,used) ->
        void $
          setCounter "glean.db.disk.used_percentage" (100 * used `div` size)

-- Todo: this needs a lot more work.
-- * We shouldn't just cancel the janitor, we should let it finish the
--   current job if there is one.
-- * We should wait for in-progress backups or restores (or cancel them safely)
closeEnv :: Env -> IO ()
closeEnv env = do
  closeDatabases env
  Warden.shutdown env.envWarden
  Catalog.close env.envCatalog

-- | Like 'System.Timeout.timeout' but more resilient against FFI calls.
--   The IO computation is run in a separate thread, and if it doesn't finish
--   before the timeout a 'Nothing' is returned. Cancellation is not attempted
asyncTimeout :: Seconds -> IO a -> IO (Maybe a)
asyncTimeout seconds action = withAsync (sleep seconds) $ \sleepA -> do
  actionA <- async action
  either Just (const Nothing) <$> waitEither actionA sleepA
