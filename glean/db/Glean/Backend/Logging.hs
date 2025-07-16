{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Backend.Logging
  ( LoggingBackend(..)
  ) where

import Control.Exception
import qualified Data.ByteString as ByteString
import qualified Data.HashMap.Strict as HashMap
import qualified Data.Map as Map
import Data.Maybe
import Data.Monoid (Sum(getSum, Sum))
import Data.Text (Text)
import qualified Data.Text as Text
import qualified Data.Text.Encoding as Text
import qualified Data.Text.Encoding.Error as Text
import TextShow (showt)

import Util.Logger

import Glean.Schema.Util (showRef)
import Glean.Backend.Local ()
import Glean.Logger.Server as Logger
import qualified Glean.Database.List as Database
import qualified Glean.Database.Types as Database
import Glean.Database.Writes (batchOwnedSize, batchDependenciesSize)
import Glean.Logger
import qualified Glean.Types as Thrift
import Util.Time

import Glean.Backend.Types


-- | A logging wrapper for Env. We do it this way because some backend
-- calls invoke other backend calls, and we only want to log the
-- outermost one. For example, userQuery will call queryFact a *lot*,
-- and it would be too expensive to log each and every call to
-- queryFact.
newtype LoggingBackend = LoggingBackend Database.Env

instance Backend LoggingBackend where
  queryFact (LoggingBackend env) repo id =
    loggingAction (runLogRepo "queryFact" env repo) (const mempty) $
      queryFact env repo id
  factIdRange (LoggingBackend env) repo =
    loggingAction (runLogRepo "factIdRange" env repo) (const mempty) $
      factIdRange env repo
  getSchemaInfo (LoggingBackend env) (Just repo) req =
    loggingAction (runLogRepo "getSchemaInfo" env repo) (const mempty) $
      getSchemaInfo env (Just repo) req
  getSchemaInfo (LoggingBackend env) Nothing req =
    loggingAction (runLogCmd "getSchemaInfo" env) (const mempty) $
      getSchemaInfo env Nothing req
  validateSchema (LoggingBackend env) req =
    loggingAction (runLogCmd "validateSchema" env) (const mempty) $
      validateSchema env req
  predicateStats (LoggingBackend env) repo opts =
    loggingAction (runLogRepo "predicateStats" env repo) (const mempty) $
      predicateStats env repo opts
  userQueryFacts (LoggingBackend env) repo req =
    loggingAction (runLogQueryFacts "userQueryFacts" env repo req)
      logQueryResults $
        userQueryFacts env repo req
  userQuery (LoggingBackend env) repo req =
    loggingAction (runLogQuery "userQuery" env repo req) logQueryResults $
      userQuery env repo req
  userQueryBatch (LoggingBackend env) repo reqBatch =
    loggingAction
      (runLogQueryBatch "userQueryBatch" env repo reqBatch)
      logQueryResultsOrException
      (userQueryBatch env repo reqBatch)

  deriveStored (LoggingBackend env) log repo q =
    loggingAction
      (runLogDerivePredicate "deriveStored" env repo q)
      (const mempty)
      (deriveStored env (runLogDerivationResult env log repo q) repo q)

  listDatabases (LoggingBackend env) req =
    loggingAction (runLogCmd "listDatabases" env) (const mempty) $
      Database.listDatabases env req
  getDatabase (LoggingBackend env) repo =
    loggingAction (runLogRepo "getDatabase" env repo) (const mempty) $
      getDatabase env repo

  kickOffDatabase (LoggingBackend env) rq =
    loggingAction
      (runLogKickOff "kickOff" env rq)
      (const mempty) $
      kickOffDatabase env rq
  finishDatabase (LoggingBackend env) repo =
    loggingAction
      (runLogRepo "finishDatabase" env repo)
      (const mempty) $
      finishDatabase env repo
  finalizeDatabase (LoggingBackend env) repo =
    loggingAction
      (runLogRepo "finalizeDatabase" env repo)
      (const mempty) $
      finalizeDatabase env repo
  updateProperties (LoggingBackend env) repo set unset =
    loggingAction
      (runLogRepo "updateProperties" env repo)
      (const mempty) $
      updateProperties env repo set unset

  completePredicates_ (LoggingBackend env) repo preds =
    loggingAction
       (runLogRepo "completePredicates" env repo)
       (const mempty) $
       completePredicates_ env repo preds

  restoreDatabase (LoggingBackend env) loc =
    loggingAction (runLogCmd "restoreDatabase" env) (const mempty) $
      restoreDatabase env loc
  deleteDatabase (LoggingBackend env) repo =
    loggingAction (runLogRepo "deleteDatabase" env repo) (const mempty) $
      deleteDatabase env repo
  enqueueBatch (LoggingBackend env) cbatch =
    loggingAction
      (runLogEnqueueBatch "enqueueBatch" env cbatch)
      (const mempty) $
        enqueueBatch env cbatch
  enqueueJsonBatch (LoggingBackend env) repo batch =
    loggingAction (runLogRepo "enqueueJsonBatch" env repo) (const mempty) $
      enqueueJsonBatch env repo batch
  enqueueBatchDescriptor (LoggingBackend env) repo batch waitPolicy =
    loggingAction
      (runLogRepo "enqueueBatchDescriptor" env repo)
      (const mempty) $
        enqueueBatchDescriptor env repo batch waitPolicy
  pollBatch (LoggingBackend env) handle =
    loggingAction (runLogCmd "pollBatch" env) (const mempty) $
      pollBatch env handle
  displayBackend (LoggingBackend b) = displayBackend b
  hasDatabase (LoggingBackend b) repo = hasDatabase b repo
  schemaId (LoggingBackend b) = schemaId b
  usingShards (LoggingBackend b) = usingShards b
  initGlobalState (LoggingBackend b) = initGlobalState b

runLogKickOff
  :: Text
  -> Database.Env
  -> Thrift.KickOff
  -> GleanServerLog
  -> IO ()
runLogKickOff cmd env kickOff log =
  runLogRepo cmd env kickOff.kickOff_repo $ log <> schemaId
  where
  schemaId = maybe mempty Logger.SetSchemaId $
    HashMap.lookup "glean.schema_id" kickOff.kickOff_properties

runLogQueryFacts
  :: Text
  -> Database.Env
  -> Thrift.Repo
  -> Thrift.UserQueryFacts
  -> GleanServerLog -> IO ()
runLogQueryFacts cmd env repo queryFacts log =
  runLogRepo cmd env repo $ log
    <> maybe mempty logQueryOptions queryFacts.userQueryFacts_options
    <> maybe mempty logQueryClientInfo queryFacts.userQueryFacts_client_info
    <> maybe mempty (Logger.SetSchemaId . Thrift.unSchemaId)
        queryFacts.userQueryFacts_schema_id

runLogQuery
  :: Text
  -> Database.Env
  -> Thrift.Repo
  -> Thrift.UserQuery
  -> GleanServerLog
  -> IO ()
runLogQuery cmd env repo query log = do
  runLogRepo cmd env repo $ mconcat
    [ log
    , Logger.SetQuery
        (Text.decodeUtf8With Text.lenientDecode $
          if ByteString.length query.userQuery_query > 1024
            then "[truncated] " <> ByteString.take 1024 query.userQuery_query
            else query.userQuery_query)
    , Logger.SetPredicate query.userQuery_predicate
    , maybe mempty (Logger.SetPredicateVersion . fromIntegral)
        query.userQuery_predicate_version
    , maybe mempty (Logger.SetSchemaId . Thrift.unSchemaId)
        query.userQuery_schema_id
    , maybe mempty logQueryOptions query.userQuery_options
    , maybe mempty logQueryClientInfo query.userQuery_client_info
    ]

runLogQueryBatch
  :: Text
  -> Database.Env
  -> Thrift.Repo
  -> Thrift.UserQueryBatch
  -> GleanServerLog
  -> IO ()
runLogQueryBatch cmd env repo batch log =
  runLogRepo cmd env repo $ mconcat
    [ log
    , Logger.SetQuery $ case batch.userQueryBatch_queries of
        [] -> "0 batched queries"
        q:rest -> Text.unlines $
          Text.decodeUtf8With Text.lenientDecode q :
          [" + " <> showt n <> " batched queries"
          | let n = length rest
          , n > 1
          ]
    , Logger.SetPredicate batch.userQueryBatch_predicate
    , maybe mempty (Logger.SetPredicateVersion . fromIntegral)
        batch.userQueryBatch_predicate_version
    , maybe mempty (Logger.SetSchemaId . Thrift.unSchemaId)
        batch.userQueryBatch_schema_id
    , maybe mempty logQueryOptions batch.userQueryBatch_options
    , maybe mempty logQueryClientInfo batch.userQueryBatch_client_info
    ]

runLogEnqueueBatch
  :: Text
  -> Database.Env
  -> Thrift.ComputedBatch
  -> GleanServerLog
  -> IO ()
runLogEnqueueBatch cmd env computedBatch log =
  let !batch = computedBatch.computedBatch_batch in
  runLogRepo cmd env computedBatch.computedBatch_repo $ mconcat
    [ log
    , Logger.SetBatchFactsSize $ ByteString.length batch.batch_facts
    , Logger.SetBatchFactsCount $ fromIntegral $
        Thrift.batch_count batch
    , Logger.SetBatchOwnedSize $ batchOwnedSize batch.batch_owned
    , Logger.SetBatchDependenciesSize $ batchDependenciesSize batch.batch_dependencies
    ]

logQueryOptions :: Thrift.UserQueryOptions -> GleanServerLog
logQueryOptions opts = mconcat
  [ Logger.SetNoBase64Binary opts.userQueryOptions_no_base64_binary
  , Logger.SetExpandResults opts.userQueryOptions_expand_results
  , Logger.SetRecursive opts.userQueryOptions_recursive
  , maybe mempty (Logger.SetMaxResults . fromIntegral)
      opts.userQueryOptions_max_results
  , Logger.SetSyntax $ case opts.userQueryOptions_syntax of
      Thrift.QuerySyntax_JSON -> "JSON"
      Thrift.QuerySyntax_ANGLE -> "Angle"
  , maybe mempty
      ( Logger.SetRequestContinuationSize
      . ByteString.length
      . Thrift.userQueryCont_continuation
      )
      opts.userQueryOptions_continuation
  ]

logQueryClientInfo :: Thrift.UserQueryClientInfo -> GleanServerLog
logQueryClientInfo info = mconcat
  [ maybe mempty Logger.SetClientUnixname info.userQueryClientInfo_unixname
  , Logger.SetClientApplication info.userQueryClientInfo_application
  , Logger.SetClientName info.userQueryClientInfo_name
  ]

logQueryResultsOrException
  :: [Thrift.UserQueryResultsOrException] -> GleanServerLog
logQueryResultsOrException results = mconcat
  [
    Logger.SetResults $ getSum $ foldMap (Sum . countQueryResults)
      [ r | Thrift.UserQueryResultsOrException_results r <- results]
  ]

logQueryResults :: Thrift.UserQueryResults -> GleanServerLog
logQueryResults it = mconcat
  [ Logger.SetResults $ countQueryResults it
  , Logger.SetTruncated (isJust it.userQueryResults_continuation)
  , maybe mempty logQueryStats it.userQueryResults_stats
  , maybe mempty Logger.SetType it.userQueryResults_type
  , maybe mempty
      ( Logger.SetResponseContinuationSize
      . ByteString.length
      . Thrift.userQueryCont_continuation
      )
      it.userQueryResults_continuation
  ]

countQueryResults :: Thrift.UserQueryResults -> Int
countQueryResults results =
  case results.userQueryResults_results of
    Thrift.UserQueryEncodedResults_bin bin ->
      Map.size (Thrift.userQueryResultsBin_facts bin)
    Thrift.UserQueryEncodedResults_json json ->
      length (Thrift.userQueryResultsJSON_facts json)
    Thrift.UserQueryEncodedResults_compact compact ->
      length (Thrift.userQueryResultsCompact_facts compact)
    _ ->
      length results.userQueryResults_facts

logQueryStats :: Thrift.UserQueryStats -> GleanServerLog
logQueryStats stats = mconcat
  [ Logger.SetResults (fromIntegral stats.userQueryStats_result_count)
  , Logger.SetFacts (fromIntegral stats.userQueryStats_num_facts)
  , Logger.SetFullScans (showRef <$> stats.userQueryStats_full_scans)
  , maybe mempty (Logger.SetBytecodeSize . fromIntegral)
      stats.userQueryStats_bytecode_size
  , maybe mempty (Logger.SetCompileTimeUs . fromIntegral . (`quot` 1000))
      stats.userQueryStats_compile_time_ns
  , maybe mempty (Logger.SetExecuteTimeUs . fromIntegral . (`quot` 1000))
      stats.userQueryStats_execute_time_ns
  , maybe mempty (Logger.SetQueryResultBytes . fromIntegral)
      stats.userQueryStats_result_bytes
  ]

runLogDerivePredicate
  :: Text
  -> Database.Env
  -> Thrift.Repo
  -> Thrift.DerivePredicateQuery
  -> GleanServerLog
  -> IO ()
runLogDerivePredicate cmd env repo query log =
  runLogRepo cmd env repo $ mconcat
    [ log
    , Logger.SetPredicate query.derivePredicateQuery_predicate
    , maybe mempty (Logger.SetPredicateVersion . fromIntegral)
        query.derivePredicateQuery_predicate_version
    , maybe mempty logQueryClientInfo query.derivePredicateQuery_client_info
    ]

runLogDerivationResult
  :: Database.Env
  -> LogDerivationResult
  -> Thrift.Repo
  -> Thrift.DerivePredicateQuery
  -> Either (DiffTimePoints, SomeException) Thrift.UserQueryStats
  -> IO ()
runLogDerivationResult env log repo query res = do
  log res
  runLogRepo "deriveStored(completed)" env repo $ mconcat
    [ Logger.SetPredicate query.derivePredicateQuery_predicate
    , maybe mempty (Logger.SetPredicateVersion . fromIntegral)
        query.derivePredicateQuery_predicate_version
    , maybe mempty logQueryClientInfo query.derivePredicateQuery_client_info
    , case res of
        Left (_,err) -> failureLog err
        Right stats -> successLog <> logQueryStats stats
    , timeLog $ toDiffSeconds $ case res of
        Left (duration, _) -> duration
        Right stats ->
          nanoseconds (fromIntegral stats.userQueryStats_elapsed_ns)
    ]
