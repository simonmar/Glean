{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module Glean.Database.Restore (
  restoreDatabase,
  restoreDatabaseFromSite,
  forRestoreSitesM, ifRestoreRepo, restorable,
) where

import Control.Exception hiding(handle)
import qualified Data.Set as Set
import Data.Text (Text)

import Util.STM

import qualified Glean.Database.Backup.Backend as Backup
import qualified Glean.Database.Backup.Locator as Backup
import qualified Glean.Database.Catalog as Catalog
import Glean.Database.Types
import qualified Glean.ServerConfig.Types as ServerConfig
import Glean.Types hiding (Database)
import qualified Glean.Types as Thrift
import Glean.Util.Observed as Observed


forRestoreSitesM
  :: Env
  -> a
  -> (forall site. Backup.Site site => Text -> site -> IO a)
  -> IO [a]
forRestoreSitesM env none inner = do
  policy <-
    ServerConfig.config_restore <$> Observed.get env.envServerConfig
  r <- atomically $ Backup.getAllSites env
  case r of
    sites@(_:_)
      | policy.databaseRestorePolicy_enabled
        || not (Set.null policy.databaseRestorePolicy_override) ->
      mapM (uncurry inner) sites
    _ -> return [none]

ifRestoreRepo
  :: Env
  -> a
  -> Repo
  -> IO a
  -> IO a
ifRestoreRepo env none repo inner = do
  policy <- ServerConfig.config_restore <$> Observed.get env.envServerConfig
  if restorable policy repo
    then inner
    else return none

restorable :: ServerConfig.DatabaseRestorePolicy -> Repo -> Bool
restorable policy repo =
  (repoName `Set.member` policy.databaseRestorePolicy_override)
     /= policy.databaseRestorePolicy_enabled
  where repoName = Thrift.repo_name repo

restoreDatabase :: Env -> Text -> IO ()
restoreDatabase env loc
  | Just (_, site, repo) <-
      Backup.fromRepoLocator (envBackupBackends env) loc =
        atomically =<< restoreDatabaseFromSite env site repo
  | otherwise = throwIO $
      Thrift.InvalidLocator $ "invalid locator '" <> loc <>  "'"

restoreDatabaseFromSite
  :: Backup.Site site
  => Env
  -> site
  -> Repo
  -> IO (STM ())
restoreDatabaseFromSite env site repo = do
  props <- Backup.inspect site repo
  return $ Catalog.startRestoring env.envCatalog repo props
