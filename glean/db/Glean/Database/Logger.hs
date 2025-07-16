{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

-- {-# OPTIONS_GHC -Wno-unused-do-bind #-}
module Glean.Database.Logger (
  logDBStatistics
) where

import Data.Text (Text)

import qualified Glean.Database.Types as Database
import qualified Glean.Types as Thrift
import Glean.Logger.Database as Logger
import Glean.RTS.Foreign.Ownership (OwnershipStats(..))
import Glean.Types
import Glean.Util.Some

logDBStatistics
  :: Database.Env -- ^ Environment
  -> Thrift.Repo  -- ^ Repo of interest
  -> [(PredicateRef, Thrift.PredicateStats)]
                  -- ^ Digested stats per query in database
  -> Maybe OwnershipStats
  -> Int          -- ^ Number of bytes backed up
  -> Text         -- ^ Backup locator
  -> Bool         -- ^ The db has exclude property
  -> IO ()
logDBStatistics
  env
  repo
  preds
  maybeOwnershipStats
  size
  locator
  excluded = do
  let preamble = mconcat
        [ Logger.SetRepoName repo.repo_name
        , Logger.SetRepoHash repo.repo_hash
        ]

  -- We shoehorn a summary row into the format for query rows
  let summary  = mconcat
        [ Logger.SetPredicateSize size            -- # bytes uploaded
        , Logger.SetPredicateCount factCount      -- # total facts
        , Logger.SetUploadDestination locator
        , Logger.SetHasExcludeProperty excluded
        ]
      factCount = fromIntegral $ sum [predicateStats_count p| (_, p) <- preds]

  let queries  =
        [ mconcat
          [ Logger.SetPredicateName predicateRef.predicateRef_name
          , Logger.SetPredicateVersion (fromIntegral predicateRef.predicateRef_version)
          , Logger.SetPredicateCount $ fromIntegral predicateStats.predicateStats_count
          , Logger.SetPredicateSize $ fromIntegral predicateStats.predicateStats_size
          ]
        | (predicateRef, predicateStats) <- preds
        ]

  let ownership
        | Just ownershipStats <- maybeOwnershipStats =
          [
            mconcat
              [ Logger.SetMetric "ownership_units"
              , Logger.SetCount $ fromIntegral ownershipStats.numUnits
              , Logger.SetSize $ fromIntegral ownershipStats.unitsSize
              ],
            mconcat
              [ Logger.SetMetric "ownership_sets"
              , Logger.SetCount $ fromIntegral ownershipStats.numSets
              , Logger.SetSize $ fromIntegral ownershipStats.setsSize
              ],
            mconcat
              [ Logger.SetMetric "ownership_fact_owners"
              , Logger.SetCount $ fromIntegral ownershipStats.numOwnerEntries
              , Logger.SetSize $ fromIntegral ownershipStats.ownersSize
              ],
            mconcat
              [ Logger.SetMetric "ownership_orphan_facts"
              , Logger.SetCount $ fromIntegral ownershipStats.numOrphanFacts
              ]
          ]
        | otherwise = []

  let logStats log = case Database.envDatabaseLogger env of
        Some logger -> Logger.runLog logger $ preamble <> log

  -- We want a new row in the log table for the summary, and a new row
  -- for each query
  mapM_ logStats (summary:queries <> ownership)
