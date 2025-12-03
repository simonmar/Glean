{-
  Derived from Data.RangeMap in static-ls

  Copyright (c) 2023 Joseph Sumabat

  See glean/lsp/LICENSE
-}

module Data.RangeMap (
  Range(..),
  Pos(..),
  RangeMap (map),
  fromList,
  lookup,
  lookupWith,
  empty,
)
where

import Data.Function ((&))
import Data.IntMap.Strict (IntMap)
import Data.IntMap.Strict qualified as IntMap
import GHC.Stack (HasCallStack)
import Prelude hiding (lookup)

data IntPair a = P !Int a
  deriving (Show, Eq, Ord)

newtype RangeMap a = RangeMap {map :: IntMap (IntPair a)}
  deriving (Show, Eq, Ord)

empty :: RangeMap a
empty = RangeMap IntMap.empty

newtype Pos = Pos { pos :: Int }
  deriving (Show, Eq, Ord)

data Range = Range
  { start :: !Pos
  , end :: !Pos
  }
  deriving (Show, Eq, Ord)

-- Invariant: ranges must be disjoint
fromList :: (HasCallStack) => [(Range, a)] -> RangeMap a
fromList ranges =
  ranges
    & map (\(r, x) -> (r.start.pos, P r.end.pos x))
    & IntMap.fromList
    & RangeMap

lookup :: Pos -> RangeMap a -> Maybe a
lookup pos rm = snd <$> lookupWith pos rm

lookupWith :: Pos -> RangeMap a -> Maybe (Range, a)
lookupWith (Pos pos) (RangeMap rm)
  | Just (start, (P end x)) <- IntMap.lookupLE pos rm
  , pos < end =
      Just (Range (Pos start) (Pos end), x)
  | otherwise = Nothing
