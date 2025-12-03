{-
  Adapted from code in static-ls

  Copyright (c) 2025 Simon Marlow
  Copyright (c) 2023 Joseph Sumabat

  See glean/lsp/LICENSE
-}

{-# LANGUAGE DeriveFunctor #-}
module Glean.LSP.Diff
  ( diffMap
  , DiffMap(..)
  , PositionMap
  , mapPosition
  , mapRange
  ) where

import qualified Data.Algorithm.Diff as Diff
import Data.RangeMap (RangeMap, Range(..), Pos(..))
import qualified Data.RangeMap as RangeMap
import qualified Language.LSP.Protocol.Types as LSP
import Data.Text.Utf16.Rope.Mixed (Rope)
import qualified Data.Text.Utf16.Rope.Mixed as Rope

{-
If the source file has been changed relative to the version that was
indexed, we would like the LSP to update its results to account for the
local changes so that we can still get meaningful IDE features using the
Glean data even for locally-modified files.

This is a basic implementation of that idea. We diff the file contents
and then apply a line offset to symbol locations, or omit results that
have been deleted locally. This depends on the indexer having stored the
full file content (e.g. the Haskell indexer supports this when the
`--store-src` option is given). Only a line offset is supported for now;
edited lines are considered deleted from the original file.

This is only approximately correct in general: every edit to a file
potentially induces arbitrary semantic changes, so just applying a
line offset won't always be right. But it's often the right thing and
very useful in practice.

Furthermore there are limitations to what we can do with reasonable
performance. If you perform a find-references request that returns
results in 100+ files, we're not going to open all of those files and
diff the source contents to find out whether the symbols still exist,
so we only do this offsetting for files that are currently open. The
LSP server caches the diff between the indexed source and the current
state of the file so that offsets can be computed quickly.

Why lines and not tokens, or something else?

1. we're already working with lines and columns, so it's easy to
translate those. Working with offsets and coverting to/from lines and
columns would be possible but it means working more closely with Glass
which already does this conversion, we don't want to duplicate all the
work (and code).

2. glean-lsp is language independent so doing tokenisation is hard.

TODO: Ideally we would also make it possible to map source locations
even when lines have been re-indented or otherwise lightly edited.
-}

data Elem a
  = Insert a
  | Delete a
  | Keep a
  deriving (Show, Eq, Ord, Functor)

type LineDiff = [Elem Int]

diff :: (Eq a) => [a] -> [a] -> [Elem a]
diff x y = map convert (Diff.getDiff x y)
 where
  convert (Diff.Second x) = Insert x
  convert (Diff.First x) = Delete x
  convert (Diff.Both x _) = Keep x

invert :: LineDiff -> LineDiff
invert = map rev
  where rev (Insert a) = Delete a
        rev (Delete a) = Insert a
        rev (Keep a) = Keep a

diffMerged :: (Eq a) => [a] -> [a] -> [Elem [a]]
diffMerged a b = go $ fmap (fmap (:[])) $ diff a b
 where
  go (Insert x : Insert y : xs) = go $ Insert (x <> y) : xs
  go (Delete x : Delete y : xs) = go $ Delete (x <> y) : xs
  go (Keep x : Keep y : xs) = go $ Keep (x <> y) : xs
  go (x : xs) = x : go xs
  go [] = []

diffByLine :: Rope -> Rope -> [Elem Int]
diffByLine a b =
  fmap (fmap length) $
    diffMerged (Rope.lines a) (Rope.lines b)

type PositionMap = RangeMap Delta

data DiffMap = DiffMap
  { toDest :: !PositionMap
  , toSource :: !PositionMap
  }
  deriving (Show, Eq)

-- | Where a range from the source appears in the destination text
data Delta
  = Offset !Int
  | Deleted
  deriving (Show, Eq)

-- invariant: range must contain pos
applyDelta :: Pos -> Range -> Delta -> Maybe Pos
applyDelta (Pos pos) _range delta = case delta of
  Offset d -> Just (Pos (pos + d))
  Deleted -> Nothing

-- invariant: returns ranges that are contiguous
getDeltaList :: LineDiff -> [(Range, Delta)]
getDeltaList diff = go diff 0 0
 where
  go diff !delta !pos = case diff of
    [] -> []
    (d : ds) -> case d of
      -- insertions are not part of the original text
      Insert t -> go ds (delta + t) pos
      -- keeps are part of the original text
      Keep t -> (Range (Pos pos) (Pos (pos + t)), Offset delta) : go ds delta (pos + t)
      -- See 'Delta' documentation
      Delete t -> (Range (Pos pos) (Pos (pos + t)), Deleted) : go ds (delta - t) (pos + t)

diffMap :: Rope -> Rope -> DiffMap
diffMap x y = getDiffMapFromDiff (diffByLine x y)

getDiffMapFromDiff :: LineDiff -> DiffMap
getDiffMapFromDiff diff =
  DiffMap
    { toDest = RangeMap.fromList (getDeltaList diff)
    , toSource = RangeMap.fromList (getDeltaList (invert diff))
    }

mapPos :: Pos -> PositionMap -> Maybe Pos
mapPos pos rangeMap = do
  (r,d) <- RangeMap.lookupWith pos rangeMap -- out of range will be Nothing
  applyDelta pos r d

mapPosition :: LSP.Position -> PositionMap -> Maybe LSP.Position
mapPosition (LSP.Position l c) m
  | Just (Pos l') <- mapPos (Pos (fromIntegral l)) m =
    Just (LSP.Position (fromIntegral l') c)
  | otherwise = Nothing

mapRange :: LSP.Range -> PositionMap -> Maybe LSP.Range
mapRange (LSP.Range a b) m = LSP.Range <$> mapPosition a m <*> mapPosition b m
