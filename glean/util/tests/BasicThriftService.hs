{-
  Copyright (c) Facebook, Inc. and its affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

module BasicThriftService (main) where

import Test.HUnit
import TestRunner

-- Just test that it compiles for now
main :: IO ()
main = testRunner $ TestList []
