{-
  Copyright (c) Meta Platforms, Inc. and affiliates.
  All rights reserved.

  This source code is licensed under the BSD-style license found in the
  LICENSE file in the root directory of this source tree.
-}

{-# LANGUAGE CPP, StandaloneDeriving #-}
module Glean.Impl.ThriftService
  ( ThriftService
  ) where

import Data.Maybe
import qualified Data.Text.Encoding as Text

import Glean.Util.Service
import Glean.Util.ThriftService

#ifdef FBTHRIFT

import qualified Data.ByteString.UTF8 as UTF8
import Network.Socket

import Thrift.Protocol.Id
import Thrift.Channel.HeaderChannel

-- | A basic 'ThriftService' supporting connections to a specific host/port
newtype ThriftService p = ThriftService
  { headerConfig :: HeaderConfig p
  }

deriving instance Show (ThriftService p)

instance IsThriftService ThriftService where
  mkThriftService (HostPort h p) opts = ThriftService
    { headerConfig = headerConfig
    }
    where
    timeout = round (fromMaybe 30 opts.processingTimeout * 1000)
    headerConfig = HeaderConfig
      { headerHost = Text.encodeUtf8 h
      , headerPort = fromIntegral p
      , headerProtocolId = compactProtocolId
      , headerConnTimeout = timeout
      , headerSendTimeout = timeout
      , headerRecvTimeout = timeout
      }
  mkThriftService _ _ = error "basic-thriftservice does not support Tier"

  thriftServiceWithDbShard t _ = t  -- shards are irrelevant if we have host/port

  runThrift evb thriftService action = do
    addrs <- getAddrInfo
      (Just defaultHints)
      (Just (UTF8.toString (headerHost thriftService.headerConfig)))
      Nothing
    headerConfig' <- case addrs of
       [] -> return thriftService.headerConfig
       (addr : _) -> do
         (mHost, _) <- getNameInfo [NI_NUMERICHOST] True False
           (addrAddress addr)
         case mHost of
           Nothing -> return thriftService.headerConfig
           Just host -> return
             thriftService.headerConfig { headerHost = UTF8.fromString host }
    withHeaderChannel evb headerConfig' action

  getSelection _evb thriftService _ =
    return
      [ (Text.decodeUtf8 (headerHost thriftService.headerConfig), headerPort thriftService.headerConfig)
      ]

#else /* !FBTHRIFT */

import Thrift.Channel.HTTP
import Thrift.Protocol.Id

-- | A basic 'ThriftService' supporting connections to a specific host/port
newtype ThriftService p = ThriftService
  { httpConfig :: HTTPConfig p
  }

deriving instance Show (ThriftService p)

instance IsThriftService ThriftService where
  mkThriftService (HostPort h p) opts = ThriftService
    { httpConfig = httpConfig
    }
    where
    httpConfig = HTTPConfig
      { httpHost = Text.encodeUtf8 h
      , httpPort = fromIntegral p
      , httpProtocolId = compactProtocolId
      , httpResponseTimeout =
          Just $ round (fromMaybe 30 opts.processingTimeout * 1000000)
      }
  mkThriftService _ _ = error "basic-thriftservice does not support Tier"

  thriftServiceWithDbShard t _ = t
    -- shards are irrelevant if we have host/port

  runThrift _evb thriftService action =
    withHTTPChannel thriftService.httpConfig action

  getSelection _evb thriftService _ =
    return
      [ (Text.decodeUtf8 (httpHost thriftService.httpConfig), httpPort thriftService.httpConfig)
      ]

#endif
