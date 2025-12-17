/*
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "glean/rts/binary.h"
#include "glean/rts/error.h"
#include "lmdb.h"

namespace facebook {
namespace glean {
namespace rocks {
namespace impl {

[[noreturn]] inline void error(int s) {
  rts::error("rocksdb: {}", mdb_strerror(s));
}

inline void check(int status) {
  if (status != MDB_SUCCESS) {
    error(status);
  }
}

inline folly::ByteRange byteRange(const MDB_val& slice) {
  return folly::ByteRange(
      reinterpret_cast<const unsigned char*>(slice.mv_data), slice.mv_size);
}

inline MDB_val slice(const folly::ByteRange& range) {
  return {
      .mv_size = range.size(),
      .mv_data = (void*)(range.data())
  };
}

inline MDB_val slice(binary::Output& output) {
  return slice(output.bytes());
}

template <typename T>
inline MDB_val toSlice(T& x) {
  return MDB_val { sizeof(x), reinterpret_cast<void*>(&x) };
}

template <typename T>
inline T fromSlice(const MDB_val& slice) {
  assert(slice.mv_size == sizeof(T));
  T x;
  std::memcpy(&x, slice.mv_data, slice.mv_size);
  return x;
}

inline binary::Input input(const MDB_val& slice) {
  return binary::Input(byteRange(slice));
}

} // namespace impl
} // namespace rocks
} // namespace glean
} // namespace facebook
