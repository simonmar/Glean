/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <string>
#include <folly/Format.h>

namespace facebook {
namespace glean {
namespace rts {

[[noreturn]] void raiseError(const std::string& msg);

template <class... Args>
[[noreturn]]
inline std::string error(folly::StringPiece fmt, Args&&... args) {
  raiseError(folly::sformat(fmt, std::forward<Args>(args)...));
}

}
}
}
