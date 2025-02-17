/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

@interface J {
  int _i;
}

@property (nonatomic, assign) int i;

@end

@implementation J

@synthesize i = _i;

- (void)testMethod
{
  _i = 1;
}

@end
