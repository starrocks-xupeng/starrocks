// This file is licensed under the Elastic License 2.0. Copyright 2021-present, StarRocks Limited.

#pragma once

#ifdef USE_STAROS

#include <starlet/starlet.h>

#include "common/statusor.h"

namespace starrocks {

void init_staros_worker();
void shutdown_staros_worker();

StatusOr<staros::starlet::ShardInfo> get_shard_info(staros::starlet::ShardId shard_id);

} // namespace starrocks

#endif // USE_STAROS
