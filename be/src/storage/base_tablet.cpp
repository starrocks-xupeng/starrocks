// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/olap/base_tablet.cpp

// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "storage/base_tablet.h"

#include "storage/data_dir.h"
#include "util/path_util.h"
#ifdef USE_STAROS
#include "service/staros_worker.h"
#endif

namespace starrocks {

BaseTablet::BaseTablet(const TabletMetaSharedPtr& tablet_meta, DataDir* data_dir)
        : _state(tablet_meta->tablet_state()), _tablet_meta(tablet_meta), _data_dir(data_dir) {
}

Status BaseTablet::set_tablet_state(TabletState state) {
    if (_tablet_meta->tablet_state() == TABLET_SHUTDOWN && state != TABLET_SHUTDOWN) {
        LOG(WARNING) << "could not change tablet state from shutdown to " << state;
        return Status::InvalidArgument(fmt::format("Change tablet state from SHUTODWN to {} is forbidden", state));
    }
    _tablet_meta->set_tablet_state(state);
    _state = state;
    if (state == TABLET_SHUTDOWN) {
        on_shutdown();
    }
    return Status::OK();
}

std::string BaseTablet::schema_hash_path() const {
    if (_tablet_meta == nullptr) {
        return "";
    }
#ifdef USE_STAROS
    auto shard_info = get_shard_info(_tablet_meta->staros_shard_id());
    if (!shard_info.ok()) {
        LOG(WARNING) << "Fail to get shard#" << _tablet_meta->staros_shard_id();
        return "";
    }
    if (shard_info->obj_store_info.uri.empty()) {
        return "";
    }
    if (shard_info->obj_store_info.uri.back() != '/') {
        return fmt::format("{}/{}/{}", shard_info->obj_store_info.uri, _tablet_meta->tablet_id(), _tablet_meta->schema_hash());
    } else {
        return fmt::format("{}{}/{}", shard_info->obj_store_info.uri, _tablet_meta->tablet_id(), _tablet_meta->schema_hash());
    }
#else
    if (_data_dir == nullptr) return "";
    return fmt::format("{}{}/{}/{}/{}",
           _data_dir->path(),
           DATA_PREFIX,
           _tablet_meta->shard_id(),
          _tablet_meta->tablet_id(),
          _tablet_meta->schema_hash());
#endif
}

std::string BaseTablet::tablet_id_path() const {
    if (_tablet_meta == nullptr) {
        return "";
    }
#ifdef USE_STAROS
    auto shard_info = get_shard_info(_tablet_meta->staros_shard_id());
    if (!shard_info.ok()) {
        LOG(WARNING) << "Fail to get shard#" << _tablet_meta->staros_shard_id();
        return "";
    }
    if (shard_info->obj_store_info.uri.empty()) {
        return "";
    }
    if (shard_info->obj_store_info.uri.back() != '/') {
        return fmt::format("{}/{}", shard_info->obj_store_info.uri, _tablet_meta->tablet_id());
    } else {
        return fmt::format("{}{}", shard_info->obj_store_info.uri, _tablet_meta->tablet_id());
    }
#else
    if (_data_dir == nullptr) return "";
    return fmt::format("{}{}/{}/{}",
           _data_dir->path(),
           DATA_PREFIX,
           _tablet_meta->shard_id(),
          _tablet_meta->tablet_id());
#endif
}

} /* namespace starrocks */
