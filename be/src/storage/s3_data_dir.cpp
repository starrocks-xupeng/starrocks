// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/s3_data_dir.h"

#include <mntent.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <sys/statfs.h>
#include <utime.h>

#include <boost/algorithm/string/classification.hpp>
#include <boost/algorithm/string/predicate.hpp>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/trim.hpp>
#include <cctype>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <set>
#include <sstream>
#include <utility>

#include "env/env.h"
#include "env/env_s3.h"
#include "gen_cpp/version.h"
#include "gutil/strings/substitute.h"
#include "runtime/exec_env.h"
#include "service/backend_options.h"
#include "storage/olap_define.h"
#include "storage/rowset/rowset_factory.h"
#include "storage/rowset/rowset_meta.h"
#include "storage/rowset/rowset_meta_manager.h"
#include "storage/storage_engine.h"
#include "storage/tablet_meta_manager.h"
#include "storage/utils.h" // for check_dir_existed
#include "util/defer_op.h"
#include "util/errno.h"
#include "util/file_utils.h"
#include "util/monotime.h"
#include "util/string_util.h"
#include "util/path_util.h"
#include "storage/fs/s3_block_manager.h"

using strings::Substitute;

namespace starrocks {

S3DataDir::S3DataDir(std::string path, TabletManager* tablet_manager,
                 TxnManager* txn_manager)
    : DataDir(path, TStorageMedium::S3, tablet_manager, txn_manager) {}

S3DataDir::~S3DataDir() {
    delete _id_generator;
    delete _kv_store;
}

Status S3DataDir::init(bool read_only) {
    RETURN_IF_ERROR_WITH_WARN(update_capacity(), "update_capacity failed");

    _is_used = true;
    return Status::OK();
}

Status S3DataDir::set_cluster_id(int32_t cluster_id) {
    if (_cluster_id != -1) {
        if (_cluster_id == cluster_id) {
            return Status::OK();
        }
        LOG(ERROR) << "going to set cluster id to already assigned store, cluster_id=" << _cluster_id
                   << ", new_cluster_id=" << cluster_id;
        return Status::InternalError("going to set cluster id to already assigned store");
    }
    // return _write_cluster_id_to_path(_cluster_id_path(), cluster_id);
    return Status::OK();
}

void S3DataDir::health_check() {
    // s3 does not need health check
    return ;
}

OLAPStatus S3DataDir::get_shard(uint64_t* shard) {
    // does not need shard on s3
    *shard = 0;
    return OLAP_SUCCESS;
}

void S3DataDir::find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths) {
    // TODO: check this
    return ;
}

OLAPStatus S3DataDir::load() {
    // TODO: support this
    return OLAP_SUCCESS;
}

void S3DataDir::perform_path_gc_by_tablet() {
    // TODO: support this
    return ;
}

void S3DataDir::perform_path_gc_by_rowsetid() {
    // TODO: support this
    return ;
}

// path producer
void S3DataDir::perform_path_scan() {
    // TODO: support this
    return ;
}

Status S3DataDir::update_capacity() {
    _available_bytes = std::numeric_limits<int64_t>::max() / 2;
    _disk_capacity_bytes = std::numeric_limits<int64_t>::max() / 2;

    return Status::OK();
}

bool S3DataDir::reach_capacity_limit(int64_t incoming_data_size) {
    return false;
}

// s3 tablet path looks like this: "<bucket>:<instance_id>/data/<tablet_id>"
std::string S3DataDir::get_tablet_path(int16_t shard_id, int64_t tablet_id, int64_t schema_hash) {
    // do not need `shard_id` and `schema_hash` in s3
    uint64_t instance_id = 0; // TODO: revise this
    return path() + ":" + std::to_string(instance_id) + "/data/" + std::to_string(tablet_id);
}

Status S3DataDir::create_tablet_dir(int16_t shard_id, int64_t tablet_id, int64_t schema_hash, std::string* dir) {
    *dir = get_tablet_path(shard_id, tablet_id, schema_hash);
    // s3 does not have dir, just return ok
    return Status::OK();
}

Env* S3DataDir::get_env() {
    return S3Env::get_s3_env();
}

fs::BlockManager* S3DataDir::get_block_mgr() {
    return fs::S3BlockManager::get_block_mgr();
}

} // namespace starrocks
