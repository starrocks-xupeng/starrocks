// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include "storage/data_dir.h"

namespace starrocks {

class TabletManager;
class TxnManager;
class Env;
namespace fs {
class BlockManager;
} // namespace fs

// A DataDir used to manage data in same path.
// Now, After DataDir was created, it will never be deleted for easy implementation.
class S3DataDir : public DataDir {
public:
    S3DataDir(std::string path, TabletManager* tablet_manager = nullptr, TxnManager* txn_manager = nullptr);
    virtual ~S3DataDir() override;

    virtual Env* get_env() override;
    virtual fs::BlockManager* get_block_mgr() override;

    virtual Status init(bool read_only = false) override;

    // save a cluster_id file under data path to prevent
    // invalid be config for example two be use the same
    // data path
    virtual Status set_cluster_id(int32_t cluster_id) override;
    virtual void health_check() override;

    virtual OLAPStatus get_shard(uint64_t* shard) override;

    virtual void find_tablet_in_trash(int64_t tablet_id, std::vector<std::string>* paths);

    virtual OLAPStatus load() override;

    virtual void perform_path_scan() override;

    virtual void perform_path_gc_by_rowsetid() override;

    virtual void perform_path_gc_by_tablet() override;

    virtual bool reach_capacity_limit(int64_t incoming_data_size) override;

    virtual Status update_capacity() override;

    virtual std::string get_tablet_path(int16_t shard_id, int64_t tablet_id, int64_t schema_hash) override;
    virtual Status create_tablet_dir(int16_t shard_id, int64_t tablet_id, int64_t schema_hash, std::string* dir) override;
};

} // namespace starrocks
