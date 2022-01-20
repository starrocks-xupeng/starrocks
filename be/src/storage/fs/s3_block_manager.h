// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "common/status.h"
#include "storage/fs/block_manager.h"
#include "util/file_cache.h"

namespace starrocks {

class BlockId;
class Env;
class MemTracker;
class RandomAccessFile;

namespace fs {
namespace internal {

class S3ReadableBlock;
class S3WritableBlock;
struct BlockManagerMetrics;

} // namespace internal

class S3BlockManager : public BlockManager {
public:
    static BlockManager* get_block_mgr();

    S3BlockManager(Env* env, BlockManagerOptions opts);
    virtual ~S3BlockManager() override;

    virtual Status open() override;

    virtual Status create_block(const CreateBlockOptions& opts, std::unique_ptr<WritableBlock>* block) override;

    virtual Status open_block(const std::string& path, std::unique_ptr<ReadableBlock>* block) override;

    virtual Status get_all_block_ids(std::vector<BlockId>* block_ids) override;

private:
    friend class internal::S3ReadableBlock;
    friend class internal::S3WritableBlock;

    // Deletes an existing block, allowing its space to be reclaimed by the
    // filesystem. The change is immediately made durable.
    //
    // Blocks may be deleted while they are open for reading or writing;
    // the actual deletion will take place after the last open reader or
    // writer is closed.
    Status _delete_block(const std::string& path);

    Env* env() const { return _env; }

    // For manipulating files.
    Env* _env;

    // The options that the FileBlockManager was created with.
    const BlockManagerOptions _opts;

    // Tracks the block directories which are dirty from block creation. This
    // lets us perform some simple coalescing when synchronizing metadata.
    std::unordered_set<std::string> _dirty_dirs;

    // Metric container for the block manager.
    // May be null if instantiated without metrics.
    std::unique_ptr<internal::BlockManagerMetrics> _metrics;

    // Underlying cache instance. Caches opened files.
    std::unique_ptr<FileCache<RandomAccessFile>> _file_cache;
};

} // namespace fs
} // namespace starrocks
