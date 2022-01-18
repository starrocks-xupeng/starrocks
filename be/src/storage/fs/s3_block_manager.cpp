// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "storage/fs/s3_block_manager.h"

#include <atomic>
#include <cstddef>
#include <memory>
#include <numeric>
#include <string>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "env/env.h"
#include "env/env_s3.h"
#include "env/env_util.h"
#include "gutil/strings/substitute.h"
#include "storage/fs/block_id.h"
#include "storage/fs/block_manager_metrics.h"
#include "storage/storage_engine.h"
#include "util/file_cache.h"
#include "util/metrics.h"
#include "util/path_util.h"
#include "util/slice.h"
#include "util/starrocks_metrics.h"

using std::accumulate;
using std::shared_ptr;
using std::string;

using strings::Substitute;

namespace starrocks::fs {

namespace internal {

// a s3-backed block that has been opened for writing.
// TODO: make api clean and adjust metrics
class S3WritableBlock : public WritableBlock {
public:
    S3WritableBlock(S3BlockManager* block_manager, const std::string &path, shared_ptr<WritableFile> writer);

    virtual ~S3WritableBlock() override;

    virtual Status close() override;

    virtual Status abort() override;

    virtual BlockManager* block_manager() const override;

    virtual const BlockId& id() const override;
    virtual const std::string& path() const override;

    virtual Status append(const Slice& data) override;

    virtual Status appendv(const Slice* data, size_t data_cnt) override;

    virtual Status finalize() override;

    virtual size_t bytes_appended() const override;

    virtual State state() const override;

    // Starts an asynchronous flush of dirty block data to disk.
    Status flush_data_async();

private:
    S3WritableBlock(const S3WritableBlock&) = delete;
    const S3WritableBlock& operator=(const S3WritableBlock&) = delete;

    enum SyncMode { SYNC, NO_SYNC };
    Status _close(SyncMode mode);

    // Back pointer to the block manager.
    //
    // Should remain alive for the lifetime of this block.
    S3BlockManager* _block_manager;
    const BlockId _block_id;
    const string _path;
    // The underlying opened writer backing this block.
    shared_ptr<WritableFile> _writer;
    State _state;
    // The number of bytes successfully appended to the block.
    size_t _bytes_appended;
};

S3WritableBlock::S3WritableBlock(S3BlockManager* block_manager, const std::string& path, shared_ptr<WritableFile> writer)
        : _block_manager(block_manager),
          _path(path),
          _writer(std::move(writer)),
          _state(CLEAN),
          _bytes_appended(0) {
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_writing->increment(1);
        _block_manager->_metrics->total_writable_blocks->increment(1);
    }
}

S3WritableBlock::~S3WritableBlock() {
    if (_state != CLOSED) {
        WARN_IF_ERROR(abort(), strings::Substitute("Failed to close block $0", _path));
    }
}

Status S3WritableBlock::close() {
    return _close(SYNC);
}

Status S3WritableBlock::abort() {
    RETURN_IF_ERROR(_close(NO_SYNC));
    return _block_manager->_delete_block(_path);
}

BlockManager* S3WritableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& S3WritableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const string& S3WritableBlock::path() const {
    return _path;
}

Status S3WritableBlock::append(const Slice& data) {
    return appendv(&data, 1);
}

Status S3WritableBlock::appendv(const Slice* data, size_t data_cnt) {
    DCHECK(_state == CLEAN || _state == DIRTY) << "path=" << _path << " invalid state=" << _state;
    RETURN_IF_ERROR(_writer->appendv(data, data_cnt));
    _state = DIRTY;

    // Calculate the amount of data written
    size_t bytes_written = accumulate(data, data + data_cnt, static_cast<size_t>(0),
                                      [&](int sum, const Slice& curr) { return sum + curr.size; });
    _bytes_appended += bytes_written;
    return Status::OK();
}

Status S3WritableBlock::flush_data_async() {
    VLOG(3) << "Flushing block " << _path;
    RETURN_IF_ERROR(_writer->flush(WritableFile::FLUSH_ASYNC));
    return Status::OK();
}

Status S3WritableBlock::finalize() {
    DCHECK(_state == CLEAN || _state == DIRTY || _state == FINALIZED)
            << "path=" << _path << "Invalid state: " << _state;

    if (_state == FINALIZED) {
        return Status::OK();
    }
    VLOG(3) << "Finalizing block " << _path;
    if (_state == DIRTY && BlockManager::block_manager_preflush_control == "finalize") {
        flush_data_async();
    }
    _state = FINALIZED;
    return Status::OK();
}

size_t S3WritableBlock::bytes_appended() const {
    return _bytes_appended;
}

WritableBlock::State S3WritableBlock::state() const {
    return _state;
}

Status S3WritableBlock::_close(SyncMode mode) {
    if (_state == CLOSED) {
        return Status::OK();
    }

    Status sync;
    if (mode == SYNC && (_state == CLEAN || _state == DIRTY || _state == FINALIZED)) {
        // Safer to synchronize data first, then metadata.
        VLOG(3) << "Syncing block " << _path;
        if (_block_manager->_metrics) {
            _block_manager->_metrics->total_disk_sync->increment(1);
        }
        sync = _writer->sync();
        WARN_IF_ERROR(sync, strings::Substitute("Failed to sync when closing block $0", _path));
    }
    Status close = _writer->close();

    _state = CLOSED;
    _writer.reset();
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_writing->increment(-1);
        _block_manager->_metrics->total_bytes_written->increment(_bytes_appended);
        _block_manager->_metrics->total_blocks_created->increment(1);
    }

    // Either Close() or Sync() could have run into an error.
    RETURN_IF_ERROR(close);
    RETURN_IF_ERROR(sync);

    // Prefer the result of Close() to that of Sync().
    return close.ok() ? close : sync;
}

// a s3-backed block that has been opened for reading.
// TODO: make api clean and adjust metrics
class S3ReadableBlock : public ReadableBlock {
public:
    S3ReadableBlock(S3BlockManager* block_manager, const std::string& path,
                      std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle);

    virtual ~S3ReadableBlock() override;

    virtual Status close() override;

    virtual BlockManager* block_manager() const override;

    virtual const BlockId& id() const override;
    virtual const std::string& path() const override;

    virtual Status size(uint64_t* sz) const override;

    virtual Status read(uint64_t offset, Slice result) const override;

    virtual Status readv(uint64_t offset, const Slice* results, size_t res_cnt) const override;

private:
    // Back pointer to the owning block manager.
    S3BlockManager* _block_manager;
    // The block's identifier.
    const BlockId _block_id;
    const string _path;
    // The underlying opened file backing this block.
    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> _file_handle;
    // the backing file of OpenedFileHandle, not owned.
    RandomAccessFile* _file;
    // Whether or not this block has been closed. Close() is thread-safe, so
    // this must be an atomic primitive.
    std::atomic_bool _closed;

    S3ReadableBlock(const S3ReadableBlock&) = delete;
    const S3ReadableBlock& operator=(const S3ReadableBlock&) = delete;
};

S3ReadableBlock::S3ReadableBlock(S3BlockManager* block_manager, const std::string& path,
                                     std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle)
        : _block_manager(block_manager), _path(path), _file_handle(std::move(file_handle)), _closed(false) {
    if (_block_manager->_metrics) {
        _block_manager->_metrics->blocks_open_reading->increment(1);
        _block_manager->_metrics->total_readable_blocks->increment(1);
    }
    _file = _file_handle->file();
}

S3ReadableBlock::~S3ReadableBlock() {
    WARN_IF_ERROR(close(), strings::Substitute("Failed to close block $0", _path));
}

Status S3ReadableBlock::close() {
    bool expected = false;
    if (_closed.compare_exchange_strong(expected, true)) {
        _file_handle.reset();
        if (_block_manager->_metrics) {
            _block_manager->_metrics->blocks_open_reading->increment(-1);
        }
    }

    return Status::OK();
}

BlockManager* S3ReadableBlock::block_manager() const {
    return _block_manager;
}

const BlockId& S3ReadableBlock::id() const {
    CHECK(false) << "Not support Block.id(). (TODO)";
    return _block_id;
}

const string& S3ReadableBlock::path() const {
    return _path;
}

Status S3ReadableBlock::size(uint64_t* sz) const {
    DCHECK(!_closed.load());

    RETURN_IF_ERROR(_file->size(sz));
    return Status::OK();
}

Status S3ReadableBlock::read(uint64_t offset, Slice result) const {
    return readv(offset, &result, 1);
}

Status S3ReadableBlock::readv(uint64_t offset, const Slice* results, size_t res_cnt) const {
    DCHECK(!_closed.load());

    RETURN_IF_ERROR(_file->readv_at(offset, results, res_cnt));

    if (_block_manager->_metrics) {
        // Calculate the read amount of data
        size_t bytes_read = accumulate(results, results + res_cnt, static_cast<size_t>(0),
                                       [&](int sum, const Slice& curr) { return sum + curr.size; });
        _block_manager->_metrics->total_bytes_read->increment(bytes_read);
    }

    return Status::OK();
}

} // namespace internal

S3BlockManager::S3BlockManager(Env* env, BlockManagerOptions opts)
        : _env(DCHECK_NOTNULL(env)), _opts(std::move(opts)) {
    if (_opts.enable_metric) {
        _metrics = std::make_unique<internal::BlockManagerMetrics>();
    }

#ifdef BE_TEST
    _file_cache.reset(new FileCache<RandomAccessFile>("Readable file cache", config::file_descriptor_cache_capacity));
#else
    _file_cache = std::make_unique<FileCache<RandomAccessFile>>("Readable file cache",
                                                                StorageEngine::instance()->file_cache());
#endif
}

S3BlockManager::~S3BlockManager() = default;

Status S3BlockManager::open() {
    return Status::NotSupported("to be implemented. (TODO)");
}

Status S3BlockManager::create_block(const CreateBlockOptions& opts, std::unique_ptr<WritableBlock>* block) {
    CHECK(!_opts.read_only);

    shared_ptr<WritableFile> writer;
    WritableFileOptions wr_opts;
    wr_opts.mode = opts.mode;
    RETURN_IF_ERROR(env_util::open_file_for_write(wr_opts, _env, opts.path, &writer));

    VLOG(1) << "Creating new block at " << opts.path;
    *block = std::make_unique<internal::S3WritableBlock>(this, opts.path, writer);
    return Status::OK();
}

Status S3BlockManager::open_block(const std::string& path, std::unique_ptr<ReadableBlock>* block) {
    VLOG(1) << "Opening block with path at " << path;
    std::shared_ptr<OpenedFileHandle<RandomAccessFile>> file_handle(new OpenedFileHandle<RandomAccessFile>());
    bool found = _file_cache->lookup(path, file_handle.get());
    if (!found) {
        std::unique_ptr<RandomAccessFile> file;
        RETURN_IF_ERROR(_env->new_random_access_file(path, &file));
        _file_cache->insert(path, file.release(), file_handle.get());
    }

    *block = std::make_unique<internal::S3ReadableBlock>(this, path, file_handle);
    return Status::OK();
}

Status S3BlockManager::get_all_block_ids(std::vector<BlockId>* block_ids) {
    return Status::OK();
};

Status S3BlockManager::_delete_block(const string& path) {
    CHECK(!_opts.read_only);

    RETURN_IF_ERROR(_env->delete_file(path));
    return Status::OK();
}

BlockManager* S3BlockManager::get_block_mgr() {
    static BlockManagerOptions opts;
    static S3BlockManager s3_block_manager(S3Env::get_s3_env(), opts);
    return &s3_block_manager;
}

} // namespace starrocks::fs
