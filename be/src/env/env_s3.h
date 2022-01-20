// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#pragma once

#include <aws/core/Aws.h>

#include <atomic>

#include "common/status.h"
#include "env/env.h"
#include "util/slice.h"

namespace starrocks {

class S3Client;

// thread safe
class S3RandomAccessFile : public RandomAccessFile {
public:
    S3RandomAccessFile(S3Client* client, const std::string& bucket_name, const std::string& object_key);

    virtual ~S3RandomAccessFile() override;

    virtual Status read(uint64_t offset, Slice* res) const override;

    virtual Status read_at(uint64_t offset, const Slice& res) const override;

    virtual Status readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const override;

    virtual Status size(uint64_t* size) const override;

    virtual const std::string& file_name() const override;

private:
    Status _get_size();

    S3Client* _client;
    std::string _bucket_name;
    std::string _object_key;
    std::atomic<size_t> _size; // used to cache object size
};

// not thread safe
class S3SequentialFile : public SequentialFile {
public:
    S3SequentialFile(S3Client* client, const std::string& bucket_name, const std::string& object_key);

    virtual ~S3SequentialFile() override;

    virtual Status read(Slice* result) override;

    virtual Status skip(uint64_t n) override;

    const std::string& filename() const override;

private:
    Status _get_size();

    S3Client* _client;
    std::string _bucket_name;
    std::string _object_key;
    size_t _offset;            // current offset in the object
    std::atomic<size_t> _size; // used to cache object size
};

// s3 writable file with a buffer and multi-upload optimization
class S3WritableFile : public WritableFile {
public:
    S3WritableFile(S3Client* client, const std::string& bucket_name, const std::string& object_key,
                   size_t singlepart_upload_size = SinglepartUploadSize);

    virtual ~S3WritableFile() override;

    virtual Status append(const Slice& data) override;

    virtual Status appendv(const Slice* data, size_t cnt) override;

    virtual Status pre_allocate(uint64_t size) override;

    virtual Status close() override;

    virtual Status flush(FlushMode mode) override;

    virtual Status sync() override;

    uint64_t size() const override;

    const std::string& filename() const override;

private:
    // an experienced value that preserve a better upload performance while saves upload cost
    const static size_t SinglepartUploadSize = 32 * 1024 * 1024; // 32MB

    void _alloc_buffer();

    S3Client* _client;
    std::string _bucket_name;
    std::string _object_key;
    uint64_t _size; // current object size
    std::shared_ptr<Aws::StringStream>
            _buffer; // buffer to accumulate write data, do not use std::string to save one memory copy when upload
    size_t _last_part_size;              // current used buffer size
    size_t _singlepart_upload_size;      // buffer size for single upload
    Aws::String _multipart_upload_id;    // id used to identify multipart upload
    std::vector<Aws::String> _part_tags; // etag for every upload part
};

// class S3RandomRWFile; not needed in s3, so do not implement it.

class S3Env : public Env {
public:
    static S3Env* get_s3_env();

    S3Env();
    virtual ~S3Env() override;

    // object format: "bucket_name" + ":" + "object_key"
    virtual Status new_sequential_file(const std::string& object, std::unique_ptr<SequentialFile>* result) override;

    virtual Status new_random_access_file(const std::string& object, std::unique_ptr<RandomAccessFile>* result) override;

    virtual Status new_random_access_file(const RandomAccessFileOptions& opts, const std::string& object, std::unique_ptr<RandomAccessFile>* result) override;

    virtual Status new_writable_file(const std::string& object, std::unique_ptr<WritableFile>* result) override;

    virtual Status new_writable_file(const WritableFileOptions& opts, const std::string& object, std::unique_ptr<WritableFile>* result) override;

    virtual Status new_random_rw_file(const std::string& object, std::unique_ptr<RandomRWFile>* result) override;

    virtual Status new_random_rw_file(const RandomRWFileOptions& opts, const std::string& object, std::unique_ptr<RandomRWFile>* result) override;

    virtual Status path_exists(const std::string& object_key) override;

    virtual Status get_children(const std::string& dirname, std::vector<std::string>* result) override;

    virtual Status iterate_dir(const std::string& dirname, const std::function<bool(const char*)>& cb) override;

    virtual Status delete_file(const std::string& object) override;

    virtual Status create_dir(const std::string& dirname) override;

    virtual Status create_dir_if_missing(const std::string& dirname, bool* created = nullptr) override;

    virtual Status delete_dir(const std::string& dirname) override;

    virtual Status sync_dir(const std::string& dirname) override;

    virtual Status is_directory(const std::string& path, bool* is_dir) override;

    virtual Status canonicalize(const std::string& path, std::string* result) override;

    virtual Status get_file_size(const std::string& fname, uint64_t* size) override;

    virtual Status get_file_modified_time(const std::string& fname, uint64_t* file_mtime) override;

    virtual Status rename_file(const std::string& src, const std::string& target) override;

    virtual Status link_file(const std::string& old_path, const std::string& new_path) override;


private:
    S3Client*   _client;
    Aws::SDKOptions _options;
};


} // namespace starrocks
