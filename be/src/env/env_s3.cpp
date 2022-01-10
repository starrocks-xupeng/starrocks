// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/env_s3.h"

#include <limits>

#include "env/s3_client.h"
#include "gutil/strings/substitute.h"

namespace starrocks {

S3RandomAccessFile::S3RandomAccessFile(S3Client* client, const std::string& bucket_name, const std::string& object_key)
        : _client(client), _bucket_name(bucket_name), _object_key(object_key), _size(0) {}

S3RandomAccessFile::~S3RandomAccessFile() {}

Status S3RandomAccessFile::read(uint64_t offset, Slice* res) const {
    size_t len = res->size;
    char* buf = res->data;
    size_t read_bytes = 0;

    Status status = _client->get_object_range(_bucket_name, _object_key, buf, offset, len, &read_bytes);

    // test if end of object, if so, return ok.
    // size_t object_size = 0;
    // if (!status.ok() && size(&object_size).ok() && ((offset + read_bytes) == object_size)) {
    if (!status.ok() && read_bytes && read_bytes < len) {
        res->size = read_bytes;
        return Status::OK();
    }

    return status;
}

Status S3RandomAccessFile::read_at(uint64_t offset, const Slice& res) const {
    size_t len = res.size;
    char* buf = res.data;
    size_t read_bytes = 0;

    return _client->get_object_range(_bucket_name, _object_key, buf, offset, len, &read_bytes);
}

Status S3RandomAccessFile::readv_at(uint64_t offset, const Slice* res, size_t res_cnt) const {
    for (size_t i = 0; i < res_cnt; ++i) {
        RETURN_IF_ERROR(read_at(offset, res[i]));
        offset += res[i].size;
    }

    return Status::OK();
}

Status S3RandomAccessFile::size(uint64_t* size) const {
    // this function is a bit tricky
    if (_size.load() != 0) {
        *size = _size.load();
        return Status::OK();
    }

    // `_size` will be set in `_get_size`
    Status status = const_cast<S3RandomAccessFile*>(this)->_get_size();
    if (status.ok()) {
        *size = _size.load();
    }
    return status;
}

// this function should only be called once in the lifetime of `S3RandomAccessFile`
Status S3RandomAccessFile::_get_size() {
    size_t size = 0;
    Status status = _client->get_object_size(_bucket_name, _object_key, &size);
    if (status.ok()) {
        _size.store(size);
    }
    return status;
}

const std::string& S3RandomAccessFile::file_name() const {
    return _object_key;
}

S3SequentialFile::S3SequentialFile(S3Client* client, const std::string& bucket_name, const std::string& object_key)
        : _client(client), _bucket_name(bucket_name), _object_key(object_key), _offset(0), _size(0) {}

S3SequentialFile::~S3SequentialFile() {}

Status S3SequentialFile::read(Slice* result) {
    size_t len = result->size;
    char* buf = result->data;
    size_t offset = _offset;
    size_t read_bytes = 0;

    Status status = _client->get_object_range(_bucket_name, _object_key, buf, offset, len, &read_bytes);

    if (read_bytes) {
        _offset += read_bytes;
    }

    // test if end of object, if so, return ok.
    // size_t object_size = 0;
    // if (!status.ok() && size(&object_size).ok() && ((offset + read_bytes) == object_size)) {
    if (!status.ok() && read_bytes && read_bytes < len) {
        result->size = read_bytes;
        return Status::OK();
    }

    return status;
}

Status S3SequentialFile::skip(uint64_t n) {
    _offset += n; // it's ok to pass beyond object size
    return Status::OK();
}

Status S3SequentialFile::_get_size() {
    if (_size.load() != 0) {
        return Status::OK();
    }

    size_t size = 0;
    Status status = _client->get_object_size(_bucket_name, _object_key, &size);
    if (status.ok()) {
        _size.store(size);
    }
    return status;
}

const std::string& S3SequentialFile::filename() const {
    return _object_key;
}

S3WritableFile::S3WritableFile(S3Client* client, const std::string& bucket_name, const std::string& object_key,
                               size_t singlepart_upload_size)
        : _client(client),
          _bucket_name(bucket_name),
          _object_key(object_key),
          _size(0),
          _singlepart_upload_size(singlepart_upload_size) {
    _alloc_buffer();
}

S3WritableFile::~S3WritableFile() {}

Status S3WritableFile::append(const Slice& data) {
    // write data to buffer first
    _buffer->write(data.data, data.size);
    if (!_buffer->good()) {
        return Status::IOError(strings::Substitute("S3WritableFile append failed for object $0.", _object_key));
    }

    _last_part_size += data.size;

    // data size exceeds singlepart upload threshold, need to use multipart upload.
    if (_multipart_upload_id.empty() && _last_part_size >= _singlepart_upload_size) {
        RETURN_IF_ERROR(_client->create_multipart_upload(_bucket_name, _object_key, &_multipart_upload_id));
    }

    if (_multipart_upload_id.size() && _last_part_size >= _singlepart_upload_size) {
        RETURN_IF_ERROR(_client->upload_part(_bucket_name, _object_key, _multipart_upload_id, _buffer, &_part_tags));
        _alloc_buffer();
    }

    _size += data.size;
    return Status::OK();
}

Status S3WritableFile::appendv(const Slice* data, size_t cnt) {
    for (size_t i = 0; i < cnt; ++i) {
        RETURN_IF_ERROR(append(data[i]));
    }

    return Status::OK();
}

Status S3WritableFile::pre_allocate(uint64_t size) {
    // do nothing
    return Status::OK();
}

Status S3WritableFile::close() {
    if (_multipart_upload_id.empty()) { // single upload
        RETURN_IF_ERROR(_client->put_string_object(_bucket_name, _object_key, _buffer->str()));
    } else {
        if (_buffer->tellp()) { // finish last part
            RETURN_IF_ERROR(_client->upload_part(_bucket_name, _object_key, _multipart_upload_id, _buffer, &_part_tags));
        }
        // complete multipart upload
        RETURN_IF_ERROR(_client->complete_multipart_upload(_bucket_name, _object_key, _multipart_upload_id, _part_tags));
    }
    return Status::OK();
}

Status S3WritableFile::flush(FlushMode mode) {
    // do nothing
    return Status::OK();
}

Status S3WritableFile::sync() {
    // do nothing
    return Status::OK();
}

uint64_t S3WritableFile::size() const {
    return _size;
}

const std::string& S3WritableFile::filename() const {
    return _object_key;
}

void S3WritableFile::_alloc_buffer() {
    _buffer = Aws::MakeShared<Aws::StringStream>("temporary buffer");
    _last_part_size = 0;
}

S3Env::S3Env() {
    // TODO: make sure call this once only
    Aws::InitAPI(_options);

    _client = S3Client::get_s3_client();

S3Env::~S3Env() {
    Aws::ShutdownAPI(_options);
}

static Status get_object_info(const std::string &object, std::string* bucket_name, std::string* object_key) {
    std::string::size_type n = object.find(':');
    if (n == std::string::npos) {
        return Status::InvalidArgument(strings::Substitute("S3 object info error, $0.", object));
    }

    *bucket_name = object.substr(0, n);
    *object_key = object.substr(n + 1);
    if (bucket_name->empty() || object_key->empty()) {
        return Status::InvalidArgument(strings::Substitute("S3 object info error, $0.", object));
    }

    return Status::OK();
}

Status S3Env::new_sequential_file(const std::string& object, std::unique_ptr<SequentialFile>* result) {
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(object, &bucket_name, &object_key));
    *result = std::make_unique<S3SequentialFile>(_client, bucket_name, object_key);
    return Status::OK();
}

Status S3Env::new_random_access_file(const std::string& object, std::unique_ptr<RandomAccessFile>* result) {
    return new_random_access_file(RandomAccessFileOptions(), object, result);
}

Status S3Env::new_random_access_file(const RandomAccessFileOptions& opts, const std::string& object, std::unique_ptr<RandomAccessFile>* result) {
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(object, &bucket_name, &object_key));
    *result = std::make_unique<S3RandomAccessFile>(_client, bucket_name, object_key);
    return Status::OK();
}

Status S3Env::new_writable_file(const std::string& object, std::unique_ptr<WritableFile>* result) {
    return new_writable_file(WritableFileOptions(), object, result);
}

Status S3Env::new_writable_file(const WritableFileOptions& opts, const std::string& object, std::unique_ptr<WritableFile>* result) {
    if (opts.mode == MUST_EXIST) {
        return Status::NotSupported("Open writable file with MUST_EXIST not supported by s3");
    }

    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(object, &bucket_name, &object_key));
    *result = std::make_unique<S3WritableFile>(_client, bucket_name, object_key);
    return Status::OK();
}

Status S3Env::new_random_rw_file(const std::string& object, std::unique_ptr<RandomRWFile>* result) {
    return new_random_rw_file(RandomRWFileOptions(), object, result);
}

Status S3Env::new_random_rw_file(const RandomRWFileOptions& opts, const std::string& object, std::unique_ptr<RandomRWFile>* result) {
    return Status::NotSupported("Open random rw file not supported by s3");
}

Status S3Env::path_exists(const std::string& object) {
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(object, &bucket_name, &object_key));
    return _client->exist_object(bucket_name, object_key);
}

Status S3Env::delete_file(const std::string& object) {
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(object, &bucket_name, &object_key));
    return _client->delete_object(bucket_name, object_key);
}

Status S3Env::create_dir(const std::string& dirname) {
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(dirname, &bucket_name, &object_key));

    Status status = _client->exist_object(bucket_name, object_key);
    if (status.ok()) {
        return Status::AlreadyExist(strings::Substitute("S3 object $0 already existed, can not create mocking directory.", object_key));
    } else if (!status.is_not_found()) { // io error
        return status;
    }

    // s3 does not have directory, just return ok
    return Status::OK();
}

Status S3Env::create_dir_if_missing(const std::string& dirname, bool* created) {
    RETURN_IF_ERROR(create_dir(dirname));

    // s3 does not have directory, just return ok
    if (created != nullptr) {
        *created = true;
    }
    return Status::OK();
}

Status S3Env::delete_dir(const std::string& dirname) {
    // s3 does not have directory, just return ok
    return Status::OK();
}

Status S3Env::sync_dir(const std::string& dirname) {
    // s3 does not have directory, just return ok
    return Status::OK();
}

Status S3Env::is_directory(const std::string& path, bool* is_dir) {
    *is_dir = false;
    // s3 does not have directory, just return ok
    return Status::OK();
}

Status S3Env::canonicalize(const std::string& path, std::string* result) {
    return Status::NotSupported("S3 does not support canonicalize");
}

Status S3Env::get_file_size(const std::string& fname, uint64_t* size) {
    size_t s = 0;
    std::string bucket_name;
    std::string object_key;
    RETURN_IF_ERROR(get_object_info(fname, &bucket_name, &object_key));

    RETURN_IF_ERROR(_client->get_object_size(bucket_name, object_key, &s));
    if (size != nullptr) {
        *size = (uint64_t)s;
    }

    return Status::OK();
}

Status S3Env::get_children(const std::string& dirname, std::vector<std::string>* result) {
    result->clear();

    std::string bucket_name;
    std::string object_prefix;
    RETURN_IF_ERROR(get_object_info(dirname, &bucket_name, &object_prefix));

    return _client->list_objects(bucket_name, object_prefix, result);
}

Status S3Env::iterate_dir(const std::string& dirname, const std::function<bool(const char*)>& cb) {
    return Status::NotSupported("S3 does not support iterator dir now");
}

Status S3Env::get_file_modified_time(const std::string& fname, uint64_t* file_mtime) {
    return Status::NotSupported("S3 does not support get file modified time now");
}

Status S3Env::rename_file(const std::string& src, const std::string& target) {
    return Status::NotSupported("S3 does not support rename");
}

Status S3Env::link_file(const std::string& old_path, const std::string& new_path) {
    return Status::NotSupported("S3 does not support link");
}

S3Env* S3Env::get_s3_env() {
    static S3Env s3_env;
    return &s3_env;
}

} // namespace starrocks
