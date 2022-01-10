// This file is licensed under the Elastic License 2.0. Copyright 2021 StarRocks Limited.

#include "env/s3_client.h"
#include "env/env_s3.h"

#include <aws/core/Aws.h>
#include <gtest/gtest.h>

#include <fstream>

#include "util/file_utils.h"
#include "util/slice.h"

namespace starrocks {

static const std::string bucket_name = "starrocks-cloud-test-2022";

class EnvS3Test : public testing::Test {
public:
    EnvS3Test() {}
    virtual ~EnvS3Test() {}
    void SetUp() override { Aws::InitAPI(_options); }
    void TearDown() override { Aws::ShutdownAPI(_options); }

private:
    Aws::SDKOptions _options;
};

TEST_F(EnvS3Test, S3RandomAccessFile) {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;

    S3Client client(config);

    // create bucket
    ASSERT_TRUE(client.create_bucket(bucket_name).ok());

    // put object
    const std::string object_key = "hello";
    const std::string object_value = "one two three";
    ASSERT_TRUE(client.put_string_object(bucket_name, object_key, object_value).ok());

    std::shared_ptr<RandomAccessFile> file = std::make_shared<S3RandomAccessFile>(&client, bucket_name, object_key);

    // test read
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read(0, &slice).ok()); // end of object will return ok
    }
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read_at(0, slice).is_io_error()); // end of object will not return ok
    }

    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 3);
    EXPECT_TRUE(file->read(4, &slice).ok());
    EXPECT_EQ(strncmp(slice.data, &object_value[4], 3), 0);
    }
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 3);
    EXPECT_TRUE(file->read_at(4, slice).ok());
    EXPECT_EQ(strncmp(slice.data, &object_value[4], 3), 0);
    }

    // test read past object size will get error
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read(100, &slice).is_io_error());
    }
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read_at(100, slice).is_io_error());
    }

    // test size
    size_t size = 0;
    EXPECT_TRUE(file->size(&size).ok());
    EXPECT_EQ(size, 13);

    ASSERT_TRUE(client.delete_object(bucket_name, object_key).ok());
    ASSERT_TRUE(client.delete_bucket(bucket_name).ok());
}

TEST_F(EnvS3Test, S3SequentialFile) {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;

    S3Client client(config);

    // create bucket
    ASSERT_TRUE(client.create_bucket(bucket_name).ok());

    // put object
    const std::string object_key = "hello";
    const std::string object_value = "one two three";
    ASSERT_TRUE(client.put_string_object(bucket_name, object_key, object_value).ok());

    std::shared_ptr<SequentialFile> file = std::make_shared<S3SequentialFile>(&client, bucket_name, object_key);

    // test read
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 3);
    EXPECT_TRUE(file->read(&slice).ok()); // end of object will return ok
    EXPECT_EQ(strncmp(slice.data, &object_value[0], 3), 0);
    }

    // test skip
    EXPECT_TRUE(file->skip(1).ok());

    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 3);
    EXPECT_TRUE(file->read(&slice).ok());
    EXPECT_EQ(strncmp(slice.data, &object_value[4], 3), 0);
    }

    EXPECT_TRUE(file->skip(1).ok());

    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read(&slice).ok()); // end of object will return ok
    EXPECT_EQ(slice.size, 5);
    EXPECT_EQ(strncmp(slice.data, &object_value[8], 5), 0);
    }

    // test read past object size will get error
    EXPECT_TRUE(file->skip(100).ok());
    {
    std::string str(1024, '\0');
    Slice slice(str.data(), 1024);
    EXPECT_TRUE(file->read(&slice).is_io_error());
    }

    ASSERT_TRUE(client.delete_object(bucket_name, object_key).ok());
    ASSERT_TRUE(client.delete_bucket(bucket_name).ok());
}

TEST_F(EnvS3Test, S3WritableFile) {
    Aws::Client::ClientConfiguration config;
    config.region = Aws::Region::AP_SOUTHEAST_1;

    S3Client client(config);

    // create bucket
    ASSERT_TRUE(client.create_bucket(bucket_name).ok());

    const std::string object_key = "hello";

    const std::string object_value1(5 * 1024 * 1024, '1'); // multipart upload object must be larger than 5MB
    const std::string object_value2 = "22";
    const std::string object_value3 = "33";
    const std::string object_value4 = "44";

    auto test_one = [&](std::shared_ptr<WritableFile> file) {
        Slice slice1(object_value1.data(), object_value1.size());
        // test append
        EXPECT_TRUE(file->append(slice1).ok());
        Slice slice2(object_value2.data(), object_value2.size());
        EXPECT_TRUE(file->append(slice2).ok());
        Slice slices[2];
        slices[0] = Slice(object_value3.data(), object_value3.size());
        slices[1] = Slice(object_value4.data(), object_value4.size());
        // test appendv
        EXPECT_TRUE(file->appendv(slices, 2).ok());
        EXPECT_EQ(file->size(), (uint64_t)5 * 1024 * 1024 + 6);
        EXPECT_TRUE(file->close().ok());
        EXPECT_TRUE(client.exist_object(bucket_name, object_key).ok());
        EXPECT_TRUE(client.delete_object(bucket_name, object_key).ok());
    };

    // test singlepart upload
    {
        std::shared_ptr<WritableFile> file = std::make_shared<S3WritableFile>(&client, bucket_name, object_key);
        test_one(file);
    }

    // test multipart upload
    {
        std::shared_ptr<WritableFile> file = std::make_shared<S3WritableFile>(&client, bucket_name, object_key, 5 * 1024 * 1024 /* singlepart_upload_size */);
        test_one(file);
    }

    ASSERT_TRUE(client.delete_bucket(bucket_name).ok());
}

} // namespace starrocks
