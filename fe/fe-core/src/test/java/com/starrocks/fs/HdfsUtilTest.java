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

package com.starrocks.fs;

import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.StarRocksException;
import com.starrocks.fs.hdfs.HdfsFs;
import com.starrocks.fs.hdfs.HdfsFsManager;
import com.starrocks.thrift.THdfsProperties;
import mockit.Mock;
import mockit.MockUp;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HdfsUtilTest {
    @Test
    public void testColumnFromPath() {
        try {
            // normal case
            List<String> columns = HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key2=val2/key3=val3/*", Arrays.asList("key3", "key2", "key1"));
            Assertions.assertEquals(3, columns.size());
            Assertions.assertEquals("val3", columns.get(0));
            Assertions.assertEquals("val2", columns.get(1));
            Assertions.assertEquals("val1", columns.get(2));

            // invalid path
            Assertions.assertThrows(StarRocksException.class, () ->
                    HdfsUtil.parseColumnsFromPath("invalid_path", Arrays.asList("key3", "key2", "key1")));

            // missing key of columns from path
            Assertions.assertThrows(StarRocksException.class, () ->
                    HdfsUtil.parseColumnsFromPath("hdfs://key1=val1/some_path/key3=val3/*", Arrays.asList("key3", "key2", "key1")));
        } catch (StarRocksException e) {
            Assertions.fail(e.getMessage());
        }
    }

    @Test
    public void testException() {
        new MockUp<HdfsFsManager>() {
            @Mock
            public HdfsFs getFileSystem(String path, Map<String, String> loadProperties, THdfsProperties tProperties)
                                        throws StarRocksException {
                return null;
            }
        };

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.deletePath("hdfs://abc/dbf", new BrokerDesc(new HashMap<>())));

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.rename("hdfs://abc/dbf", "hdfs://abc/dba", new BrokerDesc(new HashMap<>()), 1000));

        Assertions.assertThrows(StarRocksException.class, () ->
                HdfsUtil.checkPathExist("hdfs://abc/dbf", new BrokerDesc(new HashMap<>())));

        HdfsFsManager fileSystemManager = new HdfsFsManager();
        Assertions.assertThrows(StarRocksException.class, () ->
                fileSystemManager.openReader("hdfs://abc/dbf", 0, new HashMap<>()));

        Assertions.assertThrows(StarRocksException.class, () ->
                fileSystemManager.openWriter("hdfs://abc/dbf", new HashMap<>()));
    }
}
