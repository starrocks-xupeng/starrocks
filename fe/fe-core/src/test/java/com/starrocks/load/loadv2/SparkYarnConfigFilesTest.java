// Copyright 2021-present StarRocks, Inc. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


package com.starrocks.load.loadv2;

import com.google.common.collect.Maps;
import com.starrocks.common.LoadException;
import com.starrocks.server.GlobalStateMgr;
import mockit.Mocked;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.util.Map;

public class SparkYarnConfigFilesTest {
    private static final String RESOURCE_NAME = "spark0";
    private static final String SPARK_HADOOP_PREFIX = "spark.hadoop.";
    private static final String YARN_CONFIG_DIR = "./yarn_config";

    private Map<String, String> properties;

    @Mocked
    GlobalStateMgr globalStateMgr;

    @BeforeEach
    public void setUp() {
        properties = Maps.newHashMap();
        properties.put(SPARK_HADOOP_PREFIX + "hadoop.job.ugi", "test,test");
        properties.put(SPARK_HADOOP_PREFIX + "hadoop.security.authentication", "simple");
        properties.put(SPARK_HADOOP_PREFIX + "yarn.resourcemanager.address", "host:port");
        properties.put(SPARK_HADOOP_PREFIX + "yarn.resourcemanager.scheduler.address", "host:port");
    }

    @Test
    public void testNormal() {
        SparkYarnConfigFiles sparkYarnConfigFiles =
                new SparkYarnConfigFiles(RESOURCE_NAME, YARN_CONFIG_DIR, properties);
        try {
            // prepare config files
            sparkYarnConfigFiles.prepare();
            // get config files' parent directory
            String configDir = sparkYarnConfigFiles.getConfigDir();
            File dir = new File(configDir);
            File[] configFiles = dir.listFiles();
            Assertions.assertEquals(2, configFiles.length);
        } catch (LoadException e) {
            Assertions.fail();
        }
    }

    @AfterEach
    public void clear() {
        delete(YARN_CONFIG_DIR);
    }

    private void delete(String deletePath) {
        File file = new File(deletePath);
        if (!file.exists()) {
            return;
        }
        if (file.isFile()) {
            file.delete();
            return;
        }
        File[] files = file.listFiles();
        for (File file1 : files) {
            delete(file1.getAbsolutePath());
        }
        file.delete();
    }
}
