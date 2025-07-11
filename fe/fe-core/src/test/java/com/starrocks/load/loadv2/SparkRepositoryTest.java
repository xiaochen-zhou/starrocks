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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.starrocks.analysis.BrokerDesc;
import com.starrocks.common.Config;
import com.starrocks.common.LoadException;
import com.starrocks.common.StarRocksException;
import com.starrocks.common.util.BrokerUtil;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.thrift.TBrokerFileStatus;
import mockit.Expectations;
import mockit.Mock;
import mockit.MockUp;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;

public class SparkRepositoryTest {

    private static final String DPP_LOCAL_MD5SUM = "b3cd0ae3a4121e2426532484442e90ec";
    private static final String SPARK_LOCAL_MD5SUM = "6d2b052ffbdf7082c019bd202432739c";
    private static final String DPP_VERSION = Config.spark_dpp_version;
    private static final String SPARK_LOAD_WORK_DIR = "hdfs://127.0.0.1/99999/user/starrocks/etl";
    private static final String DPP_NAME = SparkRepository.SPARK_DPP + ".jar";
    private static final String SPARK_NAME = SparkRepository.SPARK_2X + ".zip";

    private String remoteRepoPath;
    private String remoteArchivePath;
    private String remoteDppLibraryPath;
    private String remoteSparkLibraryPath;

    private List<TBrokerFileStatus> files;

    @Mocked
    GlobalStateMgr globalStateMgr;
    @Mocked
    BrokerUtil brokerUtil;

    @BeforeEach
    public void setUp() {
        // e.g. hdfs://127.0.0.1/99999/user/starrocks/etl/__spark_repository__
        remoteRepoPath = SPARK_LOAD_WORK_DIR + "/" + SparkRepository.REPOSITORY_DIR;
        // e.g. hdfs://127.0.0.1/99999/user/starrocks/etl/__spark_repository__/__archive_1_0_0
        remoteArchivePath = remoteRepoPath + "/" + SparkRepository.PREFIX_ARCHIVE + DPP_VERSION;
        // e.g. hdfs://127.0.0.1/99999/user/starrocks/etl/__spark_repository__/__archive_1_0_0/__lib_b3cd0ae3a4121e2426532484442e90ec_spark-dpp.jar
        remoteDppLibraryPath = remoteArchivePath + "/" + SparkRepository.PREFIX_LIB + DPP_LOCAL_MD5SUM + "_" + DPP_NAME;
        // e.g. hdfs://127.0.0.1/99999/user/starrocks/etl/__spark_repository__/__archive_1_0_0/__lib_6d2b052ffbdf7082c019bd202432739c_spark-2x.zip
        remoteSparkLibraryPath =
                remoteArchivePath + "/" + SparkRepository.PREFIX_LIB + SPARK_LOCAL_MD5SUM + "_" + SPARK_NAME;

        files = Lists.newArrayList();
        files.add(new TBrokerFileStatus(remoteDppLibraryPath, false, 1024, false));
        files.add(new TBrokerFileStatus(remoteSparkLibraryPath, false, 10240, false));
    }

    @Test
    public void testNormal() {

        new MockUp<BrokerUtil>() {
            @Mock
            boolean checkPathExist(String remotePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return true;
            }

            @Mock
            void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
                    throws StarRocksException {
                fileStatuses.addAll(files);
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker", Maps.newHashMap());
        SparkRepository repository = new SparkRepository(remoteRepoPath, brokerDesc);
        try {
            new Expectations(repository) {
                {
                    repository.getMd5String(anyString);
                    returns(DPP_LOCAL_MD5SUM, SPARK_LOCAL_MD5SUM);
                }
            };

            // prepare repository
            repository.prepare();

            // get archive
            SparkRepository.SparkArchive archive = repository.getCurrentArchive();
            Assertions.assertEquals(archive.libraries.size(), 2);

            // check if the remote libraries are equal to local libraries
            List<SparkRepository.SparkLibrary> libraries = archive.libraries;
            for (SparkRepository.SparkLibrary library : libraries) {
                switch (library.libType) {
                    case DPP:
                        Assertions.assertEquals(library.remotePath, remoteDppLibraryPath);
                        Assertions.assertEquals(library.md5sum, DPP_LOCAL_MD5SUM);
                        Assertions.assertEquals(library.size, 1024);
                        break;
                    case SPARK2X:
                        Assertions.assertEquals(library.remotePath, remoteSparkLibraryPath);
                        Assertions.assertEquals(library.md5sum, SPARK_LOCAL_MD5SUM);
                        Assertions.assertEquals(library.size, 10240);
                        break;
                    default:
                        Assertions.fail("wrong library type: " + library.libType);
                }
            }
        } catch (Exception e) {
            Assertions.fail();
        }
    }

    @Test
    public void testArchiveNotExists() {
        new MockUp<BrokerUtil>() {
            @Mock
            boolean checkPathExist(String remotePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return false;
            }

            @Mock
            void writeFile(String srcFilePath, String destFilePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return;
            }

            @Mock
            void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return;
            }
        };

        BrokerDesc brokerDesc = new BrokerDesc("broker", Maps.newHashMap());
        SparkRepository repository = new SparkRepository(remoteRepoPath, brokerDesc);
        try {
            new Expectations(repository) {
                {
                    repository.getMd5String(anyString);
                    returns(DPP_LOCAL_MD5SUM, SPARK_LOCAL_MD5SUM);

                    repository.getFileSize(anyString);
                    returns(1024L, 10240L);
                }
            };

            // prepare repository
            repository.prepare();

            // get archive
            SparkRepository.SparkArchive archive = repository.getCurrentArchive();
            Assertions.assertEquals(archive.libraries.size(), 2);

            // check if the remote libraries are equal to local libraries
            List<SparkRepository.SparkLibrary> libraries = archive.libraries;
            for (SparkRepository.SparkLibrary library : libraries) {
                switch (library.libType) {
                    case DPP:
                        Assertions.assertEquals(library.remotePath, remoteDppLibraryPath);
                        Assertions.assertEquals(library.md5sum, DPP_LOCAL_MD5SUM);
                        Assertions.assertEquals(library.size, 1024);
                        break;
                    case SPARK2X:
                        Assertions.assertEquals(library.remotePath, remoteSparkLibraryPath);
                        Assertions.assertEquals(library.md5sum, SPARK_LOCAL_MD5SUM);
                        Assertions.assertEquals(library.size, 10240);
                        break;
                    default:
                        Assertions.fail("wrong library type: " + library.libType);
                }
            }
        } catch (LoadException e) {
            Assertions.fail();
        }
    }

    @Test
    public void testLibraryMd5MissMatch() {
        new MockUp<BrokerUtil>() {
            @Mock
            boolean checkPathExist(String remotePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return true;
            }

            @Mock
            void parseFile(String path, BrokerDesc brokerDesc, List<TBrokerFileStatus> fileStatuses)
                    throws StarRocksException {
                fileStatuses.addAll(files);
            }

            @Mock
            void deletePath(String path, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return;
            }

            @Mock
            void writeFile(String srcFilePath, String destFilePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return;
            }

            @Mock
            void rename(String origFilePath, String destFilePath, BrokerDesc brokerDesc)
                    throws StarRocksException {
                return;
            }
        };

        // new md5dum of local library
        String newMd5sum = "new_local_md5sum_value";
        // new remote path
        String newRemoteDppPath = remoteArchivePath + "/" + SparkRepository.PREFIX_LIB + newMd5sum + "_" + DPP_NAME;
        String newRemoteSparkPath = remoteArchivePath + "/" + SparkRepository.PREFIX_LIB + newMd5sum + "_" + SPARK_NAME;

        BrokerDesc brokerDesc = new BrokerDesc("broker", Maps.newHashMap());
        SparkRepository repository = new SparkRepository(remoteRepoPath, brokerDesc);
        try {
            new Expectations(repository) {
                {
                    repository.getMd5String(anyString);
                    result = newMd5sum;

                    repository.getFileSize(anyString);
                    returns(1024L, 10240L);
                }
            };

            // prepare repository
            repository.prepare();

            // get archive
            SparkRepository.SparkArchive archive = repository.getCurrentArchive();
            Assertions.assertEquals(archive.libraries.size(), 2);

            // check if the remote libraries are equal to local libraries
            List<SparkRepository.SparkLibrary> libraries = archive.libraries;
            for (SparkRepository.SparkLibrary library : libraries) {
                switch (library.libType) {
                    case DPP:
                        Assertions.assertEquals(library.remotePath, newRemoteDppPath);
                        Assertions.assertEquals(library.md5sum, newMd5sum);
                        Assertions.assertEquals(library.size, 1024);
                        break;
                    case SPARK2X:
                        Assertions.assertEquals(library.remotePath, newRemoteSparkPath);
                        Assertions.assertEquals(library.md5sum, newMd5sum);
                        Assertions.assertEquals(library.size, 10240);
                        break;
                    default:
                        Assertions.fail("wrong library type: " + library.libType);
                }
            }
        } catch (LoadException e) {
            Assertions.fail();
        }
    }

}
