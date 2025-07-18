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

// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/fe/fe-core/src/test/java/org/apache/doris/load/routineload/RoutineLoadSchedulerTest.java

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

package com.starrocks.load.routineload;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.common.DdlException;
import com.starrocks.common.LoadException;
import com.starrocks.common.MetaNotFoundException;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.jmockit.Deencapsulation;
import com.starrocks.load.RoutineLoadDesc;
import com.starrocks.planner.StreamLoadPlanner;
import com.starrocks.qe.ConnectContext;
import com.starrocks.server.GlobalStateMgr;
import com.starrocks.system.SystemInfoService;
import com.starrocks.thrift.TResourceInfo;
import mockit.Expectations;
import mockit.Injectable;
import mockit.Mocked;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;

public class RoutineLoadSchedulerTest {

    @Mocked
    ConnectContext connectContext;
    @Mocked
    TResourceInfo tResourceInfo;

    @Test
    public void testNormalRunOneCycle(@Mocked GlobalStateMgr globalStateMgr,
                                      @Injectable RoutineLoadMgr routineLoadManager,
                                      @Injectable SystemInfoService systemInfoService,
                                      @Injectable Database database,
                                      @Injectable RoutineLoadDesc routineLoadDesc,
                                      @Mocked StreamLoadPlanner planner,
                                      @Injectable OlapTable olapTable)
            throws LoadException, MetaNotFoundException {
        List<Long> beIds = Lists.newArrayList();
        beIds.add(1L);
        beIds.add(2L);

        List<Integer> partitions = Lists.newArrayList();
        partitions.add(100);
        partitions.add(200);
        partitions.add(300);

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        Deencapsulation.setField(globalStateMgr, "routineLoadTaskScheduler", routineLoadTaskScheduler);

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L,
                "xxx", "test");
        Deencapsulation.setField(kafkaRoutineLoadJob, "state", RoutineLoadJob.JobState.NEED_SCHEDULE);
        List<RoutineLoadJob> routineLoadJobList = new ArrayList<>();
        routineLoadJobList.add(kafkaRoutineLoadJob);

        Deencapsulation.setField(kafkaRoutineLoadJob, "customKafkaPartitions", partitions);
        Deencapsulation.setField(kafkaRoutineLoadJob, "desireTaskConcurrentNum", 3);

        new Expectations() {
            {
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;
                routineLoadManager.getRoutineLoadJobByState(Sets.newHashSet(RoutineLoadJob.JobState.NEED_SCHEDULE));
                minTimes = 0;
                result = routineLoadJobList;
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = beIds;
                routineLoadManager.getSizeOfIdToRoutineLoadTask();
                minTimes = 0;
                result = 1;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();
        Deencapsulation.setField(routineLoadScheduler, "routineLoadManager", routineLoadManager);
        routineLoadScheduler.runAfterCatalogReady();

        List<RoutineLoadTaskInfo> routineLoadTaskInfoList =
                Deencapsulation.getField(kafkaRoutineLoadJob, "routineLoadTaskInfoList");
        for (RoutineLoadTaskInfo routineLoadTaskInfo : routineLoadTaskInfoList) {
            KafkaTaskInfo kafkaTaskInfo = (KafkaTaskInfo) routineLoadTaskInfo;
            if (kafkaTaskInfo.getPartitions().size() == 2) {
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(100));
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(300));
            } else {
                Assertions.assertTrue(kafkaTaskInfo.getPartitions().contains(200));
            }
        }
    }

    @Test
    public void testEmptyTaskQueue(@Injectable RoutineLoadMgr routineLoadManager) {
        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler(routineLoadManager);
        new Expectations() {
            {
                routineLoadManager.getClusterIdleSlotNum();
                result = 1;
                times = 1;
            }
        };
        routineLoadTaskScheduler.runAfterCatalogReady();
    }

    public void functionTest(@Mocked GlobalStateMgr globalStateMgr,
                             @Mocked SystemInfoService systemInfoService,
                             @Injectable Database database) throws DdlException, InterruptedException {
        new Expectations() {
            {
                minTimes = 0;
                result = tResourceInfo;
            }
        };

        KafkaRoutineLoadJob kafkaRoutineLoadJob = new KafkaRoutineLoadJob(1L, "test", 1L, 1L,
                "10.74.167.16:8092", "test");
        RoutineLoadMgr routineLoadManager = new RoutineLoadMgr();
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob, "db");

        List<Long> backendIds = new ArrayList<>();
        backendIds.add(1L);

        new Expectations() {
            {
                globalStateMgr.getRoutineLoadMgr();
                minTimes = 0;
                result = routineLoadManager;
                globalStateMgr.getLocalMetastore().getDb(anyLong);
                minTimes = 0;
                result = database;
                systemInfoService.getBackendIds(true);
                minTimes = 0;
                result = backendIds;
            }
        };

        RoutineLoadScheduler routineLoadScheduler = new RoutineLoadScheduler();

        RoutineLoadTaskScheduler routineLoadTaskScheduler = new RoutineLoadTaskScheduler();
        routineLoadTaskScheduler.setInterval(5000);

        ExecutorService executorService =
                ThreadPoolManager.newDaemonFixedThreadPool(2, 2, "routine-load-task-scheduler", false);
        executorService.submit(routineLoadScheduler);
        executorService.submit(routineLoadTaskScheduler);

        KafkaRoutineLoadJob kafkaRoutineLoadJob1 = new KafkaRoutineLoadJob(1L, "test_custom_partition",
                 1L, 1L, "xxx", "test_1");
        List<Integer> customKafkaPartitions = new ArrayList<>();
        customKafkaPartitions.add(2);
        Deencapsulation.setField(kafkaRoutineLoadJob1, "customKafkaPartitions", customKafkaPartitions);
        routineLoadManager.addRoutineLoadJob(kafkaRoutineLoadJob1, "db");

        Thread.sleep(10000);
    }
}
