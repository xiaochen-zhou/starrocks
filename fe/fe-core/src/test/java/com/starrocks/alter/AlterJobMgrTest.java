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

package com.starrocks.alter;

import com.starrocks.warehouse.cngroup.WarehouseComputeResource;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Map;

public class AlterJobMgrTest {

    @Test
    public void testGetRunningAlterJobCount() {
        SchemaChangeHandler schemaChangeHandler = new SchemaChangeHandler();
        MaterializedViewHandler materializedViewHandler = new MaterializedViewHandler();
        SystemHandler systemHandler = new SystemHandler();
        AlterJobMgr alterJobMgr = new AlterJobMgr(schemaChangeHandler, materializedViewHandler, systemHandler);

        // warehouse 0
        SchemaChangeJobV2 job1 = new SchemaChangeJobV2(1L, 20001, 30001, "test", 3600000);
        job1.setComputeResource(WarehouseComputeResource.of(0));
        job1.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job1);
        SchemaChangeJobV2 job2 = new SchemaChangeJobV2(2L, 20001, 30001, "test", 3600000);
        job2.setComputeResource(WarehouseComputeResource.of(0));
        job2.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job2);
        SchemaChangeJobV2 job3 = new SchemaChangeJobV2(3L, 20001, 30001, "test", 3600000);
        job3.setComputeResource(WarehouseComputeResource.of(0));
        job3.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job3);

        // warehouse 1
        SchemaChangeJobV2 job4 = new SchemaChangeJobV2(4L, 20001, 30001, "test", 3600000);
        job4.setComputeResource(WarehouseComputeResource.of(1));
        job4.setJobState(AlterJobV2.JobState.FINISHED);
        schemaChangeHandler.addAlterJobV2(job4);
        SchemaChangeJobV2 job5 = new SchemaChangeJobV2(5L, 20001, 30001, "test", 3600000);
        job5.setComputeResource(WarehouseComputeResource.of(1));
        job5.setJobState(AlterJobV2.JobState.CANCELLED);
        schemaChangeHandler.addAlterJobV2(job5);
        SchemaChangeJobV2 job6 = new SchemaChangeJobV2(6L, 20001, 30001, "test", 3600000);
        job6.setComputeResource(WarehouseComputeResource.of(1));
        job6.setJobState(AlterJobV2.JobState.RUNNING);
        schemaChangeHandler.addAlterJobV2(job6);
        RollupJobV2 job7 = buildRollupJob(7);
        job7.setComputeResource(WarehouseComputeResource.of(1));
        job7.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job7);
        RollupJobV2 job8 = buildRollupJob(8);
        job8.setComputeResource(WarehouseComputeResource.of(1));
        job8.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job8);
        RollupJobV2 job9 = buildRollupJob(9);
        job9.setComputeResource(WarehouseComputeResource.of(1));
        job9.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job9);

        // warehouse 2
        RollupJobV2 job10 = buildRollupJob(10);
        job10.setComputeResource(WarehouseComputeResource.of(2));
        job10.setJobState(AlterJobV2.JobState.FINISHED);
        materializedViewHandler.addAlterJobV2(job10);
        RollupJobV2 job11 = buildRollupJob(11);
        job11.setComputeResource(WarehouseComputeResource.of(2));
        job11.setJobState(AlterJobV2.JobState.CANCELLED);
        materializedViewHandler.addAlterJobV2(job11);
        RollupJobV2 job12 = buildRollupJob(12);
        job12.setComputeResource(WarehouseComputeResource.of(2));
        job12.setJobState(AlterJobV2.JobState.RUNNING);
        materializedViewHandler.addAlterJobV2(job12);

        Map<Long, Long> result = alterJobMgr.getRunningAlterJobCount();
        Assertions.assertEquals(3, result.size());
        Assertions.assertEquals((Long) 1L, result.get(0L));
        Assertions.assertEquals((Long) 2L, result.get(1L));
        Assertions.assertEquals((Long) 1L, result.get(2L));
    }

    private RollupJobV2 buildRollupJob(long id) {
        return new RollupJobV2(id, 20001, 30001, "test", 3600000,
                0, 0, "test", "test", 1,
                null, null, 1, 1, null, (short) 0,
                null, null, false);
    }
}
