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
//

package com.starrocks.load.loadv2.dpp;

import com.starrocks.load.loadv2.etl.EtlJobConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

public class MinimumCoverageRollupTreeBuilderTest {

    @Test
    public void testBuild() {
        EtlJobConfig.EtlColumn column1 = new EtlJobConfig.EtlColumn(
                "column1", "INT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        EtlJobConfig.EtlColumn column2 = new EtlJobConfig.EtlColumn(
                "column2", "SMALLINT",
                true, true,
                "NONE", "0",
                0, 0, 0);
        EtlJobConfig.EtlColumn column3 = new EtlJobConfig.EtlColumn(
                "column3", "VARCHAR",
                true, true,
                "NONE", "",
                0, 0, 0);
        EtlJobConfig.EtlColumn column4 = new EtlJobConfig.EtlColumn(
                "column4", "INT",
                true, false,
                "SUM", "",
                0, 0, 0);
        List<EtlJobConfig.EtlColumn> baseColumns = new ArrayList<>();
        baseColumns.add(column1);
        baseColumns.add(column2);
        baseColumns.add(column3);
        baseColumns.add(column4);
        EtlJobConfig.EtlIndex baseIndex = new EtlJobConfig.EtlIndex(10000,
                baseColumns, 12345, "DUPLICATE", true);
        List<EtlJobConfig.EtlColumn> roll1Columns = new ArrayList<>();
        roll1Columns.add(column1);
        roll1Columns.add(column2);
        roll1Columns.add(column4);
        EtlJobConfig.EtlIndex roll1Index = new EtlJobConfig.EtlIndex(10001,
                roll1Columns, 12346, "AGGREGATE", false);
        List<EtlJobConfig.EtlColumn> roll2Columns = new ArrayList<>();
        roll2Columns.add(column1);
        roll2Columns.add(column4);
        EtlJobConfig.EtlIndex roll2Index = new EtlJobConfig.EtlIndex(10002,
                roll2Columns, 12347, "AGGREGATE", false);

        List<EtlJobConfig.EtlColumn> roll3Columns = new ArrayList<>();
        roll3Columns.add(column3);
        roll3Columns.add(column4);
        EtlJobConfig.EtlIndex roll3Index = new EtlJobConfig.EtlIndex(10003,
                roll3Columns, 12348, "AGGREGATE", false);

        List<EtlJobConfig.EtlColumn> roll4Columns = new ArrayList<>();
        roll4Columns.add(column1);
        EtlJobConfig.EtlColumn newColumn2 = new EtlJobConfig.EtlColumn(
                "column2", "SMALLINT",
                true, false,
                "SUM", "0",
                0, 0, 0);
        roll4Columns.add(newColumn2);
        EtlJobConfig.EtlIndex roll4Index = new EtlJobConfig.EtlIndex(10004,
                roll4Columns, 12349, "AGGREGATE", false);

        List<EtlJobConfig.EtlIndex> indexes = new ArrayList<>();
        indexes.add(baseIndex);
        indexes.add(roll1Index);
        indexes.add(roll2Index);
        indexes.add(roll3Index);
        indexes.add(roll4Index);
        EtlJobConfig.EtlTable table = new EtlJobConfig.EtlTable(indexes, null);

        MinimumCoverageRollupTreeBuilder builder = new MinimumCoverageRollupTreeBuilder();
        RollupTreeNode resultNode = builder.build(table);
        Assertions.assertEquals(resultNode.parent, null);
        Assertions.assertEquals(resultNode.indexId, 10000);
        Assertions.assertEquals(resultNode.level, 0);
        Assertions.assertEquals(resultNode.children.size(), 2);

        RollupTreeNode index1Node = resultNode.children.get(0);
        Assertions.assertEquals(index1Node.parent.indexId, 10000);
        Assertions.assertEquals(index1Node.indexId, 10001);
        Assertions.assertEquals(index1Node.level, 1);
        Assertions.assertEquals(index1Node.children.size(), 2);

        RollupTreeNode index3Node = resultNode.children.get(1);
        Assertions.assertEquals(index3Node.parent.indexId, 10000);
        Assertions.assertEquals(index3Node.indexId, 10003);
        Assertions.assertEquals(index3Node.level, 1);
        Assertions.assertEquals(index3Node.children, null);

        RollupTreeNode index2Node = index1Node.children.get(0);
        Assertions.assertEquals(index2Node.parent.indexId, 10001);
        Assertions.assertEquals(index2Node.indexId, 10002);
        Assertions.assertEquals(index2Node.level, 2);
        Assertions.assertEquals(index2Node.children, null);

        RollupTreeNode index4Node = index1Node.children.get(1);
        Assertions.assertEquals(index4Node.parent.indexId, 10001);
        Assertions.assertEquals(index4Node.indexId, 10004);
        Assertions.assertEquals(index4Node.level, 2);
        Assertions.assertEquals(index4Node.children, null);
    }
}