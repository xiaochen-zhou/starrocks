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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/mysql_result_writer.h

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

#pragma once

#include "runtime/buffer_control_result_writer.h"

namespace starrocks {

class ExprContext;
class MysqlRowBuffer;
class BufferControlBlock;
class RuntimeProfile;
using TFetchDataResultPtr = std::unique_ptr<TFetchDataResult>;
using TFetchDataResultPtrs = std::vector<TFetchDataResultPtr>;
// convert the row batch to mysql protocol row
class MysqlResultWriter final : public BufferControlResultWriter {
public:
    MysqlResultWriter(BufferControlBlock* sinker, const std::vector<ExprContext*>& output_expr_ctxs,
                      bool is_binary_format, RuntimeProfile* parent_profile);

    ~MysqlResultWriter() override;

    Status init(RuntimeState* state) override;

    Status append_chunk(Chunk* chunk) override;

    StatusOr<TFetchDataResultPtrs> process_chunk(Chunk* chunk) override;

private:
    // this function is only used in non-pipeline engine
    StatusOr<TFetchDataResultPtr> _process_chunk(Chunk* chunk);

    const std::vector<ExprContext*>& _output_expr_ctxs;
    MysqlRowBuffer* _row_buffer;
    bool _is_binary_format;

    const size_t _max_row_buffer_size = 1024 * 1024 * 1024;
};

} // namespace starrocks
