// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scan_node.h

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

#include "exec/scan_node.h"
#include "exec/vectorized/schema_scanner.h"
#include "gen_cpp/Descriptors_types.h"
#include "runtime/descriptors.h"

namespace starrocks {

class TextConverter;
class TupleDescriptor;
class RuntimeState;
class Status;

namespace vectorized {

class SchemaScanNode : public ScanNode {
public:
    SchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);
    ~SchemaScanNode();

    // Prepare conjuncts, create Schema columns to slots mapping
    // initialize _schema_scanner
    Status init(const TPlanNode& tnode, RuntimeState* state = nullptr) override;

    // Prepare conjuncts, create Schema columns to slots mapping
    // initialize _schema_scanner
    Status prepare(RuntimeState* state) override;

    // Start Schema scan using _schema_scanner.
    Status open(RuntimeState* state) override;

    // Fill the next row batch by calling next() on the _schema_scanner,
    Status get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) override;

    // Fill the next chunk by calling next() on the _schema_scanner,
    Status get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) override;

    // Close the _schema_scanner, and report errors.
    Status close(RuntimeState* state) override;

    // this is no use in this class
    Status set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) override;

private:
    // Write debug string of this into out.
    void debug_string(int indentation_level, std::stringstream* out) const override;

    bool _is_init;
    bool _is_finished = false;
    const std::string _table_name;
    SchemaScannerParam _scanner_param;
    // Tuple id resolved in prepare() to set _tuple_desc;
    TupleId _tuple_id;

    // Descriptor of dest tuples
    const TupleDescriptor* _dest_tuple_desc;
    // Jni helper for scanning an schema table.
    std::unique_ptr<SchemaScanner> _schema_scanner;
};

} // namespace vectorized

} // namespace starrocks

/* vim: set ts=4 sw=4 sts=4 tw=100 noet: */
