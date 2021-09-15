// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scanner/schema_collations_scanner.cpp

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

#include "exec/vectorized/schema_scanner/schema_collations_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaCollationsScanner::_s_cols_columns[] = {
        //   name,       type,          size
        {"COLLATION_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ID", TYPE_BIGINT, sizeof(int64_t), false},
        {"IS_DEFAULT", TYPE_VARCHAR, sizeof(StringValue), false},
        {"IS_COMPILED", TYPE_VARCHAR, sizeof(StringValue), false},
        {"SORTLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCollationsScanner::CollationStruct SchemaCollationsScanner::_s_collations[] = {
        {"utf8_general_ci", "utf8", 33, "Yes", "Yes", 1},
        {NULL, NULL, 0, NULL, NULL, 0},
};

SchemaCollationsScanner::SchemaCollationsScanner()
        : SchemaScanner(_s_cols_columns, sizeof(_s_cols_columns) / sizeof(SchemaScanner::ColumnDesc)), _index(0) {}

SchemaCollationsScanner::~SchemaCollationsScanner() {}

Status SchemaCollationsScanner::fill_one_row(ChunkPtr* chunk) {
    // COLLATION_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        Slice value(_s_collations[_index].name, strlen(_s_collations[_index].name));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // charset
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        Slice value(_s_collations[_index].charset, strlen(_s_collations[_index].charset));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // id
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void *)&_s_collations[_index].id);
    }
    // is_default
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        Slice value(_s_collations[_index].is_default, strlen(_s_collations[_index].is_default));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // IS_COMPILED
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        Slice value(_s_collations[_index].is_compile, strlen(_s_collations[_index].is_compile));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // sortlen
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void *)&_s_collations[_index].sortlen);
    }
    _index++;
    return Status::OK();
}

Status SchemaCollationsScanner::get_next_row(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (NULL == _s_collations[_index].name) {
        *eos = true;
        return Status::OK();
    }
    if (NULL == chunk || NULL == eos) {
        return Status::InternalError("invalid parameter.");
    }
    *eos = false;
    return fill_one_row(chunk);
}

} // namespace starrocks::vectorized
