// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scanner/schema_charsets_scanner.cpp

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

#include "exec/vectorized/schema_scanner/schema_charsets_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaCharsetsScanner::_s_css_columns[] = {
        //   name,       type,          size
        {"CHARACTER_SET_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DEFAULT_COLLATE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"DESCRIPTION", TYPE_VARCHAR, sizeof(StringValue), false},
        {"MAXLEN", TYPE_BIGINT, sizeof(int64_t), false},
};

SchemaCharsetsScanner::CharsetStruct SchemaCharsetsScanner::_s_charsets[] = {
        {"utf8", "utf8_general_ci", "UTF-8 Unicode", 3},
        {NULL, NULL, 0},
};

SchemaCharsetsScanner::SchemaCharsetsScanner()
        : SchemaScanner(_s_css_columns, sizeof(_s_css_columns) / sizeof(SchemaScanner::ColumnDesc)), _index(0) {}

SchemaCharsetsScanner::~SchemaCharsetsScanner() {}

Status SchemaCharsetsScanner::fill_one_row(ChunkPtr* chunk) {
    // variables names
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        Slice value(_s_charsets[_index].charset, strlen(_s_charsets[_index].charset));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // DEFAULT_COLLATE_NAME
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        Slice value(_s_charsets[_index].default_collation, strlen(_s_charsets[_index].default_collation));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // DESCRIPTION
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        Slice value(_s_charsets[_index].description, strlen(_s_charsets[_index].description));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // maxlen
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        fill_column_with_slot<TYPE_BIGINT>(column.get(), (void *)&_s_charsets[_index].maxlen);
    }
    _index++;
    return Status::OK();
}

Status SchemaCharsetsScanner::get_next_row(ChunkPtr* chunk, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("call this before initial.");
    }
    if (NULL == _s_charsets[_index].charset) {
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
