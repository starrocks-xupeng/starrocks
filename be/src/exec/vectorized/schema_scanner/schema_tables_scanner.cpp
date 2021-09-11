// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scanner/schema_tables_scanner.cpp

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

#include "exec/vectorized/schema_scanner/schema_tables_scanner.h"

#include "column/chunk.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "runtime/primitive_type.h"
#include "runtime/string_value.h"
#include "column/nullable_column.h"
#include "column/type_traits.h"
//#include "runtime/datetime_value.h"

namespace starrocks::vectorized {

SchemaScanner::ColumnDesc SchemaTablesScanner::_s_tbls_columns[] = {
        //   name,       type,          size,     is_null
        {"TABLE_CATALOG", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_SCHEMA", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_NAME", TYPE_VARCHAR, sizeof(StringValue), false},
        {"TABLE_TYPE", TYPE_VARCHAR, sizeof(StringValue), false},
        {"ENGINE", TYPE_VARCHAR, sizeof(StringValue), true},
        {"VERSION", TYPE_BIGINT, sizeof(int64_t), true},
        {"ROW_FORMAT", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_ROWS", TYPE_BIGINT, sizeof(int64_t), true},
        {"AVG_ROW_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"MAX_DATA_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"INDEX_LENGTH", TYPE_BIGINT, sizeof(int64_t), true},
        {"DATA_FREE", TYPE_BIGINT, sizeof(int64_t), true},
        {"AUTO_INCREMENT", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"UPDATE_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"CHECK_TIME", TYPE_DATETIME, sizeof(DateTimeValue), true},
        {"TABLE_COLLATION", TYPE_VARCHAR, sizeof(StringValue), true},
        {"CHECKSUM", TYPE_BIGINT, sizeof(int64_t), true},
        {"CREATE_OPTIONS", TYPE_VARCHAR, sizeof(StringValue), true},
        {"TABLE_COMMENT", TYPE_VARCHAR, sizeof(StringValue), false},
};

SchemaTablesScanner::SchemaTablesScanner()
        : SchemaScanner(_s_tbls_columns, sizeof(_s_tbls_columns) / sizeof(SchemaScanner::ColumnDesc)),
          _db_index(0),
          _table_index(0) {}

SchemaTablesScanner::~SchemaTablesScanner() {}

Status SchemaTablesScanner::start(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }
    TGetDbsParams db_params;
    if (NULL != _param->db) {
        db_params.__set_pattern(*(_param->db));
    }
    if (NULL != _param->current_user_ident) {
        db_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (NULL != _param->user) {
            db_params.__set_user(*(_param->user));
        }
        if (NULL != _param->user_ip) {
            db_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::get_db_names(*(_param->ip), _param->port, db_params, &_db_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    return Status::OK();
}

Status SchemaTablesScanner::fill_one_row(ChunkPtr* chunk)
{
    const TTableStatus& tbl_status = _table_result.tables[_table_index];
    // catalog
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[0]->id());
        fill_data_column_with_null(column.get());
    }
    // schema
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[1]->id());
        std::string db_name = SchemaHelper::extract_db_name(_db_result.dbs[_db_index - 1]);
        Slice value(db_name.c_str(), db_name.length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // name
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[2]->id());
        const std::string* str = &tbl_status.name;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // type
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[3]->id());
        const std::string* str = &tbl_status.type;
        Slice value(str->c_str(), str->length());
       fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // engine
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[4]->id());
        if (tbl_status.__isset.engine) {
            const std::string* str = &tbl_status.engine;
            Slice value(str->c_str(), str->length());
            fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
        } else {
            NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
            nullable_column->append_nulls(1);
        }
    }
    // version
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[5]->id());
        fill_data_column_with_null(column.get());
    }
    // row_format
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[6]->id());
        fill_data_column_with_null(column.get());
    }
    // rows
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[7]->id());
        fill_data_column_with_null(column.get());
    }
    // avg_row_length
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[8]->id());
        fill_data_column_with_null(column.get());
    }
    // data_length
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[9]->id());
        fill_data_column_with_null(column.get());
    }
    // max_data_length
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[10]->id());
        fill_data_column_with_null(column.get());
    }
    // index_length
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[11]->id());
        fill_data_column_with_null(column.get());
    }
    // data_free
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[12]->id());
        fill_data_column_with_null(column.get());
    }
    // auto_increment
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[13]->id());
        fill_data_column_with_null(column.get());
    }
    // creation_time
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[14]->id());
        NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
        if (tbl_status.__isset.create_time) {
            int64_t create_time = tbl_status.create_time;
            if (create_time <= 0) {
                nullable_column->append_nulls(1);
            } else {
                DateTimeValue t;
                t.from_unixtime(create_time, TimezoneUtils::default_time_zone);
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void *)&t);
            }
        } else {
            nullable_column->append_nulls(1);
        }
    }
    // update_time
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[15]->id());
        fill_data_column_with_null(column.get());
    }
    // check_time
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[16]->id());
        NullableColumn* nullable_column = down_cast<NullableColumn*>(column.get());
        if (tbl_status.__isset.last_check_time) {
            int64_t check_time = tbl_status.last_check_time;
            if (check_time <= 0) {
                nullable_column->append_nulls(1);
            } else {
                DateTimeValue t;
                t.from_unixtime(check_time, TimezoneUtils::default_time_zone);
                fill_column_with_slot<TYPE_DATETIME>(column.get(), (void *)&t);
            }
        } else {
            nullable_column->append_nulls(1);
        }
    }
    // collation
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[17]->id());
        const char* collation_str = "utf8_general_ci";
        Slice value(collation_str, strlen(collation_str));
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    // checksum
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[18]->id());
        fill_data_column_with_null(column.get());
    }
    // create_options
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[19]->id());
        fill_data_column_with_null(column.get());
    }
    // create_comment
    {
        ColumnPtr column = (*chunk)->get_column_by_slot_id(_slot_descs[20]->id());
        const std::string* str = &tbl_status.comment;
        Slice value(str->c_str(), str->length());
        fill_column_with_slot<TYPE_VARCHAR>(column.get(), (void *)&value);
    }
    _table_index++;
    return Status::OK();
}

Status SchemaTablesScanner::get_new_table() {
    TGetTablesParams table_params;
    table_params.__set_db(_db_result.dbs[_db_index++]);
    if (NULL != _param->wild) {
        table_params.__set_pattern(*(_param->wild));
    }
    if (NULL != _param->current_user_ident) {
        table_params.__set_current_user_ident(*(_param->current_user_ident));
    } else {
        if (NULL != _param->user) {
            table_params.__set_user(*(_param->user));
        }
        if (NULL != _param->user_ip) {
            table_params.__set_user_ip(*(_param->user_ip));
        }
    }

    if (NULL != _param->ip && 0 != _param->port) {
        RETURN_IF_ERROR(SchemaHelper::list_table_status(*(_param->ip), _param->port, table_params, &_table_result));
    } else {
        return Status::InternalError("IP or port dosn't exists");
    }
    _table_index = 0;
    return Status::OK();
}

Status SchemaTablesScanner::get_next_row(ChunkPtr* chunk, bool* eos)
{
    if (!_is_init) {
        return Status::InternalError("Used before initialized.");
    }
    if (NULL == chunk || NULL == eos) {
        return Status::InternalError("input pointer is NULL.");
    }
    while (_table_index >= _table_result.tables.size()) {
        if (_db_index < _db_result.dbs.size()) {
            RETURN_IF_ERROR(get_new_table());
        } else {
            *eos = true;
            return Status::OK();
        }
    }
    *eos = false;
    return fill_one_row(chunk);
}

} // namespace starrocks::vectorized
