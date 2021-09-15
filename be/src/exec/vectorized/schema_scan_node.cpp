// This file is made available under Elastic License 2.0.
// This file is based on code available under the Apache license here:
//   https://github.com/apache/incubator-doris/blob/master/be/src/exec/schema_scan_node.cpp

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

#include "exec/vectorized/schema_scan_node.h"

#include <boost/algorithm/string.hpp>

#include "column/column_helper.h"
#include "exec/vectorized/schema_scanner/schema_helper.h"
#include "exec/text_converter.hpp"
#include "gen_cpp/PlanNodes_types.h"
#include "gen_cpp/Types_types.h"
#include "runtime/runtime_state.h"
#include "runtime/string_value.h"
#include "util/runtime_profile.h"

namespace starrocks::vectorized {

SchemaScanNode::SchemaScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
        : ScanNode(pool, tnode, descs),
          _is_init(false),
          _table_name(tnode.schema_scan_node.table_name),
          _tuple_id(tnode.schema_scan_node.tuple_id),
          _dest_tuple_desc(NULL),
          _schema_scanner(nullptr) {}

SchemaScanNode::~SchemaScanNode() {
}

Status SchemaScanNode::init(const TPlanNode& tnode, RuntimeState* state) {
    RETURN_IF_ERROR(ExecNode::init(tnode));
    if (tnode.schema_scan_node.__isset.db) {
        _scanner_param.db = _pool->add(new std::string(tnode.schema_scan_node.db));
    }

    if (tnode.schema_scan_node.__isset.table) {
        _scanner_param.table = _pool->add(new std::string(tnode.schema_scan_node.table));
    }

    if (tnode.schema_scan_node.__isset.wild) {
        _scanner_param.wild = _pool->add(new std::string(tnode.schema_scan_node.wild));
    }

    if (tnode.schema_scan_node.__isset.current_user_ident) {
        _scanner_param.current_user_ident = _pool->add(new TUserIdentity(tnode.schema_scan_node.current_user_ident));
    } else {
        if (tnode.schema_scan_node.__isset.user) {
            _scanner_param.user = _pool->add(new std::string(tnode.schema_scan_node.user));
        }
        if (tnode.schema_scan_node.__isset.user_ip) {
            _scanner_param.user_ip = _pool->add(new std::string(tnode.schema_scan_node.user_ip));
        }
    }

    if (tnode.schema_scan_node.__isset.ip) {
        _scanner_param.ip = _pool->add(new std::string(tnode.schema_scan_node.ip));
    }
    if (tnode.schema_scan_node.__isset.port) {
        _scanner_param.port = tnode.schema_scan_node.port;
    }

    if (tnode.schema_scan_node.__isset.thread_id) {
        _scanner_param.thread_id = tnode.schema_scan_node.thread_id;
    }
    return Status::OK();
}

Status SchemaScanNode::prepare(RuntimeState* state) {
    if (_is_init) {
        return Status::OK();
    }

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    RETURN_IF_ERROR(ScanNode::prepare(state));

    // get dest tuple desc
    _dest_tuple_desc = state->desc_tbl().get_tuple_descriptor(_tuple_id);

    if (NULL == _dest_tuple_desc) {
        return Status::InternalError("Failed to get tuple descriptor.");
    }

    const SchemaTableDescriptor* schema_table =
            static_cast<const SchemaTableDescriptor*>(_dest_tuple_desc->table_desc());

    if (NULL == schema_table) {
        return Status::InternalError("Failed to get schema table descriptor.");
    }

    // new one scanner
    _schema_scanner.reset(SchemaScanner::create(schema_table->schema_table_type()));

    if (NULL == _schema_scanner.get()) {
        return Status::InternalError("schema scanner get NULL pointer.");
    }

    _schema_scanner->set_slot_descs(_dest_tuple_desc->slots());

    RETURN_IF_ERROR(_schema_scanner->init(&_scanner_param, _pool));

    _is_init = true;

    return Status::OK();
}

Status SchemaScanNode::open(RuntimeState* state) {
    if (!_is_init) {
        return Status::InternalError("Open before Init.");
    }

    if (NULL == state) {
        return Status::InternalError("input pointer is NULL.");
    }

    SCOPED_TIMER(_runtime_profile->total_time_counter());
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::OPEN));
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(ExecNode::open(state));

    if (_scanner_param.user) {
        TSetSessionParams param;
        param.__set_user(*_scanner_param.user);
        //TStatus t_status;
        //RETURN_IF_ERROR(SchemaJniHelper::set_session(param, &t_status));
        //RETURN_IF_ERROR(Status(t_status));
    }

    return _schema_scanner->start(state);
}

Status SchemaScanNode::get_next(RuntimeState* state, RowBatch* row_batch, bool* eos) {
    if (!_is_init) {
        return Status::InternalError("used before initialized.");
    }

    if (NULL == state || NULL == row_batch || NULL == eos) {
        return Status::InternalError("input pointer is NULL.");
    }

    *eos = true;
    return Status::OK();
}

Status SchemaScanNode::get_next(RuntimeState* state, ChunkPtr* chunk, bool* eos) {
    VLOG(1) << "SchemaScanNode::GetNext";

    DCHECK(state != nullptr && chunk != nullptr && eos != nullptr);
    DCHECK(_is_init);

    RETURN_IF_CANCELLED(state);
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    if (reached_limit() || _is_finished) {
        *eos = true;
        return Status::OK();
    }

    *chunk = std::make_shared<vectorized::Chunk>();
    const std::vector<SlotDescriptor*>& slot_descs = _schema_scanner->get_slot_descs();
    // init column information
    for (auto& slot_desc : slot_descs) {
        ColumnPtr column = vectorized::ColumnHelper::create_column(slot_desc->type(), slot_desc->is_nullable());
        (*chunk)->append_column(std::move(column), slot_desc->id());
    }

    bool scanner_eos = false;
    int row_num = 0;

    while (true) {
        RETURN_IF_CANCELLED(state);

        if (reached_limit()) {
            _is_finished = true;
            // if row_num is greater than 0, in this call, eos = false, and eos will be set to true
            // in the next call
            if (row_num == 0) {
                *eos = true;
            }
            return Status::OK();
        }

        if (row_num >= config::vector_chunk_size) {
            return Status::OK();
        }

        RETURN_IF_ERROR(_schema_scanner->get_next_row(chunk, &scanner_eos));

        if (scanner_eos)
        {
            _is_finished = true;
            // if row_num is greater than 0, in this call, eos = false, and eos will be set to true
            // in the next call
            if (row_num == 0) {
                *eos = true;
            }
            return Status::OK();
        }
        ++row_num;

        ++_num_rows_returned;
        COUNTER_SET(_rows_returned_counter, _num_rows_returned);
    }

    return Status::OK();
}

Status SchemaScanNode::close(RuntimeState* state) {
    if (is_closed()) {
        return Status::OK();
    }
    RETURN_IF_ERROR(exec_debug_action(TExecNodePhase::CLOSE));
    SCOPED_TIMER(_runtime_profile->total_time_counter());

    return ScanNode::close(state);
}

void SchemaScanNode::debug_string(int indentation_level, std::stringstream* out) const {
    *out << string(indentation_level * 2, ' ');
    *out << "SchemaScanNode(tupleid=" << _tuple_id << " table=" << _table_name;
    *out << ")" << std::endl;

    for (int i = 0; i < _children.size(); ++i) {
        _children[i]->debug_string(indentation_level + 1, out);
    }
}

Status SchemaScanNode::set_scan_ranges(const std::vector<TScanRangeParams>& scan_ranges) {
    return Status::OK();
}

} // namespace starrocks::vectorized

/* vim: set ts=4 sw=4 sts=4 tw=100 : */
