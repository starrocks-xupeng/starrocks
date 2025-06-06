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
//   https://github.com/apache/incubator-doris/blob/master/be/src/runtime/stream_load/stream_load_executor.cpp

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

#include "runtime/stream_load/stream_load_executor.h"

#include <fmt/format.h>

#include <string_view>

#include "agent/master_info.h"
#include "common/process_exit.h"
#include "common/status.h"
#include "common/statusor.h"
#include "common/utils.h"
#include "gen_cpp/FrontendService.h"
#include "runtime/client_cache.h"
#include "runtime/exec_env.h"
#include "runtime/fragment_mgr.h"
#include "runtime/plan_fragment_executor.h"
#include "runtime/stream_load/stream_load_context.h"
#include "testutil/sync_point.h"
#include "util/defer_op.h"
#include "util/starrocks_metrics.h"
#include "util/thrift_rpc_helper.h"

namespace starrocks {

#ifdef BE_TEST
TLoadTxnBeginResult k_stream_load_begin_result;
TLoadTxnCommitResult k_stream_load_commit_result;
TLoadTxnRollbackResult k_stream_load_rollback_result;
#endif

static Status commit_txn_internal(const TLoadTxnCommitRequest& request, int32_t rpc_timeout_ms,
                                  TLoadTxnCommitResult* result);
static StatusOr<TTransactionStatus::type> get_txn_status(const AuthInfo& auth, std::string_view db,
                                                         std::string_view table, int64_t txn_id);
static bool wait_txn_visible_until(const AuthInfo& auth, std::string_view db, std::string_view table, int64_t txn_id,
                                   int64_t deadline);

Status StreamLoadExecutor::execute_plan_fragment(StreamLoadContext* ctx) {
    if (process_exit_in_progress()) {
        return Status::ServiceUnavailable("Service is shutting down, please retry later!");
    }

    StarRocksMetrics::instance()->txn_exec_plan_total.increment(1);
// submit this params
#ifndef BE_TEST
    ctx->ref();
    ctx->start_write_data_nanos = MonotonicNanos();
    LOG(INFO) << "begin to execute job. label=" << ctx->label << ", txn_id: " << ctx->txn_id
              << ", query_id=" << print_id(ctx->put_result.params.params.query_id);
    // Once this is added into FragmentMgr, the fragment will be counted during graceful exit.
    auto st = _exec_env->fragment_mgr()->exec_plan_fragment(
            ctx->put_result.params,
            [ctx](PlanFragmentExecutor* executor) {
                ctx->runtime_profile = executor->runtime_state()->runtime_profile_ptr();
                ctx->query_mem_tracker = executor->runtime_state()->query_mem_tracker_ptr();
                ctx->instance_mem_tracker = executor->runtime_state()->instance_mem_tracker_ptr();
            },
            [ctx](PlanFragmentExecutor* executor) {
                ctx->commit_infos = std::move(executor->runtime_state()->tablet_commit_infos());
                ctx->fail_infos = std::move(executor->runtime_state()->tablet_fail_infos());
                Status status = executor->status();
                if (status.ok()) {
                    ctx->number_total_rows = executor->runtime_state()->num_rows_load_sink() +
                                             executor->runtime_state()->num_rows_load_filtered() +
                                             executor->runtime_state()->num_rows_load_unselected();
                    ctx->number_loaded_rows = executor->runtime_state()->num_rows_load_sink();
                    ctx->number_filtered_rows = executor->runtime_state()->num_rows_load_filtered();
                    ctx->number_unselected_rows = executor->runtime_state()->num_rows_load_unselected();
                    ctx->loaded_bytes = executor->runtime_state()->num_bytes_load_sink();

                    int64_t num_selected_rows = ctx->number_total_rows - ctx->number_unselected_rows;
                    if ((double)ctx->number_filtered_rows / num_selected_rows > ctx->max_filter_ratio) {
                        // NOTE: Do not modify the error message here, for historical
                        // reasons,
                        // some users may rely on this error message.
                        status = Status::InternalError("too many filtered rows");
                    }

                    if (status.ok()) {
                        StarRocksMetrics::instance()->stream_receive_bytes_total.increment(ctx->total_receive_bytes);
                        StarRocksMetrics::instance()->stream_load_rows_total.increment(ctx->number_loaded_rows);
                    }
                } else {
                    LOG(WARNING) << "fragment execute failed"
                                 << ", query_id=" << UniqueId(ctx->put_result.params.params.query_id)
                                 << ", err_msg=" << status.message() << ", " << ctx->brief();
                    // cancel body_sink, make sender known it
                    if (ctx->body_sink != nullptr) {
                        ctx->body_sink->cancel(status);
                    }

                    switch (ctx->load_src_type) {
                    // reset the stream load ctx's kafka commit offset
                    case TLoadSourceType::KAFKA:
                        ctx->kafka_info->reset_offset();
                        break;
                    case TLoadSourceType::PULSAR:
                        ctx->pulsar_info->clear_backlog();
                        break;
                    default:
                        break;
                    }
                }
                ctx->write_data_cost_nanos = MonotonicNanos() - ctx->start_write_data_nanos;
                ctx->promise.set_value(status);

                if (!executor->runtime_state()->get_error_log_file_path().empty()) {
                    ctx->error_url = to_load_error_http_path(executor->runtime_state()->get_error_log_file_path());
                }

                if (!executor->runtime_state()->get_rejected_record_file_path().empty()) {
                    ctx->rejected_record_path = fmt::format("{}:{}", BackendOptions::get_localBackend().host,
                                                            executor->runtime_state()->get_rejected_record_file_path());
                }

                if (ctx->unref()) {
                    delete ctx;
                }
            });
    if (!st.ok()) {
        if (ctx->unref()) {
            delete ctx;
        }
        return st;
    }
#else
    Status status;
    TEST_SYNC_POINT_CALLBACK("StreamLoadExecutor::execute_plan_fragment:1", &status);
    ctx->promise.set_value(status);
#endif
    return Status::OK();
}

Status StreamLoadExecutor::begin_txn(StreamLoadContext* ctx) {
    StarRocksMetrics::instance()->txn_begin_request_total.increment(1);

    TLoadTxnBeginRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.label = ctx->label;
    auto backend_id = get_backend_id();
    if (backend_id.has_value()) {
        request.__set_backend_id(backend_id.value());
    }

    // set timestamp
    request.__set_timestamp(GetCurrentTimeMicros());
    if (ctx->timeout_second != -1) {
        request.__set_timeout(ctx->timeout_second);
    }
    request.__set_request_id(ctx->id.to_thrift());

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnBeginResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnBegin(result, request); }));
#else
    result = k_stream_load_begin_result;
#endif
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "begin transaction failed, errmsg=" << status.message() << ctx->brief();
        if (result.__isset.job_status) {
            ctx->existing_job_status = result.job_status;
        }
        return status;
    }
    ctx->txn_id = result.txnId;
    ctx->need_rollback = true;
    ctx->load_deadline_sec = UnixSeconds() + result.timeout;

    return Status::OK();
}

Status StreamLoadExecutor::commit_txn(StreamLoadContext* ctx) {
    StarRocksMetrics::instance()->txn_commit_request_total.increment(1);

    TLoadTxnCommitRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    int32_t rpc_timeout_ms = config::txn_commit_rpc_timeout_ms;
    if (ctx->timeout_second != -1) {
        rpc_timeout_ms = std::min(ctx->timeout_second * 1000 / 2, rpc_timeout_ms);
        rpc_timeout_ms = std::max(ctx->timeout_second * 1000 / 4, rpc_timeout_ms);
    }
    request.__set_thrift_rpc_timeout_ms(rpc_timeout_ms);

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = std::move(attachment);
        request.__isset.txnCommitAttachment = true;
    }

    int retry = 0;
    TLoadTxnCommitResult result;
    while (true) {
        RETURN_IF_ERROR(commit_txn_internal(request, rpc_timeout_ms, &result));
        Status st(result.status);
        if (st.ok()) {
            ctx->need_rollback = false;
            return st;
        } else if (st.is_publish_timeout()) {
            ctx->need_rollback = false;
            bool visible =
                    wait_txn_visible_until(ctx->auth, request.db, request.tbl, request.txnId, ctx->load_deadline_sec);
            return visible ? Status::OK() : st;
        } else if (st.is_eagain()) {
            LOG(WARNING) << "commit transaction " << request.txnId << " failed, will retry after sleeping "
                         << result.retry_interval_ms << "ms. errmsg=" << st.message();
            std::this_thread::sleep_for(std::chrono::milliseconds(result.retry_interval_ms));
        } else if (st.is_time_out()) {
            if (++retry > 1) {
                ctx->need_rollback = true;
                return st;
            }
            LOG(WARNING) << "commit transaction " << request.txnId << " failed, will retry. errmsg=" << st.message();
            if (ctx->load_deadline_sec > 0) {
                rpc_timeout_ms = (ctx->load_deadline_sec - UnixSeconds()) * 1000;
            }
        } else {
            ctx->need_rollback = true;
            return st;
        }
    }
}

Status commit_txn_internal(const TLoadTxnCommitRequest& request, int32_t rpc_timeout_ms, TLoadTxnCommitResult* result) {
    TNetworkAddress master_addr = get_master_address();
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnCommit(*result, request); },
            rpc_timeout_ms));
#else
    *result = k_stream_load_commit_result;
#endif
    return Status::OK();
}

StatusOr<TTransactionStatus::type> get_txn_status(const AuthInfo& auth, std::string_view db, std::string_view table,
                                                  int64_t txn_id) {
    TNetworkAddress master_addr = get_master_address();
    TGetLoadTxnStatusRequest request;
    TGetLoadTxnStatusResult result;

    set_request_auth(&request, auth);
    request.db = db;
    request.tbl = table;
    request.txnId = txn_id;

    auto st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->getLoadTxnStatus(result, request); },
            config::txn_commit_rpc_timeout_ms);
    if (!st.ok()) {
        return st;
    } else {
        return result.status;
    }
}

bool wait_txn_visible_until(const AuthInfo& auth, std::string_view db, std::string_view table, int64_t txn_id,
                            int64_t deadline) {
    while (deadline > UnixSeconds()) {
        auto wait_seconds = std::min((int64_t)config::get_txn_status_internal_sec, deadline - UnixSeconds());
        LOG(WARNING) << "transaction is not visible now, will wait " << wait_seconds
                     << " seconds before retrieving the status again, txn_id: " << txn_id;
        // The following sleep might introduce delay to the commit and publish total time
        sleep(wait_seconds);
        auto status_or = get_txn_status(auth, db, table, txn_id);
        if (!status_or.ok()) {
            return false;
        } else if (status_or.value() == TTransactionStatus::VISIBLE) {
            return true;
        } else if (status_or.value() == TTransactionStatus::COMMITTED) {
            continue;
        } else {
            return false;
        }
    }
    return false;
}

Status StreamLoadExecutor::prepare_txn(StreamLoadContext* ctx) {
    StarRocksMetrics::instance()->txn_commit_request_total.increment(1);

    TLoadTxnCommitRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.sync = true;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    int32_t rpc_timeout_ms = config::txn_commit_rpc_timeout_ms;
    if (ctx->timeout_second != -1) {
        rpc_timeout_ms = std::min(ctx->timeout_second * 1000 / 2, rpc_timeout_ms);
        rpc_timeout_ms = std::max(ctx->timeout_second * 1000 / 4, rpc_timeout_ms);
    }
    request.__set_thrift_rpc_timeout_ms(rpc_timeout_ms);

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnCommitResult result;
#ifndef BE_TEST
    RETURN_IF_ERROR(ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnPrepare(result, request); },
            rpc_timeout_ms));
#else
    result = k_stream_load_commit_result;
#endif
    // Return if this transaction is prepare successful; otherwise, we need try
    // to rollback this transaction.
    Status status(result.status);
    if (!status.ok()) {
        LOG(WARNING) << "prepare transaction failed, errmsg=" << status.message() << ctx->brief();
        return status;
    }
    // commit success, set need_rollback to false
    ctx->need_rollback = false;
    return Status::OK();
}

Status StreamLoadExecutor::rollback_txn(StreamLoadContext* ctx) {
    StarRocksMetrics::instance()->txn_rollback_request_total.increment(1);

    TNetworkAddress master_addr = get_master_address();
    TLoadTxnRollbackRequest request;
    set_request_auth(&request, ctx->auth);
    request.db = ctx->db;
    request.tbl = ctx->table;
    request.txnId = ctx->txn_id;
    request.commitInfos = std::move(ctx->commit_infos);
    request.__isset.commitInfos = true;
    request.failInfos = std::move(ctx->fail_infos);
    request.__isset.failInfos = true;
    request.__set_reason(std::string(ctx->status.message()));

    // set attachment if has
    TTxnCommitAttachment attachment;
    if (collect_load_stat(ctx, &attachment)) {
        request.txnCommitAttachment = attachment;
        request.__isset.txnCommitAttachment = true;
    }

    TLoadTxnRollbackResult result;
#ifndef BE_TEST
    auto rpc_st = ThriftRpcHelper::rpc<FrontendServiceClient>(
            master_addr.hostname, master_addr.port,
            [&request, &result](FrontendServiceConnection& client) { client->loadTxnRollback(result, request); });
    if (!rpc_st.ok()) {
        LOG(WARNING) << "transaction rollback failed. errmsg=" << rpc_st.message() << ctx->brief();
        return rpc_st;
    }
    if (result.status.status_code != TStatusCode::TXN_NOT_EXISTS) {
        return result.status;
    }
#else
    result = k_stream_load_rollback_result;
#endif
    return Status::OK();
}

bool StreamLoadExecutor::collect_load_stat(StreamLoadContext* ctx, TTxnCommitAttachment* attach) {
    switch (ctx->load_type) {
    case TLoadType::MINI_LOAD: {
        attach->loadType = TLoadType::MINI_LOAD;

        TMiniLoadTxnCommitAttachment ml_attach;
        ml_attach.loadedRows = ctx->number_loaded_rows;
        ml_attach.filteredRows = ctx->number_filtered_rows;
        if (!ctx->error_url.empty()) {
            ml_attach.__set_errorLogUrl(ctx->error_url);
        }

        attach->mlTxnCommitAttachment = ml_attach;
        attach->__isset.mlTxnCommitAttachment = true;
        break;
    }
    case TLoadType::MANUAL_LOAD: {
        attach->loadType = TLoadType::MANUAL_LOAD;

        TManualLoadTxnCommitAttachment manual_load_attach;
        manual_load_attach.__set_loadedRows(ctx->number_loaded_rows);
        manual_load_attach.__set_filteredRows(ctx->number_filtered_rows);
        manual_load_attach.__set_receivedBytes(ctx->receive_bytes);
        manual_load_attach.__set_loadedBytes(ctx->loaded_bytes);
        manual_load_attach.__set_unselectedRows(ctx->number_unselected_rows);
        manual_load_attach.__set_beginTxnTime(ctx->begin_txn_cost_nanos / 1000 / 1000);
        manual_load_attach.__set_planTime(ctx->stream_load_put_cost_nanos / 1000 / 1000);
        manual_load_attach.__set_receiveDataTime(ctx->total_received_data_cost_nanos / 1000 / 1000);
        if (!ctx->error_url.empty()) {
            manual_load_attach.__set_errorLogUrl(ctx->error_url);
        }

        attach->manualLoadTxnCommitAttachment = manual_load_attach;
        attach->__isset.manualLoadTxnCommitAttachment = true;
        break;
    }
    case TLoadType::ROUTINE_LOAD: {
        attach->loadType = TLoadType::ROUTINE_LOAD;

        TRLTaskTxnCommitAttachment rl_attach;
        rl_attach.jobId = ctx->job_id;
        rl_attach.id = ctx->id.to_thrift();
        rl_attach.__set_loadedRows(ctx->number_loaded_rows);
        rl_attach.__set_filteredRows(ctx->number_filtered_rows);
        rl_attach.__set_unselectedRows(ctx->number_unselected_rows);
        rl_attach.__set_receivedBytes(ctx->receive_bytes);
        rl_attach.__set_loadedBytes(ctx->loaded_bytes);
        rl_attach.__set_loadCostMs(ctx->load_cost_nanos / 1000 / 1000);

        attach->rlTaskTxnCommitAttachment = rl_attach;
        attach->__isset.rlTaskTxnCommitAttachment = true;
        break;
    }
    default:
        // unknown load type, should not happen
        return false;
    }

    switch (ctx->load_src_type) {
    case TLoadSourceType::KAFKA: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::KAFKA;

        TKafkaRLTaskProgress kafka_progress;
        kafka_progress.partitionCmtOffset = ctx->kafka_info->cmt_offset;
        kafka_progress.partitionCmtOffsetTimestamp = ctx->kafka_info->cmt_offset_timestamp;
        kafka_progress.__isset.partitionCmtOffsetTimestamp = true;

        rl_attach.kafkaRLTaskProgress = kafka_progress;
        rl_attach.__isset.kafkaRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    case TLoadSourceType::PULSAR: {
        TRLTaskTxnCommitAttachment& rl_attach = attach->rlTaskTxnCommitAttachment;
        rl_attach.loadSourceType = TLoadSourceType::PULSAR;

        TPulsarRLTaskProgress pulsar_progress;
        pulsar_progress.partitionBacklogNum = ctx->pulsar_info->partition_backlog;

        rl_attach.pulsarRLTaskProgress = pulsar_progress;
        rl_attach.__isset.pulsarRLTaskProgress = true;
        if (!ctx->error_url.empty()) {
            rl_attach.__set_errorLogUrl(ctx->error_url);
        }
        return true;
    }
    default:
        return true;
    }
    return false;
}

} // namespace starrocks
