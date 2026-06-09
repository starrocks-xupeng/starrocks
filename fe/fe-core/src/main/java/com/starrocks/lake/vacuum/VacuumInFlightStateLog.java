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

package com.starrocks.lake.vacuum;

import com.google.gson.annotations.SerializedName;
import com.starrocks.catalog.Database;
import com.starrocks.catalog.OlapTable;
import com.starrocks.catalog.PhysicalPartition;
import com.starrocks.catalog.Table;
import com.starrocks.common.io.Writable;
import com.starrocks.server.GlobalStateMgr;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Map;

// Persisted log entry recording a partition's per-tablet vacuum in-flight pointer
// updates after each propose+commit round. Must be journaled so that after FE
// restart the tablet metadata chain walk resumes from the right next-version
// pointer instead of restarting from min_retain_version (which would race with
// BE-deleted metadata).
//
// Map semantics: entries with non-empty state are upserted into the partition's
// in-flight map; entries whose state is empty (all zeros) are removed. This lets a
// single log entry both register new in-flight ranges and clear out finished ones.
public class VacuumInFlightStateLog implements Writable {
    private static final Logger LOG = LogManager.getLogger(VacuumInFlightStateLog.class);

    @SerializedName(value = "dbId")
    private final long dbId;

    @SerializedName(value = "tableId")
    private final long tableId;

    @SerializedName(value = "partitionId")
    private final long partitionId;

    @SerializedName(value = "updates")
    private final Map<Long, TabletVacuumInFlightState> updates;

    public VacuumInFlightStateLog(long dbId, long tableId, long partitionId,
                                  Map<Long, TabletVacuumInFlightState> updates) {
        this.dbId = dbId;
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.updates = updates;
    }

    public long getDbId() {
        return dbId;
    }

    public long getTableId() {
        return tableId;
    }

    public long getPartitionId() {
        return partitionId;
    }

    public Map<Long, TabletVacuumInFlightState> getUpdates() {
        return updates != null ? updates : Collections.emptyMap();
    }

    // Apply this log entry to the in-memory catalog. Used both at journal replay
    // (after image load) and right after EditLog write so the in-memory partition
    // state always matches what's been journaled.
    public void applyToCatalog() {
        GlobalStateMgr stateMgr = GlobalStateMgr.getCurrentState();
        Database db = stateMgr.getLocalMetastore().getDb(dbId);
        if (db == null) {
            LOG.warn("apply vacuum in-flight state: db {} not found, skip", dbId);
            return;
        }
        Table table = stateMgr.getLocalMetastore().getTable(dbId, tableId);
        if (!(table instanceof OlapTable)) {
            LOG.warn("apply vacuum in-flight state: table {}.{} missing or wrong type, skip", dbId, tableId);
            return;
        }
        PhysicalPartition partition = ((OlapTable) table).getPhysicalPartition(partitionId);
        if (partition == null) {
            LOG.warn("apply vacuum in-flight state: partition {} missing in {}.{}, skip",
                    partitionId, dbId, tableId);
            return;
        }
        partition.applyVacuumInFlightStateUpdate(getUpdates());
    }
}
