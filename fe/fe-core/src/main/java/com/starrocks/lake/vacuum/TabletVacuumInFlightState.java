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

// Per-tablet propose+commit pointer state held in PhysicalPartition's in-flight map.
// Field semantics mirror the BE-side TabletInfoPB extension carrying the same names.
public class TabletVacuumInFlightState {
    // Inclusive range [start, end] proposed in the previous round, to be committed
    // in the next round. Both 0 means no in-flight commit.
    @SerializedName(value = "toDelStart")
    public long toDeleteVersionStart;

    @SerializedName(value = "toDelEnd")
    public long toDeleteVersionEnd;

    // Where to start the chain walk for the next propose phase. 0 (sentinel) means
    // restart from min_retain_version.
    @SerializedName(value = "nextStart")
    public long nextVacuumStartVersion;

    public TabletVacuumInFlightState() {
    }

    public TabletVacuumInFlightState(long toDeleteStart, long toDeleteEnd, long nextStart) {
        this.toDeleteVersionStart = toDeleteStart;
        this.toDeleteVersionEnd = toDeleteEnd;
        this.nextVacuumStartVersion = nextStart;
    }

    // All-zero entry contributes no information and should not be stored in the map.
    public boolean isEmpty() {
        return toDeleteVersionStart == 0 && toDeleteVersionEnd == 0 && nextVacuumStartVersion == 0;
    }
}
