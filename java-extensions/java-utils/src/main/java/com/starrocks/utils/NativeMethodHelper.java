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

package com.starrocks.utils;

/**
 * Note: All native methods has been registered by JNI launcher in BE.
 * This class must be loaded by {@link sun.misc.Launcher$AppClassLoader} only
 * and can not be loaded by any other independent class loader.
 */
public final class NativeMethodHelper {
    public static native long memoryTrackerMalloc(long bytes);

    public static native void memoryTrackerFree(long address);

    // return byteAddr
    public static native long resizeStringData(long columnAddr, int byteSize);

    public static native void resize(long columnAddr, int size);

    public static native int getColumnLogicalType(long columnAddr);

    // [nullAddr, dataAddr]
    public static native long[] getAddrs(long columnAddr);
}
