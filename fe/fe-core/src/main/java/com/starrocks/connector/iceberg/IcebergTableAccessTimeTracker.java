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

package com.starrocks.connector.iceberg;

import com.starrocks.connector.DatabaseTableName;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Tracks last access time per (catalog, db, table) for all Iceberg catalogs.
 * Used by metadata refresh (skip refresh for cold tables) and auto maintenance
 * (skip maintenance for tables not accessed within 48h). Recording is done in
 * {@link com.starrocks.connector.iceberg.IcebergMetadata#getTable} so every
 * catalog benefits regardless of caching.
 */
public class IcebergTableAccessTimeTracker {
    private static final IcebergTableAccessTimeTracker INSTANCE = new IcebergTableAccessTimeTracker();

    private final ConcurrentHashMap<String, ConcurrentHashMap<DatabaseTableName, Long>> catalogToTableAccessTime =
            new ConcurrentHashMap<>();

    public static IcebergTableAccessTimeTracker getInstance() {
        return INSTANCE;
    }

    private IcebergTableAccessTimeTracker() {
    }

    public void recordAccess(String catalogName, String dbName, String tableName) {
        catalogToTableAccessTime
                .computeIfAbsent(catalogName, k -> new ConcurrentHashMap<>())
                .put(DatabaseTableName.of(dbName, tableName), System.currentTimeMillis());
    }

    /**
     * Returns table names under the given catalog that have been accessed within the given window (seconds).
     */
    public Set<DatabaseTableName> getTableNamesAccessedWithinSecs(String catalogName, long secs) {
        ConcurrentHashMap<DatabaseTableName, Long> map = catalogToTableAccessTime.get(catalogName);
        if (map == null) {
            return Set.of();
        }
        long now = System.currentTimeMillis();
        long thresholdMillis = secs * 1000L;
        Set<DatabaseTableName> result = new HashSet<>();
        for (ConcurrentHashMap.Entry<DatabaseTableName, Long> entry : map.entrySet()) {
            if (now - entry.getValue() <= thresholdMillis) {
                result.add(entry.getKey());
            }
        }
        return result;
    }

    /**
     * Removes access time for a table (e.g. when cache is invalidated). Called by CachingIcebergCatalog.
     */
    public void removeAccess(String catalogName, String dbName, String tableName) {
        ConcurrentHashMap<DatabaseTableName, Long> map = catalogToTableAccessTime.get(catalogName);
        if (map != null) {
            map.remove(DatabaseTableName.of(dbName, tableName));
        }
    }
}
