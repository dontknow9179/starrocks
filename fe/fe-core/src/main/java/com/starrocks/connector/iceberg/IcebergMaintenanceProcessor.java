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

import com.google.common.collect.Lists;
import com.starrocks.common.Config;
import com.starrocks.common.Pair;
import com.starrocks.common.ThreadPoolManager;
import com.starrocks.common.util.FrontendDaemon;
import com.starrocks.connector.DatabaseTableName;
import com.starrocks.connector.HdfsEnvironment;
import com.starrocks.connector.iceberg.procedure.ExpireSnapshotsProcedure;
import com.starrocks.connector.iceberg.procedure.IcebergTableProcedureContext;
import com.starrocks.connector.iceberg.procedure.RemoveOrphanFilesProcedure;
import com.starrocks.connector.iceberg.procedure.RewriteManifestsProcedure;
import com.starrocks.qe.ConnectContext;
import org.apache.iceberg.Transaction;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

/**
 * Standalone processor for Iceberg catalog metadata auto maintenance (expire_snapshots,
 * remove_orphan_files, rewrite_manifests). Independent of metadata cache: works with or without
 * CachingIcebergCatalog. When cache is enabled, maintains only cached tables; when cache is
 * disabled, lists all databases and tables from the catalog for maintenance.
 */
public class IcebergMaintenanceProcessor extends FrontendDaemon {
    private static final Logger LOG = LogManager.getLogger(IcebergMaintenanceProcessor.class);

    // Thread pool for executing per-table metadata maintenance tasks. This allows concurrent
    // processing of tables across catalogs while keeping the daemon loop simple.
    private static final int MAINTENANCE_THREAD_NUM =
            Math.max(1, Math.min(Config.iceberg_background_maintenance_pool_size, Runtime.getRuntime().availableProcessors()));
    private static final int MAINTENANCE_QUEUE_SIZE = Integer.MAX_VALUE;

    private final ConcurrentHashMap<String, IcebergMaintenanceInfo> maintenanceInfoMap = new ConcurrentHashMap<>();
    private final ExecutorService maintenanceExecutor;

    public IcebergMaintenanceProcessor() {
        super(IcebergMaintenanceProcessor.class.getName(),
                Config.iceberg_background_check_maintenance_interval_secs * 1000L);

        this.maintenanceExecutor = ThreadPoolManager.newDaemonFixedThreadPool(
                MAINTENANCE_THREAD_NUM,
                MAINTENANCE_QUEUE_SIZE,
                "iceberg-maintenance-pool",
                true);
    }

    private static class IcebergMaintenanceInfo {
        final String catalogName;
        final IcebergCatalog catalog;
        final HdfsEnvironment hdfsEnvironment;
        final int cleanupIntervalHours;
        final int rewriteIntervalHours;
        volatile long lastCleanupTimeMillis;
        volatile long lastRewriteTimeMillis;

        IcebergMaintenanceInfo(String catalogName, IcebergCatalog catalog, HdfsEnvironment hdfsEnvironment,
                               int cleanupIntervalHours, int rewriteIntervalHours) {
            this.catalogName = catalogName;
            this.catalog = catalog;
            this.hdfsEnvironment = hdfsEnvironment;
            this.cleanupIntervalHours = cleanupIntervalHours;
            this.rewriteIntervalHours = rewriteIntervalHours;
            long now = System.currentTimeMillis();
            this.lastCleanupTimeMillis = now;
            this.lastRewriteTimeMillis = now;
        }
    }

    public void registerIcebergCatalogForMaintenance(String catalogName, IcebergCatalog catalog,
                                                     HdfsEnvironment hdfsEnvironment,
                                                     int cleanupIntervalHours, int rewriteIntervalHours) {
        LOG.info("Register iceberg catalog {} for auto maintenance: cleanup_interval_hours={}, rewrite_manifests_interval_hours={}",
                catalogName, cleanupIntervalHours, rewriteIntervalHours);
        maintenanceInfoMap.put(catalogName,
                new IcebergMaintenanceInfo(catalogName, catalog, hdfsEnvironment, cleanupIntervalHours, rewriteIntervalHours));
    }

    public void unRegisterIcebergCatalogForMaintenance(String catalogName) {
        maintenanceInfoMap.remove(catalogName);
        LOG.info("Unregister iceberg catalog {} from auto maintenance", catalogName);
    }

    @Override
    protected void runAfterCatalogReady() {
        if (maintenanceInfoMap.isEmpty()) {
            return;
        }
        long now = System.currentTimeMillis();
        ConnectContext ctx = new ConnectContext();
        for (IcebergMaintenanceInfo info : maintenanceInfoMap.values()) {
            List<Pair<String, String>> tableNames = listTablesForMaintenance(info.catalogName, info.catalog, ctx);
            if (tableNames.isEmpty()) {
                continue;
            }
            boolean cleanupDue = info.cleanupIntervalHours > 0
                    && (now - info.lastCleanupTimeMillis) >= info.cleanupIntervalHours * 3600L * 1000L;
            boolean rewriteDue = info.rewriteIntervalHours > 0
                    && (now - info.lastRewriteTimeMillis) >= info.rewriteIntervalHours * 3600L * 1000L;
            if (!cleanupDue && !rewriteDue) {
                continue;
            }
            if (cleanupDue) {
                LOG.info("Start auto maintenance cleanup (expire_snapshots + remove_orphan_files) on iceberg catalog {}",
                        info.catalogName);
                runCleanupForCatalog(info, tableNames);
                info.lastCleanupTimeMillis = now;
                LOG.info("Finish auto maintenance cleanup on iceberg catalog {}", info.catalogName);
            }
            if (rewriteDue) {
                LOG.info("Start auto maintenance rewrite_manifests on iceberg catalog {}", info.catalogName);
                runRewriteManifestsForCatalog(info, tableNames);
                info.lastRewriteTimeMillis = now;
                LOG.info("Finish auto maintenance rewrite_manifests on iceberg catalog {}", info.catalogName);
            }
        }
    }

    private List<Pair<String, String>> listTablesForMaintenance(String catalogName, IcebergCatalog catalog,
                                                                ConnectContext ctx) {
        List<Pair<String, String>> result = Lists.newArrayList();
        try {
            List<String> dbs = catalog.listAllDatabases(ctx);
            for (String db : dbs) {
                try {
                    List<String> tables = catalog.listTables(ctx, db);
                    for (String tbl : tables) {
                        result.add(Pair.create(db, tbl));
                    }
                } catch (Exception e) {
                    LOG.warn("List tables failed for catalog {} db {}: {}", catalog.toString(), db, e.getMessage());
                }
            }
        } catch (Exception e) {
            LOG.warn("List databases failed for catalog {}: {}", catalog.toString(), e.getMessage());
        }

        // Skip cold tables: only maintain tables accessed within iceberg_background_maintenance_time_secs_since_last_access_secs.
        // Access time is recorded in IcebergMetadata.getTable() for all catalogs via IcebergTableAccessTimeTracker.
        if (!result.isEmpty()) {
            Set<DatabaseTableName> accessedWithinWindow =
                    IcebergTableAccessTimeTracker.getInstance().getTableNamesAccessedWithinSecs(
                            catalogName, Config.iceberg_background_maintenance_time_secs_since_last_access_secs);
            result = result.stream()
                    .filter(p -> accessedWithinWindow.contains(DatabaseTableName.of(p.first, p.second)))
                    .collect(Collectors.toList());
        }
        return result;
    }

    private void runCleanupForCatalog(IcebergMaintenanceInfo info, List<Pair<String, String>> tableNames) {
        List<Future<?>> futures = Lists.newArrayListWithCapacity(tableNames.size());
        for (Pair<String, String> name : tableNames) {
            Future<?> future = maintenanceExecutor.submit(() -> {
                ConnectContext taskCtx = new ConnectContext();
                try {
                    org.apache.iceberg.Table table = info.catalog.getTable(taskCtx, name.first, name.second);
                    if (table == null || table.currentSnapshot() == null) {
                        return;
                    }
                    runExpireSnapshots(info.catalog, table, info.hdfsEnvironment);
                    runRemoveOrphanFiles(info.catalog, table, info.hdfsEnvironment);
                } catch (Exception e) {
                    LOG.warn("Auto maintenance cleanup failed on {}.{}.{}: {}",
                            info.catalogName, name.first, name.second, e.getMessage(), e);
                }
            });
            futures.add(future);
        }
        // Wait for all cleanup tasks for this catalog to complete to avoid unbounded
        // task accumulation across daemon cycles while still leveraging thread pool concurrency.
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.warn("Unexpected exception while waiting for cleanup task completion", e);
            }
        }
    }

    private void runRewriteManifestsForCatalog(IcebergMaintenanceInfo info, List<Pair<String, String>> tableNames) {
        List<Future<?>> futures = Lists.newArrayListWithCapacity(tableNames.size());
        for (Pair<String, String> name : tableNames) {
            Future<?> future = maintenanceExecutor.submit(() -> {
                ConnectContext taskCtx = new ConnectContext();
                try {
                    org.apache.iceberg.Table table = info.catalog.getTable(taskCtx, name.first, name.second);
                    if (table == null || table.currentSnapshot() == null) {
                        return;
                    }
                    runRewriteManifests(info.catalog, table, info.hdfsEnvironment);
                } catch (Exception e) {
                    LOG.warn("Auto maintenance rewrite_manifests failed on {}.{}.{}: {}",
                            info.catalogName, name.first, name.second, e.getMessage(), e);
                }
            });
            futures.add(future);
        }
        // Wait for all rewrite manifests tasks for this catalog to complete.
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                LOG.warn("Unexpected exception while waiting for rewrite_manifests task completion", e);
            }
        }
    }

    private void runExpireSnapshots(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                    HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        ExpireSnapshotsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }

    private void runRemoveOrphanFiles(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                      HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        RemoveOrphanFilesProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }

    private void runRewriteManifests(IcebergCatalog catalog, org.apache.iceberg.Table table,
                                    HdfsEnvironment hdfsEnvironment) {
        Transaction txn = table.newTransaction();
        IcebergTableProcedureContext procedureContext = new IcebergTableProcedureContext(
                catalog, table, null, txn, hdfsEnvironment, null, null);
        RewriteManifestsProcedure.getInstance().execute(procedureContext, Collections.emptyMap());
    }
}
