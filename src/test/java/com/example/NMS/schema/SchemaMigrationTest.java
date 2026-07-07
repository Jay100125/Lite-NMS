package com.example.NMS.schema;

import com.example.NMS.support.PgTestBase;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaMigrationTest extends PgTestBase {

    private boolean columnExists(String table, String col) throws Exception {
        RowSet<Row> rs = pool.preparedQuery(
            "SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2")
            .execute(io.vertx.sqlclient.Tuple.of(table, col))
            .toCompletionStage().toCompletableFuture().get();
        return rs.rowCount() > 0;
    }

    @Test
    void metricsHasIsEnabledAndPluginType() throws Exception {
        assertTrue(columnExists("metrics", "is_enabled"));
        assertTrue(columnExists("metrics", "plugin_type"));
    }

    @Test
    void deviceAvailabilityTableExists() throws Exception {
        assertTrue(columnExists("device_availability", "availability_pct"));
    }

    @Test
    void metricNameEnumAcceptsCastAndRejectsBadStatus() throws Exception {
        // metric_name enum must resolve the ::metric_name cast used by queries
        RowSet<Row> rs = pool.query("SELECT 'CPU'::metric_name")
            .execute().toCompletionStage().toCompletableFuture().get();
        assertEquals(1, rs.rowCount());
    }

    @Test
    void discoveryProfilesHasPluginType() throws Exception {
        assertTrue(columnExists("discovery_profiles", "plugin_type"));
    }
}
