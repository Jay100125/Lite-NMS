package com.example.NMS.availability;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.support.PgTestBase;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the device_availability upsert SQL (QueryConstant.UPSERT_AVAILABILITY).
 * Exercises the exact parameter order/types the Availability verticle builds when it publishes a
 * sample: $1 = provisioning_job_id, $2 = up increment (1 on success, 0 on failure), $3 = is_up
 * (boolean of the current sample). This validates the column names and param indices in the SQL
 * that are otherwise only exercised at runtime via the event bus.
 */
class AvailabilityDbTest extends PgTestBase {

    @Test
    void upsertAccumulatesSamplesAndTracksLatestState() throws Exception {
        // Minimal FK chain required by device_availability: credential_profile -> provisioning_jobs
        var credentialId = pool.preparedQuery(
                "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) RETURNING id")
            .execute(Tuple.of("avail-test-cred", "LINUX", "unused-cred-data"))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next().getInteger("id");

        var jobId = pool.preparedQuery(
                "INSERT INTO provisioning_jobs (credential_profile_id, plugin_type, ip, port) VALUES ($1, $2, $3, $4) RETURNING id")
            .execute(Tuple.of(credentialId, "LINUX", "10.0.0.1", 22))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next().getInteger("id");

        // Sample 1: success -> up indicator 1, is_up true (same Tuple shape Availability.java builds)
        pool.preparedQuery(QueryConstant.UPSERT_AVAILABILITY)
            .execute(Tuple.of((long) jobId, 1, true))
            .toCompletionStage().toCompletableFuture().get();

        // Sample 2: failure -> up indicator 0, is_up false
        pool.preparedQuery(QueryConstant.UPSERT_AVAILABILITY)
            .execute(Tuple.of((long) jobId, 0, false))
            .toCompletionStage().toCompletableFuture().get();

        Row row = pool.preparedQuery("SELECT * FROM device_availability WHERE provisioning_job_id = $1")
            .execute(Tuple.of(jobId))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next();

        assertEquals(2L, row.getLong("total_samples"));
        assertEquals(1L, row.getLong("up_samples"));
        assertEquals(0, row.getBigDecimal("availability_pct").compareTo(new BigDecimal("50.00")));
        assertFalse(row.getBoolean("is_up"));
    }
}
