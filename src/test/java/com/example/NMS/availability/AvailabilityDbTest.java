package com.example.NMS.availability;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.support.PgTestBase;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;

import static com.example.NMS.constant.Constant.AVAILABILITY_DOWN_THRESHOLD;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration test for the ping-driven device_availability upsert
 * (QueryConstant.UPSERT_AVAILABILITY). Exercises the exact parameter order/types the
 * Availability verticle builds: $1 = provisioning_job_id, $2 = is_up sample (boolean of
 * the latest ping), $3 = down threshold. Verifies the flap-damping state machine: a
 * device only flips Up→Down after {@code threshold} consecutive failed pings, and any
 * success resets the counter and marks it Up.
 */
class AvailabilityDbTest extends PgTestBase {

    // Unique per insert so tests sharing the DB don't collide on unique keys.
    private static final java.util.concurrent.atomic.AtomicInteger SEQ = new java.util.concurrent.atomic.AtomicInteger();

    private long insertJob() throws Exception {
        var n = SEQ.incrementAndGet();

        var credentialId = pool.preparedQuery(
                "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) RETURNING id")
            .execute(Tuple.of("avail-test-cred-" + n, "LINUX", "unused-cred-data"))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next().getInteger("id");

        return pool.preparedQuery(
                "INSERT INTO provisioning_jobs (credential_profile_id, plugin_type, ip, port) VALUES ($1, $2, $3, $4) RETURNING id")
            .execute(Tuple.of(credentialId, "LINUX", "10.0.0." + n, 22))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next().getInteger("id");
    }

    private void sample(long jobId, boolean isUp) throws Exception {
        pool.preparedQuery(QueryConstant.UPSERT_AVAILABILITY)
            .execute(Tuple.of(jobId, isUp, AVAILABILITY_DOWN_THRESHOLD))
            .toCompletionStage().toCompletableFuture().get();
    }

    private Row fetch(long jobId) throws Exception {
        return pool.preparedQuery("SELECT * FROM device_availability WHERE provisioning_job_id = $1")
            .execute(Tuple.of(jobId))
            .toCompletionStage().toCompletableFuture().get()
            .iterator().next();
    }

    @Test
    void successMarksUpAndResetsFailures() throws Exception {
        var jobId = insertJob();

        sample(jobId, true);

        var row = fetch(jobId);
        assertTrue(row.getBoolean("is_up"));
        assertEquals(1L, row.getLong("up_samples"));
        assertEquals(1L, row.getLong("total_samples"));
        assertEquals(0, row.getInteger("consecutive_failures"));
        assertEquals(0, row.getBigDecimal("availability_pct").compareTo(new BigDecimal("100.00")));
    }

    @Test
    void staysUpUntilThresholdThenFlipsDownAndRecovers() throws Exception {
        var jobId = insertJob();

        // Establish an Up baseline.
        sample(jobId, true);

        // Misses below the threshold must NOT flip the device Down (flap damping).
        for (int i = 1; i < AVAILABILITY_DOWN_THRESHOLD; i++) {
            sample(jobId, false);
            var row = fetch(jobId);
            assertTrue(row.getBoolean("is_up"), i + " consecutive miss(es) < threshold should stay Up");
            assertEquals(i, row.getInteger("consecutive_failures"));
        }

        // The threshold-th consecutive miss flips it Down.
        sample(jobId, false);
        var down = fetch(jobId);
        assertFalse(down.getBoolean("is_up"), "threshold consecutive misses should flip Down");
        assertEquals(AVAILABILITY_DOWN_THRESHOLD, down.getInteger("consecutive_failures"));

        // A single success recovers immediately and clears the counter.
        sample(jobId, true);
        var up = fetch(jobId);
        assertTrue(up.getBoolean("is_up"), "a successful ping should mark it Up again");
        assertEquals(0, up.getInteger("consecutive_failures"));
        // total = 1 up + 3 down + 1 up = 5 samples, 2 of them up -> 40.00%
        assertEquals(5L, up.getLong("total_samples"));
        assertEquals(2L, up.getLong("up_samples"));
        assertEquals(0, up.getBigDecimal("availability_pct").compareTo(new BigDecimal("40.00")));
    }
}
