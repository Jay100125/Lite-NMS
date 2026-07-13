package com.example.NMS.availability;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;

/**
 * Tracks per-device up/down state and rolling uptime percentage.
 * Consumes one availability sample per poll result (published by ResponseProcessor
 * on {@link com.example.NMS.constant.Constant#AVAILABILITY_SAMPLE}, kept separate from
 * STORAGE_RESULTS so it doesn't compete with ResponseProcessor's point-to-point consumer
 * for the same messages) and upserts device_availability.
 */
public class Availability extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Availability.class);

    /** Uptime percentage; 0 when no samples yet. */
    public static double recompute(long upSamples, long totalSamples)
    {
        if (totalSamples <= 0) return 0.0;

        return (upSamples * 100.0) / totalSamples;
    }

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().<JsonObject>consumer(AVAILABILITY_SAMPLE, message ->
        {
            var data = message.body();

            var jobId = data.getLong(JOB_ID);

            if (jobId == null)
            {
                LOGGER.warn("Availability sample missing job_id: {}", data.encode());

                return;
            }

            var isUp = SUCCESS.equals(data.getString(STATUS));

            // $1 job_id, $2 is_up sample, $3 down threshold — the SQL applies the
            // consecutive-failure damping so is_up doesn't flap on a single miss.
            var query = new JsonObject()
                .put(QUERY, QueryConstant.UPSERT_AVAILABILITY)
                .put(PARAMS, new JsonArray().add(jobId).add(isUp).add(AVAILABILITY_DOWN_THRESHOLD));

            vertx.eventBus().send(DB_EXECUTE_QUERY, query);
        });

        LOGGER.info("Availability deployed");

        startPromise.complete();
    }
}
