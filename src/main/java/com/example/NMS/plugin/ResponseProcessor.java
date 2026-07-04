package com.example.NMS.plugin;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;

/**
 * Vert.x verticle for storing polling and discovery results in the database.
 * Listens for plugin results on a single event bus address and batches results for database storage.
 */
public class ResponseProcessor extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseProcessor.class);

    private static final long BATCH_TIMEOUT_MS = 15_000; // 30 seconds

    private final JsonArray pollResultsBuffer = new JsonArray();

    private long timerId = -1;

    /** Routing decision based on the always-present event_type discriminator. */
    public static String classify(JsonObject result)
    {
        var eventType = result.getString(EVENT_TYPE);

        if (EVENT_POLL.equals(eventType))
        {
            return EVENT_POLL;
        }

        if (EVENT_DISCOVERY.equals(eventType))
        {
            return EVENT_DISCOVERY;
        }

        return "unknown";
    }

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().<JsonObject>localConsumer(STORAGE_RESULTS, message ->
        {
            var data = message.body();

            switch (classify(data))
            {
                case EVENT_DISCOVERY -> storeDiscoveryResults(data);
                case EVENT_POLL ->
                {
                    bufferPollResult(data);

                    publishAvailabilitySample(data);
                }
                default -> LOGGER.error("Unknown event_type on result: {}", data.getString(REQUEST_ID));
            }
        });

        vertx.eventBus().<JsonObject>localConsumer(EVENT_COMPLETION, message ->
            {
                var data = message.body();

                var requestType = data.getString(REQUEST_TYPE);

                if (DISCOVERY.equals(requestType))
                {
                    var discoveryId = data.getInteger(DISCOVERY_ID);

                    var query = new JsonObject()
                        .put(QUERY, QueryConstant.UPDATE_DISCOVERY_PROFILE_STATUS)
                        .put(PARAMS, new JsonArray().add(DISCOVERY_STATUS_COMPLETED).add(discoveryId));

                    vertx.eventBus().send(DB_EXECUTE_QUERY, query);
                }

            });


        timerId = vertx.setPeriodic(BATCH_TIMEOUT_MS, id ->
        {
            if (!pollResultsBuffer.isEmpty())
            {
                storePollResults(new JsonObject().put("results", pollResultsBuffer));

                pollResultsBuffer.clear();
            }
        });

        LOGGER.info("ResponseProcessor deployed with batching");

        startPromise.complete();
    }

    @Override
    public void stop(Promise<Void> stopPromise)
    {
        if (timerId != -1)
        {
            vertx.cancelTimer(timerId);
        }
        stopPromise.complete();
    }

    /** Publishes a single availability sample per poll result, kept separate from the STORAGE_RESULTS batching consumer. */
    private void publishAvailabilitySample(JsonObject data)
    {
        vertx.eventBus().publish(AVAILABILITY_SAMPLE, new JsonObject()
            .put(JOB_ID, data.getLong(JOB_ID))
            .put(STATUS, data.getString(STATUS)));
    }

    private void bufferPollResult(JsonObject data)
    {
        pollResultsBuffer.add(data);

        if (pollResultsBuffer.size() >= BATCH_SIZE)
        {
            storePollResults(new JsonObject().put("results", pollResultsBuffer));

            pollResultsBuffer.clear();
        }
    }

    private void storePollResults(JsonObject data)
    {
        var results = data.getJsonArray("results");

        if (results == null || results.isEmpty())
        {
            LOGGER.info("No polling results to store");

            return;
        }

        var batchParams = new JsonArray();

        results.forEach(result ->
        {
            var resultObj = (JsonObject) result;

            if (SUCCESS.equals(resultObj.getString(STATUS)))
            {
                var jobId = resultObj.getLong(JOB_ID);

                var metricsData = resultObj.getJsonObject(RESULT);

                var timestamp = resultObj.getLong("timestamp");

                LOGGER.info("Storing polling data: {}", metricsData);

                if (metricsData != null)
                {
                    metricsData.fieldNames().forEach(metric ->
                        batchParams.add(new JsonArray()
                            .add(jobId)
                            .add(metric)
                            .add(metricsData.getJsonObject(metric))
                            .add(timestamp)));
                }
            }
            else
            {
                LOGGER.info("Skipping failed polling result: {}", resultObj.encodePrettily());
            }
        });


        var batchQuery = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_POLLED_DATA)
            .put(BATCHPARAMS, batchParams);

        vertx.eventBus().send(DB_EXECUTE_BATCH_QUERY, batchQuery);

    }

    private void storeDiscoveryResults(JsonObject data)
    {
        var status = data.getString(STATUS);

        // Canonical discovery_result vocabulary is COMPLETED|FAILED (Task 2 schema);
        // the engine's own status field remains success|failed.
        var discoveryResult = SUCCESS.equals(status) ? "COMPLETED" : "FAILED";

        var queryParams = new JsonArray()
            .add(data.getInteger(DISCOVERY_ID))
            .add(data.getString(IP))
            .add(data.getInteger(PORT))
            .add(discoveryResult)
            .add(data.getString(RESULT))
            .add(data.getValue(CREDENTIAL_ID));

        LOGGER.info("Storing discovery results: {}", queryParams.encodePrettily());

        var query = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
            .put(PARAMS, queryParams);

        vertx.eventBus().send(DB_EXECUTE_QUERY, query);
    }
}
