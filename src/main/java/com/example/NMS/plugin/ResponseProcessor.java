package com.example.NMS.plugin;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.discovery.DiscoveryRequestId;
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

                // The envelope carries event_type ("discovery"|"poll"); only discovery marks a profile COMPLETED.
                if (EVENT_DISCOVERY.equals(data.getString(EVENT_TYPE)))
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

        Integer discoveryId;
        String ip;
        Integer port;
        Object credentialId;
        String msg;

        // v2 engine result lines omit ip/discovery_id; recover them from the echoed request_id.
        var context = DiscoveryRequestId.decode(data.getString(REQUEST_ID));

        if (context != null)
        {
            discoveryId = context.getInteger("discovery_id");
            ip = context.getString("ip");
            port = context.getInteger("port");
            credentialId = context.getInteger("credential_id");
            msg = SUCCESS.equals(status) ? "Discovery succeeded" : data.getString(ERROR, "Discovery failed");
        }
        else
        {
            // Direct result: Discovery short-circuits an unreachable IP straight to storage (no engine run).
            discoveryId = data.getInteger(DISCOVERY_ID);
            ip = data.getString(IP);
            port = data.getInteger(PORT);
            credentialId = data.getValue(CREDENTIAL_ID);
            msg = data.getString(RESULT);
        }

        // Canonical discovery_result vocabulary is COMPLETED|FAILED (Task 2 schema);
        // the engine's own status field remains success|failed.
        var discoveryResult = SUCCESS.equals(status) ? "COMPLETED" : "FAILED";

        var queryParams = new JsonArray()
            .add(discoveryId)
            .add(ip)
            .add(port)
            .add(discoveryResult)
            .add(msg)
            .add(credentialId);

        LOGGER.info("Storing discovery result: discovery_id={}, ip={}, result={}", discoveryId, ip, discoveryResult);

        var query = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
            .put(PARAMS, queryParams);

        vertx.eventBus().send(DB_EXECUTE_QUERY, query);
    }
}
