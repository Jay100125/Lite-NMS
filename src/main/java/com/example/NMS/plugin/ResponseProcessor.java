package com.example.NMS.plugin;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

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

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().<JsonObject>localConsumer(STORAGE_RESULTS, message ->
        {
            var data = message.body();

            var requestType = data.getString(REQUEST_TYPE);

            if (DISCOVERY.equals(requestType))
            {
                storeDiscoveryResults(data);
            }
            else if (POLLING.equals(requestType))
            {
                pollResultsBuffer.add(data);

                if (pollResultsBuffer.size() >= BATCH_SIZE)
                {
                    storePollResults(new JsonObject().put("results", pollResultsBuffer));

                    pollResultsBuffer.clear();
                }
            }
            else
            {
                LOGGER.error("Unknown request type: {}", Optional.ofNullable(requestType));
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
                var jobId = resultObj.getLong(PROVISIONING_JOB_ID);

                var metricsData = resultObj.getJsonObject("data");

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
        var queryParams = new JsonArray()
            .add(data.getInteger(DISCOVERY_ID))
            .add(data.getString(IP))
            .add(data.getInteger(PORT))
            .add(data.getString(STATUS))
            .add(data.getString(RESULT))
            .add(data.getValue(CREDENTIAL_ID));

        LOGGER.info("Storing discovery results: {}", queryParams.encodePrettily());

        var query = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
            .put(PARAMS, queryParams);

        vertx.eventBus().send(DB_EXECUTE_QUERY, query);
    }
}
