//package com.example.NMS.plugin;
//
//import com.example.NMS.constant.QueryConstant;
//import com.example.NMS.utility.DBUtils;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Promise;
//import io.vertx.core.eventbus.Message;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static com.example.NMS.constant.Constant.*;
//
///**
// * Vert.x verticle for storing polling and discovery results in the database.
// * Listens for plugin results on the event bus and performs batch inserts into the database.
// */
//public class ResponseProcessor extends AbstractVerticle
//{
//    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseProcessor.class);
//
//    @Override
//    public void start(Promise<Void> startPromise)
//    {
//        vertx.eventBus().localConsumer(STORAGE_POLL_RESULTS, this::storePollResults);
//
//        vertx.eventBus().localConsumer(STORAGE_DISCOVERY_RESULTS, this::storeDiscoveryResults);
//
//        LOGGER.info("ResponseProcessor deployed");
//
//        startPromise.complete();
//    }
//
//    private void storePollResults(Message<JsonObject> message)
//    {
//        var data = message.body();
//
//        var results = data.getJsonArray("results");
//
//        if (results == null || results.isEmpty())
//        {
//            LOGGER.info("No polling results to store ");
//
//            return;
//        }
//
//        var batchParams = new JsonArray();
//
//        results.forEach(result ->
//        {
//            var resultObj = (JsonObject) result;
//
//            if (SUCCESS.equals(resultObj.getString(STATUS)))
//            {
//                var jobId = resultObj.getLong(PROVISIONING_JOB_ID);
//
//                var metricsData = resultObj.getJsonObject("data");
//
//                LOGGER.info("Storing polling data: {}", metricsData);
//
//                if (metricsData != null)
//                {
//                    metricsData.fieldNames().forEach(metric ->
//                        batchParams.add(new JsonArray()
//                            .add(jobId)
//                            .add(metric)
//                            .add(metricsData.getJsonObject(metric))));
//                }
//            }
//            else
//            {
//                LOGGER.info("Skipping failed polling result: {}", resultObj.encodePrettily());
//            }
//        });
//
//        var batchQuery = new JsonObject()
//            .put(QUERY, QueryConstant.INSERT_POLLED_DATA)
//            .put(BATCHPARAMS, batchParams);
//
//        vertx.eventBus().send(DB_EXECUTE_BATCH_QUERY, batchQuery);
//    }
//
//
//
//    private void storeDiscoveryResults(Message<JsonObject> message)
//    {
//        var data  = message.body();
//
//        var discoveryResults = data.getJsonArray("results");
//
//        if (discoveryResults == null || discoveryResults.isEmpty())
//        {
//            LOGGER.info("No discovery results to store");
//
//            return;
//        }
//
//        var batchParams = new JsonArray();
//
//        for (int i = 0; i < discoveryResults.size(); i++)
//        {
//            var result = discoveryResults.getJsonObject(i);
//
//            batchParams.add(new JsonArray()
//                .add(result.getInteger("discovery_id"))
//                .add(result.getString(IP))
//                .add(result.getInteger(PORT))
//                .add(result.getString(STATUS))
//                .add(result.getString("result"))
//                .add(result.getValue("credential_id")));
//        }
//
//        var query = new JsonObject()
//            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
//            .put(BATCHPARAMS, batchParams);
//
//        vertx.eventBus().send(DB_EXECUTE_BATCH_QUERY, query);
//    }
//}

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

    private static final long BATCH_TIMEOUT_MS = 60_000; // 1 minute

    private final JsonArray pollResultsBuffer = new JsonArray();

    private final JsonArray discoveryResultsBuffer = new JsonArray();

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
                discoveryResultsBuffer.add(data);

                if (discoveryResultsBuffer.size() >= BATCH_SIZE)
                {
                    storeDiscoveryResults(new JsonObject().put("results", discoveryResultsBuffer));

                    discoveryResultsBuffer.clear();
                }
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

        timerId = vertx.setPeriodic(BATCH_TIMEOUT_MS, id ->
        {
            if (!pollResultsBuffer.isEmpty())
            {
                storePollResults(new JsonObject().put("results", pollResultsBuffer));

                pollResultsBuffer.clear();
            }
            if (!discoveryResultsBuffer.isEmpty())
            {
                storeDiscoveryResults(new JsonObject().put("results", discoveryResultsBuffer));

                discoveryResultsBuffer.clear();
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

                LOGGER.info("Storing polling data: {}", metricsData);

                if (metricsData != null)
                {
                    metricsData.fieldNames().forEach(metric ->
                        batchParams.add(new JsonArray()
                            .add(jobId)
                            .add(metric)
                            .add(metricsData.getJsonObject(metric))));
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
        var results = data.getJsonArray("results");

        if (results == null || results.isEmpty()) {
            LOGGER.info("No discovery results to store");
            return;
        }

        var batchParams = new JsonArray();

        results.forEach(result ->
        {
            var resultObj = (JsonObject) result;

            batchParams.add(new JsonArray()
                .add(resultObj.getInteger("discovery_id"))
                .add(resultObj.getString(IP))
                .add(resultObj.getInteger(PORT))
                .add(resultObj.getString(STATUS))
                .add(resultObj.getString("result"))
                .add(resultObj.getValue("credential_id")));
        });

        var query = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
            .put(BATCHPARAMS, batchParams);

        vertx.eventBus().send(DB_EXECUTE_BATCH_QUERY, query);
    }
}
