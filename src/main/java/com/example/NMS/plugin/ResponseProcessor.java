package com.example.NMS.plugin;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.DBUtils;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.eventbus.Message;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;

/**
 * Vert.x verticle for storing polling and discovery results in the database.
 * Listens for plugin results on the event bus and performs batch inserts into the database.
 */
public class ResponseProcessor extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ResponseProcessor.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().localConsumer(STORAGE_POLL_RESULTS, this::storePollResults);

        vertx.eventBus().localConsumer(STORAGE_DISCOVERY_RESULTS, this::storeDiscoveryResults);

        LOGGER.info("ResponseProcessor deployed");

        startPromise.complete();
    }

    private void storePollResults(Message<JsonObject> message)
    {
        var data = message.body();

        var results = data.getJsonArray("results");

        if (results == null || results.isEmpty())
        {
            LOGGER.info("No polling results to store ");

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



    private void storeDiscoveryResults(Message<JsonObject> message)
    {
        var data  = message.body();

        var discoveryResults = data.getJsonArray("results");

        var discoveryId = data.getLong(ID, -1L);

        if (discoveryResults == null || discoveryResults.isEmpty())
        {
            LOGGER.info("No discovery results to store for discovery_id={}", discoveryId);

            return;
        }

        var batchParams = new JsonArray();

        for (int i = 0; i < discoveryResults.size(); i++)
        {
            var result = discoveryResults.getJsonObject(i);

            batchParams.add(new JsonArray()
                .add(discoveryId)
                .add(result.getString(IP))
                .add(result.getInteger(PORT))
                .add(result.getString(STATUS))
                .add(result.getString("result"))
                .add(result.getValue("credential_id")));
        }

        var query = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
            .put(BATCHPARAMS, batchParams);

        vertx.eventBus().send(DB_EXECUTE_BATCH_QUERY, query);
    }
}
