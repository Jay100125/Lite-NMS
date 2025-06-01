package com.example.NMS.utility;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;

public class DBUtils
{
    public static final Logger LOGGER = LoggerFactory.getLogger(DBUtils.class);

  /**
   * Execute a single database query and return a Future with the result.
   *
   * @param query The query JSON object with "query" and "params" fields
   * @return Future containing the query result
   */
    public static Future<JsonArray> executeQuery(JsonObject query)
    {
        try
        {
            return  vertx.eventBus().<JsonArray>request(DB_EXECUTE_QUERY, query)
                   .map(message ->
                   {
                       var result = message.body();

                       LOGGER.info("Database query executed: {}", query);

                       return result;
                   })
                   .onFailure(error -> LOGGER.error("Database query failed: {}", error.getMessage(), error));
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error executing query: {}", exception.getMessage(), exception);

            return Future.failedFuture("Unexpected error executing query: " + exception.getMessage());
        }

    }


  /**
   * Execute a batch database query and return a Future with the result.
   *
   * @param batchQuery The batch query JSON object with "query" and "batchParams" fields
   * @return Future containing the batch query result
   */
    public static Future<JsonArray> executeBatchQuery(JsonObject batchQuery)
    {
        try
        {
            return vertx.eventBus().<JsonArray>request(DB_EXECUTE_BATCH_QUERY, batchQuery)
                .map(queryResult ->
                {
                    var result = queryResult.body();

                    LOGGER.info("Batch query executed: {}", batchQuery.getString(QUERY));

                    return result;
                })
                .onFailure(error -> LOGGER.error("Batch query failed: {}", error.getMessage(), error));
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error executing batch query: {}", exception.getMessage());

            return Future.failedFuture("Unexpected error executing batch query: " + exception.getMessage());
        }

    }
}

