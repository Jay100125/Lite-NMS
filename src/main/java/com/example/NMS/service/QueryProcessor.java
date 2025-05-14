package com.example.NMS.service;

import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;

public class QueryProcessor
{
  public static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessor.class);

  /**
   * Execute a single database query and return a Future with the result.
   *
   * @param query The query JSON object with "query" and "params" fields
   * @return Future containing the query result
   */
  public static Future<JsonArray> executeQuery(JsonObject query)
  {
    return Future.future(promise ->
    {
      try
      {
        vertx.eventBus().<JsonArray>request(DB_EXECUTE_QUERY, query, queryResult ->
        {
          if (queryResult.succeeded())
          {
            var result = queryResult.result().body();

            LOGGER.info("Database query executed: {}", query);

            promise.complete(result);
          }
          else
          {
            LOGGER.error("Database query failed: {}", queryResult.cause().getMessage());

            promise.fail(queryResult.cause());
          }
        });
      }
      catch (Exception exception)
      {
        LOGGER.error("Unexpected error executing query: {}", exception.getMessage(), exception);

        promise.fail("Unexpected error executing query: " + exception.getMessage());
      }
    });
  }


  /**
   * Execute a batch database query and return a Future with the result.
   *
   * @param batchQuery The batch query JSON object with "query" and "batchParams" fields
   * @return Future containing the batch query result
   */
  public static Future<JsonArray> executeBatchQuery(JsonObject batchQuery)
  {
    return Future.future(promise ->
    {
      try
      {
        vertx.eventBus().<JsonArray>request(DB_EXECUTE_BATCH_QUERY, batchQuery, queryResult ->
        {
          if (queryResult.succeeded())
          {
            var result = queryResult.result().body();

            LOGGER.info("Batch query executed: {}", batchQuery.getString(QUERY));

            promise.complete(result);
          }
          else
          {
            LOGGER.error("Batch query failed: {}", queryResult.cause().getMessage());

            promise.fail(queryResult.cause());
          }
        });
      }
      catch (Exception exception)
      {
        LOGGER.error("Unexpected error executing batch query: {}", exception.getMessage());

        promise.fail("Unexpected error executing batch query: " + exception.getMessage());
      }
    });
  }
}

