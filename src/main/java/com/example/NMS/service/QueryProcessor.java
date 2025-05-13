package com.example.NMS.service;

import io.vertx.core.Future;
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
  public static Future<JsonObject> executeQuery(JsonObject query)
  {
    return Future.future(promise ->
    {
      try
      {
        vertx.eventBus().<JsonObject>request(DB_EXECUTE_QUERY, query, ar ->
        {
          if (ar.succeeded())
          {
            var result = ar.result().body();

            LOGGER.info("Database query executed: {}", query);

//            LOGGER.info("Database query result: {}", result);

            promise.complete(result);
          }
          else
          {
            LOGGER.error("Database query failed: {}", ar.cause().getMessage());

            promise.fail(ar.cause());
          }
        });
      }
      catch (Exception e)
      {
        LOGGER.error("Unexpected error executing query: {}", e.getMessage(), e);

        promise.fail("Unexpected error executing query: " + e.getMessage());
      }
    });
  }


  /**
   * Execute a batch database query and return a Future with the result.
   *
   * @param batchQuery The batch query JSON object with "query" and "batchParams" fields
   * @return Future containing the batch query result
   */
  public static Future<JsonObject> executeBatchQuery(JsonObject batchQuery)
  {
    return Future.future(promise ->
    {
      try
      {
        vertx.eventBus().request(DB_EXECUTE_BATCH_QUERY, batchQuery, ar ->
        {
          if (ar.succeeded())
          {
            var result = (JsonObject) ar.result().body();

            LOGGER.info("Batch query executed: {}", batchQuery.getString("query"));

//            LOGGER.info("Batch query result: {}", result);

            promise.complete(result);
          }
          else
          {
            LOGGER.error("Batch query failed: {}", ar.cause().getMessage());

            promise.fail(ar.cause());
          }
        });
      }
      catch (Exception e)
      {
        LOGGER.error("Unexpected error executing batch query: {}", e.getMessage(), e);

        promise.fail("Unexpected error executing batch query: " + e.getMessage());
      }
    });
  }
}

