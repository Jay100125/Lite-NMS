package com.example.NMS.service;

import com.example.NMS.MetricJobCache;
import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.Utility.*;

public class QueryProcessor
{
  public static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

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
      vertx.eventBus().<JsonObject>request(EVENTBUS_ADDRESS, query, ar -> {
        if (ar.succeeded())
        {
          var result = ar.result().body();

          logger.info("Database query executed: {}", query);

          logger.info("Database query result: {}", result);

          promise.complete(result);
        }
        else
        {
          logger.error("Database query failed: {}", ar.cause().getMessage());
          promise.fail(ar.cause());
        }
      });
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
      vertx.eventBus().request(EVENTBUS_BATCH_ADDRESS, batchQuery, ar ->
      {
        if (ar.succeeded())
        {
          var result = (JsonObject) ar.result().body();

          logger.info("Batch query executed: {}", batchQuery.getString("query"));

          logger.info("Batch query result: {}", result);

          promise.complete(result);
        }
        else
        {
          logger.error("Batch query failed: {}", ar.cause().getMessage());

          promise.fail(ar.cause());
        }
      });
    });
  }
}

