//package com.example.NMS.database;
//
//import io.vertx.core.*;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import io.vertx.pgclient.PgBuilder;
//import io.vertx.pgclient.PgConnectOptions;
//import io.vertx.sqlclient.SqlClient;
//import io.vertx.sqlclient.PoolOptions;
//import io.vertx.sqlclient.Tuple;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class Database extends AbstractVerticle {
//
//  private static final Logger logger = LoggerFactory.getLogger(Database.class);
//
//  public static final String DB_HOST = "localhost";
//  public static final int DB_PORT = 5432;
//  public static final String DB_NAME = "nms";
//  public static final String DB_USER = "jay";
//  public static final String DB_PASSWORD = "Mind@123";
//
//  private static SqlClient client;
//
//  @Override
//  public void start(Promise<Void> startPromise) {
//    var connectOptions = new PgConnectOptions()
//      .setHost(DB_HOST)
//      .setPort(DB_PORT)
//      .setDatabase(DB_NAME)
//      .setUser(DB_USER)
//      .setPassword(DB_PASSWORD);
//
//    var poolOptions = new PoolOptions().setMaxSize(10);
//
//    var sqlClient = PgBuilder
//      .client()
//      .with(poolOptions)
//      .connectingTo(connectOptions)
//      .using(vertx)
//      .build();
//
//    client = sqlClient; // Assign to static field if you need global access
//
//    startPromise.complete();
//
//
//    vertx.eventBus().consumer("database", message -> {
//
//      var input = (JsonObject) message.body();
//
//      try {
//
//        var query = input.getString("query");
//
////        if ((query.trim().toLowerCase().startsWith("insert") || query.trim().toLowerCase().startsWith("update") && !query.toLowerCase().contains("returning")))
////        {
////          query += "returning id";
////        }
//
//        var paramArray = input.getJsonArray("params", new JsonArray());
//
//        var params = Tuple.tuple();
//
//        for (int i = 0; i < paramArray.size(); i++)
//        {
//          params.addValue(paramArray.getValue(i));
//        }
//
//        logger.info("Executing Query: {}",query);
//
//        client.preparedQuery(query).execute(params, ar ->
//        {
//          if (ar.succeeded())
//          {
//            var rows = ar.result();
//
//            var jsonRows = new JsonArray();
//
//            rows.forEach(row ->
//            {
//              var obj = new JsonObject();
//
//              for (int i = 0; i < row.size(); i++)
//              {
//                obj.put(row.getColumnName(i), row.getValue(i));
//              }
//
//              jsonRows.add(obj);
//            });
//
//            message.reply(new JsonObject()
//              .put("msg", "Success")
//              .put("result", jsonRows));
//          }
//          else
//          {
//            logger.error("❌ Query failed: {}", ar.cause().getMessage());
//
//            message.reply(new JsonObject()
//              .put("msg", "fail")
//              .put("ERROR", ar.cause().getMessage()));
//          }
//        });
//
//      }
//      catch (Exception exception)
//      {
//        message.reply(new JsonObject()
//          .put("SUCCESS", false)
//          .put("ERROR", exception.getMessage()));
//      }
//    });
//  }
//}

package com.example.NMS.database;

import com.example.NMS.constant.Constant;
import io.vertx.core.*;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import static com.example.NMS.constant.Constant.*;


public class Database extends AbstractVerticle {

  private static final Logger logger = LoggerFactory.getLogger(Database.class);


  private static SqlClient client;

  @Override
  public void start(Promise<Void> startPromise) {
    var connectOptions = new PgConnectOptions()
      .setHost(DB_HOST)
      .setPort(DB_PORT)
      .setDatabase(DB_NAME)
      .setUser(DB_USER)
      .setPassword(DB_PASSWORD);

    var poolOptions = new PoolOptions().setMaxSize(10);

    var sqlClient = PgBuilder
      .client()
      .with(poolOptions)
      .connectingTo(connectOptions)
      .using(vertx)
      .build();

    client = sqlClient;

    startPromise.complete();

    vertx.eventBus().consumer(EVENTBUS_ADDRESS, message -> {
      JsonObject input = (JsonObject) message.body();
      {
        // Process single query (existing code)
        var query = input.getString("query");
        var paramArray = input.getJsonArray("params", new JsonArray());
        var params = Tuple.tuple();

        for (int i = 0; i < paramArray.size(); i++) {
          Object value = paramArray.getValue(i);
          if (value instanceof JsonArray) {
            // If it's a JsonArray, convert it to a Java array (Long[])
            JsonArray jsonArray = (JsonArray) value;
            Long[] longs = new Long[jsonArray.size()];
            for (int j = 0; j < jsonArray.size(); j++) {
              longs[j] = Long.parseLong(jsonArray.getValue(j).toString());
            }
            params.addValue(longs); // ✅ Just use addValue with Long[]
          } else {
            params.addValue(value); // normal value
          }
        }


        client.preparedQuery(query).execute(params, ar -> {
          if (ar.succeeded()) {
            var rows = ar.result();

            var jsonRows = new JsonArray();

            rows.forEach(row -> {
              var obj = new JsonObject();
              for (int i = 0; i < row.size(); i++) {
                String columnName = row.getColumnName(i);
                Object columnValue = row.getValue(i);

                if (columnValue != null && columnValue.getClass().isArray()) {
                  // If it's an array, convert to JsonArray
                  Object[] array = (Object[]) columnValue;
                  JsonArray jsonArray = new JsonArray();
                  for (Object item : array) {
                    jsonArray.add(item);
                  }
                  obj.put(columnName, jsonArray);
                } else {
                  obj.put(columnName, columnValue);
                }
              }
              jsonRows.add(obj);
            });

            message.reply(new JsonObject()
              .put("msg", "Success")
              .put("result", jsonRows));
          } else {
            logger.error("❌ Query failed: {}", ar.cause().getMessage());
            message.reply(new JsonObject()
              .put("msg", "fail")
              .put("ERROR", ar.cause().getMessage()));
          }
        });
      }
    });

    // Consumer for batch inserts
    vertx.eventBus().consumer(EVENTBUS_BATCH_ADDRESS, message -> {
      JsonObject request = (JsonObject) message.body();
      String query = request.getString("query");
      JsonArray batchParams = request.getJsonArray("batchParams");

      if (query == null || batchParams == null || batchParams.isEmpty()) {
        logger.error("Invalid batch request: query={}, batchParams={}", query, batchParams);
        message.reply(new JsonObject()
          .put("msg", "Error")
          .put("ERROR", "Missing query or batchParams"));
        return;
      }

      // Prepare batch tuples
      List<Tuple> batch = new ArrayList<>();
      for (int i = 0; i < batchParams.size(); i++) {
        JsonArray params = batchParams.getJsonArray(i);
        Tuple tuple = Tuple.tuple();
        tuple.addLong(params.getLong(0)); // discovery_id
        tuple.addString(params.getString(1)); // ip
        tuple.addInteger(params.getInteger(2)); // port
        tuple.addString(params.getString(3)); // result
        tuple.addString(params.getString(4)); // msg (nullable)
        Object credId = params.getValue(5); // credential_profile_id (nullable)
        tuple.addLong(credId instanceof Number ? ((Number) credId).longValue() : null);
        batch.add(tuple);
      }

      logger.debug("Executing batch query: {}, tuples: {}", query, batch.size());
      client.preparedQuery(query)
        .executeBatch(batch)
        .onSuccess(result -> {
          logger.info("Batch insert executed, inserted {} rows", batch.size());
          JsonArray insertedIds = new JsonArray();
          result.forEach(row -> insertedIds.add(row.getLong("id")));
          message.reply(new JsonObject()
            .put("msg", "Success")
            .put("insertedIds", insertedIds));
        })
        .onFailure(err -> {
          logger.error("Batch insert failed: {}, error: {}", query, err.getMessage());
          message.reply(new JsonObject()
            .put("msg", "Error")
            .put("ERROR", err.getMessage()));
        });
    });
  }
}

//    vertx.eventBus().consumer(EVENTBUS_ADDRESS, message -> {
//      var input = (JsonObject) message.body();
//
//      try {
//        // Check if this is a batch request
//        if (input.containsKey("queries")) {
//          // Process batch queries
//          JsonArray queries = input.getJsonArray("queries");
//          Promise<JsonObject> batchPromise = Promise.promise();
//          processBatchQueries(queries, batchPromise);
//          batchPromise.future().onComplete(ar -> {
//            if (ar.succeeded()) {
//              message.reply(ar.result());
//            } else {
//              logger.error("❌ Batch query failed: {}", ar.cause().getMessage());
//              message.reply(new JsonObject()
//                .put("msg", "fail")
//                .put("ERROR", ar.cause().getMessage()));
//            }
//          });
//        } else
//        {
//          // Process single query (existing code)
//          var query = input.getString("query");
//          var paramArray = input.getJsonArray("params", new JsonArray());
//          var params = Tuple.tuple();
//
//          for (int i = 0; i < paramArray.size(); i++) {
//            Object value = paramArray.getValue(i);
//            if (value instanceof JsonArray) {
//              // If it's a JsonArray, convert it to a Java array (Long[])
//              JsonArray jsonArray = (JsonArray) value;
//              Long[] longs = new Long[jsonArray.size()];
//              for (int j = 0; j < jsonArray.size(); j++) {
//                longs[j] = Long.parseLong(jsonArray.getValue(j).toString());
//              }
//              params.addValue(longs); // ✅ Just use addValue with Long[]
//            } else {
//              params.addValue(value); // normal value
//            }
//          }
//
//
//          client.preparedQuery(query).execute(params, ar -> {
//            if (ar.succeeded()) {
//              var rows = ar.result();
//
//              var jsonRows = new JsonArray();
//
//              rows.forEach(row -> {
//                var obj = new JsonObject();
//                for (int i = 0; i < row.size(); i++) {
//                  String columnName = row.getColumnName(i);
//                  Object columnValue = row.getValue(i);
//
//                  if (columnValue != null && columnValue.getClass().isArray()) {
//                    // If it's an array, convert to JsonArray
//                    Object[] array = (Object[]) columnValue;
//                    JsonArray jsonArray = new JsonArray();
//                    for (Object item : array) {
//                      jsonArray.add(item);
//                    }
//                    obj.put(columnName, jsonArray);
//                  } else {
//                    obj.put(columnName, columnValue);
//                  }
//                }
//                jsonRows.add(obj);
//              });
//
//              message.reply(new JsonObject()
//                .put("msg", "Success")
//                .put("result", jsonRows));
//            } else {
//              logger.error("❌ Query failed: {}", ar.cause().getMessage());
//              message.reply(new JsonObject()
//                .put("msg", "fail")
//                .put("ERROR", ar.cause().getMessage()));
//            }
//          });
//        }
//      } catch (Exception exception) {
//        logger.error("❌ Database exception: {}", exception.getMessage());
//        message.reply(new JsonObject()
//          .put("msg", "fail")
//          .put("ERROR", exception.getMessage()));
//      }
//    });
//  }
//
//  /**
//   * Process a batch of queries and aggregate their results
//   * @param queries JsonArray containing query objects with "query" and "params" fields
//   * @param promise Promise to be completed with results or failed with an error
//   */
//  private void processBatchQueries(JsonArray queries, Promise<JsonObject> promise) {
//    List<Future> futures = new ArrayList<>();
//
//    // Create a future for each query
//    for (int i = 0; i < queries.size(); i++) {
//      JsonObject queryObj = queries.getJsonObject(i);
//      String query = queryObj.getString("query");
//      JsonArray params = queryObj.getJsonArray("params", new JsonArray());
//
//      logger.info("Executing batch query: {}", query);
//
//      Promise<JsonArray> queryPromise = Promise.promise();
//      futures.add(queryPromise.future());
//
//      Tuple tuple = Tuple.tuple();
//      for (int j = 0; j < params.size(); j++) {
//        tuple.addValue(params.getValue(j));
//      }
//
//      client.preparedQuery(query).execute(tuple, ar -> {
//        if (ar.succeeded()) {
//          var rows = ar.result();
//          var jsonRows = new JsonArray();
//
//          rows.forEach(row -> {
//            var obj = new JsonObject();
//            for (int k = 0; k < row.size(); k++) {
//              obj.put(row.getColumnName(k), row.getValue(k));
//            }
//            jsonRows.add(obj);
//          });
//
//          queryPromise.complete(jsonRows);
//        } else {
//          logger.error("❌ Batch item query failed: {}", ar.cause().getMessage());
//          queryPromise.fail(ar.cause());
//        }
//      });
//    }
//
//    // Combine all futures
//    CompositeFuture.all(new ArrayList<>(futures))
//      .onComplete(ar -> {
//        if (ar.succeeded()) {
//          // All queries succeeded
//          JsonArray allResults = new JsonArray();
//          for (int i = 0; i < futures.size(); i++) {
//            allResults.add(((Future<JsonArray>) futures.get(i)).result());
//          }
//          promise.complete(new JsonObject()
//            .put("msg", "Success")
//            .put("results", allResults));
//        } else {
//          // At least one query failed
//          logger.error("❌ Composite future failed: {}", ar.cause().getMessage());
//          promise.fail(ar.cause());
//        }
//      });
//  }
//}


//

//-- Table: credential_profile
//-- Stores credential information for accessing devices
//CREATE TABLE credential_profile (
//  id SERIAL PRIMARY KEY,
//  credential_name VARCHAR(255) NOT NULL,
//system_type VARCHAR(50) NOT NULL CHECK (system_type IN ('windows', 'linux', 'snmp')),
//cred_data JSONB NOT NULL, -- Stores user and password in JSON format
//created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
//);
//
//CREATE TABLE discovery_profiles (
//  id SERIAL PRIMARY KEY,
//  discovery_profile_name VARCHAR(255) NOT NULL,
//credential_profile_ids BIGINT[] NOT NULL,
//ip TEXT NOT NULL,
//port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535)
//);


//CREATE TABLE discovery_result (
//  id SERIAL PRIMARY KEY,
//  discovery_id BIGINT NOT NULL,
//  ip VARCHAR(15) NOT NULL,
//port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
//credential_profile_id BIGINT,
//result VARCHAR(20) NOT NULL CHECK (result IN ('completed', 'failed')),
//msg TEXT,
//FOREIGN KEY (discovery_id) REFERENCES discovery_profiles(id) ON DELETE CASCADE,
//FOREIGN KEY (credential_profile_id) REFERENCES credential_profile(id)
//  );
