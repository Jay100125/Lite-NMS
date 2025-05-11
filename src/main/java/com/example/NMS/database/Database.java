//package com.example.NMS.database;
//
//import com.example.NMS.constant.QueryConstant;
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
//import java.nio.charset.StandardCharsets;
//import java.util.ArrayList;
//import java.util.List;
//import static com.example.NMS.constant.Constant.*;
//
//public class Database extends AbstractVerticle {
//
//  private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);
//
//  private static SqlClient client;
//
//  @Override
//  public void start(Promise<Void> startPromise)
//  {
//    var connectOptions = new PgConnectOptions()
//      .setHost(DB_HOST)
//      .setPort(DB_PORT)
//      .setDatabase(DB_NAME)
//      .setUser(DB_USER)
//      .setPassword(DB_PASSWORD);
//
//    var poolOptions = new PoolOptions().setMaxSize(10);
//
//    client = PgBuilder
//      .client()
//      .with(poolOptions)
//      .connectingTo(connectOptions)
//      .using(vertx)
//      .build();
//
//
//    initializeSchema()
//      .onSuccess(v -> {
//        startPromise.complete();
//        LOGGER.info("Database schema initialized");
//      })
//      .onFailure(err -> {
//        LOGGER.error("Schema initialization failed: {}", err.getMessage());
//        startPromise.fail(err);
//      });
//
//    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_ADDRESS, message ->
//    {
//      var input = message.body();
//
//      var query = input.getString("query");
//
//      var paramArray = input.getJsonArray("params", new JsonArray());
//
//      var params = Tuple.tuple();
//
//      for (int i = 0; i < paramArray.size(); i++)
//      {
//        Object value = paramArray.getValue(i);
//
//        if (value instanceof JsonArray jsonArray)
//        {
//          String[] s = new String[jsonArray.size()];
//
//          for (int j = 0; j < jsonArray.size(); j++)
//          {
//            s[j] = jsonArray.getString(j);
//          }
//          params.addValue(s);
//        }
//        else
//        {
//          params.addValue(value);
//        }
//      }
//
//      client.preparedQuery(query).execute(params, ar ->
//      {
//        if (ar.succeeded())
//        {
//          var rows = ar.result();
//
//          var jsonRows = new JsonArray();
//
//          rows.forEach(row -> {
//
//            var obj = new JsonObject();
//
//            for (int i = 0; i < row.size(); i++)
//            {
//              String columnName = row.getColumnName(i);
//
//              Object columnValue = row.getValue(i);
//
//              if (columnValue != null && columnValue.getClass().isArray())
//              {
//                Object[] array = (Object[]) columnValue;
//
//                JsonArray jsonArray = new JsonArray();
//
//                for (Object item : array)
//                {
//                  jsonArray.add(item);
//                }
//                obj.put(columnName, jsonArray);
//              }
//              else
//              {
//                obj.put(columnName, columnValue);
//              }
//            }
//            jsonRows.add(obj);
//          });
//
//          message.reply(new JsonObject()
//            .put("msg", "Success")
//            .put("result", jsonRows));
//        }
//        else
//        {
//          LOGGER.error("❌ Query failed: {}", ar.cause().getMessage());
//
//          message.reply(new JsonObject()
//            .put("msg", "fail")
//            .put("ERROR", ar.cause().getMessage()));
//        }
//      });
//    });
//
//    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_BATCH_ADDRESS, message -> {
//
//      JsonObject request =  message.body();
//
//      String query = request.getString("query");
//
//      JsonArray batchParams = request.getJsonArray("batchParams");
//
//      if (query == null || batchParams == null || batchParams.isEmpty())
//      {
//        LOGGER.error("Invalid batch request: query={}, batchParams={}", query, batchParams);
//
//        message.reply(new JsonObject()
//          .put("msg", "Error")
//          .put("ERROR", "Missing query or batchParams"));
//        return;
//      }
//
//      List<Tuple> batch = new ArrayList<>();
//
//      for (int i = 0; i < batchParams.size(); i++)
//      {
//        var params = batchParams.getJsonArray(i);
//        Tuple tuple = Tuple.tuple();
//
//        if (query.equals(QueryConstant.INSERT_DISCOVERY_CREDENTIAL))
//        {
//          tuple.addLong(params.getLong(0)); // discovery_id
//
//          tuple.addLong(params.getLong(1)); // credential_profile_id
//        }
//        else if (query.equals(QueryConstant.INSERT_DISCOVERY_RESULT))
//        {
//          tuple.addLong(params.getLong(0)); // discovery_id
//
//          tuple.addString(params.getString(1)); // ip
//
//          tuple.addInteger(params.getInteger(2)); // port
//
//          tuple.addString(params.getString(3)); // result
//
//          tuple.addString(params.getString(4)); // msg (nullable)
//
//          Object credId = params.getValue(5); // credential_profile_id (nullable)
//
//          tuple.addLong(credId instanceof Number ? ((Number) credId).longValue() : null);
//        }
//        else if (query.equals(QueryConstant.INSERT_DEFAULT_METRICS) ||
//                   query.equals(QueryConstant.UPSERT_METRICS))
//        {
//          tuple.addLong(params.getLong(0)); // provisioning_job_id
//
//          tuple.addString(params.getString(1)); // metric_name
//
//          tuple.addInteger(params.getInteger(2)); // polling_interval
//        }
//        else if (query.equals(QueryConstant.INSERT_POLLING_RESULT))
//        {
//          tuple.addLong(params.getLong(0)); // provisioning_job_id
//
//          tuple.addString(params.getString(1)); // metric_name
//
//          tuple.addJsonObject(params.getJsonObject(2)); // value
//        }
//        else if (query.equals(QueryConstant.INSERT_PROVISIONING_JOB))
//        {
//          tuple.addLong(params.getLong(0));
//
//          tuple.addString(params.getString(1)); // ip
//
//          tuple.addInteger(params.getInteger(2)); // port
//        }
//        else if (query.equals(QueryConstant.INSERT_POLLED_DATA))
//        {
//          tuple.addLong(params.getLong(0));
//
//          tuple.addString(params.getString(1)); // metric_name
//
//          tuple.addJsonObject(params.getJsonObject(2)); // value
//        }
//        else
//        {
//          LOGGER.error("Unsupported batch query: {}", query);
//
//          message.reply(new JsonObject()
//            .put("msg", "Error")
//            .put("ERROR", "Unsupported batch query: " + query));
//
//          return;
//        }
//        batch.add(tuple);
//      }
//
//      LOGGER.info("Executing batch query: {}, tuples: {}", query, batch.size());
//
//      client.preparedQuery(query)
//        .executeBatch(batch)
//        .onSuccess(result ->
//        {
//          LOGGER.info("Batch insert executed, inserted {} rows", batch.size());
//
//          var insertedIds = new JsonArray();
//
//          result.forEach(row -> insertedIds.add(row.getLong("id")));
//
//          message.reply(new JsonObject()
//            .put("msg", "Success")
//            .put("insertedIds", insertedIds));
//        })
//        .onFailure(err ->
//        {
//          LOGGER.error("Batch insert failed: {}, error: {}", query, err.getMessage());
//
//          message.reply(new JsonObject()
//            .put("msg", "Error")
//            .put("ERROR", err.getMessage()));
//        });
//    });
//  }
//
//  private Future<Void> initializeSchema() {
//    return vertx.executeBlocking(promise -> {
//      try {
//        String schema = new String(
//          getClass().getResourceAsStream("/schema.sql").readAllBytes(),
//          StandardCharsets.UTF_8
//        );
//        // Split into individual statements
//        String[] ddlStatements = schema.split(";");
//
//        for (String statement : ddlStatements) {
//          if (statement.trim().isEmpty()) continue;
//          client.query(statement).execute()
//            .onFailure(err -> {
//              LOGGER.error("Failed to execute DDL: {}", statement);
//              promise.fail(err);
//            });
//        }
//        promise.complete();
//      } catch (Exception e) {
//        promise.fail(e);
//      }
//    });
//  }
//}


package com.example.NMS.database;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static com.example.NMS.constant.Constant.EVENTBUS_ADDRESS;
import static com.example.NMS.constant.Constant.EVENTBUS_BATCH_ADDRESS;

/**
 * The Database verticle handles database operations by listening on the event bus.
 * It uses a shared SqlClient instance obtained from DatabaseClient.
 */
public class Database extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

  private SqlClient client; // Instance of the shared SQL client

  @Override
  public void start(Promise<Void> startPromise)
  {
    // Get the shared SqlClient instance from DatabaseClient
    // This ensures that all Database verticle instances share the same client and connection pool
    client = DatabaseClient.getClient(vertx);

    // Initialize the database schema
    initializeSchema()
      .onSuccess(v ->
      {
        LOGGER.info("Database schema initialization successful.");
        // Register event bus consumers after schema initialization
        registerEventBusConsumers();

        startPromise.complete();

        LOGGER.info("Database Verticle started and event bus consumers registered.");
      })
      .onFailure(err ->
      {
        LOGGER.error("Schema initialization failed: {}", err.getMessage(), err);

        startPromise.fail(err);
      });
  }

  /**
   * Registers consumers on the event bus to handle database queries.
   */
  private void registerEventBusConsumers()
  {
    // Consumer for single queries
    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_ADDRESS, message ->
    {
      var input = message.body();

      var query = input.getString("query");

      var paramArray = input.getJsonArray("params", new JsonArray());

      var params = Tuple.tuple();

      // Convert JsonArray params to Tuple
      for (var i = 0; i < paramArray.size(); i++)
      {
        var value = paramArray.getValue(i);

        if (value instanceof JsonArray jsonArray)
        { // Handle array types for SQL (e.g., ANY($2::varchar[]))
          String[] s = new String[jsonArray.size()];

          for (var j = 0; j < jsonArray.size(); j++)
          {
            s[j] = jsonArray.getString(j);
          }
          params.addValue(s);
        }
        else
        {
          params.addValue(value);
        }
      }

      LOGGER.debug("Executing query: {} with params: {}", query, params);

      client.preparedQuery(query).execute(params, ar ->
      {
        if (ar.succeeded())
        {
          var jsonRows = new JsonArray();

          ar.result().forEach(row ->
          {
            var obj = new JsonObject();

            for (var i = 0; i < row.size(); i++)
            {
              var columnName = row.getColumnName(i);

              var columnValue = row.getValue(i);
              // Handle array types from database result
              if (columnValue != null && columnValue.getClass().isArray())
              {
                Object[] array = (Object[]) columnValue;

                var jsonArrayValue = new JsonArray();

                for (Object item : array)
                {
                  jsonArrayValue.add(item);
                }

                obj.put(columnName, jsonArrayValue);
              }
              else
              {
                obj.put(columnName, columnValue);
              }
            }
            jsonRows.add(obj);
          });

          LOGGER.debug("Query successful: {}, result size: {}", query, jsonRows.size());

          message.reply(new JsonObject().put("msg", "Success").put("result", jsonRows));
        }
        else
        {
          LOGGER.error("❌ Query failed: {}. Error: {}", query, ar.cause().getMessage(), ar.cause());

          message.reply(new JsonObject().put("msg", "fail").put("ERROR", ar.cause().getMessage()));
        }
      });
    });

    // Consumer for batch queries
    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_BATCH_ADDRESS, message ->
    {
      var request = message.body();

      var query = request.getString("query");

      var batchParams = request.getJsonArray("batchParams");

      if (query == null || batchParams == null || batchParams.isEmpty())
      {
        LOGGER.error("Invalid batch request: query={}, batchParams={}", query, batchParams);

        message.reply(new JsonObject().put("msg", "Error").put("ERROR", "Missing query or batchParams"));

        return;
      }

      List<Tuple> batch = new ArrayList<>();
      // Convert JsonArray of batch parameters to a List of Tuples
      for (int i = 0; i < batchParams.size(); i++)
      {
        var params = batchParams.getJsonArray(i);

        var tuple = Tuple.tuple();
        // This part needs to be robust and handle different types and nulls correctly based on your specific queries
        for (int j = 0; j < params.size(); j++)
        {
          var value = params.getValue(j);
          // Add specific type handling if necessary, e.g., for JSONB
          if (query.equals(QueryConstant.INSERT_POLLED_DATA) && j == 2 && value instanceof String)
          {
            // Assuming the third parameter for INSERT_POLLED_DATA is JSON data stored as a string
            try
            {
              tuple.addJsonObject(new JsonObject((String) value));
            }
            catch (Exception e)
            {
              LOGGER.warn("Failed to parse string to JsonObject for batch query: {}, param index: {}, value: {}", query, j, value);

              tuple.addValue(null); // Or handle error appropriately
            }
          }
          else if (value instanceof JsonObject || value instanceof JsonArray)
          {
            tuple.addValue(value); // Directly add JsonObject/JsonArray if SQL client supports it or it's for JSONB
          }
          else
          {
            tuple.addValue(value);
          }
        }
        batch.add(tuple);
      }

      LOGGER.debug("Executing batch query: {}, number of tuples: {}", query, batch.size());

      client.preparedQuery(query).executeBatch(batch, ar -> {

        if (ar.succeeded())
        {
          var insertedIds = new JsonArray();
          // Assuming all batch queries that return IDs have an 'id' column
          // You might need to adjust this if your returning columns differ
          try
          {
            ar.result().forEach(row ->
            {
              if (row != null && row.size() > 0 && (row.getColumnIndex("id") != -1))
              {
                insertedIds.add(row.getLong("id"));
              }
              else if (row != null && row.size() > 0 && (row.getColumnIndex("metric_id") != -1))
              { // For UPSERT_METRICS
                insertedIds.add(row.getLong("metric_id"));
              }
            });
          }
          catch (Exception e)
          {
            LOGGER.warn("Could not extract all IDs from batch result for query {}: {}", query, e.getMessage());
          }

          LOGGER.debug("Batch query successful: {}, affected rows: {}", query, ar.result().rowCount());

          message.reply(new JsonObject().put("msg", "Success").put("insertedIds", insertedIds).put("rowCount", ar.result().rowCount()));
        }
        else
        {
          LOGGER.error("❌ Batch query failed: {}. Error: {}", query, ar.cause().getMessage(), ar.cause());

          message.reply(new JsonObject().put("msg", "Error").put("ERROR", ar.cause().getMessage()));
        }
      });
    });
  }

  /**
   * Initializes the database schema by executing SQL commands from a schema file.
   *
   * @return A Future that completes when schema initialization is done or fails.
   */
  private Future<Void> initializeSchema()
  {
    Promise<Void> promise = Promise.promise();
    // Using executeBlocking as schema loading might involve file I/O
    vertx.executeBlocking(blockingPromise ->
    {
      try
      {
        // Load schema.sql from resources
        // 'schema.sql' is in src/main/resources directory
        var inputStream = getClass().getResourceAsStream("/schema.sql");

        if (inputStream == null)
        {
          LOGGER.error("schema.sql not found in resources.");

          blockingPromise.fail("schema.sql not found in resources.");

          return;
        }

        var schema = new String(inputStream.readAllBytes(), StandardCharsets.UTF_8);

        inputStream.close();

        // Split schema into individual DDL statements
        String[] ddlStatements = schema.split(";");

        List<Future<Void>> executionFutures = new ArrayList<>();

        for (String statement : ddlStatements)
        {
          String trimmedStatement = statement.trim();

          if (!trimmedStatement.isEmpty())
          {
            // Execute each DDL statement
            Promise<Void> statementPromise = Promise.promise();

            LOGGER.debug("Executing DDL: {}", trimmedStatement);

            client.query(trimmedStatement).execute()
              .onSuccess(res -> {
                LOGGER.debug("Successfully executed DDL: {}", trimmedStatement);
                statementPromise.complete();
              })
              .onFailure(err -> {
                // Log DDL execution errors but don't necessarily fail the whole process
                // if the error is like "table already exists" etc.
                // More sophisticated error handling might be needed here.
                LOGGER.warn("Failed to execute DDL: {} - Error: {}. This might be okay if the object already exists.", trimmedStatement, err.getMessage());

                statementPromise.complete(); //  Complete even on specific errors to allow startup
              });
            executionFutures.add(statementPromise.future());
          }
        }

        // Wait for all DDL statements to complete
        CompositeFuture.all(new ArrayList<>(executionFutures))
          .onSuccess(v -> {
            LOGGER.info("All DDL statements processed.");

            blockingPromise.complete();
          })
          .onFailure(err -> {
            LOGGER.error("Error processing DDL statements: {}", err.getMessage(), err);

            blockingPromise.fail(err);
          });

      }
      catch (Exception e)
      {
        LOGGER.error("Failed to read or process schema.sql: {}", e.getMessage(), e);

        blockingPromise.fail(e);
      }
    }, res -> {
      if (res.succeeded())
      {
        promise.complete();
      }
      else
      {
        promise.fail(res.cause());
      }
    });
    return promise.future();
  }

  @Override
  public void stop(Promise<Void> stopPromise)
  {
    LOGGER.info("Database Verticle stopped.");

    stopPromise.complete();
  }
}
