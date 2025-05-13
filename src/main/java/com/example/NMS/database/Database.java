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

import static com.example.NMS.constant.Constant.DB_EXECUTE_QUERY;
import static com.example.NMS.constant.Constant.DB_EXECUTE_BATCH_QUERY;

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
    vertx.eventBus().<JsonObject>localConsumer(DB_EXECUTE_QUERY, message ->
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
          var s = new String[jsonArray.size()];

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
                var array = (Object[]) columnValue;

                var jsonArrayValue = new JsonArray();

                for (var item : array)
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
    vertx.eventBus().<JsonObject>localConsumer(DB_EXECUTE_BATCH_QUERY, message ->
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

      var batch = new ArrayList<Tuple>();
      // Convert JsonArray of batch parameters to a List of Tuples
      for (var i = 0; i < batchParams.size(); i++)
      {
        var params = batchParams.getJsonArray(i);

        var tuple = Tuple.tuple();
        // This part needs to be robust and handle different types and nulls correctly based on your specific queries
        for (var j = 0; j < params.size(); j++)
        {
          var value = params.getValue(j);
          // Add specific type handling if necessary, e.g., for JSONB
          if (query.equals(QueryConstant.INSERT_POLLED_DATA) && j == 2 && value instanceof String)
          {
            //  the third parameter for INSERT_POLLED_DATA is JSON data stored as a string
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
            tuple.addValue(value); // Directly add JsonObject/JsonArray
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
        var ddlStatements = schema.split(";");

        var executionFutures = new ArrayList<Future<Void>>();

        for (var statement : ddlStatements)
        {
          var trimmedStatement = statement.trim();

          if (!trimmedStatement.isEmpty())
          {
            // Execute each DDL statement
            Promise<Void> statementPromise = Promise.promise();

            LOGGER.debug("Executing DDL: {}", trimmedStatement);

            client.query(trimmedStatement).execute()
              .onSuccess(res ->
              {
                LOGGER.debug("Successfully executed DDL: {}", trimmedStatement);

                statementPromise.complete();
              })
              .onFailure(err -> {

                LOGGER.error("Failed to execute DDL: {} - Error: {}", trimmedStatement, err.getMessage(), err);

                statementPromise.fail(err);
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
