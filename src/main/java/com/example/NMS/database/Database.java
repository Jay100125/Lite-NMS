package com.example.NMS.database;

import com.example.NMS.constant.QueryConstant;
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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import static com.example.NMS.constant.Constant.*;

public class Database extends AbstractVerticle {

  private static final Logger LOGGER = LoggerFactory.getLogger(Database.class);

  private static SqlClient client;

  @Override
  public void start(Promise<Void> startPromise)
  {
    var connectOptions = new PgConnectOptions()
      .setHost(DB_HOST)
      .setPort(DB_PORT)
      .setDatabase(DB_NAME)
      .setUser(DB_USER)
      .setPassword(DB_PASSWORD);

    var poolOptions = new PoolOptions().setMaxSize(10);

    client = PgBuilder
      .client()
      .with(poolOptions)
      .connectingTo(connectOptions)
      .using(vertx)
      .build();


    initializeSchema()
      .onSuccess(v -> {
        startPromise.complete();
        LOGGER.info("Database schema initialized");
      })
      .onFailure(err -> {
        LOGGER.error("Schema initialization failed: {}", err.getMessage());
        startPromise.fail(err);
      });

    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_ADDRESS, message ->
    {
      var input = message.body();

      var query = input.getString("query");

      var paramArray = input.getJsonArray("params", new JsonArray());

      var params = Tuple.tuple();

      for (int i = 0; i < paramArray.size(); i++)
      {
        Object value = paramArray.getValue(i);

        if (value instanceof JsonArray jsonArray)
        {
          String[] s = new String[jsonArray.size()];

          for (int j = 0; j < jsonArray.size(); j++)
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

      client.preparedQuery(query).execute(params, ar ->
      {
        if (ar.succeeded())
        {
          var rows = ar.result();

          var jsonRows = new JsonArray();

          rows.forEach(row -> {

            var obj = new JsonObject();

            for (int i = 0; i < row.size(); i++)
            {
              String columnName = row.getColumnName(i);

              Object columnValue = row.getValue(i);

              if (columnValue != null && columnValue.getClass().isArray())
              {
                Object[] array = (Object[]) columnValue;

                JsonArray jsonArray = new JsonArray();

                for (Object item : array)
                {
                  jsonArray.add(item);
                }
                obj.put(columnName, jsonArray);
              }
              else
              {
                obj.put(columnName, columnValue);
              }
            }
            jsonRows.add(obj);
          });

          message.reply(new JsonObject()
            .put("msg", "Success")
            .put("result", jsonRows));
        }
        else
        {
          LOGGER.error("❌ Query failed: {}", ar.cause().getMessage());

          message.reply(new JsonObject()
            .put("msg", "fail")
            .put("ERROR", ar.cause().getMessage()));
        }
      });
    });

    vertx.eventBus().<JsonObject>localConsumer(EVENTBUS_BATCH_ADDRESS, message -> {

      JsonObject request =  message.body();

      String query = request.getString("query");

      JsonArray batchParams = request.getJsonArray("batchParams");

      if (query == null || batchParams == null || batchParams.isEmpty())
      {
        LOGGER.error("Invalid batch request: query={}, batchParams={}", query, batchParams);

        message.reply(new JsonObject()
          .put("msg", "Error")
          .put("ERROR", "Missing query or batchParams"));
        return;
      }

      List<Tuple> batch = new ArrayList<>();

      for (int i = 0; i < batchParams.size(); i++)
      {
        var params = batchParams.getJsonArray(i);
        Tuple tuple = Tuple.tuple();

        if (query.equals(QueryConstant.INSERT_DISCOVERY_CREDENTIAL))
        {
          tuple.addLong(params.getLong(0)); // discovery_id

          tuple.addLong(params.getLong(1)); // credential_profile_id
        }
        else if (query.equals(QueryConstant.INSERT_DISCOVERY_RESULT))
        {
          tuple.addLong(params.getLong(0)); // discovery_id

          tuple.addString(params.getString(1)); // ip

          tuple.addInteger(params.getInteger(2)); // port

          tuple.addString(params.getString(3)); // result

          tuple.addString(params.getString(4)); // msg (nullable)

          Object credId = params.getValue(5); // credential_profile_id (nullable)

          tuple.addLong(credId instanceof Number ? ((Number) credId).longValue() : null);
        }
        else if (query.equals(QueryConstant.INSERT_DEFAULT_METRICS) ||
                   query.equals(QueryConstant.UPSERT_METRICS))
        {
          tuple.addLong(params.getLong(0)); // provisioning_job_id

          tuple.addString(params.getString(1)); // metric_name

          tuple.addInteger(params.getInteger(2)); // polling_interval
        }
        else if (query.equals(QueryConstant.INSERT_POLLING_RESULT))
        {
          tuple.addLong(params.getLong(0)); // provisioning_job_id

          tuple.addString(params.getString(1)); // metric_name

          tuple.addJsonObject(params.getJsonObject(2)); // value
        }
        else if (query.equals(QueryConstant.INSERT_PROVISIONING_JOB))
        {
          tuple.addLong(params.getLong(0));

          tuple.addString(params.getString(1)); // ip

          tuple.addInteger(params.getInteger(2)); // port
        }
        else if (query.equals(QueryConstant.INSERT_POLLED_DATA))
        {
          tuple.addLong(params.getLong(0));

          tuple.addString(params.getString(1)); // metric_name

          tuple.addJsonObject(params.getJsonObject(2)); // value
        }
        else
        {
          LOGGER.error("Unsupported batch query: {}", query);

          message.reply(new JsonObject()
            .put("msg", "Error")
            .put("ERROR", "Unsupported batch query: " + query));

          return;
        }
        batch.add(tuple);
      }

      LOGGER.info("Executing batch query: {}, tuples: {}", query, batch.size());

      client.preparedQuery(query)
        .executeBatch(batch)
        .onSuccess(result ->
        {
          LOGGER.info("Batch insert executed, inserted {} rows", batch.size());

          var insertedIds = new JsonArray();

          result.forEach(row -> insertedIds.add(row.getLong("id")));

          message.reply(new JsonObject()
            .put("msg", "Success")
            .put("insertedIds", insertedIds));
        })
        .onFailure(err ->
        {
          LOGGER.error("Batch insert failed: {}, error: {}", query, err.getMessage());

          message.reply(new JsonObject()
            .put("msg", "Error")
            .put("ERROR", err.getMessage()));
        });
    });
  }

  private Future<Void> initializeSchema() {
    return vertx.executeBlocking(promise -> {
      try {
        String schema = new String(
          getClass().getResourceAsStream("/schema.sql").readAllBytes(),
          StandardCharsets.UTF_8
        );
        // Split into individual statements
        String[] ddlStatements = schema.split(";");

        for (String statement : ddlStatements) {
          if (statement.trim().isEmpty()) continue;
          client.query(statement).execute()
            .onFailure(err -> {
              LOGGER.error("Failed to execute DDL: {}", statement);
              promise.fail(err);
            });
        }
        promise.complete();
      } catch (Exception e) {
        promise.fail(e);
      }
    });
  }
}
