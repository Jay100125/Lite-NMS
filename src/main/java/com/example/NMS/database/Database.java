package com.example.NMS.database;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.CompositeFuture;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import io.vertx.sqlclient.SqlClient;
import io.vertx.sqlclient.Tuple;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.stream.Collectors;

import static com.example.NMS.constant.Constant.*;

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
        client =  DatabaseClient.getInstance(vertx).getClient();

        // Initialize the database schema
        initializeSchema()
            .onComplete(result ->
            {
                if (result.succeeded())
                {
                    LOGGER.info("Database schema initialization successful.");
                    // Set event bus consumers after schema initialization
                    setUpConsumers();

                    startPromise.complete();

                    LOGGER.info("Database Verticle started and event bus consumers registered.");
                }
                else
                {
                    LOGGER.error("Schema initialization failed: {}", result.cause().getMessage(), result.cause());

                    startPromise.fail(result.cause());
                }
            });
    }

    /**
     * Registers consumers on the event bus to handle database queries.
     */
    private void setUpConsumers()
    {
        // Consumer for single queries
        vertx.eventBus().<JsonObject>localConsumer(DB_EXECUTE_QUERY, message ->
        {
            var input = message.body();

            var query = input.getString(QUERY);

            var paramArray = input.getJsonArray(PARAMS, new JsonArray());

            var params = Tuple.tuple();

            paramArray.forEach(params::addValue);

            LOGGER.debug("Executing query: {} with params: {}", query, params);


            client.preparedQuery(query).execute(params).map(this::toJsonArray).onComplete(result ->
            {

                if(result.succeeded())
                {
                    var jsonRows = result.result();

                    LOGGER.debug("Query successful: {}, result size: {}", query, jsonRows.size());

                    if (message.replyAddress() != null)
                    {
                        message.reply(jsonRows);
                    }
                }
                else
                {
                    LOGGER.error("❌ Query failed: {}. Error: {}", query, result.cause().getMessage());

                    if (message.replyAddress() != null)
                    {
                        message.fail(500, result.cause().getMessage());
                    }
                }
            });
        });

        // Consumer for batch queries
        vertx.eventBus().<JsonObject>localConsumer(DB_EXECUTE_BATCH_QUERY, message ->
        {
            var request = message.body();

            var query = request.getString(QUERY);

            var batchParams = request.getJsonArray(BATCHPARAMS);

            var batch = new ArrayList<Tuple>();

            // Convert JsonArray of batch parameters to a List of Tuples
            for (var i = 0; i < batchParams.size(); i++)
            {
                var paramArray = batchParams.getJsonArray(i);

                var params = Tuple.tuple();

                paramArray.forEach(params::addValue);

                batch.add(params);
            }

            LOGGER.debug("Executing batch query: {}, number of tuples: {}", query, batch.size());

            client.preparedQuery(query).executeBatch(batch).map(this::toJsonArray).onComplete(result ->
            {
                if(result.succeeded())
                {
                    var insertedIds = result.result();

                    LOGGER.info("Batch query successful: {}, extracted IDs: {}", query, insertedIds.size());

                    if (message.replyAddress() != null)
                    {
                        message.reply(insertedIds);
                    }
                }
                else
                {
                    LOGGER.error("❌ Batch query failed: {}. Error: {}", query, result.cause().getMessage());

                    if (message.replyAddress() != null)
                    {
                        message.fail(500, result.cause().getMessage());
                    }
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
        var promise = Promise.<Void>promise();
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

                var executionFutures = new ArrayList<Future>();

                for (var statement : ddlStatements)
                {
                    var trimmedStatement = statement.trim();

                    if (!trimmedStatement.isEmpty())
                    {
                        // Execute each DDL statement
                        var statementPromise = Promise.<Void>promise();

                        LOGGER.debug("Executing DDL: {}", trimmedStatement);

                        client.query(trimmedStatement).execute()
                            .onSuccess(result ->
                            {
                                LOGGER.debug("Successfully executed DDL: {}", trimmedStatement);

                                statementPromise.complete();
                            })
                            .onFailure(error ->
                            {
                                LOGGER.error("Failed to execute DDL: {} - Error: {}", trimmedStatement, error.getMessage());

                                statementPromise.fail(error);
                            });
                        executionFutures.add(statementPromise.future());
                    }
                }

                // Wait for all DDL statements to complete
                CompositeFuture.all((executionFutures))
                    .onSuccess(result ->
                    {
                        LOGGER.info("All DDL statements processed.");

                        blockingPromise.complete();
                    })
                    .onFailure(error ->
                    {
                        LOGGER.error("Error processing DDL statements: {}", error.getMessage(), error);

                        blockingPromise.fail(error);
                    });

            }
            catch (Exception exception)
            {
                LOGGER.error("Failed to read or process schema.sql: {}", exception.getMessage(), exception);

                blockingPromise.fail(exception);
            }
        }, result ->
        {
            if (result.succeeded())
            {
                promise.complete();
            }
            else
            {
                promise.fail(result.cause());
            }
        });
        return promise.future();
    }

    private JsonArray toJsonArray(RowSet<Row> rows)
    {
        var results = new JsonArray();

        for (Row row : rows)
        {
            results.add(row.toJson());
        }

        return results;
    }
    @Override
    public void stop(Promise<Void> stopPromise)
    {
        LOGGER.info("Database Verticle stopped.");

        stopPromise.complete();
    }
}
