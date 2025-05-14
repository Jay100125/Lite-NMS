package com.example.NMS.api.handlers;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.ApiUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.service.QueryProcessor.*;


/**
 * Manages CRUD operations and execution of discovery profiles in Lite NMS, enabling network device detection.
 */
public class Discovery
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

    public void init(Router discoveryRoute)
    {
        discoveryRoute.post("/api/discovery").handler(this::create);

        discoveryRoute.get("/api/discovery"+ "/:id").handler(this::getById);

        discoveryRoute.get("/api/discovery").handler(this::getAll);

        discoveryRoute.delete("/api/discovery" + "/:id").handler(this::delete);

        discoveryRoute.put("/api/discovery" + "/:id").handler(this::update);

        discoveryRoute.post("/api/discovery" + "/:id/run").handler(this::run);

        discoveryRoute.get("/api/discovery" + "/:id/result").handler(this::getResults);
    }

  /**
   * Handles POST requests to create a new discovery profile, associating it with credentials and storing it in PostgreSQL.
   *
   * @param context The routing context containing the HTTP request.
   */

  private void create(RoutingContext context)
  {
    try
    {
      var body = context.body().asJsonObject();

      if (body == null || !body.containsKey(DISCOVERY_PROFILE_NAME) || !body.containsKey(CREDENTIAL_PROFILE_ID) || !body.containsKey(IP_ADDRESS) || !body.containsKey(PORT))
      {
        ApiUtils.sendError(context, 400, "missing field or invalid data");

        return;
      }

      var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

      var credentialIdsArray = body.getJsonArray(CREDENTIAL_PROFILE_ID);

      var ip = body.getString(IP_ADDRESS);

      var portStr = body.getString(PORT);

      if (discoveryName.isEmpty() || credentialIdsArray.isEmpty() || ip.isEmpty() || portStr.isEmpty())
      {
        ApiUtils.sendError(context, 400, "missing field or invalid data");

        return;
      }

      int port;

      try
      {
        port = Integer.parseInt(portStr);
      }
      catch (Exception e)
      {
        ApiUtils.sendError(context, 400, "Invalid port");

        return;
      }

      var credentialIds = new JsonArray();

      for (var i = 0; i < credentialIdsArray.size(); i++)
      {
        try
        {
          var credentialId = Long.parseLong(credentialIdsArray.getString(i));

          credentialIds.add(credentialId);
        }
        catch (Exception e)
        {
          ApiUtils.sendError(context, 400, "Invalid credential_profile_id: " + credentialIdsArray.getString(i));

          return;
        }
      }

      // Insert discovery profile into database.
      var query = new JsonObject()
        .put(QUERY, QueryConstant.INSERT_DISCOVERY)
        .put(PARAMS, new JsonArray().add(discoveryName).add(ip).add(port));

      executeQuery(query)
        .compose(result ->
        {
          if(result.isEmpty())
          {
            return Future.failedFuture("Failed to create discovery profile");
          }

          var discoveryId = result.getJsonObject(0).getLong(ID);

          var batchParams = new JsonArray();

          for (var i = 0; i < credentialIds.size(); i++)
          {
            batchParams.add(new JsonArray().add(discoveryId).add(credentialIds.getLong(i)));
          }

          var batchQuery = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_CREDENTIAL)
            .put(BATCHPARAMS, batchParams);

          return executeBatchQuery(batchQuery)
            .map(discoveryId);

        })
        .onSuccess(discoveryId -> context.response()
                .setStatusCode(201)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                        .put(MESSAGE, SUCCESS)
                        .put(ID, discoveryId)
                        .encodePrettily()))
        .onFailure(err -> ApiUtils.sendError(context, 500, "Failed to create discovery: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error creating discovery: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

  /**
   * Handles GET requests to retrieve a discovery profile by its ID.
   *
   * @param context The routing context containing the HTTP request.
   */
  private void getById(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      // Prepare query to fetch discovery profile by ID.
      var query = new JsonObject()
        .put(QUERY, QueryConstant.GET_DISCOVERY_BY_ID)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onComplete(queryResult ->
        {
          if(queryResult.succeeded())
          {
            var result = queryResult.result();

            if (!result.isEmpty())
            {
              context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                  .put(MESSAGE, SUCCESS)
                  .put(RESULT, result)
                  .encodePrettily());
            }
            else
            {
              ApiUtils.sendError(context, 404, "Discovery profile not found");
            }
          }
          else
          {
            var error = queryResult.cause();

            LOGGER.error("Error executing query: {}", error.getMessage());

            ApiUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
          }
        });
    }
    catch (Exception e)
    {
      LOGGER.error("Error getting discovery by ID: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }


  /**
   * Handles GET requests to retrieve all discovery profiles from the database.
   *
   * @param context The routing context containing the HTTP request.
   */
  private void getAll(RoutingContext context)
  {
    var query = new JsonObject().put(QUERY, QueryConstant.GET_ALL_DISCOVERIES);

    executeQuery(query)
      .onComplete(queryResult ->
      {
        if(queryResult.succeeded())
        {
          var result = queryResult.result();

          if (!result.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MESSAGE, SUCCESS)
                .put(RESULT, result)
                .encodePrettily());
          }
          else
          {
            ApiUtils.sendError(context, 404, "No discovery profiles found");
          }
        }
        else
        {
          var error = queryResult.cause();

          LOGGER.error("Error executing query: {}", error.getMessage());

          ApiUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
        }
      });
  }

  /**
   * Handles DELETE requests to remove a discovery profile by its ID.
   *
   * @param context The routing context containing the HTTP request.
   */
  private void delete(RoutingContext context)
  {
    try
    {
      // Parse and validate discovery ID from path.
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      // Prepare query to delete discovery profile by ID.
      var query = new JsonObject()
                  .put(QUERY, QueryConstant.DELETE_DISCOVERY)
                  .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onComplete(queryResult ->
        {
          if(queryResult.succeeded())
          {
            var result = queryResult.result();

            if (!result.isEmpty())
            {
              context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(result.encodePrettily());
            }
            else
            {
              ApiUtils.sendError(context, 404, "Discovery profile not found");
            }
          }
          else
          {
            var error = queryResult.cause();

            ApiUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
          }
        });
    }
    catch (Exception e)
    {
      LOGGER.error("Error deleting discovery: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }

  }

  /**
   * Handles PUT requests to update an existing discovery profile, including its credentials.
   *
   * @param context The routing context containing the HTTP request.
   */
  private void update(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      var body = context.body().asJsonObject();

      if (body == null || body.isEmpty())
      {
        ApiUtils.sendError(context, 400, "Missing or empty request body");

        return;
      }

      var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

      var credIdsArray = body.getJsonArray(CREDENTIAL_PROFILE_ID);

      var ip = body.getString(IP_ADDRESS);

      var portStr = body.getString(PORT);

      if (discoveryName == null || credIdsArray == null || ip == null || portStr == null)
      {
        ApiUtils.sendError(context, 400, "Missing required fields");

        return;
      }

      int port;

      try
      {
        port = Integer.parseInt(portStr);
      }
      catch (Exception e)
      {
        ApiUtils.sendError(context, 400, "Invalid port");

        return;
      }

      var credIds = new JsonArray();

      for (var i = 0; i < credIdsArray.size(); i++)
      {
        try
        {
          var credId = Long.parseLong(credIdsArray.getString(i));

          credIds.add(credId);
        }
        catch (Exception e)
        {
          ApiUtils.sendError(context, 400, "Invalid credential_profile_id: " + credIdsArray.getString(i));

          return;
        }
      }

      var existsQuery = new JsonObject()
        .put(QUERY, QueryConstant.GET_DISCOVERY_BY_ID)
        .put(PARAMS, new JsonArray().add(id));


      executeQuery(existsQuery)
        .compose(result ->
        {
          if (result.isEmpty())
          {
            return Future.failedFuture("Discovery profile not found");
          }

          // Update discovery profile in database
          var updateQuery = new JsonObject()
            .put(QUERY, QueryConstant.UPDATE_DISCOVERY)
            .put(PARAMS, new JsonArray().add(discoveryName).add(ip).add(port).add(id));

          // Delete existing credential mappings
          var deleteQuery = new JsonObject()
            .put(QUERY, QueryConstant.DELETE_DISCOVERY_CREDENTIALS)
            .put(PARAMS, new JsonArray().add(id));

          // Prepare batch insert for new credential mappings
          var batchParams = new JsonArray();

          for (var i = 0; i < credIds.size(); i++)
          {
            batchParams.add(new JsonArray().add(id).add(credIds.getLong(i)));
          }

          var batchQuery = new JsonObject()
            .put(QUERY, QueryConstant.INSERT_DISCOVERY_CREDENTIAL)
            .put(BATCHPARAMS, batchParams);

          return executeQuery(updateQuery)
            .compose(v -> executeQuery(deleteQuery))
            .compose(v -> executeBatchQuery(batchQuery));
        })
        .onComplete(queryResult ->
        {
          if(queryResult.succeeded())
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MESSAGE, SUCCESS)
                .put(ID, id)
                .encodePrettily());
          }
          else
          {
            var error = queryResult.cause();

            LOGGER.error("Error updating discovery profile {}: {}", id, error.getMessage());

            if (error.getMessage().equals("Discovery profile not found"))
            {
              ApiUtils.sendError(context, 404, "Discovery profile not found");
            }
            else
            {
              ApiUtils.sendError(context, 500, "Failed to update discovery: " + error.getMessage());
            }
          }
        });
    }
    catch (Exception exception)
    {
      LOGGER.error("Error updating discovery: {}", exception.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

  /**
   * Handles POST requests to run a discovery profile, scanning the network for devices.
   *
   * @param context The routing context containing the HTTP request.
   */
  private void run(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      var checkQuery = new JsonObject()
        .put(QUERY, QueryConstant.GET_DISCOVERY_BY_ID)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(checkQuery).onComplete(queryResult ->
      {
        if (queryResult.succeeded())
        {
          var result = queryResult.result();

          if (result.isEmpty())
          {
            ApiUtils.sendError(context, 404, "Discovery profile not found");

            return;
          }

          var request = new JsonObject().put(ID, id);
          vertx.eventBus().send(DISCOVERY_RUN,request);

          context.response()
            .setStatusCode(202)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
              .put(MESSAGE, "Discovery is currently being processed")
              .put(ID, id)
              .encodePrettily());
        }
        else
        {
          var error = queryResult.cause();

          ApiUtils.sendError(context, 500, "Failed to check discovery profile: " + error.getMessage());
        }
      });
    }
    catch (Exception e)
    {
      LOGGER.error("Failed to process discovery request");
    }
  }

  private void getResults(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        ApiUtils.sendError(context, 400, "Invalid discovery ID");
        return;
      }

      var query = new JsonObject()
        .put(QUERY, QueryConstant.GET_DISCOVERY_RESULTS)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onComplete(queryResult -> {
          if(queryResult.succeeded())
          {
            var result = queryResult.result();

            if (result.isEmpty())
            {
              context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                  .put(MESSAGE, "No discovery results found")
                  .put(RESULT, new JsonArray())
                  .encodePrettily());
            }
            else
            {
              context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                  .put(MESSAGE, SUCCESS)
                  .put(RESULT, result)
                  .encodePrettily());
            }
          }
          else
          {
            var error = queryResult.cause();

            ApiUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
          }
        });
    }
    catch (Exception exception)
    {
      LOGGER.error("Error retrieving discovery results: {}", exception.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

}
