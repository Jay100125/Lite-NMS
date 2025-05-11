package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.DiscoveryService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
        sendError(context, 400, "missing field or invalid data");

        return;
      }

      var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

      var credentialIdsArray = body.getJsonArray(CREDENTIAL_PROFILE_ID);

      var ip = body.getString(IP_ADDRESS);

      var portStr = body.getString(PORT);

      if (discoveryName.isEmpty() || credentialIdsArray.isEmpty() || ip.isEmpty() || portStr.isEmpty())
      {
        sendError(context, 400, "missing field or invalid data");

        return;
      }

      int port;

      try
      {
        port = Integer.parseInt(portStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "Invalid port");

        return;
      }

      var credentialIds = new JsonArray();

      for (int i = 0; i < credentialIdsArray.size(); i++)
      {
        try
        {
          var credentialId = Long.parseLong(credentialIdsArray.getString(i));

          credentialIds.add(credentialId);
        }
        catch (Exception e)
        {
          sendError(context, 400, "Invalid credential_profile_id: " + credentialIdsArray.getString(i));

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
          var resultArray = result.getJsonArray("result");

          if (!SUCCESS.equals(result.getString(MSG)) || resultArray.isEmpty())
          {
            return Future.failedFuture("Failed to create discovery profile");
          }

          var discoveryId = resultArray.getJsonObject(0).getLong(ID);

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
                        .put(MSG, SUCCESS)
                        .put(ID, discoveryId)
                        .encodePrettily()))
        .onFailure(err -> sendError(context, 500, "Failed to create discovery: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error creating discovery: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
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
      var idStr = context.pathParam(ID);

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "invalid ID");

        return;
      }

      // Prepare query to fetch discovery profile by ID.
      var query = new JsonObject()
        .put(QUERY, QueryConstant.GET_DISCOVERY_BY_ID)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .end(result.encodePrettily());
          }
          else
          {
            sendError(context, 404, "Discovery profile not found");
          }
        })
        .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error getting discovery by ID: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
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
      .onSuccess(result ->
      {
        if (SUCCESS.equals(result.getString(MSG)))
        {
          context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(result.encodePrettily());
        }
        else
        {
          sendError(context, 404, "No discovery profiles found");
        }
      })
      .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
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
      var idStr = context.pathParam(ID);

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "invalid ID");

        return;
      }

      // Prepare query to delete discovery profile by ID.
      var query = new JsonObject()
                  .put(QUERY, QueryConstant.DELETE_DISCOVERY)
                  .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(result.encodePrettily());
          }
          else
          {
            sendError(context, 404, "Discovery profile not found");
          }
        })
        .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error deleting discovery: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
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
      var idStr = context.pathParam(ID);

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "Invalid ID");

        return;
      }

      var body = context.body().asJsonObject();

      if (body == null || body.isEmpty())
      {
        sendError(context, 400, "Missing or empty request body");

        return;
      }

      var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

      var credIdsArray = body.getJsonArray(CREDENTIAL_PROFILE_ID);

      var ip = body.getString(IP_ADDRESS);

      var portStr = body.getString(PORT);

      if (discoveryName == null || credIdsArray == null || ip == null || portStr == null)
      {
        sendError(context, 400, "Missing required fields");

        return;
      }

      int port;

      try
      {
        port = Integer.parseInt(portStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "Invalid port");

        return;
      }

      var credIds = new JsonArray();

      for (int i = 0; i < credIdsArray.size(); i++)
      {
        try
        {
          var credId = Long.parseLong(credIdsArray.getString(i));

          credIds.add(credId);
        }
        catch (Exception e)
        {
          sendError(context, 400, "Invalid credential_profile_id: " + credIdsArray.getString(i));

          return;
        }
      }

      // Update discovery profile in database.
      var updateQuery = new JsonObject()
        .put(QUERY, QueryConstant.UPDATE_DISCOVERY)
        .put(PARAMS, new JsonArray().add(discoveryName).add(ip).add(port).add(id));

      // Delete existing credential mappings.
      var deleteQuery = new JsonObject()
        .put(QUERY, QueryConstant.DELETE_DISCOVERY_CREDENTIALS)
        .put(PARAMS, new JsonArray().add(id));

      // Prepare batch insert for new credential mappings.
      var batchParams = new JsonArray();

      for (int i = 0; i < credIds.size(); i++)
      {
        batchParams.add(new JsonArray().add(id).add(credIds.getLong(i)));
      }

      var batchQuery = new JsonObject()
        .put(QUERY, QueryConstant.INSERT_DISCOVERY_CREDENTIAL)
        .put(BATCHPARAMS, batchParams);

      executeQuery(updateQuery)
            .compose(v -> executeQuery(deleteQuery))
            .compose(v -> executeBatchQuery(batchQuery))
            .onSuccess(v -> context.response()
                  .setStatusCode(200)
                  .putHeader("Content-Type", "application/json")
                  .end(new JsonObject()
                        .put(MSG, SUCCESS)
                        .put("id", id)
                        .encodePrettily()))
            .onFailure(err -> sendError(context, 500, "Failed to update discovery: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error updating discovery: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
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
      var idStr = context.pathParam(ID);

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "Invalid ID");

        return;
      }

      // Execute discovery using the DiscoveryService.
      DiscoveryService.runDiscovery(id)
          .onSuccess(results -> context.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(new JsonObject()
                      .put(MSG, SUCCESS)
                      .put("results", results)
                      .encodePrettily()))
        .onFailure(err ->
        {
          var status = err.getMessage().contains("Discovery profile not found") ? 404 : 500;

          sendError(context, status, "Failed to run discovery: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error("Failed to process discovery request");
    }
  }

    private void sendError(RoutingContext ctx, int statusCode, String errorMessage)
    {
      ctx.response()
        .setStatusCode(statusCode)
        .end(new JsonObject()
          .put(statusCode == 404 ? "status" : "insertion", "failed")
          .put("error", errorMessage)
          .encodePrettily());
    }
}
