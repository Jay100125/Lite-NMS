package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.DiscoveryService;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.service.QueryProcessor.*;


public class Discovery {

  public void init(Router discoveryRoute)
  {
    discoveryRoute.post("/api/discovery").handler(this::create);

    discoveryRoute.get("/api/discovery"+ "/:id").handler(this::getById);

    discoveryRoute.get("/api/discovery").handler(this::getAll);

    discoveryRoute.delete("/api/discovery" + "/:id").handler(this::delete);

    discoveryRoute.put("/api/discovery" + "/:id").handler(this::update);

    discoveryRoute.post("/api/discovery" + "/:id/run").handler(this::run);
  }

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

    var credIdsArray = body.getJsonArray(CREDENTIAL_PROFILE_ID);

    var ip = body.getString(IP_ADDRESS);

    var portStr = body.getString(PORT);

    if (discoveryName.isEmpty() || credIdsArray.isEmpty() || ip.isEmpty() || portStr.isEmpty())
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

    // Insert discovery profile
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

        for (var i = 0; i < credIds.size(); i++)
        {
          batchParams.add(new JsonArray().add(discoveryId).add(credIds.getLong(i)));
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
    logger.error("Error creating discovery: {}", e.getMessage());

    sendError(context, 500, "Internal server error");
  }
}

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
      logger.error("Error getting discovery by ID: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }

  }

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
            .end(result.encodePrettily());
        }
        else
        {
          sendError(context, 404, "No discovery profiles found");
        }
      })
      .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
  }

  private void delete(RoutingContext context)
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

      var query = new JsonObject()
        .put(QUERY, QueryConstant.DELETE_DISCOVERY)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onSuccess(result ->
        {
          JsonArray resultArray = result.getJsonArray("result");

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
      logger.error("Error deleting discovery: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }

  }


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
      // Update discovery profile
      var updateQuery = new JsonObject()
        .put(QUERY, QueryConstant.UPDATE_DISCOVERY)
        .put(PARAMS, new JsonArray().add(discoveryName).add(ip).add(port).add(id));

      var deleteQuery = new JsonObject()
        .put(QUERY, QueryConstant.DELETE_DISCOVERY_CREDENTIALS)
        .put(PARAMS, new JsonArray().add(id));

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
      logger.error("Error updating discovery: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }
  }

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

//      runDiscovery(id, context);

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
      logger.info("Something is not right");
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
