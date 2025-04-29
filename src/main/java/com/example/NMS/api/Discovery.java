package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import static com.example.NMS.QueryProcessor.*;


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

      if (body == null ||
        !body.containsKey("discovery_profile_name") ||
        !body.containsKey("credential_profile_id") ||
        !body.containsKey("ip_address") ||
        !body.containsKey("port")) {
        sendError(context, 400, "missing field or invalid data");
        return;
      }

      var discoveryName = body.getString("discovery_profile_name");

      var credIdsArray = body.getJsonArray("credential_profile_id");

      var ip = body.getString("ip_address");

      var portStr = body.getString("port");


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

      JsonArray credIds = new JsonArray();

      for (int i = 0; i < credIdsArray.size(); i++)
      {
        try
        {
          long credId = Long.parseLong(credIdsArray.getString(i));

          credIds.add(credId);
        }
        catch (Exception e)
        {
          sendError(context, 400, "Invalid credential_profile_id: " + credIdsArray.getString(i));

          return;
        }
      }

      for(int i = 0; i < credIds.size(); i++)
      {
        logger.info(credIds.getString(i) + " is the cred id");
      }
      JsonObject query = new JsonObject()
        .put("query", QueryConstant.INSERT_DISCOVERY)
        .put("params", new JsonArray()
          .add(discoveryName)
          .add(credIds)
          .add(ip)
          .add(port));

      executeQuery(query, context);
    }
    catch (Exception e)
    {
      sendError(context, 500, "Internal server error");
    }

  }

  private void getById(RoutingContext context)
  {
    try
    {
      var idStr = context.pathParam("id");

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

      JsonObject query = new JsonObject()
        .put("query", QueryConstant.GET_DISCOVERY_BY_ID)
        .put("params", new JsonArray().add(id));

      executeQuery(query, context);
    }
    catch (Exception e)
    {
      logger.error("Error getting discovery by ID: {}", e.getMessage());
    }
  }

  private void getAll(RoutingContext context)
  {
    JsonObject query = new JsonObject().put("query", QueryConstant.GET_ALL_DISCOVERIES);

    executeQuery(query, context);
  }

  private void delete(RoutingContext context)
  {
    try
    {
      var idStr = context.pathParam("id");

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

      JsonObject query = new JsonObject()
        .put("query", QueryConstant.DELETE_DISCOVERY)
        .put("params", new JsonArray().add(id));

      executeQuery(query, context);
    }
    catch (Exception e)
    {
      logger.error("Error deleting discovery: {}", e.getMessage());
    }

  }

  private void update(RoutingContext context)
  {
    try
    {
      var idStr = context.pathParam("id");

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

      var discoveryName = body.getString("discovery_profile_name");

      var credIdsArray = body.getJsonArray("credential_profile_id");

      var ip = body.getString("ip_address");

      var portStr = body.getString("port");

      // Validate required fields
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

      JsonArray credIds = new JsonArray();

      for (int i = 0; i < credIdsArray.size(); i++)
      {
        try
        {
          long credId = Long.parseLong(credIdsArray.getString(i));

          credIds.add(credId);
        }
        catch (Exception e)
        {
          sendError(context, 400, "Invalid credential_profile_id: " + credIdsArray.getString(i));

          return;
        }
      }

      var query = new JsonObject()
        .put("query", QueryConstant.UPDATE_DISCOVERY)
        .put("params", new JsonArray()
          .add(discoveryName)
          .add(credIds)
          .add(ip)
          .add(port)
          .add(id));

      executeQuery(query, context);
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
      String idStr = context.pathParam("id");

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

//      JsonObject query = new JsonObject()
//        .put("query", Constant.RUN_DISCOVERY)
//        .put("params", new JsonArray().add(id));


      runDiscovery(id, context);

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
