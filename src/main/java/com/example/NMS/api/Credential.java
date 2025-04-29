package com.example.NMS.api;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RequestBody;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.example.NMS.QueryProcessor.executeQuery;
import static com.example.NMS.constant.QueryConstant.*;

public class Credential
{
  private static final Logger logger = LoggerFactory.getLogger(Credential.class);

  public void init(Router credentialRouter)
  {
    credentialRouter.post("/api/credential").handler(this::handlePostCredential);

    credentialRouter.patch("/api/credential/:id").handler(this::handlePatchCredential);

    credentialRouter.get("/api/credential").handler(this::handleGetAllCredentials);

    credentialRouter.get("/api/credential/:id").handler(this::handleGetCredentialById);

    credentialRouter.delete("/api/credential/:id").handler(this::handleDeleteCredential);
  }

  private void handlePostCredential(RoutingContext context)
  {
    try
    {
      var body =  context.body().asJsonObject();

      if (body == null || body.isEmpty() || !body.containsKey("credential_name") || !body.containsKey("system_type") || !body.containsKey("cred_data"))
      {
        sendError(context, 400, "missing field or invalid data");

        return;
      }

      var credentialName = body.getString("credential_name");

      var sysType = body.getString("system_type");

      var credData = body.getJsonObject("cred_data");

      if (credentialName.isEmpty() || sysType.isEmpty() || !credData.containsKey("user") || !credData.containsKey("password"))
      {
        sendError(context, 400, "missing field or invalid data");

        return;
      }

      if (!sysType.equals("windows") && !sysType.equals("linux") && !sysType.equals("snmp"))
      {
        sendError(context, 400, "invalid system_type");

        return;
      }

      var insertQuery = new JsonObject()
        .put("query", INSERT_CREDENTIAL)
        .put("params", new JsonArray().add(credentialName).add(sysType).add(credData));

      executeQuery(insertQuery, context);
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }



  private void handlePatchCredential(RoutingContext context)
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

      var body = context.getBodyAsJson();

      if (body == null || body.isEmpty())
      {
        sendError(context, 400, "Missing or invalid data");

        return;
      }


      var credentialName = body.getString("credential_name");

      var sysType = body.getString("sys_type");

      var credData = body.getJsonObject("cred_data");

      // Validate sys_type if provided
      if (sysType != null && !sysType.isEmpty())
      {
        if (!sysType.equals("windows") && !sysType.equals("linux") && !sysType.equals("snmp"))
        {
          sendError(context, 400, "Invalid sys_type");

          return;
        }
      }


      if (credData != null && (!credData.containsKey("user") || !credData.containsKey("password")))
      {
        sendError(context, 400, "cred_data must contain user and password");

        return;
      }


      JsonArray params = new JsonArray()
        .add(credentialName != null && !credentialName.isEmpty() ? credentialName : null)
        .add(sysType != null && !sysType.isEmpty() ? sysType : null)
        .add(credData != null ? credData : null)
        .add(id);

      JsonObject updateQuery = new JsonObject()
        .put("query", UPDATE_CREDENTIAL)
        .put("params", params);

      executeQuery(updateQuery, context);
    }
    catch (Exception e)
    {
      logger.error("Error in patch credential: {}", e.getMessage(), e);

      sendError(context, 500, "Internal server error");
    }
  }

  private void handleGetAllCredentials(RoutingContext ctx)
  {
    logger.info("Get all credentials");

    JsonObject req = new JsonObject();

    req.put("query", GET_ALL_CREDENTIALS);

    executeQuery(req, ctx);
  }

  private void handleGetCredentialById(RoutingContext ctx)
  {
    try
    {
      String idStr = ctx.pathParam("id");

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        sendError(ctx, 400, "Wrong ID");

        return;
      }

      JsonObject query = new JsonObject()
        .put("query", GET_CREDENTIAL_BY_ID)
        .put("params", new JsonArray().add(id));

      executeQuery(query, ctx);
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }


  private void handleDeleteCredential(RoutingContext context)
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
        sendError(context, 400, "Wrong ID");

        return;
      }

      JsonObject checkQuery = new JsonObject()
        .put("query", DELETE_CREDENTIAL)
        .put("params", new JsonArray().add(id));

      executeQuery(checkQuery, context);
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }

  }


  private void sendError(RoutingContext ctx, int statusCode, String errorMessage)
  {
    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put(statusCode == 400 || statusCode == 409 ? "msg" : "status", "failed")
        .put("error", errorMessage)
        .encode());
  }
}
