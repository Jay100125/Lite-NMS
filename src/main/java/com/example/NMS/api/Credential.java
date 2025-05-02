package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.example.NMS.service.QueryProcessor.executeQuery;
import static com.example.NMS.constant.Constant.*;
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

  /**
   * Handles the POST request to create a new credential.
   *
   * @param context The routing context.
   */
  private void handlePostCredential(RoutingContext context)
  {
    try
    {
      var body =  context.body().asJsonObject();

      // Validate the request body
      if (body == null || body.isEmpty() || !body.containsKey(CREDENTIAL_NAME) || !body.containsKey(SYSTEM_TYPE) || !body.containsKey(CRED_DATA))
      {
        sendError(context, 400, "missing field or invalid data");

        return;
      }

      var credentialName = body.getString(CREDENTIAL_NAME);

      var sysType = body.getString(SYSTEM_TYPE);

      var credData = body.getJsonObject(CRED_DATA);

      if (credentialName.isEmpty() || sysType.isEmpty() || !credData.containsKey(USER) || !credData.containsKey(PASSWORD))
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
        .put(QUERY, INSERT_CREDENTIAL)
        .put(PARAMS, new JsonArray().add(credentialName).add(sysType).add(credData));

      executeQuery(insertQuery)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(201)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MSG, SUCCESS)
                .put(ID, resultArray.getJsonObject(0).getLong(ID))
                .encodePrettily());
          }
          else
          {
            sendError(context, 409, result.getString(ERROR));
          }
        })
        .onFailure(err ->
        {
          logger.error("Failed to create credential: {}", err.getMessage(), err);

          sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }

  /**
   * Handles the PATCH request to update a credential.
   *
   * @param context The routing context.
   */

  // TODO: only take required fields
  private void handlePatchCredential(RoutingContext context)
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
        sendError(context, 400, "Missing or invalid data");

        return;
      }


      var credentialName = body.getString(CREDENTIAL_NAME);

      var sysType = body.getString(SYSTEM_TYPE);

      var credData = body.getJsonObject(CRED_DATA);

      // Validate sys_type if provided
      if (sysType != null && !sysType.isEmpty())
      {
        if (!sysType.equals("windows") && !sysType.equals("linux") && !sysType.equals("snmp"))
        {
          sendError(context, 400, "Invalid sys_type");

          return;
        }
      }


      if (credData != null && (!credData.containsKey(USER) || !credData.containsKey(PASSWORD)))
      {
        sendError(context, 400, "cred_data must contain user and password");

        return;
      }


      var params = new JsonArray()
        .add(credentialName != null && !credentialName.isEmpty() ? credentialName : null)
        .add(sysType != null && !sysType.isEmpty() ? sysType : null)
        .add(credData != null ? credData : null)
        .add(id);

      var updateQuery = new JsonObject()
        .put("query", UPDATE_CREDENTIAL)
        .put("params", params);

      executeQuery(updateQuery)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MSG, SUCCESS)
                .put(ID, resultArray.getJsonObject(0).getLong(ID))
                .encodePrettily());
          }
          else
          {
            sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          logger.error("Failed to update credential: {}", err.getMessage(), err);

          sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      logger.error("Error in patch credential: {}", e.getMessage(), e);

      sendError(context, 500, "Internal server error");
    }
  }

  /**
   * Handles the GET request to fetch all credentials.
   *
   * @param context The routing context.
   */
  private void handleGetAllCredentials(RoutingContext context)
  {
    logger.info("Get all credentials");

    var getAllQuery = new JsonObject()
      .put(QUERY, QueryConstant.GET_ALL_CREDENTIALS);

    executeQuery(getAllQuery)
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
          sendError(context, 404, "No credentials found");
        }
      })
      .onFailure(err ->
      {
        logger.error("Failed to fetch credentials: {}", err.getMessage(), err);

        sendError(context, 500, "Database error: " + err.getMessage());
      });
  }


  /**
   * Handles the GET request to fetch a credential by its ID.
   *
   * @param context The routing context.
   */
  private void handleGetCredentialById(RoutingContext context)
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
        sendError(context, 400, "Wrong ID");

        return;
      }

      var getQuery = new JsonObject()
        .put(QUERY, GET_CREDENTIAL_BY_ID)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(getQuery)
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
            sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          logger.error("Failed to fetch credential {}: {}", id, err.getMessage(), err);

          sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }
  }


  /**
   * Handles the DELETE request to delete a credential by its ID.
   *
   * @param context The routing context.
   */
  private void handleDeleteCredential(RoutingContext context)
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
        sendError(context, 400, "Wrong ID");

        return;
      }

      var deleteQuery = new JsonObject()
        .put(QUERY, DELETE_CREDENTIAL)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(deleteQuery)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");
          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MSG, SUCCESS)
                .put(ID, resultArray.getJsonObject(0).getLong(ID))
                .encodePrettily());
          }
          else
          {
            sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          logger.error("Failed to delete credential {}: {}", id, err.getMessage(), err);

          sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      logger.error(e.getMessage(), e);
    }

  }

  /**
   * Sends an error response to the client.
   *
   * @param ctx         The routing context.
   * @param statusCode  The HTTP status code.
   * @param errorMessage The error message.
   */

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
