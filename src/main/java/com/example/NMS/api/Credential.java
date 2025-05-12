package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.QueryProcessor;
import com.example.NMS.utility.ApiUtils;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.example.NMS.service.QueryProcessor.executeQuery;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.*;

/**
 * Manages CRUD operations for SSH credentials in Lite NMS, handling creation, updating, retrieval, and deletion.
 */
public class Credential
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Credential.class);

  public void init(Router credentialRouter)
  {
    credentialRouter.post("/api/credential").handler(this::createCredential);

    credentialRouter.patch("/api/credential/:id").handler(this::updateCredential);

    credentialRouter.get("/api/credential").handler(this::getAllCredentials);

    credentialRouter.get("/api/credential/:id").handler(this::getCredentialById);

    credentialRouter.delete("/api/credential/:id").handler(this::deleteCredential);
  }

  /**
   * Handles the POST request to create a new credential.
   *
   * @param context The routing context.
   */
  private void createCredential(RoutingContext context)
  {
    try
    {
      var body =  context.body().asJsonObject();

      // Validate the request body
      if (body == null || body.isEmpty() || !body.containsKey(CREDENTIAL_NAME) || !body.containsKey(SYSTEM_TYPE) || !body.containsKey(CRED_DATA))
      {
        ApiUtils.sendError(context, 400, "missing field or invalid data");

        return;
      }

      var credentialName = body.getString(CREDENTIAL_NAME);

      var systemType = body.getString(SYSTEM_TYPE);

      var credentialData = body.getJsonObject(CRED_DATA);

      if (credentialName.isEmpty() || systemType.isEmpty() || !credentialData.containsKey(USER) || !credentialData.containsKey(PASSWORD) || credentialData.getString(USER).isEmpty() || credentialData.getString(PASSWORD).isEmpty())
      {
        ApiUtils.sendError(context, 400, "missing field or invalid data");

        return;
      }

      if (!systemType.equals(WINDOWS) && !systemType.equals(LINUX) && !systemType.equals(SNMP))
      {
        ApiUtils.sendError(context, 400, "invalid system_type");

        return;
      }

      var insertQuery = new JsonObject()
        .put(QUERY, INSERT_CREDENTIAL)
        .put(PARAMS, new JsonArray().add(credentialName).add(systemType).add(credentialData));

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
            ApiUtils.sendError(context, 409, result.getString(ERROR));
          }
        })
        .onFailure(err ->
        {
          LOGGER.error("Failed to create credential: {}", err.getMessage(), err);

          ApiUtils.sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage(), e);
    }
  }

  /**
   * Handles the PATCH request to update a credential.
   *
   * @param context The routing context.
   */

  private void updateCredential(RoutingContext context)
  {
    try
    {
      // Parse and validate ID
      var idStr = context.pathParam(ID);

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (Exception e)
      {
        ApiUtils.sendError(context, 400, "Invalid ID");

        return;
      }

      // Parse request body
      var body = context.body().asJsonObject();

      if (body == null || body.isEmpty())
      {
        ApiUtils.sendError(context, 400, "Missing or invalid data");

        return;
      }

      // Validate sys_type if provided
      var systemType = body.getString(SYSTEM_TYPE);

      if (systemType != null && !systemType.isEmpty())
      {
        if (!systemType.equals(WINDOWS) && !systemType.equals(LINUX) && !systemType.equals(SNMP))
        {

          ApiUtils.sendError(context, 400, "Invalid sys_type");

          return;
        }
      }

      // Validate cred_data if provided
      var credentialData = body.getJsonObject(CRED_DATA);

      if (credentialData != null && (!credentialData.containsKey(USER) || !credentialData.containsKey(PASSWORD)))
      {
        ApiUtils.sendError(context, 400, "cred_data must contain user and password");

        return;
      }

      // Check if credential exists
      var existsQuery = new JsonObject()
        .put(QUERY, GET_CREDENTIAL_BY_ID)
        .put(PARAMS, new JsonArray().add(id));

      QueryProcessor.executeQuery(existsQuery)
        .compose(result ->
        {
          if (!SUCCESS.equals(result.getString(MSG)) || result.getJsonArray("result").isEmpty())
          {
            return Future.failedFuture("Credential not found");
          }

          // Prepare update parameters
          var params = new JsonArray()
                    .add(body.getString(CREDENTIAL_NAME)) // Can be null
                    .add(systemType) // Can be null
                    .add(credentialData) // Can be null
                    .add(id);

          var updateQuery = new JsonObject()
                              .put(QUERY, UPDATE_CREDENTIAL)
                              .put(PARAMS, params);

          return QueryProcessor.executeQuery(updateQuery);
        })
        .onSuccess(result -> {

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
            ApiUtils.sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          LOGGER.error("Failed to update credential: {}", err.getMessage(), err);

          var statusCode = err.getMessage().equals("Credential not found") ? 404 : 500;

          var errorMsg = statusCode == 404 ? err.getMessage() : "Database error: " + err.getMessage();

          ApiUtils.sendError(context, statusCode, errorMsg);
        });
    }
    catch (Exception e)
    {
      LOGGER.error("Error in patch credential: {}", e.getMessage(), e);

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

  /**
   * Handles the GET request to fetch all credentials.
   *
   * @param context The routing context.
   */
  private void getAllCredentials(RoutingContext context)
  {
    LOGGER.info("Get all credentials");

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
          ApiUtils.sendError(context, 404, "No credentials found");
        }
      })
      .onFailure(err ->
      {
        LOGGER.error("Failed to fetch credentials: {}", err.getMessage(), err);

        ApiUtils.sendError(context, 500, "Database error: " + err.getMessage());
      });
  }


  /**
   * Handles the GET request to fetch a credential by its ID.
   *
   * @param context The routing context.
   */
  private void getCredentialById(RoutingContext context)
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
        ApiUtils.sendError(context, 400, "Wrong ID");

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
            ApiUtils.sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          LOGGER.error("Failed to fetch credential {}: {}", id, err.getMessage(), err);

          ApiUtils.sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage(), e);
    }
  }


  /**
   * Handles the DELETE request to delete a credential by its ID.
   *
   * @param context The routing context.
   */
  private void deleteCredential(RoutingContext context)
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
        ApiUtils.sendError(context, 400, "Wrong ID");

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
            ApiUtils.sendError(context, 404, "Credential not found");
          }
        })
        .onFailure(err ->
        {
          LOGGER.error("Failed to delete credential {}: {}", id, err.getMessage(), err);

          ApiUtils.sendError(context, 500, "Database error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage(), e);
    }

  }
}
