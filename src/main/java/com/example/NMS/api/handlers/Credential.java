package com.example.NMS.api.handlers;

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
        credentialRouter.post("/api/credential").handler(this::create);

        credentialRouter.patch("/api/credential/:id").handler(this::update);

        credentialRouter.get("/api/credential").handler(this::getAll);

        credentialRouter.get("/api/credential/:id").handler(this::getById);

        credentialRouter.delete("/api/credential/:id").handler(this::delete);
    }

  /**
   * Handles the POST request to create a new credential.
   *
   * @param context The routing context.
   */
    private void create(RoutingContext context)
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

            // insert our data to credential table
            var insertQuery = new JsonObject()
              .put(QUERY, INSERT_CREDENTIAL)
              .put(PARAMS, new JsonArray().add(credentialName).add(systemType).add(credentialData));

            executeQuery(insertQuery)
              .onComplete(queryResult ->
              {
                  if (queryResult.succeeded())
                  {
                      var result = queryResult.result();

                      if (result.isEmpty())
                      {
                          ApiUtils.sendError(context, 409, "Cannot create credential");
                      }

//                      context.response()
//                        .setStatusCode(201)
//                        .putHeader("Content-Type", "application/json")
//                        .end(new JsonObject()
//                          .put(MESSAGE, SUCCESS)
//                          .put(ID, result.getJsonObject(0).getLong(ID))
//                          .encodePrettily());

                    ApiUtils.sendSuccess(context,201, "Credential profile created", result);

                  }
                  else
                  {
                      var error = queryResult.cause();

                      if(error.getMessage().contains("unique_credential_name"))
                      {
                          ApiUtils.sendError(context, 409, "Credential name already exists");
                      }
                      else
                      {
                          LOGGER.error("Failed to create credential: {}", error.getMessage());

                          ApiUtils.sendError(context, 500, "Database error: " + error.getMessage());
                      }
                  }
              });
        }
        catch (Exception exception)
        {
          LOGGER.error(exception.getMessage(), exception);
        }
    }

  /**
   * Handles the PATCH request to update a credential.
   *
   * @param context The routing context.
   */

    private void update(RoutingContext context)
    {
        try
        {
          // Parse and validate ID
            var id = ApiUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            // Parse request body
            var body = context.body().asJsonObject();

            if (body == null || body.isEmpty())
            {
                ApiUtils.sendError(context, 400, "Missing or invalid data");

                return;
            }

            // Validate system_type if provided
            var systemType = body.getString(SYSTEM_TYPE);

            // Validate credential_data if provided
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
                  if (result.isEmpty())
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
              .onComplete(queryResult ->
              {
                  if (queryResult.succeeded())
                  {
                      var result = queryResult.result();

                      if (!result.isEmpty())
                      {
//                          context.response().setStatusCode(200)
//                            .putHeader("Content-Type", "application/json")
//                            .end(new JsonObject().put(MESSAGE, SUCCESS)
//                              .put(ID, result.getJsonObject(0).getLong(ID))
//                              .encodePrettily());

                        ApiUtils.sendSuccess(context,200, "Updated credential profile",result);
                      }
                      else
                      {
                          ApiUtils.sendError(context, 404, "Credential not found");
                      }
                  }
                  else
                  {
                      var error = queryResult.cause();

                      LOGGER.error("Failed to update credential: {}", error.getMessage());

                      var statusCode = error.getMessage().equals("Credential not found") ? 404 : 500;

                      var errorMsg = statusCode == 404 ? error.getMessage() : "Database error: " + error.getMessage();

                      ApiUtils.sendError(context, statusCode, errorMsg);
                  }
              });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while updating credential: {}", exception.getMessage());

            ApiUtils.sendError(context, 500, "Internal server error");
        }
    }

  /**
   * Handles the GET request to fetch all credentials.
   *
   * @param context The routing context.
   */
    private void getAll(RoutingContext context)
    {
        LOGGER.info("Get all credentials");

        var getAllQuery = new JsonObject()
          .put(QUERY, QueryConstant.GET_ALL_CREDENTIALS);

        executeQuery(getAllQuery)
          .onComplete(queryResult ->
          {
              if (queryResult.succeeded())
              {
                  var result = queryResult.result();

                  if (!result.isEmpty())
                  {
//                      context.response().setStatusCode(200)
//                        .putHeader("Content-Type", "application/json")
//                        .end(new JsonObject()
//                              .put(MESSAGE, SUCCESS)
//                              .put(RESULT, result)
//                              .encodePrettily());
                    ApiUtils.sendSuccess(context,200,"Credential profiles", result);
                  }
                  else
                  {
                      ApiUtils.sendError(context, 404, "No credentials found");
                  }
              }
            else
            {
                var error = queryResult.cause();

                LOGGER.error("Failed to fetch credentials: {}", error.getMessage());

                ApiUtils.sendError(context, 500, "Database error: " + error.getMessage());
            }
          });
    }

  /**
   * Handles the GET request to fetch a credential by its ID.
   *
   * @param context The routing context.
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

            // get credential by id
            var getQuery = new JsonObject()
              .put(QUERY, GET_CREDENTIAL_BY_ID)
              .put(PARAMS, new JsonArray().add(id));

            executeQuery(getQuery)
              .onComplete(queryResult ->
              {
                  if (queryResult.succeeded())
                  {
                      var result = queryResult.result();

                      if (!result.isEmpty())
                      {
//                          context.response().setStatusCode(200)
//                            .putHeader("Content-Type", "application/json")
//                            .end(new JsonObject()
//                              .put(MESSAGE, SUCCESS)
//                              .put(RESULT, result)
//                              .encodePrettily());

                        ApiUtils.sendSuccess(context, 200, "Credential profile for current Id",result);
                      }
                      else
                      {
                          ApiUtils.sendError(context, 404, "Credential not found");
                      }
                  }
                  else
                  {
                      var error = queryResult.cause();

                      LOGGER.error("Failed to fetch credential {}: {}", id, error.getMessage());

                      ApiUtils.sendError(context, 500, "Database error: " + error.getMessage());
                  }
              });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while fetching credential: {}", exception.getMessage());

            ApiUtils.sendError(context, 500, "Unexpected error: " + exception.getMessage());
        }
    }


  /**
   * Handles the DELETE request to delete a credential by its ID.
   *
   * @param context The routing context.
   */
    private void delete(RoutingContext context)
    {
        try
        {
            var id = ApiUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            // delete credential by id
            var deleteQuery = new JsonObject()
              .put(QUERY, DELETE_CREDENTIAL)
              .put(PARAMS, new JsonArray().add(id));

            executeQuery(deleteQuery)
              .onComplete(queryResult ->
              {
                  if (queryResult.succeeded())
                  {
                      var result = queryResult.result();

                      if (!result.isEmpty())
                      {
//                          context.response()
//                            .setStatusCode(200)
//                            .putHeader("Content-Type", "application/json")
//                            .end(new JsonObject()
//                              .put(MESSAGE, SUCCESS)
//                              .put(ID, result.getJsonObject(0).getLong(ID))
//                              .encodePrettily());

                          ApiUtils.sendSuccess(context, 200,"deleted credential profile", result);
                      }
                      else
                      {
                          ApiUtils.sendError(context, 404, "Credential not found");
                      }
                  }
                  else
                  {
                      var error = queryResult.cause();

                      LOGGER.error("Failed to delete credential {}: {}", id, error.getMessage());

                      ApiUtils.sendError(context, 500, "Database error: " + error.getMessage());
                  }
              });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while deleting credential: {}", exception.getMessage());

            ApiUtils.sendError(context, 500, "Unexpected error: " + exception.getMessage());
        }
    }
}
