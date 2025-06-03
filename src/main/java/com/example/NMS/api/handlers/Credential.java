package com.example.NMS.api.handlers;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.APIUtils;
import com.example.NMS.utility.Validator;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.example.NMS.utility.DBUtils.executeQuery;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.*;

/**
 * Manages CRUD operations for SSH credentials in Lite NMS, handling creation, updating, retrieval, and deletion.
 * This class provides REST-ful API endpoints to manage credential profiles, including validation of input data
 * and interaction with the database to store or retrieve credential information.
 */
public class Credential extends AbstractAPI
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Credential.class);

    public Credential()
    {
        super(LOGGER, "Credential"); // Pass logger and names to AbstractAPI
    }

  /**
   * Initializes API routes for credential management endpoints.
   * Sets up routes for creating, updating, retrieving, and deleting credential profiles.
   *
   * @param credentialRouter The Vert.x router to attach the credential endpoints to.
   */
    public void init(Router credentialRouter)
    {
        credentialRouter.post("/api/credential").handler(this::create);

        credentialRouter.patch("/api/credential/:id").handler(this::update);

        // Use common methods from AbstractAPI via lambda
        credentialRouter.get("/api/credential").handler(ctx ->
            getAll(ctx, QueryConstant.GET_ALL_CREDENTIALS)
        );
        credentialRouter.get("/api/credential/:id").handler(ctx ->
            getById(ctx, QueryConstant.GET_CREDENTIAL_BY_ID)
        );
        credentialRouter.delete("/api/credential/:id").handler(ctx ->
            delete(ctx, QueryConstant.DELETE_CREDENTIAL)
        );
    }

  /**
   * Handles the POST request to create a new credential profile.
   * Validates the request body for required fields (credential name, system type, and credential data),
   * then inserts the credential into the database.
   *
   * @param context The routing context containing the HTTP request with credential data.
   */
     @Override
    protected void create(RoutingContext context)
    {
        try
        {
            var fields = new String[]{CREDENTIAL_NAME, PROTOCOL, CRED_DATA};

            if (Validator.checkRequestFields(context, fields, true)) {
                return;
            }

            // Extract JSON request body
            var body =  context.body().asJsonObject();

            var credentialName = body.getString(CREDENTIAL_NAME);

            var protocol = body.getString(PROTOCOL);

            var credentialData = body.getJsonObject(CRED_DATA);

            if (!credentialData.containsKey(USER) || !credentialData.containsKey(PASSWORD) || credentialData.getString(USER).isEmpty() || credentialData.getString(PASSWORD).isEmpty())
            {
                LOGGER.warn("Create credential failed: Invalid credential name, system type, or credential data");

                APIUtils.sendError(context, 400, "missing field or invalid data");

                return;
            }

            // Prepare database query to insert the new credential
            var insertQuery = new JsonObject()
              .put(QUERY, INSERT_CREDENTIAL)
              .put(PARAMS, new JsonArray().add(credentialName).add(protocol).add(credentialData));

            executeQuery(insertQuery)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result.isEmpty())
                        {
                            APIUtils.sendError(context, 409, "Cannot create credential");
                        }

                        APIUtils.sendSuccess(context,201, "Credential profile created", result);

                    }
                    else
                    {
                        var error = queryResult.cause();

                        // Handle duplicate credential name
                        if(error.getMessage().contains("unique_credential_name"))
                        {
                            APIUtils.sendError(context, 409, "Credential name already exists");
                        }
                        else
                        {
                            LOGGER.error("Failed to create credential: {}", error.getMessage());

                            APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
                        }
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error(exception.getMessage(), exception);

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

  /**
   * Handles the PATCH request to update an existing SSH credential profile.
   * Validates the credential ID and request body, checks if the credential exists,
   * and updates the specified fields in the database.
   *
   * @param context The routing context containing the HTTP request with credential ID and update data.
   */
     @Override
     protected void update(RoutingContext context)
    {
        try
        {
            // Parse and validate credential ID from path parameter
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            var fields = new String[]{CREDENTIAL_NAME, PROTOCOL, CRED_DATA};

            if (Validator.checkRequestFields(context, fields, false))
            {
                return;
            }

            // Extract JSON request body
            var body = context.body().asJsonObject();

            // Validate protocol if provided
            var protocol = body.getString(PROTOCOL);

            // Validate credential_data if provided
            var credentialData = body.getJsonObject(CRED_DATA);

            if (credentialData != null && (!credentialData.containsKey(USER) || !credentialData.containsKey(PASSWORD)))
            {
                APIUtils.sendError(context, 400, "cred_data must contain user and password");

                return;
            }

            var params = new JsonArray()
              .add(body.getString(CREDENTIAL_NAME)) // Can be null
              .add(protocol) // Can be null
              .add(credentialData) // Can be null
              .add(id);

            var query = new JsonObject()
              .put(QUERY, UPDATE_CREDENTIAL)
              .put(PARAMS, params);

            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (!result.isEmpty())
                        {
                            LOGGER.info("Credential updated successfully: ID={}", id);

                            APIUtils.sendSuccess(context, 200, "Credential profile updated", result);
                        }
                        else
                        {
                            LOGGER.warn("Update credential failed: Credential not found for ID={}", id);

                            APIUtils.sendError(context, 404, "Credential not found");
                        }
                    }
                    else
                    {
                        var queryError = queryResult.cause();

                        LOGGER.error("Update credential failed for ID={}: Database error - {}", id, queryError.getMessage(), queryError);

                        APIUtils.sendError(context, 500, "Database error: " + queryError.getMessage());
                    }
                });

        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while updating credential: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

//  /**
//   * Handles the GET request to fetch all credential profiles.
//   * Retrieves all credentials from the database and returns them in the response.
//   *
//   * @param context The routing context containing the HTTP request.
//   */
//    private void getAll(RoutingContext context)
//    {
//        LOGGER.info("Fetching all credential profiles");
//
//        // Prepare query to fetch all credentials
//        var getAllQuery = new JsonObject()
//            .put(QUERY, QueryConstant.GET_ALL_CREDENTIALS);
//
//        executeQuery(getAllQuery)
//            .onComplete(queryResult ->
//            {
//                if (queryResult.succeeded())
//                {
//                    var result = queryResult.result();
//
//                    if (!result.isEmpty())
//                    {
//                        APIUtils.sendSuccess(context,200,"Credential profiles", result);
//                    }
//                    else
//                    {
//                        APIUtils.sendError(context, 404, "No credentials found");
//                    }
//                }
//                else
//                {
//                    var error = queryResult.cause();
//
//                    LOGGER.error("Failed to fetch credentials: {}", error.getMessage());
//
//                    APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
//                }
//            });
//    }
//
//  /**
//   * Handles the GET request to fetch a specific credential profile by its ID.
//   * Retrieves the credential from the database if it exists and returns it in the response.
//   *
//   * @param context The routing context containing the HTTP request with credential ID.
//   */
//    private void getById(RoutingContext context)
//    {
//        try
//        {
//            // Parse and validate credential ID from path parameter
//            var id = APIUtils.parseIdFromPath(context, ID);
//
//            if (id == -1)
//            {
//                return;
//            }
//
//            // Prepare query to fetch credential by ID
//            var getQuery = new JsonObject()
//              .put(QUERY, GET_CREDENTIAL_BY_ID)
//              .put(PARAMS, new JsonArray().add(id));
//
//            executeQuery(getQuery)
//                .onComplete(queryResult ->
//                {
//                    if (queryResult.succeeded())
//                    {
//                        var result = queryResult.result();
//
//                        if (!result.isEmpty())
//                        {
//                            APIUtils.sendSuccess(context, 200, "Credential profile for current Id",result);
//                        }
//                        else
//                        {
//                            APIUtils.sendError(context, 404, "Credential not found");
//                        }
//                    }
//                    else
//                    {
//                        var error = queryResult.cause();
//
//                        LOGGER.error("Failed to fetch credential {}: {}", id, error.getMessage());
//
//                        APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
//                    }
//                });
//        }
//        catch (Exception exception)
//        {
//            LOGGER.error("Unexpected error while fetching credential: {}", exception.getMessage());
//
//            APIUtils.sendError(context, 500, "Unexpected error: " + exception.getMessage());
//        }
//    }
//
//  /**
//   * Handles the DELETE request to remove an SSH credential profile by its ID.
//   * Deletes the credential from the database if it exists and returns a success response.
//   *
//   * @param context The routing context containing the HTTP request with credential ID.
//   */
//    private void delete(RoutingContext context)
//    {
//        try
//        {
//            // Parse and validate credential ID from path parameter
//            var id = APIUtils.parseIdFromPath(context, ID);
//
//            if (id == -1)
//            {
//                return;
//            }
//
//            // Prepare query to delete credential by ID
//            var deleteQuery = new JsonObject()
//              .put(QUERY, DELETE_CREDENTIAL)
//              .put(PARAMS, new JsonArray().add(id));
//
//            executeQuery(deleteQuery)
//              .onComplete(queryResult ->
//              {
//                  if (queryResult.succeeded())
//                  {
//                      var result = queryResult.result();
//
//                      if (!result.isEmpty())
//                      {
//                          APIUtils.sendSuccess(context, 200,"deleted credential profile", result);
//                      }
//                      else
//                      {
//                          APIUtils.sendError(context, 404, "Credential not found");
//                      }
//                  }
//                  else
//                  {
//                      var error = queryResult.cause();
//
//                      LOGGER.error("Failed to delete credential {}: {}", id, error.getMessage());
//
//                      APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
//                  }
//              });
//        }
//        catch (Exception exception)
//        {
//            LOGGER.error("Unexpected error while deleting credential: {}", exception.getMessage());
//
//            APIUtils.sendError(context, 500, "Unexpected error: " + exception.getMessage());
//        }
//    }
}
