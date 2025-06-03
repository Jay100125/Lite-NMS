package com.example.NMS.api.handlers;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.APIUtils;
import com.example.NMS.utility.Validator;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.DBUtils.*;


/**
 * Manages CRUD operations and execution of discovery profiles in Lite NMS, enabling network device detection.
 * This class provides REST-ful API endpoints to create, retrieve, update, delete, and run discovery profiles,
 * as well as fetch their results. Discovery profiles define network scanning parameters, including IP addresses,
 * ports, and associated credentials.
 */
public class Discovery extends AbstractAPI
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

    public Discovery() {
        super(LOGGER, "Discovery Profiles");
    }
    /**
     * Initializes API routes for discovery profile management endpoints.
     * Sets up routes for creating, retrieving, updating, deleting, running, and fetching results of discovery profiles.
     *
     * @param discoveryRoute The Vert.x router to attach the discovery endpoints to.
     */
    public void init(Router discoveryRoute)
    {
        discoveryRoute.post("/api/discovery").handler(this::create);

        discoveryRoute.put("/api/discovery" + "/:id").handler(this::update);

        discoveryRoute.post("/api/discovery" + "/:id/run").handler(this::run);

        discoveryRoute.get("/api/discovery" + "/:id/result").handler(this::getResults);

        discoveryRoute.get("/api/discovery/:id").handler(context -> getById(context, QueryConstant.GET_DISCOVERY_BY_ID));

        discoveryRoute.get("/api/discovery").handler(context -> getAll(context, QueryConstant.GET_ALL_DISCOVERIES));

        discoveryRoute.delete("/api/discovery/:id").handler(context ->  delete(context, QueryConstant.DELETE_DISCOVERY));
    }

    /**
     * Handles POST requests to create a new discovery profile.
     * Validates the request body, inserts the profile into the database, and associates it with specified credentials.
     *
     * @param context The routing context containing the HTTP request with discovery profile data.
     */
    protected void create(RoutingContext context)
    {
        try
        {

            var fields = new String[]{DISCOVERY_PROFILE_NAME, CREDENTIAL_PROFILE_ID, IP_ADDRESS, PORT};

            if (Validator.checkRequestFields(context, fields, true))
            {
                return;
            }
            // Extract JSON request body
            var body = context.body().asJsonObject();

            // Extract and validate discovery details
            var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

            var credentialIds = body.getJsonArray(CREDENTIAL_PROFILE_ID);

            var ip = body.getString(IP_ADDRESS);

            var port = body.getInteger(PORT);

            if (discoveryName.isEmpty() || credentialIds.isEmpty() || ip.isEmpty() || port <= 0 || port >= 65536)
            {
                APIUtils.sendError(context, 400, "Missing field or invalid data");

                return;
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

                    // Prepare batch query to insert credential mappings
                    var batchParams = new JsonArray();

                    for (var i = 0; i < credentialIds.size(); i++)
                    {
                        batchParams.add(new JsonArray().add(discoveryId).add(credentialIds.getLong(i)));
                    }

                    var batchQuery = new JsonObject()
                        .put(QUERY, QueryConstant.INSERT_DISCOVERY_CREDENTIAL)
                        .put(BATCHPARAMS, batchParams);

                    return executeBatchQuery(batchQuery).map(discoveryId);

                })
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        APIUtils.sendSuccess(context, 201, "discovery profile created",new JsonArray().add(result));
                    }
                    else
                    {
                        APIUtils.sendError(context, 500, "Failed to create discovery profile: " + queryResult.cause().getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error creating discovery: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

//    /**
//     * Handles GET requests to retrieve a discovery profile by its ID.
//     * Fetches the profile from the database and returns it if found.
//     *
//     * @param context The routing context containing the HTTP request with discovery ID.
//     */
//    private void getById(RoutingContext context)
//    {
//        try
//        {
//            // Parse and validate discovery ID from path
//            var id = APIUtils.parseIdFromPath(context, ID);
//
//            if (id == -1)
//            {
//                return;
//            }
//
//            // Prepare query to fetch discovery profile by ID.
//            var query = new JsonObject()
//                .put(QUERY, QueryConstant.GET_DISCOVERY_BY_ID)
//                .put(PARAMS, new JsonArray().add(id));
//
//            executeQuery(query)
//                .onComplete(queryResult ->
//                {
//                    if(queryResult.succeeded())
//                    {
//                        var result = queryResult.result();
//
//                        if (!result.isEmpty())
//                        {
//                            APIUtils.sendSuccess(context, 200, "Discovery profile for current Id",result);
//                        }
//                        else
//                        {
//                            APIUtils.sendError(context, 404, "Discovery profile not found");
//                        }
//                    }
//                    else
//                    {
//                        var error = queryResult.cause();
//
//                        LOGGER.error("Failed to fetch discovery ID={}: {}", id, error.getMessage());
//
//                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
//                    }
//                });
//        }
//        catch (Exception exception)
//        {
//            LOGGER.error("Unexpected error during discovery retrieval : {}", exception.getMessage());
//
//            APIUtils.sendError(context, 500, "Internal server error");
//        }
//    }

//    /**
//     * Handles GET requests to retrieve all discovery profiles.
//     * Fetches all profiles from the database and returns them.
//     *
//     * @param context The routing context containing the HTTP request.
//     */
//    private void getAll(RoutingContext context)
//    {
//        // Prepare query to fetch all discovery profiles
//        var query = new JsonObject().put(QUERY, QueryConstant.GET_ALL_DISCOVERIES);
//
//        executeQuery(query)
//            .onComplete(queryResult ->
//            {
//                if(queryResult.succeeded())
//                {
//                    var result = queryResult.result();
//
//                    if (!result.isEmpty())
//                    {
//                        APIUtils.sendSuccess(context, 200, "Discovery profiles",result);
//                    }
//                    else
//                    {
//                        APIUtils.sendError(context, 404, "No discovery profiles found");
//                    }
//                }
//                else
//                {
//                    var error = queryResult.cause();
//
//                    LOGGER.error("Error executing query: {}", error.getMessage());
//
//                    APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
//                }
//            });
//    }

//    /**
//     * Handles DELETE requests to remove a discovery profile by its ID.
//     * Deletes the profile from the database if it exists.
//     *
//     * @param context The routing context containing the HTTP request with discovery ID.
//     */
//    private void delete(RoutingContext context)
//    {
//        try
//        {
//            // Parse and validate discovery ID from path.
//            var id = APIUtils.parseIdFromPath(context, ID);
//
//            if (id == -1)
//            {
//                return;
//            }
//
//            // Prepare query to delete discovery profile by ID.
//            var query = new JsonObject()
//                .put(QUERY, QueryConstant.DELETE_DISCOVERY)
//                .put(PARAMS, new JsonArray().add(id));
//
//            executeQuery(query)
//                .onComplete(queryResult ->
//                {
//                    if(queryResult.succeeded())
//                    {
//                        var result = queryResult.result();
//
//                        if (!result.isEmpty())
//                        {
//                            LOGGER.info("Discovery profile deleted: ID={}", id);
//
//                            APIUtils.sendSuccess(context, 200,"Discovery profile deleted successfully" ,result);
//                        }
//                        else
//                        {
//                            LOGGER.warn("Discovery profile not found for ID={}", id);
//
//                            APIUtils.sendError(context, 404, "Discovery profile not found");
//                        }
//                    }
//                    else
//                    {
//                        var error = queryResult.cause();
//
//                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
//                    }
//                });
//        }
//        catch (Exception exception)
//        {
//            LOGGER.error("Error deleting discovery: {}", exception.getMessage());
//
//            APIUtils.sendError(context, 500, "Internal server error");
//        }
//
//    }

    /**
     * Handles PUT requests to update an existing discovery profile, including its credentials.
     *
     * @param context The routing context containing the HTTP request.
     */
    protected void update(RoutingContext context)
    {
        try
        {
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            var fields = new String[]{DISCOVERY_PROFILE_NAME, CREDENTIAL_PROFILE_ID, IP_ADDRESS, PORT};

            if (Validator.checkRequestFields(context, fields, true))
            {
                return;
            }

            var body = context.body().asJsonObject();

            var discoveryName = body.getString(DISCOVERY_PROFILE_NAME);

            var credentialIds = body.getJsonArray(CREDENTIAL_PROFILE_ID);

            var ip = body.getString(IP_ADDRESS);

            var port = body.getInteger(PORT);

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

                    for (var i = 0; i < credentialIds.size(); i++)
                    {
                        batchParams.add(new JsonArray().add(id).add(credentialIds.getLong(i)));
                    }

                    var batchQuery = new JsonObject()
                        .put(QUERY, QueryConstant.INSERT_DISCOVERY_CREDENTIAL)
                        .put(BATCHPARAMS, batchParams);

                    return executeQuery(updateQuery)
                        .compose(v -> executeQuery(deleteQuery))
                        .compose(v -> executeBatchQuery(batchQuery)).map(id);
                })
                .onComplete(queryResult ->
                {
                    if(queryResult.succeeded())
                    {
                        APIUtils.sendSuccess(context, 200, "Discovery Profile Updated Successfully",new JsonArray().add(id));
                    }
                    else
                    {
                        var error = queryResult.cause();

                        LOGGER.error("Error updating discovery profile {}: {}", id, error.getMessage());

                        if (error.getMessage().equals("Discovery profile not found"))
                        {
                            APIUtils.sendError(context, 404, "Discovery profile not found");
                        }
                        else
                        {
                            APIUtils.sendError(context, 500, "Failed to update discovery: " + error.getMessage());
                        }
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error updating discovery: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    /**
     * Handles POST requests to run a discovery profile.
     * Validates the profile's existence and triggers a network scan via the event bus.
     *
     * @param context The routing context containing the HTTP request with discovery ID.
     */
    private void run(RoutingContext context)
    {
        try
        {
            // Parse and validate discovery ID from path
            var id = APIUtils.parseIdFromPath(context, ID);

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
                        APIUtils.sendError(context, 404, "Discovery profile not found");

                        return;
                    }

                    // Trigger discovery via event bus

                    var request = new JsonObject().put(ID, id);

                    vertx.eventBus().send(DISCOVERY_RUN,request);

                    APIUtils.sendSuccess(context, 200, "Discovery Profile is currently running", new JsonArray());
                }
                else
                {
                    var error = queryResult.cause();

                    APIUtils.sendError(context, 500, "Failed to check discovery profile: " + error.getMessage());
                }
            });
        }
        catch (Exception exception)
        {
            LOGGER.error("Failed to process discovery request");

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

    /**
     * Handles GET requests to retrieve the results of a discovery profile.
     * Fetches scan results from the database and returns them, or an empty result if none exist.
     *
     * @param context The routing context containing the HTTP request with discovery ID.
     */
    private void getResults(RoutingContext context)
    {
        try
        {
            // Parse and validate discovery ID from path
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                APIUtils.sendError(context, 400, "Invalid discovery ID");

                return;
            }

            var query = new JsonObject()
                .put(QUERY, QueryConstant.GET_DISCOVERY_RESULTS)
                .put(PARAMS, new JsonArray().add(id));

            executeQuery(query)
                .onComplete(queryResult ->
                {
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
                            LOGGER.info("Retrieved discovery results for ID={}", id);

                            APIUtils.sendSuccess(context, 200, "Discovery result for current profile",result);
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        APIUtils.sendError(context, 500, "Database query failed: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Error retrieving discovery results: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "Internal server error");
        }
    }

}
