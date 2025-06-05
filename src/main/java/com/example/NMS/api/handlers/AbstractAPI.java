package com.example.NMS.api.handlers;

import com.example.NMS.utility.APIUtils;
import com.example.NMS.utility.DBUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger; // SLF4J Logger import

import static com.example.NMS.constant.Constant.*; // For ID, QUERY, PARAMS, MESSAGE, RESULT, ERROR

/**
 * Abstract base class for API handlers providing common CRUD operations.
 * Subclasses should provide implementations for create and update operations
 * and can override common operations if specific logic is required.
 */
public abstract class AbstractAPI
{
    protected final Logger LOGGER;

    protected final String entityName; // e.g., "Credential", "Discovery Profile"

    /**
     * Constructor for AbstractAPI.
     * @param logger The SLF4J logger instance from the subclass.
     * @param entityName Singular name of the entity (e.g., "Credential").
     */
    protected AbstractAPI(Logger logger, String entityName)
    {
        this.LOGGER = logger;

        this.entityName = entityName;
    }

    // Abstract methods to be implemented by subclasses.
    // These define the contract for creating and updating entities.
    // Subclasses must provide their specific logic for these operations.
    // Routing to these methods is handled in the subclass's init() method.
    protected abstract void create(RoutingContext context);

    protected abstract void update(RoutingContext context);

    /**
     * Handles GET requests to fetch all entities of a certain type.
     * @param context The routing context from Vert.x.
     * @param getAllQueryConstant The database query constant (from QueryConstant class) for fetching all entities.
     */
    protected void getAll(RoutingContext context, String getAllQueryConstant)
    {
        LOGGER.info("Fetching all {}", entityName.toLowerCase());

        var query = new JsonObject().put(QUERY, getAllQueryConstant);

        DBUtils.executeQuery(query)
            .onComplete(queryResult ->
            {
                if (queryResult.succeeded())
                {
                    var result = queryResult.result();

                    // Ensure result is not null, though executeQuery should return non-null JsonArray
                    if (result != null && !result.isEmpty())
                    {
                        APIUtils.sendSuccess(context, 200, entityName, result);
                    }
                    else
                    {
                        APIUtils.sendError(context, 404, "No " + entityName.toLowerCase() + " found");
                    }
                }
                else
                {
                    var error = queryResult.cause();

                    LOGGER.error("Failed to fetch {}: {}", entityName.toLowerCase(), error.getMessage(), error);

                    APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
                }
            });
    }

    /**
     * Handles GET requests to fetch a specific entity by its ID.
     * @param context The routing context from Vert.x.
     * @param getByIdQueryConstant The database query constant (from QueryConstant class) for fetching an entity by ID.
     */
    protected void getById(RoutingContext context, String getByIdQueryConstant)
    {
        try
        {
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                return;
            }

            LOGGER.info("Fetching {} with ID: {}", entityName.toLowerCase(), id);

            var queryParams = new JsonArray().add(id);

            var query = new JsonObject()
                .put(QUERY, getByIdQueryConstant)
                .put(PARAMS, queryParams);

            DBUtils.executeQuery(query)
                .onComplete(queryResult -> {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result != null && !result.isEmpty())
                        {
                            APIUtils.sendSuccess(context, 200, entityName + " details", result);
                        }
                        else
                        {
                            LOGGER.warn("{} not found for ID: {}", entityName, id);

                            APIUtils.sendError(context, 404, entityName + " not found");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        LOGGER.error("Failed to fetch {} with ID {}: {}", entityName.toLowerCase(), id, error.getMessage(), error);

                        APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            // Catching potential errors from APIUtils.parseIdFromPath or JsonObject creation
            LOGGER.error("Unexpected error while fetching {} by ID: {}", entityName.toLowerCase(), exception.getMessage(), exception);

            APIUtils.sendError(context, 500, "An unexpected error occurred while fetching " + entityName.toLowerCase() + ".");
        }
    }

    /**
     * Handles DELETE requests to remove an entity by its ID.
     * Subclasses can override this method if additional actions are needed upon deletion (e.g., cache clearing).
     * @param context The routing context from Vert.x.
     * @param deleteQueryConstant The database query constant (from QueryConstant class) for deleting an entity by ID.
     */
    protected void delete(RoutingContext context, String deleteQueryConstant)
    {
        try
        {
            var id = APIUtils.parseIdFromPath(context, ID);

            if (id == -1)
            {
                // APIUtils.parseIdFromPath already sends an error response and logs
                return;
            }

            LOGGER.info("Deleting {} with ID: {}", entityName.toLowerCase(), id);

            var queryParams = new JsonArray().add(id);

            var query = new JsonObject()
                .put(QUERY, deleteQueryConstant)
                .put(PARAMS, queryParams);

            DBUtils.executeQuery(query)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result != null && !result.isEmpty())
                        {
                            LOGGER.info("{} with ID {} deleted successfully.", entityName, id);

                            APIUtils.sendSuccess(context, 200, entityName + " deleted successfully", result);
                        }
                        else
                        {
                            LOGGER.warn("Attempted to delete non-existent {} with ID: {}", entityName, id);

                            APIUtils.sendError(context, 404, entityName + " not found");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        LOGGER.error("Failed to delete {} with ID {}: {}", entityName.toLowerCase(), id, error.getMessage(), error);

                        APIUtils.sendError(context, 500, "Database error: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error while deleting {}: {}", entityName.toLowerCase(), exception.getMessage(), exception);

            APIUtils.sendError(context, 500, "An unexpected error occurred during deletion of " + entityName.toLowerCase() + ".");
        }
    }
}
