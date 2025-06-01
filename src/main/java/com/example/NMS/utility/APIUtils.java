package com.example.NMS.utility;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;


/**
 * Utility class for handling common API response operations in Lite NMS.
 * Provides methods to send standardized success and error responses, and to parse path parameters
 * as long IDs with error handling. Ensures consistent JSON response formats across API endpoints.
 */
public class APIUtils
{
    private static final Logger LOGGER = LoggerFactory.getLogger(APIUtils.class);

  /**
   * Sends a successful JSON response with the specified status code, message, and result data.
   * The response includes standardized fields for status, message, and result.
   *
   * @param context     The routing context for the HTTP response.
   * @param statusCode  The HTTP status code (e.g., 200, 201).
   * @param message     The success message to include in the response.
   * @param result      The JSON array containing the result data.
   */
    public static void sendSuccess(RoutingContext context, int statusCode, String message,  JsonArray result)
    {
        context.response()
            .setStatusCode(statusCode)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
                .put(STATUS_CODE, statusCode)
                .put(STATUS, SUCCESS)
                .put(MESSAGE, message)
                .put(RESULT, result)
                .encode());
    }

  /**
   * Sends an error JSON response with the specified status code and error message.
   * The response includes standardized fields for status and error details.
   *
   * @param context      The routing context for the HTTP response.
   * @param statusCode   The HTTP status code (e.g., 400, 404, 500).
   * @param errorMessage The error message to include in the response.
   */
    public static void sendError(RoutingContext context, int statusCode, String errorMessage)
    {
        LOGGER.warn(errorMessage);

        context.response()
          .setStatusCode(statusCode)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put(STATUS_CODE, statusCode)
            .put(STATUS, FAILURE)
            .put(ERROR, errorMessage)
            .encode());
    }


  /**
   * Parses a path parameter as a long ID, sending a 400 error response if invalid.
   * Validates that the parameter is present, non-empty, and a valid long integer.
   *
   * @param context        The routing context containing the path parameters.
   * @param paramName  The name of the path parameter (e.g., "id").
   * @return The parsed long ID, or -1 if parsing fails (after sending an error response).
   */
    public static long parseIdFromPath(RoutingContext context, String paramName)
    {
        var id = context.pathParam(paramName);

        if (id == null || id.trim().isEmpty())
        {
            sendError(context, 400, "Missing or empty ID in path");

            return -1;
        }

        try
        {
            return Long.parseLong(id);
        }
        catch (Exception exception)
        {
            sendError(context, 400, "Invalid ID format: " + id);

            return -1;
        }
    }
}
