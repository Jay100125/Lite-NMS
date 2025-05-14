package com.example.NMS.utility;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiUtils
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiUtils.class);

  public static void sendError(RoutingContext context, int statusCode, String errorMessage)
  {
    LOGGER.warn(errorMessage);

    context.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("status", "failed")
        .put("error", errorMessage)
        .encode());
  }

  /**
   * Parses a path parameter as a long ID, sending a 400 error response if invalid.
   *
   * @param context        The routing context containing the path parameters.
   * @param paramName  The name of the path parameter (e.g., "id").
   * @return The parsed long ID, or -1 if parsing fails (after sending an error response).
   */
  public static long parseIdFromPath(RoutingContext context, String paramName)
  {
    var idStr = context.pathParam(paramName);

    if (idStr == null || idStr.trim().isEmpty())
    {
      sendError(context, 400, "Missing or empty ID in path");

      return -1;
    }

    try
    {
      return Long.parseLong(idStr);
    }
    catch (Exception exception)
    {
      sendError(context, 400, "Invalid ID format: " + idStr);

      return -1;
    }
  }
}
