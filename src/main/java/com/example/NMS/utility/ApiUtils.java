package com.example.NMS.utility;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ApiUtils
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiUtils.class);

  public static void sendError(RoutingContext ctx, int statusCode, String errorMessage)
  {
    LOGGER.warn(errorMessage);

    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("status", "failed")
        .put("error", errorMessage)
        .encode());
  }
}
