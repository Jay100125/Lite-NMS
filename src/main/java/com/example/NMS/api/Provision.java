package com.example.NMS.api;

import com.example.NMS.utility.Utility;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static com.example.NMS.QueryProcessor.createProvisioningJobs;
import static com.example.NMS.QueryProcessor.executeQuery;

public class Provision {

  public static final Logger logger = LoggerFactory.getLogger(Provision.class);

  public void init(Router provisionRouter)
  {
    provisionRouter.post("/api/provision/:id").handler(this::handlePostProvision);

    provisionRouter.get("/api/provision").handler(this::handleGetAllProvisions);

    provisionRouter.delete("/api/provision/:id").handler(this::handleDeleteProvision);
  }

  public void handlePostProvision(RoutingContext context)
  {
    try
    {
      // Get the discovery profile ID from path parameter
      var discoveryIdStr = context.pathParam("id");

      long discoveryId;

      try
      {
        discoveryId = Long.parseLong(discoveryIdStr);
      }
      catch (NumberFormatException e)
      {
        sendError(context, 400, "Invalid discovery ID");

        return;
      }

      // Get the request body
      var body = context.getBodyAsJson();

      if (body == null || !body.containsKey("selected_ips"))
      {
        sendError(context, 400, "Missing selected IPs");

        return;
      }

      var selectedIps = body.getJsonArray("selected_ips");

      if (selectedIps == null || selectedIps.isEmpty())
      {
        sendError(context, 400, "No IPs selected for provisioning");

        return;
      }


      for (var i = 0; i < selectedIps.size(); i++)
      {
        String ip = selectedIps.getString(i);

        if (!Utility.isValidIPv4(ip))
        {
          sendError(context, 400, "Invalid IP address: " + ip);

          return;
        }
      }

      createProvisioningJobs(discoveryId, selectedIps, context);
    } catch (Exception e) {
        logger.error(e.getMessage());
    }
  }
  public void handleGetAllProvisions(RoutingContext context)
  {
    try {
      var query = new JsonObject()
        .put("query", "SELECT pj.*, cp.credential_name, cp.system_type " +
          "FROM provisioning_jobs pj " +
          "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
          "ORDER BY pj.id DESC");

      executeQuery(query, context);
    } catch (Exception e) {
      logger.error("Error getting all provisions: {}", e.getMessage());
      sendError(context, 500, "Internal server error: " + e.getMessage());
    }
  }


  public void handleDeleteProvision(RoutingContext context)
  {
    try {
      var idStr = context.pathParam("id");

      long id;
      try {
        id = Long.parseLong(idStr);
      } catch (NumberFormatException e) {
        sendError(context, 400, "Invalid provision job ID");
        return;
      }

      var query = new JsonObject()
        .put("query", "DELETE FROM provisioning_jobs WHERE id = $1 RETURNING id")
        .put("params", new JsonArray().add(id));

      executeQuery(query, context);
    } catch (Exception e) {
      logger.error("Error deleting provision job: {}", e.getMessage());
      sendError(context, 500, "Internal server error: " + e.getMessage());
    }
  }

  private void sendError(RoutingContext ctx, int statusCode, String errorMessage) {
    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put(statusCode == 400 || statusCode == 409 ? "msg" : "status", "failed")
        .put("error", errorMessage)
        .encode());
  }
}
