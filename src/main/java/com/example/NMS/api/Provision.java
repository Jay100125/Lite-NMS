package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.Utility;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.DELETE_PROVISIONING_JOB;
import static com.example.NMS.constant.QueryConstant.GET_ALL_PROVISIONING_JOBS;
import static com.example.NMS.service.QueryProcessor.*;

public class Provision {

  public static final Logger logger = LoggerFactory.getLogger(Provision.class);

  public void init(Router provisionRouter)
  {
    provisionRouter.post("/api/provision/:id").handler(this::handlePostProvision);

    provisionRouter.get("/api/provision").handler(this::handleGetAllProvisions);

    provisionRouter.delete("/api/provision/:id").handler(this::handleDeleteProvision);

    provisionRouter.put("/api/provision/:id/metrics").handler(this::handleUpdateMetrics);

    provisionRouter.get("/api/polled-data").handler(this::handleGetAllPolledData);
  }

  public void handlePostProvision(RoutingContext context)
  {
    try
    {
      // Get the discovery profile ID from path parameter
      var discoveryIdStr = context.pathParam(ID);

      long discoveryId;

      try
      {
        discoveryId = Long.parseLong(discoveryIdStr);
      }
      catch (Exception e)
      {
        sendError(context, 400, "Invalid discovery ID");

        return;
      }

      // Get the request body
      var body = context.body().asJsonObject();

      if (body == null || !body.containsKey(SELECTED_IPS))
      {
        sendError(context, 400, "Missing selected IPs");

        return;
      }

      var selectedIps = body.getJsonArray(SELECTED_IPS);

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
    }
    catch (Exception e)
    {
        logger.error(e.getMessage());
    }
  }


  public void handleGetAllProvisions(RoutingContext context)
  {
    try
    {
      var query = new JsonObject()
        .put(QUERY, GET_ALL_PROVISIONING_JOBS);

      executeQuery(query)
        .onSuccess(result ->
        {
          if (SUCCESS.equals(result.getString(MSG)))
          {
            context.response()
              .setStatusCode(200)
              .end(result.encodePrettily());
          }
          else
          {
            sendError(context, 404, "No provisioning jobs found");
          }
        })
        .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      logger.error("Error getting all provisions: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }
  }

  public void handleDeleteProvision(RoutingContext context)
  {
    try
    {
      var idStr = context.pathParam("id");

      long id;

      try
      {
        id = Long.parseLong(idStr);
      }
      catch (NumberFormatException e)
      {
        sendError(context, 400, "Invalid provision job ID");

        return;
      }

      var query = new JsonObject()
        .put(QUERY, DELETE_PROVISIONING_JOB)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray("result");

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(200)
              .end(result.encodePrettily());
          }
          else
          {
            sendError(context, 404, "Provisioning job not found");
          }
        })
        .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      logger.error("Error deleting provision job: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }
  }

  public void handleUpdateMetrics(RoutingContext context)
  {
    try
    {
      var idStr = context.pathParam("id");

      long provisioningJobId;

      try
      {
        provisioningJobId = Long.parseLong(idStr);
      }
      catch (NumberFormatException e)
      {
        sendError(context, 400, "Invalid provisioning job ID");

        return;
      }

      var body = context.body().asJsonObject();

      if (body == null || !body.containsKey("metrics"))
      {
        sendError(context, 400, "Missing metrics configuration");

        return;
      }

      var metrics = body.getJsonArray("metrics");

      if (metrics == null || metrics.isEmpty())
      {
        sendError(context, 400, "No metrics specified");

        return;
      }

      var batchParams = new JsonArray();

      var metricNames = new JsonArray();

      for (int i = 0; i < metrics.size(); i++)
      {
        var metric = metrics.getJsonObject(i);

        var name = metric.getString("name");

        var interval = metric.getInteger("polling_interval");

        if (name == null || interval == null || interval <= 0)
        {
          sendError(context, 400, "Invalid metric configuration: " + metric.encode());

          return;
        }
        if (!Arrays.asList("CPU", "MEMORY", "DISK", "FILE", "PROCESS", "NETWORK", "PING", "SYSINFO").contains(name))
        {
          sendError(context, 400, "Invalid metric name: " + name);

          return;
        }

        batchParams.add(new JsonArray()
          .add(provisioningJobId)
          .add(name)
          .add(interval));

        metricNames.add(name);
      }

      // Delete stale metrics
      var deleteStaleQuery = new JsonObject()
        .put("query", QueryConstant.DELETE_STALE_METRICS)
        .put("params", new JsonArray().add(provisioningJobId).add(metricNames));

      // Upsert metrics
      var upsertQuery = new JsonObject()
        .put("query", QueryConstant.UPSERT_METRICS)
        .put("batchParams", batchParams);

      executeQuery(deleteStaleQuery)
        .compose(v -> executeBatchQuery(upsertQuery))
        .onSuccess(v -> context.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put("msg", "Success")
            .put("provisioning_job_id", provisioningJobId)
            .encodePrettily()))
        .onFailure(err -> sendError(context, 500, "Failed to update metrics: " + err.getMessage()));
    }
    catch (Exception e)
    {
      logger.error("Error updating metrics: {}", e.getMessage());

      sendError(context, 500, "Internal server error");
    }
  }

  public void handleGetAllPolledData(RoutingContext context) {
    try {
      var query = new JsonObject()
        .put(QUERY, QueryConstant.GET_ALL_POLLED_DATA);

      executeQuery(query)
        .onSuccess(result -> {
          if (SUCCESS.equals(result.getString(MSG))) {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put("msg", "Success")
                .put("results", result.getJsonArray("result"))
                .encodePrettily());
          } else {
            sendError(context, 404, "No polled data found");
          }
        })
        .onFailure(err -> sendError(context, 500, "Database query failed: " + err.getMessage()));
    } catch (Exception e) {
      logger.error("Error fetching polled data: {}", e.getMessage());
      sendError(context, 500, "Internal server error");
    }
  }


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
