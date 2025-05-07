package com.example.NMS.api;

import com.example.NMS.MetricJobCache;
import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.Utility;
import io.vertx.core.Future;
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
            MetricJobCache.removeMetricJobsByProvisioningJobId(id);

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
//        .onSuccess(v -> context.response()
//          .setStatusCode(200)
//          .putHeader("Content-Type", "application/json")
//          .end(new JsonObject()
//            .put("msg", "Success")
//            .put("provisioning_job_id", provisioningJobId)
//            .encodePrettily()))
//        .onFailure(err -> sendError(context, 500, "Failed to update metrics: " + err.getMessage()));

        .compose(v ->
        {
          // Combined query to fetch provisioning job and cred_data
          JsonObject combinedQuery = new JsonObject()
            .put("query", "SELECT pj.ip, pj.port, pj.credential_profile_id, cp.cred_data AS cred_data " +
              "FROM provisioning_jobs pj " +
              "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
              "WHERE pj.id = $1")
            .put("params", new JsonArray().add(provisioningJobId));

          return executeQuery(combinedQuery)
            .compose(jobResult ->
            {
              if (!SUCCESS.equals(jobResult.getString("msg")) || jobResult.getJsonArray("result").isEmpty())
              {
                return Future.failedFuture("Provisioning job not found");
              }

              var job = jobResult.getJsonArray("result").getJsonObject(0);
              var ip = job.getString("ip");
              var port = job.getInteger("port");
              var credData = job.getJsonObject("cred_data", new JsonObject());

              // Fetch current metric IDs
              JsonObject fetchMetricsQuery = new JsonObject()
                .put("query", "SELECT metric_id, name, polling_interval FROM metrics WHERE provisioning_job_id = $1")
                .put("params", new JsonArray().add(provisioningJobId));

              return executeQuery(fetchMetricsQuery)
                .compose(metricsResult ->
                {
                  var currentMetrics = metricsResult.getJsonArray("result");

                  // Remove stale metric jobs from cache
                  var metricJobs = MetricJobCache.getMetricJobsByProvisioningJobId(provisioningJobId);
                  for (var entry : metricJobs.entrySet())
                  {
                    var metricId = entry.getKey();
                    var metricJob = entry.getValue();
                    var metricName = metricJob.getString("metric_name");
                    if (!metricNames.contains(metricName))
                    {
                      MetricJobCache.removeMetricJob(metricId);
                    }
                  }

                  // Add or update metric jobs in cache
                  for (int i = 0; i < currentMetrics.size(); i++)
                  {
                    logger.info("(((((((((((((((((((((((((((9999");
                    var metric = currentMetrics.getJsonObject(i);
                    logger.info("(((((((((((((((((((((((((((1");

                    var metricId = metric.getLong("metric_id");
                    logger.info("(((((((((((((((((((((((((((2");

                    var metricName = metric.getString("name");
                    logger.info("(((((((((((((((((((((((((((3");

                    var pollingInterval = metric.getInteger("polling_interval");
                    logger.info("(((((((((((((((((((((((((((4");

                    MetricJobCache.updateMetricJob(
                      metricId,
                      provisioningJobId,
                      metricName,
                      pollingInterval,
                      ip,
                      port,
                      credData
                    );
                  }

                  return Future.succeededFuture();
                });
            });
        })
        .onSuccess(v -> context.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put("msg", "Success")
            .put("provisioning_job_id", provisioningJobId)
            .encodePrettily()))
        .onFailure(err -> sendError(context, err.getMessage().contains("not found") ? 404 : 500, "Failed to update metrics: " + err.getMessage()));
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
