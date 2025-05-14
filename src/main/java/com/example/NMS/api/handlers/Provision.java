package com.example.NMS.api.handlers;

import com.example.NMS.cache.MetricCache;
import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.ProvisionService;
import com.example.NMS.utility.ApiUtils;
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

public class Provision
{
  public static final Logger LOGGER = LoggerFactory.getLogger(Provision.class);

  public void init(Router provisionRouter)
  {
    provisionRouter.post("/api/provision/:id").handler(this::createProvision);

    provisionRouter.get("/api/provision").handler(this::getAllProvisions);

    provisionRouter.delete("/api/provision/:id").handler(this::deleteProvision);

    provisionRouter.put("/api/provision/:id/metrics").handler(this::updateMetrics);

    provisionRouter.get("/api/polled-data").handler(this::getAllPolledData);
  }

  public void createProvision(RoutingContext context)
  {
    try
    {
      // Get the discovery profile ID from path parameter
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }
      // Get the request body
      var body = context.body().asJsonObject();

      if (body == null || !body.containsKey(SELECTED_IPS))
      {
        ApiUtils.sendError(context, 400, "Missing selected IPs");

        return;
      }

      var selectedIps = body.getJsonArray(SELECTED_IPS);

      if (selectedIps == null || selectedIps.isEmpty())
      {
        ApiUtils.sendError(context, 400, "No IPs selected for provisioning");

        return;
      }


      for (var i = 0; i < selectedIps.size(); i++)
      {
        var ip = selectedIps.getString(i);

        if (!Utility.isValidIPv4(ip))
        {
          ApiUtils.sendError(context, 400, "Invalid IP address: " + ip);

          return;
        }
      }

      ProvisionService.createProvisioningJobs(id, selectedIps)
        .onSuccess(result -> context.response()
          .setStatusCode(201)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put(MSG, SUCCESS)
            .put("Provision_created", result.getJsonArray("insertedRecords"))
            .put("invalid_ips", result.getJsonArray("invalidIps"))
            .encodePrettily()))
        .onFailure(err ->
        {
          var status = err.getMessage().contains("No valid IPs") || err.getMessage().contains("No IPs provided") ? 400 : 500;

          ApiUtils.sendError(context, status, "Failed to create provisioning jobs: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error(e.getMessage());
    }
  }


  public void getAllProvisions(RoutingContext context)
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
            ApiUtils.sendError(context, 404, "No provisioning jobs found");
          }
        })
        .onFailure(err -> ApiUtils.sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error getting all provisions: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

  public void deleteProvision(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      var query = new JsonObject()
        .put(QUERY, DELETE_PROVISIONING_JOB)
        .put(PARAMS, new JsonArray().add(id));

      executeQuery(query)
        .onSuccess(result ->
        {
          var resultArray = result.getJsonArray(RESULT);

          if (SUCCESS.equals(result.getString(MSG)) && !resultArray.isEmpty())
          {
            MetricCache.removeMetricJobsByProvisioningJobId(id);

            context.response()
              .setStatusCode(200)
              .end(result.encodePrettily());
          }
          else
          {
            ApiUtils.sendError(context, 404, "Provisioning job not found");
          }
        })
        .onFailure(err -> ApiUtils.sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error deleting provision job: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

  public void updateMetrics(RoutingContext context)
  {
    try
    {
      var id = ApiUtils.parseIdFromPath(context, ID);

      if (id == -1)
      {
        return;
      }

      var body = context.body().asJsonObject();

      if (body == null || !body.containsKey("metrics"))
      {
        ApiUtils.sendError(context, 400, "Missing metrics configuration");
        return;
      }

      var metrics = body.getJsonArray("metrics");

      if (metrics == null || metrics.isEmpty())
      {
        ApiUtils.sendError(context, 400, "No metrics specified");
        return;
      }

      var batchParams = new JsonArray();

      for (int i = 0; i < metrics.size(); i++)
      {
        var metric = metrics.getJsonObject(i);

        var name = metric.getString("metric_type");

        var isEnabled = metric.getBoolean("is_enabled");

        if (name == null || isEnabled == null)
        {
          ApiUtils.sendError(context, 400, "Invalid metric configuration: metric_type and is_enabled are required");
          return;
        }

        if (!Arrays.asList("CPU", "MEMORY", "DISK", "NETWORK", "PROCESS", "UPTIME").contains(name))
        {
          ApiUtils.sendError(context, 400, "Invalid metric name: " + name);
          return;
        }

        Integer interval = metric.getInteger("polling_interval");

        if (isEnabled && (interval == null || interval <= 0))
        {
          ApiUtils.sendError(context, 400, "Invalid metric configuration: interval must be provided and positive when is_enabled is true");
          return;
        }

        // Use default interval of 300 when is_enabled is false and interval is null
        int effectiveInterval = (interval != null && interval > 0) ? interval : 300;

        batchParams.add(new JsonArray()
          .add(id)
          .add(name)
          .add(effectiveInterval)
          .add(isEnabled));
      }

      // Upsert provided metrics
      var upsertQuery = new JsonObject()
        .put(QUERY, QueryConstant.UPSERT_METRICS)
        .put(BATCHPARAMS, batchParams);

      executeBatchQuery(upsertQuery)
        .compose(v ->
        {
          // Fetch provisioning job details
          JsonObject combinedQuery = new JsonObject()
            .put("query", "SELECT pj.ip, pj.port, pj.credential_profile_id, cp.cred_data AS cred_data " +
              "FROM provisioning_jobs pj " +
              "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
              "WHERE pj.id = $1")
            .put(PARAMS, new JsonArray().add(id));

          return executeQuery(combinedQuery)
            .compose(jobResult ->
            {
              if (!SUCCESS.equals(jobResult.getString("msg")) || jobResult.getJsonArray(RESULT).isEmpty())
              {
                return Future.failedFuture("Provisioning job not found");
              }

              var job = jobResult.getJsonArray(RESULT).getJsonObject(0);

              var ip = job.getString("ip");

              var port = job.getInteger(PORT);

              var credData = job.getJsonObject(CRED_DATA, new JsonObject());

              // Fetch updated metrics
              JsonObject fetchMetricsQuery = new JsonObject()
                .put("query", "SELECT metric_id, name, polling_interval, is_enabled FROM metrics WHERE provisioning_job_id = $1 AND name = ANY($2::metric_name[])")
                .put("params", new JsonArray().add(id).add(new JsonArray(metrics.stream()
                  .map(m -> ((JsonObject) m).getString("metric_type"))
                  .toList())));

              return executeQuery(fetchMetricsQuery)
                .compose(metricsResult ->
                {
                  var updatedMetrics = metricsResult.getJsonArray(RESULT);

                  // Update cache only for provided metrics
                  for (var i = 0; i < updatedMetrics.size(); i++)
                  {
                    var metric = updatedMetrics.getJsonObject(i);

                    var metricId = metric.getLong("metric_id");

                    var metricName = metric.getString("name");

                    var pollingInterval = metric.getInteger("polling_interval");

                    var isEnabled = metric.getBoolean("is_enabled");

                    MetricCache.updateMetricJob(
                      metricId,
                      id,
                      metricName,
                      pollingInterval,
                      ip,
                      port,
                      credData,
                      isEnabled
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
            .put(MSG, SUCCESS)
            .put("provisioning_job_id", id)
            .encodePrettily()))
        .onFailure(err -> ApiUtils.sendError(context, err.getMessage().contains("not found") ? 404 : 500, "Failed to update metrics: " + err.getMessage()));
    }
    catch (Exception exception)
    {
      LOGGER.error("Error updating metrics: {}", exception.getMessage());
      ApiUtils.sendError(context, 500, "Internal server error");
    }

  }

  public void getAllPolledData(RoutingContext context)
  {
    try
    {
      var query = new JsonObject()
        .put(QUERY, QueryConstant.GET_ALL_POLLED_DATA);

      executeQuery(query)
        .onSuccess(result ->
        {
          if (SUCCESS.equals(result.getString(MSG)))
          {
            context.response()
              .setStatusCode(200)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put(MSG, SUCCESS)
                .put(RESULT, result.getJsonArray(RESULT))
                .encodePrettily());
          }
          else
          {
            ApiUtils.sendError(context, 404, "No polled data found");
          }
        })
        .onFailure(err -> ApiUtils.sendError(context, 500, "Database query failed: " + err.getMessage()));
    }
    catch (Exception e)
    {
      LOGGER.error("Error fetching polled data: {}", e.getMessage());

      ApiUtils.sendError(context, 500, "Internal server error");
    }
  }

}
