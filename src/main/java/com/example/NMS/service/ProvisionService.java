package com.example.NMS.service;

import com.example.NMS.MetricJobCache;
import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

import static com.example.NMS.constant.Constant.SUCCESS;
import static com.example.NMS.service.QueryProcessor.*;

public class ProvisionService
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ProvisionService.class);

  /**
   * Create provisioning jobs for the given discovery ID and selected IPs.
   *
   * @param discoveryId The discovery profile ID
   * @param selectedIps Array of IPs to provision
   * @return Future containing provisioning results (valid/invalid IPs, inserted records)
   */
  public static Future<JsonObject> createProvisioningJobs(long discoveryId, JsonArray selectedIps)
  {
    if (selectedIps == null || selectedIps.isEmpty())
    {
      return Future.failedFuture("No IPs provided for provisioning");
    }

    var validateQuery = new JsonObject()
      .put("query", "SELECT ip, result, credential_profile_id, port " +
        "FROM discovery_result WHERE discovery_id = $1 AND ip = ANY($2::varchar[])")
      .put("params", new JsonArray().add(discoveryId).add(selectedIps));

    LOGGER.info(validateQuery.encodePrettily());

    return executeQuery(validateQuery)
      .compose(result ->
      {
        if (!SUCCESS.equals(result.getString("msg")))
        {
          return Future.failedFuture("Failed to validate IPs");
        }

        var resultArray = result.getJsonArray("result");

        Map<String, JsonObject> discoveryResults = new HashMap<>();

        for (var i = 0; i < resultArray.size(); i++)
        {
          JsonObject row = resultArray.getJsonObject(i);

          discoveryResults.put(row.getString("ip"), row);
        }

        var validIps = new JsonArray();

        var invalidIps = new JsonArray();

        var batchParams = new JsonArray();

        for (int i = 0; i < selectedIps.size(); i++)
        {
          var ip = selectedIps.getString(i);

          var discoveryResult = discoveryResults.get(ip);

          if (discoveryResult == null)
          {
            invalidIps.add(new JsonObject().put("ip", ip).put("error", "IP not found in discovery results"));
          }
          else if
          (!"completed".equals(discoveryResult.getString("result")))
          {
            invalidIps.add(new JsonObject().put("ip", ip).put("error", "Discovery not completed"));
          }
          else
          {
            validIps.add(ip);

            batchParams.add(new JsonArray()
              .add(discoveryResult.getLong("credential_profile_id"))
              .add(ip)
              .add(discoveryResult.getInteger("port")));
          }
        }

        if (validIps.isEmpty())
        {
          return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
        }

        var batchQuery = new JsonObject()
          .put("query", QueryConstant.INSERT_PROVISIONING_JOB)
          .put("batchParams", batchParams);

        return executeBatchQuery(batchQuery)
          .compose(insertResult ->
          {
            if (!SUCCESS.equals(insertResult.getString("msg")))
            {
              var error = insertResult.getString("ERROR", "Batch insert failed");

              if (error.contains("provisioning_jobs_ip_unique"))
              {
                for (int i = 0; i < validIps.size(); i++)
                {
                  var ip = validIps.getString(i);

                  invalidIps.add(new JsonObject().put("ip", ip).put("error", "IP already provisioned"));
                }
                return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
              }
              return Future.failedFuture(error);
            }

            var insertedIds = insertResult.getJsonArray("insertedIds");

            var metricsBatch = new JsonArray();

            String[] defaultMetrics = {"CPU", "MEMORY", "DISK"};

            var defaultInterval = 300;

            for (var i = 0; i < insertedIds.size(); i++)
            {
              var provisioningJobId = insertedIds.getLong(i);

              for (var metric : defaultMetrics)
              {
                metricsBatch.add(new JsonArray()
                  .add(provisioningJobId)
                  .add(metric)
                  .add(defaultInterval));
              }
            }

            var metricsQuery = new JsonObject()
              .put("query", QueryConstant.INSERT_DEFAULT_METRICS)
              .put("batchParams", metricsBatch);

            return executeBatchQuery(metricsQuery)
              .compose(v ->
              {
                var insertedRecords = new JsonArray();

                if (SUCCESS.equals(v.getString("msg")))
                {
                  var metricId = v.getJsonArray("insertedIds").getLong(0);

                  for (var j = 0; j < validIps.size(); j++)
                  {
                    var ip = validIps.getString(j);

                    var discoveryResult = discoveryResults.get(ip);

                    var provisioningJobId = insertedIds.getLong(j);

                    var credData = fetchCredData(discoveryResult.getLong("credential_profile_id"));

                    for (var metric : defaultMetrics)
                    {
                      MetricJobCache.addMetricJob(
                        metricId++,
                        provisioningJobId,
                        metric,
                        defaultInterval,
                        ip,
                        discoveryResult.getInteger("port"),
                        credData);

                      insertedRecords.add(new JsonObject()
                        .put("ip", ip)
                        .put("status", "created")
                        .put("provisioning_job_id", provisioningJobId)
                        .put("metric_id", metricId - 1)
                        .put("metric_name", metric));
                    }
                  }
                }

                return Future.succeededFuture(new JsonObject()
                  .put("validIps", validIps)
                  .put("invalidIps", invalidIps)
                  .put("insertedRecords", insertedRecords));
              });
          });
      });
  }

  private static JsonObject fetchCredData(long credentialProfileId)
  {
    var query = new JsonObject()
      .put("query", "SELECT cred_data FROM credential_profile WHERE id = $1")
      .put("params", new JsonArray().add(credentialProfileId));

    return executeQuery(query)
      .map(result ->
      {
        if (result != null && SUCCESS.equals(result.getString("msg")) && !result.getJsonArray("result").isEmpty())
        {
          return result.getJsonArray("result").getJsonObject(0).getJsonObject("cred_data");
        }
        return new JsonObject();
      })
      .result();
  }
}
