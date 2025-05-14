package com.example.NMS.service;

import com.example.NMS.cache.MetricCache;
import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.constant.QueryConstant.GET_CREDENTIAL_DATA;
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
      .put(QUERY, "SELECT ip, result, credential_profile_id, port " +
        "FROM discovery_result WHERE discovery_id = $1 AND ip = ANY($2::varchar[])")
      .put("params", new JsonArray().add(discoveryId).add(selectedIps));

    LOGGER.info(validateQuery.encodePrettily());

    return executeQuery(validateQuery)
      .compose(result ->
      {
        var discoveryResults = new HashMap<String, JsonObject>();

        for (var i = 0; i < result.size(); i++)
        {
          var row = result.getJsonObject(i);

          discoveryResults.put(row.getString("ip"), row);
        }

        var validIps = new JsonArray();

        var invalidIps = new JsonArray();

        var batchParams = new JsonArray();

        for (var i = 0; i < selectedIps.size(); i++)
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

        LOGGER.info(batchParams.encodePrettily());

        if (validIps.isEmpty())
        {
          return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
        }

        var batchQuery = new JsonObject()
          .put(QUERY, QueryConstant.INSERT_PROVISIONING_JOB)
          .put(BATCHPARAMS, batchParams);

        return executeBatchQuery(batchQuery)
          .compose(insertResult ->
          {
//            if (!SUCCESS.equals(insertResult.getString("msg")))
//            {
//              var error = insertResult.getString("ERROR", "Batch insert failed");
//
//              if (error.contains("provisioning_jobs_ip_unique"))
//              {
//                for (int i = 0; i < validIps.size(); i++)
//                {
//                  var ip = validIps.getString(i);
//
//                  invalidIps.add(new JsonObject().put("ip", ip).put("error", "IP already provisioned"));
//                }
//                return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
//              }
//              return Future.failedFuture(error);
//            }

            LOGGER.info(insertResult.encodePrettily());

            var metricsBatch = new JsonArray();

            String[] allMetrics = {"CPU", "MEMORY", "DISK", "UPTIME", "NETWORK", "PROCESS"};

            var defaultInterval = 300;

            for (var i = 0; i < insertResult.size(); i++)
            {
              var provisioningJobId = insertResult.getLong(i);

              for (var metric : allMetrics)
              {
                metricsBatch.add(new JsonArray()
                  .add(provisioningJobId)
                  .add(metric)
                  .add(defaultInterval)
                  .add(true));
              }
            }

            var metricsQuery = new JsonObject()
              .put(QUERY, QueryConstant.INSERT_DEFAULT_METRICS)
              .put(BATCHPARAMS, metricsBatch);

            return executeBatchQuery(metricsQuery)
              .compose(queryResult ->
              {
                var insertedRecords = new JsonArray();

                if (!queryResult.isEmpty())
                {
                  var metricId = queryResult.getLong(0);

                  for (var j = 0; j < validIps.size(); j++)
                  {
                    var ip = validIps.getString(j);

                    var discoveryResult = discoveryResults.get(ip);

                    var provisioningJobId = insertResult.getLong(j);

                    var credData = fetchCredData(discoveryResult.getLong(CREDENTIAL_PROFILE_ID));

                    for (var metric : allMetrics)
                    {
                      MetricCache.addMetricJob(
                        metricId++,
                        provisioningJobId,
                        metric,
                        defaultInterval,
                        ip,
                        discoveryResult.getInteger(PORT),
                        credData);

                      insertedRecords.add(new JsonObject()
                        .put("ip", ip)
                        .put("status", "created")
                        .put(PROVISIONING_JOB_ID, provisioningJobId)
                        .put(METRIC_ID, metricId - 1)
                        .put(METRIC_NAME, metric));
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
      .put(QUERY, GET_CREDENTIAL_DATA)
      .put(PARAMS, new JsonArray().add(credentialProfileId));

    return executeQuery(query)
      .map(result ->
      {
        if (!result.isEmpty())
        {
          return result.getJsonObject(0).getJsonObject(CRED_DATA);
        }
        return new JsonObject();
      })
      .result();
  }
}
