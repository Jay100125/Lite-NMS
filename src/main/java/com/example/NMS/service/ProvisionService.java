package com.example.NMS.service;

import com.example.NMS.cache.MetricCache;
import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.service.QueryProcessor.*;

public class ProvisionService
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProvisionService.class);

     /**
     * Creates provisioning jobs for the given discovery ID and selected IP addresses.
     * Executes a single database query to validate IPs, insert provisioning jobs, and create default metrics.
     * Updates the metric cache and returns valid/invalid IPs and inserted records.
     *
     * @param discoveryId   The discovery profile ID associated with the IPs.
     * @param selectedIps   The JSON array of IP addresses to provision.
     * @return A Future containing a JSON object with valid IPs, invalid IPs, and inserted records.
     */
    public static Future<JsonObject> createProvisioningJobs(long discoveryId, JsonArray selectedIps)
    {
        if (selectedIps == null || selectedIps.isEmpty())
        {
            return Future.failedFuture("No IPs provided for provisioning");
        }

        // Prepare the provisioning query
        var query = new JsonObject()
          .put(QUERY, QueryConstant.INSERT_PROVISIONING_AND_METRICS)
          .put(PARAMS, new JsonArray().add(discoveryId).add(selectedIps));

        LOGGER.info("Executing provisioning query: {}", query.encodePrettily());

        return executeQuery(query)
          .compose(result ->
          {
              if (result.isEmpty())
              {
                  return Future.failedFuture("No provisioning jobs created");
              }

              var resultRow = result.getJsonObject(0);
              var validIpsArray = resultRow.getJsonArray("valid_ips", new JsonArray());
              var invalidIps = resultRow.getJsonArray("invalid_ips", new JsonArray());
              var validIps = new JsonArray();
              var insertedRecords = new JsonArray();

              // Process valid IP records and update MetricCache

              validIpsArray.forEach(entry ->
              {
                var record = (JsonObject) entry;
                var ip = record.getString(IP);
                var provisioningJobId = record.getLong(PROVISIONING_JOB_ID);
                var port = record.getInteger(PORT);
                var credData = record.getJsonObject(CRED_DATA);
                var metrics = record.getJsonArray("metrics");

                validIps.add(ip);

                metrics.forEach(metricEntry ->
                {
                  var metric = (JsonObject) metricEntry;
                  var metricId = metric.getLong(METRIC_ID);
                  var metricName = metric.getString("name");
                  var pollingInterval = 300; // Default from query

                  // Update MetricCache
                  MetricCache.addMetricJob(metricId, provisioningJobId, metricName, pollingInterval, ip, port, credData);

                  // Build insertedRecords
                  insertedRecords.add(new JsonObject()
                    .put("ip", ip)
                    .put("status", "created")
                    .put(PROVISIONING_JOB_ID, provisioningJobId)
                    .put(METRIC_ID, metricId)
                    .put(METRIC_NAME, metricName));
                });
            });

              if (validIps.isEmpty())
              {
                  return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
              }

              return Future.succeededFuture(new JsonObject()
                .put("validIps", validIps)
                .put("invalidIps", invalidIps)
                .put("insertedRecords", insertedRecords));
          })
          .onFailure(throwable ->
          {
              LOGGER.error("Provisioning failed: {}", throwable.getMessage());
          });
    }
}
