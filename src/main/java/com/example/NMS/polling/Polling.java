
package com.example.NMS.polling;

import com.example.NMS.MetricJobCache;
import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.QueryProcessor;
import com.example.NMS.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Polling extends AbstractVerticle
{
  private static final Logger logger = LoggerFactory.getLogger(Polling.class);

  // Timer interval (seconds) for polling
  private static final int TIMER_INTERVAL_SECONDS = 10;

  @Override
  public void start()
  {
    // Initialize the cache
    MetricJobCache.refreshCache(vertx);

    vertx.setPeriodic(TIMER_INTERVAL_SECONDS * 1000, this::handlePolling);

    logger.info("PollingVerticle started with timer interval {} seconds", TIMER_INTERVAL_SECONDS);
  }

  // Handle periodic polling
  private void handlePolling(Long timerId)
  {
    var jobsToPoll = MetricJobCache.handleTimer();

    if (!jobsToPoll.isEmpty())
    {
      pollJobs(jobsToPoll);
    }
  }

  // Poll the collected jobs
  private void pollJobs(List<JsonObject> jobs)
  {
    try
    {
      // Group jobs by IP and credentials to batch SSH calls
      Map<String, List<JsonObject>> jobsByDevice = new HashMap<>();

      for (var job : jobs)
      {
        var deviceKey = job.getString("ip") + ":" + job.getJsonObject("cred_data").encode();

        jobsByDevice.computeIfAbsent(deviceKey, k -> new ArrayList<>()).add(job);
      }

      var ips = jobsByDevice.keySet().stream()
        .map(key -> key.split(":")[0])
        .distinct()
        .toList();

      var reachResults = Utility.checkReachability(ips, 22);

      var targets = new JsonArray();

      reachResults.forEach(result ->
      {
        var res = (JsonObject) result;

        if (res.getBoolean("reachable") && res.getBoolean("port_open"))
        {
          var ip = res.getString("ip");

          jobsByDevice.forEach((deviceKey, jobList) ->
          {
            if (deviceKey.startsWith(ip + ":"))
            {
              var metrics = jobList.stream()
                .map(job -> job.getString("metric_name"))
                .toList();

              var sampleJob = jobList.get(0); // All jobs in list have same IP/cred

              targets.add(new JsonObject()
                .put("ip.address", ip)
                .put("port", sampleJob.getInteger("port"))
                .put("user", sampleJob.getJsonObject("cred_data").getString("user"))
                .put("password", sampleJob.getJsonObject("cred_data").getString("password"))
                .put("provision_profile_id", sampleJob.getLong("provisioning_job_id"))
                .put("metric_type", new JsonArray(metrics)));
            }
          });
        }
      });

      if (targets.isEmpty())
      {
        logger.info("No reachable targets for polling");

        return;
      }

      var pluginInput = new JsonObject()
        .put("category", "polling")
        .put("targets", targets);

      vertx.executeBlocking(promise ->
      {
        logger.info("Plugin input: {}", pluginInput.encodePrettily());

        JsonArray results = Utility.runSSHPlugin(pluginInput);

        logger.info("Plugin result: {}", results.encodePrettily());

        storePollResults(results);

        promise.complete();
      }, false);
    }
    catch (Exception e)
    {
      logger.error("Polling failed: {}", e.getMessage());
    }
  }

  // Store polling results in the database
  private void storePollResults(JsonArray results)
  {
    if (results == null || results.isEmpty()) return;

    var batchParams = new JsonArray();

    results.forEach(result ->
    {
      var resultObj = (JsonObject) result;

//      logger.info("Result: {}", resultObj.encodePrettily());

      if ("success".equals(resultObj.getString("status")))
      {
        var jobId = resultObj.getLong("provision_profile_id");

        var data = resultObj.getJsonObject("data");

        if (data != null)
        {
          data.fieldNames().forEach(metric ->
            batchParams.add(new JsonArray()
              .add(jobId)
              .add(metric)
              .add(new JsonObject(data.getString(metric)))));
        }
      }
    });

    if (batchParams.isEmpty()) return;

    var batchQuery = new JsonObject()
      .put("query", QueryConstant.INSERT_POLLED_DATA)
      .put("batchParams", batchParams);

    QueryProcessor.executeBatchQuery(batchQuery)
      .onSuccess(r -> logger.info("Stored {} metrics", batchParams.size()))
      .onFailure(err -> logger.error("Store failed: {}", err.getMessage()));
  }
}
