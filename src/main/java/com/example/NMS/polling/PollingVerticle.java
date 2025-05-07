//
////
////// TODO : going to be changed (temporary) Currently creating separate process for all ip address
////// Current flow :- create group of metric job like if for ip 10.20.40.123 we have to collect cpu and memory for 5 minute interval it will only spawn one process for that and give our result
//////                but if for another ip 10.20.41.10 we have cpu and memory for 5 minute interval it will spawn another process for that
////// Improvement :- generate single process for all the same interval like one process for all targets and their metrics if their metrics intervals are same
//
//// DONE
//package com.example.NMS.polling;
//
//import com.example.NMS.constant.QueryConstant;
//import com.example.NMS.service.QueryProcessor;
//import com.example.NMS.utility.Utility;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.*;
//import java.util.concurrent.ConcurrentHashMap;
//
//public class PollingVerticle extends AbstractVerticle
//{
//  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);
//
//  private final Map<Integer, List<Job>> intervalGroups = new ConcurrentHashMap<>();
//
//  private final Map<Integer, Long> activeTimers = new ConcurrentHashMap<>();
//
//  private record Job(Long id, String ip, int port, JsonObject credData, List<String> metrics) {}
//
//  @Override
//  public void start()
//  {
//    vertx.setPeriodic(10_000, l -> refreshCache());
//  }
//
//  private void refreshCache()
//  {
//    String query = "SELECT m.provisioning_job_id, m.polling_interval, " +
//      "pj.ip, pj.port, cp.cred_data, ARRAY_AGG(m.name) AS metrics " +
//      "FROM metrics m " +
//      "JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id " +
//      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
//      "GROUP BY m.provisioning_job_id, pj.ip, pj.port, cp.cred_data, m.polling_interval";
//
//    QueryProcessor.executeQuery(new JsonObject().put("query", query))
//      .onSuccess(result -> updateIntervalGroups(result.getJsonArray("result")))
//      .onFailure(err -> logger.error("Cache refresh failed: {}", err.getMessage()));
//  }
//
//  private void updateIntervalGroups(JsonArray metrics)
//  {
//    Map<Integer, List<Job>> newGroups = new HashMap<>();
//
//    metrics.forEach(entry ->
//    {
//      JsonObject metric = (JsonObject) entry;
//
//      int interval = metric.getInteger("polling_interval");
//
//      newGroups.computeIfAbsent(interval, k -> new ArrayList<>())
//        .add(new Job(
//          metric.getLong("provisioning_job_id"),
//          metric.getString("ip"),
//          metric.getInteger("port"),
//          metric.getJsonObject("cred_data"),
//          metric.getJsonArray("metrics").getList()
//        ));
//    });
//
//    // Update timers only if intervals change
//    if (!intervalGroups.keySet().equals(newGroups.keySet()))
//    {
//      // Cancel timers for removed intervals
//      new ArrayList<>(activeTimers.keySet()).forEach(interval ->
//      {
//        if (!newGroups.containsKey(interval))
//        {
//          vertx.cancelTimer(activeTimers.remove(interval));
//
//          logger.info("Cancelled timer for interval {}s", interval);
//        }
//      });
//
//      // Schedule timers for new intervals
//      newGroups.keySet().forEach(interval ->
//      {
//        if (!activeTimers.containsKey(interval))
//        {
//          long timerId = vertx.setPeriodic(interval * 1000, l -> pollInterval(interval, intervalGroups.get(interval)));
//
//          activeTimers.put(interval, timerId);
//
//          logger.info("Scheduled timer for interval {}s", interval);
//        }
//      });
//    }
//
//    // Always update the cache with the latest jobs
//    intervalGroups.clear();
//
//    intervalGroups.putAll(newGroups);
//  }
//
//  private void pollInterval(int interval, List<Job> jobs)
//  {
//    try
//    {
//      List<String> ips = jobs.stream().map(Job::ip).toList();
//
//      JsonArray reachResults = Utility.checkReachability(ips, 22);
//
//      JsonArray targets = new JsonArray();
//
//      reachResults.forEach(result -> {
//        JsonObject res = (JsonObject) result;
//
//        if (res.getBoolean("reachable") && res.getBoolean("port_open"))
//        {
//          jobs.stream()
//            .filter(job -> job.ip().equals(res.getString("ip")))
//            .findFirst()
//            .ifPresent(job -> targets.add(createTargetJson(job)));
//        }
//      });
//
//      if (targets.isEmpty())
//      {
//        logger.info("No reachable targets for {}s interval", interval);
//
//        return;
//      }
//
//      JsonObject pluginInput = new JsonObject()
//        .put("category", "polling")
//        .put("targets", targets);
//
//      vertx.executeBlocking(promise -> {
//        logger.info("Plugin input: {}", pluginInput.encodePrettily());
//
//        JsonArray results = Utility.runSSHPlugin(pluginInput);
//
//        logger.info("Plugin result: {}", results.encodePrettily());
//
//        storePollResults(results);
//
//        promise.complete();
//      }, false);
//    }
//    catch (Exception e)
//    {
//      logger.error("Polling failed for interval {}s: {}", interval, e.getMessage());
//    }
//  }
//
//  private JsonObject createTargetJson(Job job)
//  {
//    return new JsonObject()
//      .put("ip.address", job.ip())
//      .put("port", job.port())
//      .put("user", job.credData().getString("user"))
//      .put("password", job.credData().getString("password"))
//      .put("provision_profile_id", job.id())
//      .put("metric_type", new JsonArray(job.metrics()));
//  }
//
//  private void storePollResults(JsonArray results)
//  {
//    if (results == null || results.isEmpty()) return;
//
//    JsonArray batchParams = new JsonArray();
//
//    results.forEach(result -> {
//      JsonObject resultObj = (JsonObject) result;
//
//      logger.info("*******************************************************");
//
//      logger.info("Result: {}", resultObj.encodePrettily());
//
//      if ("success".equals(resultObj.getString("status")))
//      {
//        Long jobId = resultObj.getLong("provision_profile_id");
//
//        JsonObject data = resultObj.getJsonObject("data");
//
//        if (data != null)
//        {
//          data.fieldNames().forEach(metric ->
//            batchParams.add(new JsonArray()
//              .add(jobId)
//              .add(metric)
//              .add(data.getString(metric))));
//        }
//      }
//    });
//
//    if (batchParams.isEmpty()) return;
//
//    JsonObject batchQuery = new JsonObject()
//      .put("query", QueryConstant.INSERT_POLLED_DATA)
//      .put("batchParams", batchParams);
//
//    QueryProcessor.executeBatchQuery(batchQuery)
//      .onSuccess(r -> logger.debug("Stored {} metrics", batchParams.size()))
//      .onFailure(err -> logger.error("Store failed: {}", err.getMessage()));
//  }
//}


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

public class PollingVerticle extends AbstractVerticle
{
  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);

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
    List<JsonObject> jobsToPoll = MetricJobCache.handleTimer();

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

      for (JsonObject job : jobs)
      {
        String deviceKey = job.getString("ip") + ":" + job.getJsonObject("cred_data").encode();

        jobsByDevice.computeIfAbsent(deviceKey, k -> new ArrayList<>()).add(job);
      }

      List<String> ips = jobsByDevice.keySet().stream()
        .map(key -> key.split(":")[0])
        .distinct()
        .toList();

      var reachResults = Utility.checkReachability(ips, 22);

      var targets = new JsonArray();

      reachResults.forEach(result ->
      {
        JsonObject res = (JsonObject) result;

        if (res.getBoolean("reachable") && res.getBoolean("port_open"))
        {
          String ip = res.getString("ip");

          jobsByDevice.forEach((deviceKey, jobList) ->
          {
            if (deviceKey.startsWith(ip + ":"))
            {
              List<String> metrics = jobList.stream()
                .map(job -> job.getString("metric_name"))
                .toList();

              JsonObject sampleJob = jobList.get(0); // All jobs in list have same IP/cred

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

      JsonObject pluginInput = new JsonObject()
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

    JsonArray batchParams = new JsonArray();

    results.forEach(result ->
    {
      JsonObject resultObj = (JsonObject) result;

//      logger.info("Result: {}", resultObj.encodePrettily());

      if ("success".equals(resultObj.getString("status")))
      {
        Long jobId = resultObj.getLong("provision_profile_id");

        JsonObject data = resultObj.getJsonObject("data");

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

    JsonObject batchQuery = new JsonObject()
      .put("query", QueryConstant.INSERT_POLLED_DATA)
      .put("batchParams", batchParams);

    QueryProcessor.executeBatchQuery(batchQuery)
      .onSuccess(r -> logger.debug("Stored {} metrics", batchParams.size()))
      .onFailure(err -> logger.error("Store failed: {}", err.getMessage()));
  }
}
