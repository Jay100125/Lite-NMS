//package com.example.NMS.polling;
//
//import com.example.NMS.constant.QueryConstant;
//import com.example.NMS.utility.Utility;
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.Future;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.Arrays;
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//
//import static com.example.NMS.service.QueryProcessor.executeBatchQuery;
//import static com.example.NMS.service.QueryProcessor.executeQuery;
//
//public class PollingVerticle extends AbstractVerticle {
//  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);
//  private static final long DEFAULT_POLLING_INTERVAL_MS = 5000; // 60 seconds
//  private static final List<String> ALL_METRICS = Arrays.asList(
//    "CPU", "MEMORY", "DISK", "FILE", "PROCESS", "NETWORK", "PING", "SYSINFO");
//
//  @Override
//  public void start() {
//    long pollingInterval = config().getLong("polling.interval", DEFAULT_POLLING_INTERVAL_MS);
//    logger.info("Starting PollingVerticle with interval {} ms", pollingInterval);
//    vertx.setTimer(pollingInterval, id -> pollAllJobs());
//  }
//
//  private void pollAllJobs() {
//    logger.info("Starting polling cycle for all provisioning jobs");
//
//    // Query provisioning jobs with credentials and metrics
//    JsonObject query = new JsonObject()
//      .put("query", "SELECT pj.id, pj.ip, pj.port, cp.cred_data->>'user' as user, " +
//        "cp.cred_data->>'password' as password, pj.credential_profile_id, " +
//        "array_agg(m.name) as metric_types " +
//        "FROM provisioning_jobs pj " +
//        "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
//        "LEFT JOIN metrics m ON pj.id = m.provisioning_job_id " +
//        "GROUP BY pj.id, cp.cred_data, pj.ip, pj.port, pj.credential_profile_id")
//      .put("params", new JsonArray());
//
//    executeQuery(query)
//      .compose(result -> {
//        if (!"Success".equals(result.getString("msg"))) {
//          return Future.failedFuture("Failed to fetch provisioning jobs: " + result.getString("ERROR", "Unknown error"));
//        }
//        Object resultObj = result.getValue("result");
//        if (!(resultObj instanceof JsonArray)) {
//          logger.error("Expected JsonArray for query result, got: {}", resultObj != null ? resultObj.getClass().getName() : "null");
//          return Future.failedFuture("Invalid query result type: " + (resultObj != null ? resultObj.getClass().getName() : "null"));
//        }
//        JsonArray jobs = (JsonArray) resultObj;
//        if (jobs.isEmpty()) {
//          logger.info("No provisioning jobs to poll");
//          return Future.succeededFuture();
//        }
//        // Log the jobs array for debugging
//        logger.debug("Jobs array: {}", jobs.encode());
//        return processJobs(jobs);
//      })
//      .onFailure(err -> logger.error("Polling failed: {}", err.getMessage(), err));
//  }
//
//  private Future<Void> processJobs(JsonArray jobs) {
//    JsonArray targets = new JsonArray();
//    Map<Long, List<String>> jobMetrics = new HashMap<>();
//
//    // Validate jobs is a JsonArray
//    if (!(jobs instanceof JsonArray)) {
//      logger.error("Expected JsonArray for jobs, got: {}", jobs.getClass().getName());
//      return Future.failedFuture("Invalid jobs data type: " + jobs.getClass().getName());
//    }
//
//    // Build plugin input
//    for (int i = 0; i < jobs.size(); i++) {
//      Object jobObj = jobs.getValue(i);
//      if (!(jobObj instanceof JsonObject)) {
//        logger.warn("Skipping invalid job entry at index {}: type {}, value {}", i, jobObj != null ? jobObj.getClass().getName() : "null", jobObj);
//        continue;
//      }
//      JsonObject job = (JsonObject) jobObj;
//      long provisioningJobId = job.getLong("id", -1L);
//      if (provisioningJobId == -1L) {
//        logger.warn("Skipping job with missing id: {}", job.encode());
//        continue;
//      }
//
//      // Handle metric_types safely
//      JsonArray metricTypes;
//      Object metricTypesObj = job.getValue("metric_types");
//      if (metricTypesObj instanceof JsonArray) {
//        metricTypes = (JsonArray) metricTypesObj;
//      } else {
//        logger.warn("Invalid metric_types for job {}: type {}, value {}. Using ALL_METRICS", provisioningJobId, metricTypesObj != null ? metricTypesObj.getClass().getName() : "null", metricTypesObj);
//        metricTypes = new JsonArray(ALL_METRICS);
//      }
//      if (metricTypes.isEmpty() || metricTypes.getList().contains(null)) {
//        logger.info("Empty or null metric_types for job {}. Using ALL_METRICS", provisioningJobId);
//        metricTypes = new JsonArray(ALL_METRICS);
//      }
//
//      JsonArray pluginMetricTypes = new JsonArray();
//      for (Object mt : metricTypes.getList()) {
//        if (mt == null) {
//          logger.warn("Skipping null metric type for job {}", provisioningJobId);
//          continue;
//        }
//        String metricType = mt.toString();
//        String pluginMetricType;
//        switch (metricType) {
//          case "CPU":
//            pluginMetricType = "cpu";
//            break;
//          case "MEMORY":
//            pluginMetricType = "memory";
//            break;
//          case "DISK":
//            pluginMetricType = "disk";
//            break;
//          case "FILE":
//          case "PROCESS":
//          case "NETWORK":
//          case "PING":
//          case "SYSINFO":
//            pluginMetricType = metricType.toLowerCase(); // Use actual metric type
//            break;
//          default:
//            logger.warn("Unknown metric type: {} for job {}", metricType, provisioningJobId);
//            continue;
//        }
//        pluginMetricTypes.add(pluginMetricType);
//      }
//
//      if (pluginMetricTypes.isEmpty()) {
//        logger.warn("No valid metric types for job {}", provisioningJobId);
//        continue;
//      }
//
//      targets.add(new JsonObject()
//        .put("ip.address", job.getString("ip"))
//        .put("user", job.getString("user"))
//        .put("password", job.getString("password"))
//        .put("port", job.getInteger("port", 22))
//        .put("credential_profile_id", job.getLong("credential_profile_id", 0L))
//        .put("metric_type", pluginMetricTypes));
//      jobMetrics.put(provisioningJobId, pluginMetricTypes.getList());
//    }
//
//    if (targets.isEmpty()) {
//      logger.info("No valid targets to poll");
//      return Future.succeededFuture();
//    }
//
//    JsonObject pluginInput = new JsonObject()
//      .put("category", "polling")
//      .put("targets", targets);
//
//    // Log plugin input for debugging
//    logger.debug("Plugin input: {}", pluginInput.encode());
//
//    // Execute plugin in a blocking context
//    return vertx.executeBlocking(promise -> {
//      JsonArray pluginResults = Utility.runSSHPlugin(pluginInput);
//      promise.complete(pluginResults);
//    }).compose(pluginResultsObj -> {
//      // Validate pluginResults is a JsonArray
//      if (!(pluginResultsObj instanceof JsonArray)) {
//        logger.error("Expected JsonArray for pluginResults, got: {}", pluginResultsObj != null ? pluginResultsObj.getClass().getName() : "null");
//        return Future.failedFuture("Invalid plugin results type: " + (pluginResultsObj != null ? pluginResultsObj.getClass().getName() : "null"));
//      }
//      JsonArray pluginResults = (JsonArray) pluginResultsObj;
//
//      logger.info("result" + " : {}", pluginResults.encode());
//
//      // Log plugin results for debugging
//      logger.debug("Plugin results: {}", pluginResults.encode());
//
//      JsonArray batchParams = new JsonArray();
//      Map<String, String> metricTypeMapping = new HashMap<>();
//      metricTypeMapping.put("cpu", "CPU");
//      metricTypeMapping.put("memory", "MEMORY");
//      metricTypeMapping.put("disk", "DISK");
//      metricTypeMapping.put("file", "FILE");
//      metricTypeMapping.put("process", "PROCESS");
//      metricTypeMapping.put("network", "NETWORK");
//      metricTypeMapping.put("ping", "PING");
//      metricTypeMapping.put("sysinfo", "SYSINFO");
//
//      for (int i = 0; i < pluginResults.size(); i++) {
//        Object resultObj = pluginResults.getValue(i);
//        logger.info("Result {}: {}", i, resultObj);
//        if (!(resultObj instanceof JsonObject)) {
//          logger.warn("Skipping invalid plugin result at index {}: type {}, value {}", i, resultObj != null ? resultObj.getClass().getName() : "null", resultObj);
//          continue;
//        }
//        JsonObject result = (JsonObject) resultObj;
//        String ip = result.getString("ip.address");
//        long provisioningJobId = findJobIdByIp(jobs, ip);
//        if (provisioningJobId == -1) {
//          logger.warn("No job found for IP {}", ip);
//          continue;
//        }
//
//        if (!"success".equals(result.getString("status"))) {
//          logger.warn("Plugin failed for job {} (IP {}): {}", provisioningJobId, ip, result.getString("error", "Unknown error"));
//          continue;
//        }
//
//        JsonObject data = result.getJsonObject("data", new JsonObject());
//        System.out.println("---------------------------------- "+data);
//
//        List<String> metricTypes = jobMetrics.getOrDefault(provisioningJobId, ALL_METRICS);
//
//        for (String pluginMetricType : data.fieldNames()) {
//          String metricType = metricTypeMapping.getOrDefault(pluginMetricType, pluginMetricType.toUpperCase());
//          if (!metricTypes.contains(metricType)) {
//            logger.debug("Skipping metric type {} for job {}: not in requested metrics", metricType, provisioningJobId);
//            continue;
//          }
//          String output = data.getString(pluginMetricType);
//          if (output == null || output.isEmpty()) {
//            logger.warn("Empty output for metric type {} for job {}", metricType, provisioningJobId);
//            continue;
//          }
//          batchParams.add(new JsonArray()
//            .add(provisioningJobId)
//            .add(metricType)
//            .add(output));
//        }
//      }
//
//      if (batchParams.isEmpty()) {
//        logger.warn("No valid metric data from plugin");
//        return Future.succeededFuture();
//      }
//
//      // Log batch params for debugging
//      logger.debug("Batch params: {}", batchParams.encode());
//
//      // Insert results into metric_results
//      JsonObject insertQuery = new JsonObject()
//        .put("query", QueryConstant.INSERT_POLLED_DATA)
//        .put("batchParams", batchParams);
//
//      return executeBatchQuery(insertQuery)
//        .map(v -> {
//          if (!"Success".equals(v.getString("msg"))) {
//            throw new RuntimeException("Failed to insert metric results: " + v.getString("ERROR", "Unknown error"));
//          }
//          logger.info("Inserted {} metric results", batchParams.size());
//          return null;
//        });
//    });
//  }
//
//  private long findJobIdByIp(JsonArray jobs, String ip) {
//    for (int i = 0; i < jobs.size(); i++) {
//      Object jobObj = jobs.getValue(i);
//      if (!(jobObj instanceof JsonObject)) {
//        logger.warn("Skipping invalid job entry in findJobIdByIp at index {}: type {}, value {}", i, jobObj != null ? jobObj.getClass().getName() : "null", jobObj);
//        continue;
//      }
//      JsonObject job = (JsonObject) jobObj;
//      if (ip != null && ip.equals(job.getString("ip"))) {
//        return job.getLong("id", -1L);
//      }
//    }
//    return -1;
//  }
//}
package com.example.NMS.polling;

import com.example.NMS.service.QueryProcessor;
import com.example.NMS.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.*;
//
//public class PollingVerticle extends AbstractVerticle {
//  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);
//
//  // Cache: Map<provisioning_job_id, Map<metric_name, MetricInfo>>
//  private final Map<Long, Map<String, MetricInfo>> metricsCache = new ConcurrentHashMap<>();
//  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
//  private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();
//
//  @Override
//  public void start() {
//    // Initial cache load
//    refreshCache();
//
//    // Refresh cache every 5 minutes
//    vertx.setPeriodic(10000, l -> refreshCache());
//  }
//
//  private void refreshCache() {
//    String query = "SELECT pj.id AS job_id, pj.ip, pj.port, cp.cred_data, " +
//      "m.name AS metric_name, m.polling_interval " +
//      "FROM provisioning_jobs pj " +
//      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
//      "JOIN metrics m ON pj.id = m.provisioning_job_id";
//
//    QueryProcessor.executeQuery(new JsonObject().put("query", query))
//      .onSuccess(result -> {
//        JsonArray rows = result.getJsonArray("result");


////        logger.info(rows.encodePrettily());
//        updateCache(rows);
//        schedulePollingTasks();
//      })
//      .onFailure(err -> logger.error("Cache refresh failed: {}", err.getMessage()));
//  }
//
//  private void updateCache(JsonArray rows) {
//    metricsCache.clear();
//    for (Object row : rows) {
//      JsonObject entry = (JsonObject) row;
//      Long jobId = entry.getLong("job_id");
//      String metricName = entry.getString("metric_name"); // e.g., "CPU"
//
//      metricsCache
//        .computeIfAbsent(jobId, k -> new ConcurrentHashMap<>())
//        .put(metricName, new MetricInfo(
//          jobId,
//          entry.getString("ip"),
//          entry.getInteger("port"),
//          entry.getJsonObject("cred_data"),
//          metricName, // Pass name here
//          entry.getInteger("polling_interval")
//        ));
//    }
//  }
//
//
//  private void schedulePollingTasks() {
//    // Cancel existing tasks
//    scheduledTasks.values().forEach(task -> task.cancel(false));
//    scheduledTasks.clear();
//
//    // Group metrics by polling interval
//    Map<Integer, List<MetricInfo>> intervalGroups = new HashMap<>();
//    metricsCache.values().forEach(metrics ->
//      metrics.values().forEach(info ->
//        intervalGroups
//          .computeIfAbsent(info.interval, k -> new ArrayList<>())
//          .add(info)
//      )
//    );
//
//
//    // Schedule tasks for each interval
//    intervalGroups.forEach((interval, metricsList) -> {
//      ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
//        () -> executePolling(metricsList),
//        interval, interval, TimeUnit.SECONDS
//      );
//      scheduledTasks.put(interval + "sec", task);
//    });
//  }
//
//  private void executePolling(List<MetricInfo> metrics) {
//    metrics.forEach(metric -> {
//      JsonObject pluginInput = new JsonObject()
//        .put("category", "polling")
//        .put("ip.address", metric.ip)
//        .put("port", metric.port)
//        .put("user", metric.credData.getString("user"))
//        .put("password", metric.credData.getString("password"))
//        .put("metric_type", metric.name);
//
//      logger.info("Polling metric: {}", pluginInput.encodePrettily());
//
//      vertx.executeBlocking(promise -> {
//        try {
//          JsonArray results = Utility.runSSHPlugin(pluginInput);
//          logger.info("I'm here");
//          logger.info(results.encodePrettily());
//          storePollResults(metric.jobId, metric.name, results); // <-- Pass jobId
//          promise.complete();
//        } catch (Exception e) {
//          promise.fail(e);
//        }
//      }, false);
//    });
//  }
//
//  private void storePollResults(Long jobId, String metricName, JsonArray results) {
//    String query = "INSERT INTO polled_data (job_id, metric_type, data, polled_at) " +
//      "VALUES ($1, $2, $3, NOW())";
//
//    QueryProcessor.executeQuery(new JsonObject()
//      .put("query", query)
//      .put("params", new JsonArray()
//        .add(jobId)
//        .add(metricName)
//        .add(results.encode()))
//    ).onFailure(err -> logger.error("Failed to store polling data: {}", err.getMessage()));
//  }

//  static class MetricInfo {
//    Long jobId;
//    String ip;
//    int port;
//    JsonObject credData;
//    String name; // Metric name (e.g., "CPU")
//    int interval;
//
//    public MetricInfo(Long jobId, String ip, int port, JsonObject credData, String name, int interval) {
//      this.jobId = jobId;
//      this.ip = ip;
//      this.port = port;
//      this.credData = credData;
//      this.name = name; // Initialize name
//      this.interval = interval;
//    }
//  }
//}

//public class PollingVerticle extends AbstractVerticle {
//  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);
//
//  // Cache: Map<provisioning_job_id, TargetInfo>
//  private final Map<Long, TargetInfo> targetsCache = new ConcurrentHashMap<>();
//  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
//  private final Map<String, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();
//
//  @Override
//  public void start() {
//    refreshCache();
//    vertx.setPeriodic(10000, l -> refreshCache()); // Refresh cache every 5 minutes
//  }
//
//  private void refreshCache() {
//    String query = "SELECT pj.id AS job_id, pj.ip, pj.port, cp.cred_data, " +
//      "ARRAY_AGG(m.name) AS metric_names " +  // Get all metrics as array
//      "FROM provisioning_jobs pj " +
//      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
//      "JOIN metrics m ON pj.id = m.provisioning_job_id " +
//      "GROUP BY pj.id, pj.ip, pj.port, cp.cred_data";
//
//    QueryProcessor.executeQuery(new JsonObject().put("query", query))
//      .onSuccess(result -> {
//        logger.info(result.toString());
//        updateTargetsCache(result.getJsonArray("result"));
//        schedulePollingTasks();
//      })
//      .onFailure(err -> logger.error("Cache refresh failed: {}", err.getMessage()));
//  }
//
//  private void updateTargetsCache(JsonArray rows) {
//    targetsCache.clear();
//    for (Object row : rows) {
//      JsonObject entry = (JsonObject) row;
//      Long jobId = entry.getLong("job_id");
//
//      TargetInfo target = new TargetInfo(
//        jobId,
//        entry.getString("ip"),
//        entry.getInteger("port"),
//        entry.getJsonObject("cred_data"),
//        entry.getJsonArray("metric_names").getList() // Metric types as List<String>
//      );
//
//      targetsCache.put(jobId, target);
//    }
//  }
//
//  private void schedulePollingTasks() {
//    scheduledTasks.values().forEach(task -> task.cancel(false));
//    scheduledTasks.clear();
//
//    Map<Integer, List<TargetInfo>> intervalGroups = new HashMap<>();
//
//    // Group targets by their max polling interval (simplified example)
//    targetsCache.values().forEach(target -> {
//      int interval = 2; // Default to 300s polling
//      intervalGroups.computeIfAbsent(interval, k -> new ArrayList<>()).add(target);
//    });
//
//    intervalGroups.forEach((interval, targets) -> {
//      ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
//        () -> executePolling(targets),
//        interval, interval, TimeUnit.SECONDS
//      );
//      scheduledTasks.put(interval + "sec", task);
//    });
//  }
//
//  private void executePolling(List<TargetInfo> targets) {
//    targets.forEach(target -> {
//      JsonObject pluginInput = new JsonObject()
//        .put("category", "polling")
//        .put("targets", new JsonArray().add( // Build targets array
//          new JsonObject()
//            .put("ip.address", target.ip)
//            .put("port", target.port)
//            .put("user", target.credData.getString("user"))
//            .put("password", target.credData.getString("password"))
//            .put("credential_profile_id", target.jobId)
//            .put("metric_type", "cpu") // Array or single value
//        ));
//
//      logger.info(pluginInput.encodePrettily());
//      vertx.executeBlocking(promise -> {
//        try {
//          JsonArray results = Utility.runSSHPlugin(pluginInput);
//
//          logger.info(results.encodePrettily());
////          storePollResults(target.jobId, results);
//          promise.complete();
//        } catch (Exception e) {
//          promise.fail(e);
//        }
//      }, false);
//    });
//  }
//
////  private void storePollResults(Long jobId, JsonArray results) {
////    String query = "INSERT INTO polled_data (job_id, metric_type, data, polled_at) " +
////      "VALUES ($1, $2, $3, NOW())";
////
////    QueryProcessor.executeQuery(new JsonObject()
////      .put("query", query)
////      .put("params", new JsonArray()
////        .add(jobId)
////        .add(metricName)
////        .add(results.encode()))
////    ).onFailure(err -> logger.error("Failed to store polling data: {}", err.getMessage()));
////  }
//
//
//  static class TargetInfo {
//    Long jobId;
//    String ip;
//    int port;
//    JsonObject credData;
//    List<String> metricTypes; // ["cpu", "memory"], ["uptime"], etc.
//
//    public TargetInfo(Long jobId, String ip, int port, JsonObject credData, List<String> metricTypes) {
//      this.jobId = jobId;
//      this.ip = ip;
//      this.port = port;
//      this.credData = credData;
//      this.metricTypes = metricTypes;
//    }
//  }
//}

  // TODO
public class PollingVerticle extends AbstractVerticle {
  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);

  // Structure: Map<PollingIntervalSeconds, Map<JobId, JobMetrics>>
  private final Map<Integer, Map<Long, JobMetrics>> intervalGroups = new ConcurrentHashMap<>();
  private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(4);
  private final Map<Integer, ScheduledFuture<?>> scheduledTasks = new ConcurrentHashMap<>();

  @Override
  public void start() {
    refreshCache();
    vertx.setPeriodic(300_000, l -> refreshCache()); // Refresh cache every 5 minutes
  }

  private void refreshCache() {
    String query = "SELECT m.provisioning_job_id, m.name AS metric_name, m.polling_interval, " +
      "pj.ip, pj.port, cp.cred_data " +
      "FROM metrics m " +
      "JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id " +
      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id";

    QueryProcessor.executeQuery(new JsonObject().put("query", query))
      .onSuccess(result -> {
        rebuildIntervalGroups(result.getJsonArray("result"));
        rescheduleTasks();
      })
      .onFailure(err -> logger.error("Cache refresh failed: {}", err.getMessage()));
  }

  private void rebuildIntervalGroups(JsonArray metrics) {
    intervalGroups.clear();

    metrics.forEach(entry -> {
      JsonObject metric = (JsonObject) entry;
      int interval = metric.getInteger("polling_interval");
      Long jobId = metric.getLong("provisioning_job_id");

      intervalGroups
        .computeIfAbsent(interval, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(jobId, k -> new JobMetrics(
          metric.getString("ip"),
          metric.getInteger("port"),
          metric.getJsonObject("cred_data")
        ))
        .addMetric(metric.getString("metric_name"));
    });
  }

  private void rescheduleTasks() {
    // Cancel old tasks
    scheduledTasks.values().forEach(task -> task.cancel(false));
    scheduledTasks.clear();

    intervalGroups.forEach((interval, jobs) -> {
      ScheduledFuture<?> task = scheduler.scheduleAtFixedRate(
        () -> pollIntervalGroup(interval, jobs),
        0, interval, TimeUnit.SECONDS
      );
      scheduledTasks.put(interval, task);
    });
  }

  private void pollIntervalGroup(int interval, Map<Long, JobMetrics> jobs) {
    logger.info("Polling {} jobs at {}-second interval", jobs.size(), interval);

    jobs.forEach((jobId, jobMetrics) -> {
      JsonObject pluginInput = new JsonObject()
        .put("category", "polling")
        .put("targets", new JsonArray().add(
          new JsonObject()
            .put("ip.address", jobMetrics.ip)
            .put("port", jobMetrics.port)
            .put("user", jobMetrics.credData.getString("user"))
            .put("password", jobMetrics.credData.getString("password"))
            .put("credential_profile_id", jobId)
            .put("metric_type", "cpu") // Array of metrics
        ));

      logger.info(pluginInput.encodePrettily());
      vertx.executeBlocking(promise -> {
        try {
          JsonArray results = Utility.runSSHPlugin(pluginInput);
//          storePollResults(jobId, results);

          logger.info(results.encodePrettily());
          promise.complete();
        } catch (Exception e) {
          logger.error("Polling failed for job {}: {}", jobId, e.getMessage());
          promise.fail(e);
        }
      }, false);
    });
  }

  static class JobMetrics {
    String ip;
    int port;
    JsonObject credData;
    List<String> metrics = new ArrayList<>();

    public JobMetrics(String ip, int port, JsonObject credData) {
      this.ip = ip;
      this.port = port;
      this.credData = credData;
    }

    void addMetric(String metricName) {
      metrics.add(metricName);
    }
  }
}
