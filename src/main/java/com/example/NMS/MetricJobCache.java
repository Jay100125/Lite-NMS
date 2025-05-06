package com.example.NMS;

import com.example.NMS.service.QueryProcessor;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

public class MetricJobCache {
  private static final Logger logger = LoggerFactory.getLogger(MetricJobCache.class);

  // Static cache of metric jobs: metric_id -> JsonObject
  private static final ConcurrentHashMap<Long, JsonObject> metricJobCache = new ConcurrentHashMap<>();

  // Timer interval (seconds) for decrementing remaining times
  private static final int TIMER_INTERVAL_SECONDS = 10;

  // Flag to ensure cache is initialized only once
  private static boolean isCacheInitialized = false;

  // Initialize cache by querying the database
  public static synchronized void refreshCache(Vertx vertx) {
    if (isCacheInitialized) {
      logger.info("Cache already initialized, skipping refresh");
      return;
    }

    String query = "SELECT m.metric_id, m.provisioning_job_id, m.name AS metric_name, m.polling_interval, " +
      "pj.ip, pj.port, cp.cred_data " +
      "FROM metrics m " +
      "JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id " +
      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id";

    QueryProcessor.executeQuery(new JsonObject().put("query", query))
      .onSuccess(result -> {
        updateCacheFromQueryResult(result.getJsonArray("result"));
        isCacheInitialized = true;
        logger.info("Initial cache populated with {} jobs", metricJobCache.size());
      })
      .onFailure(err -> logger.error("Initial cache refresh failed: {}", err.getMessage()));
  }

  // Update cache from query results
  private static void updateCacheFromQueryResult(JsonArray results) {
    results.forEach(entry -> {
      JsonObject metric = (JsonObject) entry;
      Long metricId = metric.getLong("metric_id");
      JsonObject job = new JsonObject()
        .put("metric_id", metricId)
        .put("provisioning_job_id", metric.getLong("provisioning_job_id"))
        .put("metric_name", metric.getString("metric_name"))
        .put("ip", metric.getString("ip"))
        .put("port", metric.getInteger("port"))
        .put("cred_data", metric.getJsonObject("cred_data"))
        .put("original_interval", metric.getInteger("polling_interval"))
        .put("remaining_time", metric.getInteger("polling_interval"));

      metricJobCache.put(metricId, job);
    });
  }

  // Add a new metric job to the cache
  public static void addMetricJob(Long metricId, Long provisioningJobId, String metricName, int pollingInterval, String ip, int port, JsonObject credData) {
    JsonObject job = new JsonObject()
      .put("metric_id", metricId)
      .put("provisioning_job_id", provisioningJobId)
      .put("metric_name", metricName)
      .put("ip", ip)
      .put("port", port)
      .put("cred_data", credData)
      .put("original_interval", pollingInterval)
      .put("remaining_time", pollingInterval);

    metricJobCache.put(metricId, job);
    logger.info("Added metric job to cache: metric_id={}", metricId);
  }

  // Remove a metric job from the cache
  public static void removeMetricJob(Long metricId) {
    metricJobCache.remove(metricId);
    logger.info("Removed metric job from cache: metric_id={}", metricId);
  }

  // Update an existing metric job
  public static void updateMetricJob(Long metricId, Long provisioningJobId, String metricName, int pollingInterval, String ip, int port, JsonObject credData) {
    JsonObject job = new JsonObject()
      .put("metric_id", metricId)
      .put("provisioning_job_id", provisioningJobId)
      .put("metric_name", metricName)
      .put("ip", ip)
      .put("port", port)
      .put("cred_data", credData)
      .put("original_interval", pollingInterval)
      .put("remaining_time", pollingInterval);

    metricJobCache.put(metricId, job);
    logger.info("Updated metric job in cache: metric_id={}", metricId);
  }

  // Handle timer to decrement intervals and return jobs to poll
  public static List<JsonObject> handleTimer() {
    List<JsonObject> jobsToPoll = new ArrayList<>();

    // Decrement remaining time and collect jobs ready to poll
    metricJobCache.forEach((metricId, job) -> {
      int newRemainingTime = job.getInteger("remaining_time") - TIMER_INTERVAL_SECONDS;
      if (newRemainingTime <= 0) {
        jobsToPoll.add(job);
        // Reset remaining time to original interval
        JsonObject updatedJob = new JsonObject()
          .put("metric_id", job.getLong("metric_id"))
          .put("provisioning_job_id", job.getLong("provisioning_job_id"))
          .put("metric_name", job.getString("metric_name"))
          .put("ip", job.getString("ip"))
          .put("port", job.getInteger("port"))
          .put("cred_data", job.getJsonObject("cred_data"))
          .put("original_interval", job.getInteger("original_interval"))
          .put("remaining_time", job.getInteger("original_interval"));
        metricJobCache.put(metricId, updatedJob);
      } else {
        // Update remaining time
        JsonObject updatedJob = new JsonObject()
          .put("metric_id", job.getLong("metric_id"))
          .put("provisioning_job_id", job.getLong("provisioning_job_id"))
          .put("metric_name", job.getString("metric_name"))
          .put("ip", job.getString("ip"))
          .put("port", job.getInteger("port"))
          .put("cred_data", job.getJsonObject("cred_data"))
          .put("original_interval", job.getInteger("original_interval"))
          .put("remaining_time", newRemainingTime);
        metricJobCache.put(metricId, updatedJob);
      }
    });

    if (!jobsToPoll.isEmpty()) {
      logger.info("Found {} jobs to poll", jobsToPoll.size());
    }

    return jobsToPoll;
  }
}
