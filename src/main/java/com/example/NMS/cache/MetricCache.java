package com.example.NMS.cache;

import com.example.NMS.service.QueryProcessor;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static com.example.NMS.constant.Constant.*;

public class MetricCache
{
  private static final Logger LOGGER = LoggerFactory.getLogger(MetricCache.class);

  // Static cache of metric jobs: metric_id -> JsonObject
  private static final ConcurrentHashMap<Long, JsonObject> metricJobCache = new ConcurrentHashMap<>();

  // Timer interval (seconds) for decrementing remaining times

  // Flag to ensure cache is initialized only once
  private static boolean isCacheInitialized = false;

  // Initialize cache by querying the database
  public static void init()
  {
    if (isCacheInitialized)
    {
      LOGGER.info("Cache already initialized, skipping refresh");

      return;
    }

    var query = "SELECT m.metric_id, m.provisioning_job_id, m.name AS metric_name, m.polling_interval, " +
      "m.is_enabled, pj.ip, pj.port, cp.cred_data " +
      "FROM metrics m " +
      "JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id " +
      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
      "WHERE m.is_enabled = true";

    QueryProcessor.executeQuery(new JsonObject().put(QUERY, query))
      .onComplete(queryResult -> {
        if(queryResult.succeeded())
        {
          var result = queryResult.result();

          updateCacheFromQueryResult(result);

          isCacheInitialized = true;

          LOGGER.info("Initial cache populated with {} jobs", metricJobCache.size());
        }
        else
        {
          var error = queryResult.cause();

          LOGGER.error("Initial cache refresh failed: {}", error.getMessage());
        }
      });
  }

  // Update cache from query results
  private static void updateCacheFromQueryResult(JsonArray results)
  {
    results.forEach(entry ->
    {
      var metric = (JsonObject) entry;

      var metricId = metric.getLong(METRIC_ID);

      var job = new JsonObject()
        .put(METRIC_ID, metricId)
        .put(PROVISIONING_JOB_ID, metric.getLong(PROVISIONING_JOB_ID))
        .put(METRIC_NAME, metric.getString(METRIC_NAME))
        .put(IP, metric.getString(IP))
        .put(PORT, metric.getInteger(PORT))
        .put(CRED_DATA, metric.getJsonObject(CRED_DATA))
        .put(ORIGINAL_INTERVAL, metric.getInteger(POLLING_INTERVAL))
        .put(REMAINING_TIME, metric.getInteger(POLLING_INTERVAL))
        .put(IS_ENABLED, metric.getBoolean(IS_ENABLED));


      metricJobCache.put(metricId, job);
    });
  }

  // Add a new metric job to the cache
  public static void addMetricJob(Long metricId, Long provisioningJobId, String metricName, int pollingInterval, String ip, int port, JsonObject credData)
  {
    var job = new JsonObject()
      .put(METRIC_ID, metricId)
      .put(PROVISIONING_JOB_ID, provisioningJobId)
      .put(METRIC_NAME, metricName)
      .put(IP, ip)
      .put(PORT, port)
      .put(CRED_DATA, credData)
      .put(ORIGINAL_INTERVAL, pollingInterval)
      .put(REMAINING_TIME, pollingInterval);

    metricJobCache.put(metricId, job);

    LOGGER.info("Added metric job to cache: metric_id={}", metricId);
  }

  // Remove a metric job from the cache
  public static void removeMetricJob(Long metricId)
  {
    metricJobCache.remove(metricId);

    LOGGER.info("Removed metric job from cache: metric_id={}", metricId);
  }


  public static void removeMetricJobsByProvisioningJobId(Long provisioningJobId)
  {
    var removedIds = metricJobCache.entrySet().stream()
      .filter(entry -> provisioningJobId.equals(entry.getValue().getLong(PROVISIONING_JOB_ID)))
      .map(Map.Entry::getKey)
      .collect(Collectors.toList());

    removedIds.forEach(metricJobCache::remove);

    if (!removedIds.isEmpty())
    {
      LOGGER.info("Removed {} metric jobs for provisioning_job_id={}", removedIds.size(), provisioningJobId);
    }
  }

  // Update an existing metric job
  public static void updateMetricJob(Long metricId, Long provisioningJobId, String metricName, int pollingInterval, String ip, int port, JsonObject credData, Boolean isEnabled)
  {
    var job = new JsonObject()
      .put(METRIC_ID, metricId)
      .put(PROVISIONING_JOB_ID, provisioningJobId)
      .put(METRIC_NAME, metricName)
      .put(IP, ip)
      .put(PORT, port)
      .put(CRED_DATA, credData)
      .put(ORIGINAL_INTERVAL, pollingInterval)
      .put(REMAINING_TIME, pollingInterval)
      .put(IS_ENABLED, isEnabled);

    if (isEnabled)
    {
      metricJobCache.put(metricId, job);
    }
    else
    {
      metricJobCache.remove(metricId);
    }


    LOGGER.info("Updated metric job in cache: metric_id={}", metricId);
  }

  public static Map<Long, JsonObject> getMetricJobsByProvisioningJobId(Long provisioningJobId)
  {
    return metricJobCache.entrySet().stream()
      .filter(entry -> provisioningJobId.equals(entry.getValue().getLong(PROVISIONING_JOB_ID)))
      .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
  }

  // Handle timer to decrement intervals and return jobs to poll
  public static List<JsonObject> handleTimer()
  {
    var jobsToPoll = new ArrayList<JsonObject>();

    // Decrement remaining time and collect jobs ready to poll
    metricJobCache.forEach((metricId, job) ->
    {
      var newRemainingTime = job.getInteger(REMAINING_TIME) - TIMER_INTERVAL_SECONDS;

      if (newRemainingTime <= 0)
      {
        jobsToPoll.add(job);
        // Reset remaining time to original interval
        job.put(REMAINING_TIME, job.getInteger(ORIGINAL_INTERVAL));
      }
      else
      {
        // Update remaining time
        job.put(REMAINING_TIME, newRemainingTime);
      }
    });

    if (!jobsToPoll.isEmpty())
    {
      LOGGER.info("Found {} jobs to poll", jobsToPoll.size());
    }

    return jobsToPoll;
  }
}
