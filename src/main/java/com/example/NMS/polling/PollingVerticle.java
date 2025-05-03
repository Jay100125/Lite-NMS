package com.example.NMS.polling;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.QueryProcessor;
import com.example.NMS.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;


// TODO : going to be changed (temporary) Currently creating separate process for all ip address
// Current flow :- create group of metric job like if for ip 10.20.40.123 we have to collect cpu and memory for 5 minute interval it will only spawn one process for that and give our result
//                but if for another ip 10.20.41.10 we have cpu and memory for 5 minute interval it will spawn another process for that
// Improvement :- generate single process for all the same interval like one process for all targets and their metrics if their metrics intervals are same


public class PollingVerticle extends AbstractVerticle
{
  private static final Logger logger = LoggerFactory.getLogger(PollingVerticle.class);

  // Map of interval to job metrics
  private final Map<Integer, Map<Long, JobMetrics>> intervalGroups = new HashMap<>();

  // Map of interval to timer ID
  private final Map<Integer, Long> activeTimers = new HashMap<>();

  @Override
  public void start()
  {
    refreshCache();

    vertx.setPeriodic(10_000, l -> refreshCache()); // Refresh cache every 10s
  }

  private void refreshCache()
  {
    String query = "SELECT m.provisioning_job_id, m.name AS metric_name, m.polling_interval, "
      + "pj.ip, pj.port, cp.cred_data "
      + "FROM metrics m "
      + "JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id "
      + "JOIN credential_profile cp ON pj.credential_profile_id = cp.id";

    QueryProcessor.executeQuery(new JsonObject().put("query", query))
      .onSuccess(result ->
      {
        rebuildIntervalGroups(result.getJsonArray("result"));

        rescheduleTasks();
      })
      .onFailure(err -> logger.error("Cache refresh failed: {}", err.getMessage()));
  }

  private void rebuildIntervalGroups(JsonArray metrics)
  {
    Map<Integer, Map<Long, JobMetrics>> newGroups = new HashMap<>();

    metrics.forEach(entry -> {
      JsonObject metric = (JsonObject) entry;
      int interval = metric.getInteger("polling_interval");
      Long jobId = metric.getLong("provisioning_job_id");

      newGroups
        .computeIfAbsent(interval, k -> new HashMap<>())
        .computeIfAbsent(jobId, k -> new JobMetrics(
          metric.getString("ip"),
          metric.getInteger("port"),
          metric.getJsonObject("cred_data")
        ))
        .addMetric(metric.getString("metric_name"));

    });

    if (!intervalGroups.keySet().equals(newGroups.keySet()))
    {
      intervalGroups.clear();

      intervalGroups.putAll(newGroups);

      rescheduleTasks();
    }

  }

  private void rescheduleTasks()
  {
    // Remove old intervals
    new ArrayList<>(activeTimers.keySet()).forEach(interval -> {
      if (!intervalGroups.containsKey(interval)) {
        vertx.cancelTimer(activeTimers.remove(interval));
      }
    });

    // Add new intervals
    intervalGroups.keySet().forEach(interval -> {
      if (!activeTimers.containsKey(interval)) {
        long timerId = vertx.setPeriodic(interval * 1000, l ->
          pollIntervalGroup(interval, intervalGroups.get(interval))
        );
        activeTimers.put(interval, timerId);
      }
    });

  }

  private void pollIntervalGroup(int interval, Map<Long, JobMetrics> jobs)
  {
    jobs.forEach((jobId, jobMetrics) ->
    {
      JsonObject pluginInput = buildPluginInput(jobId, jobMetrics);

      logger.info("Polling job {} with interval {}: {}", jobId, interval, pluginInput.encodePrettily());

      vertx.executeBlocking(promise ->
      {
        try
        {
          JsonArray results = Utility.runSSHPlugin(pluginInput);

          logger.info("Polling results for job {}: {}", jobId, results.encodePrettily());

          storePollResults(jobId, results);

          promise.complete();
        }
        catch (Exception e)
        {
          logger.error("Polling failed for job {}: {}", jobId, e.getMessage());

          promise.fail(e);
        }
      }, false);
    });
  }

  private JsonObject buildPluginInput(Long jobId, JobMetrics jobMetrics)
  {
    return new JsonObject()
      .put("category", "polling")
      .put("targets", new JsonArray().add(
        new JsonObject()
          .put("ip.address", jobMetrics.ip)
          .put("port", jobMetrics.port)
          .put("user", jobMetrics.credData.getString("user"))
          .put("password", jobMetrics.credData.getString("password"))
          .put("credential_profile_id", jobId)
          .put("metric_type", jobMetrics.metrics)
      ));
  }
  private void storePollResults(Long jobId, JsonArray results) {
    if (results == null || results.isEmpty()) {
      logger.warn("No results to store for job {}", jobId);
      return;
    }

    JsonArray batchParams = new JsonArray();

    results.forEach(result -> {
      JsonObject resultObj = (JsonObject) result;
      if ("success".equals(resultObj.getString("status"))) {
        Object data = resultObj.getValue("data");
        JsonArray metricTypes = resultObj.getJsonArray("metric.type");

        if (data instanceof String) {
          // Single metric result
          String metricValue = (String) data;
          String metricName = metricTypes.getString(0);
          batchParams.add(new JsonArray()
            .add(jobId)
            .add(metricName)
            .add(metricValue)
          );
        } else if (data instanceof JsonObject) {
          // Multiple metrics result
          JsonObject dataObj = (JsonObject) data;
          dataObj.fieldNames().forEach(metricName -> {
            batchParams.add(new JsonArray()
              .add(jobId)
              .add(metricName)
              .add(dataObj.getString(metricName))
            );
          });
        }
      }
    });

    if (batchParams.isEmpty()) return;

    JsonObject batchQuery = new JsonObject()
      .put("query", QueryConstant.INSERT_POLLED_DATA)
      .put("batchParams", batchParams);

    QueryProcessor.executeBatchQuery(batchQuery)
      .onSuccess(r -> logger.debug("Stored {} metrics for job {}", batchParams.size(), jobId))
      .onFailure(err -> logger.error("Failed to store polling data: {}", err.getMessage()));
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
