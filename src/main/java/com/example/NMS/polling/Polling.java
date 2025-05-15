
package com.example.NMS.polling;

import com.example.NMS.cache.MetricCache;
import com.example.NMS.constant.QueryConstant;
import com.example.NMS.service.QueryProcessor;
import com.example.NMS.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import static com.example.NMS.constant.Constant.*;

public class Polling extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Polling.class);

     @Override
    public void start(Promise<Void> startPromise)
    {
        try
        {
          // Set up event bus consumer for polling jobs
            vertx.eventBus().<JsonArray>localConsumer(POLLING_BATCH_PROCESS, message -> {

                var jobs = message.body();

                if (!jobs.isEmpty())
                {
                    LOGGER.info("Received {} jobs for polling", jobs.size());

                    var jobsToPoll = jobs.stream()
                      .map(obj -> (JsonObject) obj)
                      .collect(Collectors.toList());

                    pollJobs(jobsToPoll);
                }
                else
                {
                    LOGGER.debug("Received empty job list for polling");
                }
            });

            LOGGER.info("PollingVerticle started");

            // Signal successful deployment
            startPromise.complete();
        }
        catch (Exception exception)
        {
            LOGGER.error("Failed to start PollingVerticle", exception);

            startPromise.fail(exception);
        }
    }


  // Poll the collected jobs
    private void pollJobs(List<JsonObject> jobs)
    {
        try
        {
          // Group jobs by IP and credentials to batch SSH calls
            var jobsByDevice = new HashMap<String, List<JsonObject>>();

            for (var job : jobs)
            {
                var deviceKey = job.getString(IP) + ":" + job.getJsonObject(CRED_DATA).encode();

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
                    var ip = res.getString(IP);

                    jobsByDevice.forEach((deviceKey, jobList) ->
                    {
                        if (deviceKey.startsWith(ip + ":"))
                        {
                            var metrics = jobList.stream()
                              .map(job -> job.getString(METRIC_NAME))
                              .toList();

                            var sampleJob = jobList.get(0); // All jobs in list have same IP/cred

                            targets.add(new JsonObject()
                              .put("ip.address", ip)
                              .put(PORT, sampleJob.getInteger(PORT))
                              .put("user", sampleJob.getJsonObject(CRED_DATA).getString("user"))
                              .put(PASSWORD, sampleJob.getJsonObject(CRED_DATA).getString(PASSWORD))
                              .put("provision_profile_id", sampleJob.getLong("provisioning_job_id"))
                              .put("metric_type", new JsonArray(metrics)));
                        }
                    });
                }
            });

            if (targets.isEmpty())
            {
                LOGGER.info("No reachable targets for polling");

                return;
            }

            var pluginInput = new JsonObject()
              .put("category", "polling")
              .put("targets", targets);

            vertx.executeBlocking(promise ->
            {
                LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());

                JsonArray results = Utility.spawnPlugin(pluginInput);

                LOGGER.info("Plugin result: {}", results.encodePrettily());

                storePollResults(results);

                promise.complete();
            }, false);
        }
        catch (Exception exception)
        {
            LOGGER.error("Polling failed: {}", exception.getMessage());
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
          .put(QUERY, QueryConstant.INSERT_POLLED_DATA)
          .put(BATCHPARAMS, batchParams);

        QueryProcessor.executeBatchQuery(batchQuery)
          .onSuccess(r -> LOGGER.info("Stored {} metrics", batchParams.size()))
          .onFailure(err -> LOGGER.error("Store failed: {}", err.getMessage()));
    }
}
