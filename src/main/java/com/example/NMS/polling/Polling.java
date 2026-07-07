package com.example.NMS.polling;

import com.example.NMS.plugin.PluginEnvelope;
import com.example.NMS.util.CryptoUtil;
import com.example.NMS.utility.Utility;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.example.NMS.constant.Constant.*;

/**
 * Vert.x verticle for polling metric jobs in Lite NMS.
 * Consumes metric jobs from the event bus, checks device reachability, executes an SSH plugin to collect metrics,
 * and stores the results in the database.
 */
public class Polling extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Polling.class);


    /**
     * Starts the polling verticle.
     * Sets up an event bus consumer to receive metric jobs for polling and signals successful deployment.
     *
     * @param startPromise The promise to complete or fail based on startup success.
     */
    @Override
    public void start(Promise<Void> startPromise)
    {
        try
        {
            // Set up event bus consumer for polling jobs
            vertx.eventBus().<JsonArray>localConsumer(POLLING_BATCH_PROCESS, message ->
            {
                var jobs = message.body();

                if (!jobs.isEmpty())
                {
                    LOGGER.info("Received {} jobs for polling", jobs.size());

                    // Convert JSON array to list of JSON objects
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


//    private void pollJobs(List<JsonObject> jobs)
//    {
//        try
//        {
//            // Extract unique IPs from jobs
//            var ips = jobs.stream()
//                .map(job -> job.getString(IP))
//                .distinct()
//                .collect(Collectors.toList());
//
//            // Check reachability for all IPs
//            var reachResults = Utility.checkReachability(ips, 22);
//
//            var targets = new JsonArray();
//
//            // Process each job individually
//            for (var job : jobs)
//            {
//                var ip = job.getString(IP);
//
//                // Find reachability result for this IP
//                var reachResult = reachResults.stream()
//                    .map(obj -> (JsonObject) obj)
//                    .filter(res -> res.getString(IP).equals(ip))
//                    .findFirst()
//                    .orElse(null);
//
//                if (reachResult != null && reachResult.getBoolean("reachable") && reachResult.getBoolean("port_open"))
//                {
//                    // Create a target for this individual metric job
//                    targets.add(new JsonObject()
//                        .put(IP_ADDRESS, ip)
//                        .put(PORT, job.getInteger(PORT))
//                        .put(USER, job.getJsonObject(CRED_DATA).getString(USER))
//                        .put(PASSWORD, job.getJsonObject(CRED_DATA).getString(PASSWORD))
//                        .put(PROVISIONING_JOB_ID, job.getLong(PROVISIONING_JOB_ID))
//                        .put(METRIC_NAME, job.getString(METRIC_NAME))
//                        .put(PROTOCOL, job.getString(PROTOCOL))
//                        .put(PLUGIN_TYPE, LINUX + job.getString(METRIC_NAME).toLowerCase()));
//                }
//            }
//
//            if (targets.isEmpty())
//            {
//                LOGGER.info("No reachable targets for polling");
//
//                return;
//            }
//
//            var pluginInput = new JsonObject()
//                .put(REQUEST_TYPE, POLLING)
//                .put(TARGETS, targets);
//
//            // send for plugin
//            vertx.eventBus().send(PLUGIN_EXECUTE, pluginInput);
//
//            LOGGER.info("Sent polling plugin input: {}", pluginInput.encodePrettily());
//        }
//        catch (Exception exception)
//        {
//            LOGGER.error("Polling failed: {}", exception.getMessage());
//        }
//    }

    private void pollJobs(List<JsonObject> jobs)
    {
        try
        {
            // Extract unique IPs from jobs
            var ips = jobs.stream()
                .map(job -> job.getString(IP))
                .distinct()
                .collect(Collectors.toList());

            // Use executeBlocking to avoid blocking the event loop
            vertx.<Set<String>>executeBlocking(promise -> {
                try
                {
                    var alive = Utility.pingCheck(ips);

                    promise.complete(Utility.portCheck(alive, 22));
                }
                catch (Exception e)
                {
                    promise.fail(e);
                }
            }, res -> {
                if (res.succeeded())
                {
                    var reachableIps = res.result();

                    // Collect the cache jobs whose device is reachable, shaped for the v2 envelope.
                    var dueJobs = new JsonArray();

                    for (var job : jobs)
                    {
                        var ip = job.getString(IP);

                        if (reachableIps.contains(ip))
                        {
                            dueJobs.add(new JsonObject()
                                .put(JOB_ID, job.getLong(PROVISIONING_JOB_ID))
                                .put(PLUGIN_TYPE, job.getString(PROTOCOL).toUpperCase())
                                .put(IP, ip)
                                .put(PORT, job.getInteger(PORT))
                                .put(CRED_DATA, job.getString(CRED_DATA))
                                .put("metrics", new JsonArray().add(job.getString(METRIC_NAME))));
                        }
                    }

                    if (dueJobs.isEmpty())
                    {
                        LOGGER.info("No reachable targets for polling");
                        return;
                    }

                    vertx.eventBus().send(PLUGIN_EXECUTE, buildEnvelope(dueJobs));

                    LOGGER.info("Sent polling plugin input with {} target(s)", dueJobs.size());
                }
                else
                {
                    LOGGER.error("Reachability check failed", res.cause());
                }
            });
        }
        catch (Exception exception)
        {
            LOGGER.error("Polling failed: {}", exception.getMessage());
        }
    }

    /**
     * Builds the v2 "poll" engine envelope (spec §4) from a list of due jobs.
     * Each job carries an encrypted {@code cred_data} string that is decrypted here
     * so credentials never live in plaintext in the cache; every job becomes exactly
     * one target with its metrics preserved.
     *
     * @param dueJobs jobs shaped as {@code {job_id, plugin_type, ip, port, cred_data, metrics}}.
     * @return the {@code {version, event_type:"poll", targets:[...]}} envelope.
     */
    public static JsonObject buildEnvelope(JsonArray dueJobs)
    {
        var targets = new JsonArray();

        for (var entry : dueJobs)
        {
            var job = (JsonObject) entry;

            // cred_data is stored as {"user","password"}; the engine's SSH plugin expects
            // {"username","password"} — remap exactly as the discovery path does, otherwise
            // the engine receives no username and auth fails ("attempted methods [none password]").
            var plain = new JsonObject(CryptoUtil.decrypt(job.getString(CRED_DATA)));

            var credential = new JsonObject()
                .put(USERNAME, plain.getString(USER))
                .put(PASSWORD, plain.getString(PASSWORD));

            targets.add(PluginEnvelope.target(
                UUID.randomUUID().toString(),
                job.getLong(JOB_ID),
                job.getString(PLUGIN_TYPE),
                job.getString(IP),
                job.getInteger(PORT),
                credential,
                job.getJsonArray("metrics")));
        }

        return PluginEnvelope.build(EVENT_POLL, targets);
    }

}

