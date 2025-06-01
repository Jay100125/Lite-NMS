package com.example.NMS.discovery;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.DBUtils.executeBatchQuery;
import static com.example.NMS.utility.DBUtils.executeQuery;
import static com.example.NMS.utility.Utility.*;


/**
 * Vert.x verticle for running network discovery in Lite NMS.
 * Consumes discovery requests from the event bus, fetches discovery profiles, resolves IP addresses,
 * checks reachability, executes an SSH plugin for discovery, and stores results in the database.
 */
// TODO: Single responsibility and event-driven design principles
public class Discovery extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);


    /**
     * Starts the discovery verticle.
     * Sets up an event bus consumer to process discovery requests and signals successful deployment.
     *
     * @param startPromise The promise to complete or fail based on startup success.
     */
    @Override
    public void start(Promise<Void> startPromise)
    {
        // Set up event bus consumer for discovery requests
        vertx.eventBus().<JsonObject>localConsumer(DISCOVERY_RUN, message ->
        {
            var id = message.body().getLong(ID);

            runDiscovery(id)
                .onComplete(result ->
                {
                    if (result.failed())
                    {
                        LOGGER.error("Discovery failed for ID {}: {}", id, result.cause().getMessage());
                    }
                });
        });

        LOGGER.info("Discovery verticle deployed");

        startPromise.complete();
    }

    /**
     * Runs a discovery process for the specified discovery profile ID.
     * Fetches the profile, resolves IPs, checks reachability, performs SSH discovery, and stores results.
     *
     * @param id The discovery profile ID.
     * @return A Future containing the reachability results as a JSON array.
     */
    private Future<JsonArray> runDiscovery(long id)
    {
        return fetchDiscoveryProfile(id)
            .compose(body -> executeDiscovery(body, id))
            .compose(results -> setDiscoveryStatus(id).map(results))
            .recover(Future::failedFuture);
    }

    /**
     * Updates the discovery profile status to true (completed) in the database.
     * By default, profiles are created with a false (pending) status.
     *
     * @param id The discovery profile ID.
     * @return A Future indicating success or failure.
     */
    private Future<Void> setDiscoveryStatus(long id)
    {
        var query = new JsonObject()
            .put(QUERY, QueryConstant.SET_DISCOVERY_STATUS)
            .put(PARAMS, new JsonArray().add(true).add(id));

        return executeQuery(query)
            .compose(result ->
            {
                if (!result.isEmpty())
                {
                    LOGGER.info("Discovery status set to {} for ID {}", true, id);

                    return Future.succeededFuture();
                }
                return Future.failedFuture("Failed to set discovery status");
            });
    }

    /**
     * Fetches the discovery profile details, including IP addresses, port, and credentials.
     *
     * @param id The discovery profile ID.
     * @return A Future containing a JSON array with the profile data.
     */
    private Future<JsonArray> fetchDiscoveryProfile(long id)
    {
        var fetchQuery = new JsonObject()
            .put(QUERY, QueryConstant.GET_BY_RUN_ID)
            .put(PARAMS, new JsonArray().add(id));

        return executeQuery(fetchQuery)
            .compose(result ->
            {
                if (result.isEmpty())
                {
                    LOGGER.info("Discovery profile not found for ID {}", id);

                    return Future.failedFuture("Discovery profile not found");
                }
                return Future.succeededFuture(result);
            });
    }

    /**
     * Executes the discovery process using the profile data.
     * Resolves IP addresses, checks reachability, performs SSH discovery, and prepares results.
     *
     * @param result The JSON array containing the discovery profile data.
     * @param id The discovery profile ID.
     * @return A Future containing a JSON array of reachability results.
     */
    private Future<JsonArray> executeDiscovery(JsonArray result, long id)
    {
        var profile = result.getJsonObject(0);

        var ipInput = profile.getString(IP);

        var port = profile.getInteger(PORT);

        var credentials = profile.getJsonArray("credential");

        LOGGER.info("Discovery profile: {}", ipInput);

//        return resolveIps(ipInput)
//            .compose(ips -> checkReach(ips, port))
//            .compose(reachResults -> handleConnection(reachResults, credentials, port))
//            .compose(sshResult ->
//            {
//                var reachabilityResults = sshResult.getJsonArray(REACHABILITY_RESULTS);
//                var discoveryResults = sshResult.getJsonArray(DISCOVERY_RESULTS);
//                return storeDiscoveryResults(discoveryResults, id)
//                    .map(reachabilityResults);
//            });
        return resolveIps(ipInput)
            .compose(ips -> checkReach(ips, port))
            .compose(reachResults -> handleConnection(reachResults, credentials, port, id));
    }

    /**
     * Resolves IP addresses from the input string (single IP, range, or CIDR).
     *
     * @param ipInput The IP address input string.
     * @return A Future containing a list of resolved IP addresses.
     */
    private Future<List<String>> resolveIps(String ipInput)
    {
        return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
    }

    /**
     * Checks reachability and port availability for the given IP addresses.
     *
     * @param ips The list of IP addresses to check.
     * @param port        The port to verify (e.g., 22 for SSH).
     * @return A Future containing a JSON array of reachability results.
     */
    private Future<JsonArray> checkReach(List<String> ips, int port)
    {
        return vertx.executeBlocking(() -> checkReachability(ips, port), false);
    }

    /**
     * Performs SSH discovery using the provided credentials for reachable IPs.
     * Executes an SSH plugin to collect system information (e.g., uname).
     *
     * @param reachResults The JSON array of reachability results.
     * @param credentials  The JSON array of credential profiles.
     * @param port         The port for SSH connections.
     * @return A Future containing a JSON object with reachability and discovery results.
     */
    private Future<JsonArray> handleConnection(JsonArray reachResults, JsonArray credentials, int port, long discoveryId)
    {
        var reachableIps = new JsonArray();

        var discoveryResults = new JsonArray();

        var targets = new JsonArray();

        LOGGER.info(reachResults.encodePrettily());

        var credentialProfiles = new JsonArray();

        for (int j = 0; j < credentials.size(); j++)
        {

            var cred = credentials.getJsonObject(j);

            credentialProfiles.add(new JsonObject()
                .put(USERNAME, cred.getString(USERNAME))
                .put(PASSWORD, cred.getString(PASSWORD))
                .put(ID, cred.getLong(ID)));

        }
        // Process reachability and plugin results
        for (var i = 0; i < reachResults.size(); i++)
        {
            var obj = reachResults.getJsonObject(i);

            var up = obj.getBoolean("reachable");

            var open = obj.getBoolean("port_open");

            if (up && open)
            {

                // Add target with credential_profiles
                var target = new JsonObject()
                    .put(IP, obj.getString(IP))
                    .put(PROTOCOL, "ssh") // Assuming SSH from credentials
                    .put(PORT, port)
                    .put("credential_profiles", credentialProfiles)
                    .put(PLUGIN_TYPE, LINUX);

                targets.add(target);
            }
            else
            {
                var errorMsg = up ? "Port closed" : "Device unreachable";
                discoveryResults.add(new JsonObject()
                    .put(IP, obj.getString(IP))
                    .put(PORT, port)
                    .put("status", FAILURE)
                    .put("result", errorMsg)
                    .put("credential_id", null));
            }
        }

        if (!discoveryResults.isEmpty())
        {
            var storageMessage = new JsonObject()
                .put("results", discoveryResults)
                .put(ID, discoveryId);
            vertx.eventBus().send(STORAGE_DISCOVERY_RESULTS, storageMessage);
            LOGGER.info("Sent unreachable/port-closed results to {}: {}", STORAGE_DISCOVERY_RESULTS, storageMessage.encodePrettily());
        }

        var pluginInput = new JsonObject()
            .put(REQUEST_TYPE, DISCOVERY)
            .put(TARGETS, targets)
            .put("id", discoveryId);



        LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());


//        // Execute SSH plugin
//        return vertx.executeBlocking(() -> spawnPlugin(pluginInput), false)
//            .compose(pluginResults ->
//            {
//                var processedIps = new HashMap<String, Boolean>();
//
//                for (var j = 0; j < pluginResults.size(); j++)
//                {
//                    var res = pluginResults.getJsonObject(j);
//
//                    var ip = res.getString(IP);
//
//                    var status = res.getString(STATUS);
//
//                    if (processedIps.getOrDefault(ip, false))
//                    {
//                        continue;
//                    }
//                    // Find the corresponding reachResults entry
//                    var reachObj = reachResults.getJsonObject(j);
//
//                    Long successfulCredId = null;
//
//                    String uname = null;
//
//                    String errorMsg;
//
//                    if ("success".equals(status))
//                    {
//                        reachableIps.add(ip);
//
//                        successfulCredId = res.getLong("credential_id");
//
//                        uname = res.getJsonObject("data").getString("uname");
//
//                        errorMsg = null;
//
//                        processedIps.put(ip, true);
//                    }
//                    else
//                    {
//                        errorMsg = res.getString("error");
//                    }
//
//                    if (!processedIps.getOrDefault(ip, false) || "success".equals(status))
//                    {
//                        discoveryResults.add(new JsonObject()
//                            .put(IP, ip)
//                            .put(PORT, port)
//                            .put(RESULT, errorMsg == null ? "completed" : "failed")
//                            .put(MESSAGE, errorMsg == null ? uname : errorMsg)
//                            .put(CREDENTIAL_PROFILE_ID, successfulCredId));
//                    }
//
//                    reachObj.put("uname", uname);
//                    reachObj.put("ssh_error", errorMsg);
//                    reachObj.put(CREDENTIAL_PROFILE_ID, successfulCredId);
//                }
//
//                return Future.succeededFuture(new JsonObject()
//                    .put(REACHABILITY_RESULTS, reachResults)
//                    .put(REACHABLEIPS, reachableIps)
//                    .put(DISCOVERY_RESULTS, discoveryResults));
//            });
        vertx.eventBus().send(PLUGIN_EXECUTE, pluginInput);

        // Return reachability results as the verticle no longer waits for plugin results
        return Future.succeededFuture(reachResults);
    }

    /**
     * Stores discovery results in the database.
     * Inserts or updates discovery result records with IP, port, status, message, and credential ID.
     *
     * @param discoveryResults The JSON array of discovery results.
     * @param discoveryId      The discovery profile ID.
     * @return A Future indicating success or failure.
     */
    private Future<Void> storeDiscoveryResults(JsonArray discoveryResults, long discoveryId)
    {
        var batchParams = new JsonArray();

        for (var i = 0; i < discoveryResults.size(); i++)
        {
            var result = discoveryResults.getJsonObject(i);

            batchParams.add(new JsonArray()
                .add(discoveryId)
                .add(result.getString(IP))
                .add(result.getInteger(PORT))
                .add(result.getString(RESULT))
                .add(result.getString(MESSAGE))
                .add(result.getValue(CREDENTIAL_PROFILE_ID)));
        }

        var message = new JsonObject()
                            .put(QUERY, QueryConstant.INSERT_DISCOVERY_RESULT)
                            .put(BATCHPARAMS, batchParams);

        return executeBatchQuery(message)
            .compose(result ->
            {
                if (!result.isEmpty())
                {
                    LOGGER.info("Successfully inserted/updated {} discovery results", batchParams.size());

                    return Future.succeededFuture();
                }
                return Future.failedFuture("Batch insert failed: " + result);
            });
    }
}
