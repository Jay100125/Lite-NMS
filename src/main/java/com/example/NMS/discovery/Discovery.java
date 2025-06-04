package com.example.NMS.discovery;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.example.NMS.constant.Constant.*;
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
                    .put(PROTOCOL, "ssh")
                    .put(PORT, port)
                    .put(CREDENTIAL_PROFILES, credentialProfiles)
                    .put(PLUGIN_TYPE, LINUX)
                    .put("discovery_id",discoveryId);

                targets.add(target);
            }
            else
            {
                var errorMsg = up ? "Port closed" : "Device unreachable";

//                discoveryResults.add(new JsonObject()
//                    .put(IP, obj.getString(IP))
//                    .put(PORT, port)
//                    .put("status", FAILURE)
//                    .put("result", errorMsg)
//                    .put("discovery_id", discoveryId)
//                    .put("credential_id", null));
                JsonObject unreachableResult = new JsonObject()
                    .put(IP, obj.getString(IP))
                    .put(PORT, port)
                    .put("status", FAILURE)
                    .put("result", errorMsg) // The actual outcome string
                    .put("discovery_id", discoveryId)
                    .put("credential_id", null)
                    .put("request.type", "discovery"); // Or determine appropriate credential_id if possible

//                // Send this single result to STORAGE_RESULTS, matching Plugin's format
//                var storageMessage = new JsonObject()
//                    .put("result", unreachableResult) // Key "result" (singular) with the JsonObject
//                    .put(ID, discoveryId) // The overall discovery run ID
//                    .put(REQUEST_TYPE, DISCOVERY);
                vertx.eventBus().send(STORAGE_RESULTS, unreachableResult);
//                LOGGER.info("Sent unreachable/port-closed result for IP {} to {}: {}",
//                    unreachableResult.getString(IP), STORAGE_RESULTS, storageMessage.encodePrettily());
            }
        }

//        if (!discoveryResults.isEmpty())
//        {
////            var storageMessage = new JsonObject()
////                .put("results", discoveryResults)
////                .put(ID, discoveryId);
////
////            vertx.eventBus().send(STORAGE_DISCOVERY_RESULTS, storageMessage);
////
////            LOGGER.info("Sent unreachable/port-closed results to {}: {}", STORAGE_DISCOVERY_RESULTS, storageMessage.encodePrettily());
//            var storageMessage = new JsonObject()
//                .put("results", discoveryResults)
//                .put(ID, discoveryId)
//                .put(REQUEST_TYPE, DISCOVERY);
//            vertx.eventBus().send(STORAGE_RESULTS, storageMessage);
//            LOGGER.info("Sent unreachable/port-closed results to {}: {}", STORAGE_RESULTS, storageMessage.encodePrettily());
//        }

        var pluginInput = new JsonObject()
            .put(REQUEST_TYPE, DISCOVERY)
            .put(TARGETS, targets)
            .put("id", discoveryId);



        LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());

        vertx.eventBus().send(PLUGIN_EXECUTE, pluginInput);

        // Return reachability results as the verticle no longer waits for plugin results
        return Future.succeededFuture(reachResults);
    }
}
