package com.example.NMS.discovery;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.plugin.PluginEnvelope;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.Utility.*;

public class Discovery extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Discovery.class);

    @Override
    public void start(Promise<Void> startPromise)
    {
        vertx.eventBus().<JsonObject>localConsumer(DISCOVERY_RUN, message ->
        {
            var id = message.body().getLong(ID);

            var profile = message.body().getJsonObject("profile");

            if (profile == null)
            {
                LOGGER.error("No profile data received for discovery ID {}", id);

                return;
            }

            runDiscovery(id, profile);
        });

        LOGGER.info("Discovery verticle deployed");

        startPromise.complete();
    }

    private void runDiscovery(long id, JsonObject profile)
    {
        var query = new JsonObject()
            .put(QUERY, QueryConstant.UPDATE_DISCOVERY_PROFILE_STATUS)
            .put(PARAMS, new JsonArray().add(DISCOVERY_STATUS_RUNNING).add(id));

        vertx.eventBus().send(DB_EXECUTE_QUERY, query);

        var ipInput = profile.getString(IP);

        var port = profile.getInteger(PORT);

        var credentials = profile.getJsonArray("credential");

        LOGGER.info("Discovery profile: {}", ipInput);

         resolveIps(ipInput)
            .compose(ips -> checkReach(ips, port))
            .onComplete(asyncResult ->
            {
                if(asyncResult.succeeded())
                {
                    var reachResult = asyncResult.result();

                    handleConnection(reachResult, credentials, port, id);
                }
                else
                {
                    LOGGER.error("Discovery {} failed during resolve/reachability: {}", id, asyncResult.cause().getMessage());

                    // Record a FAILED discovery result and mark the profile FAILED so it never hangs in RUNNING.
                    var failedResult = new JsonObject()
                        .put(IP, ipInput)
                        .put(PORT, port)
                        .put(STATUS, FAILURE)
                        .put(RESULT, asyncResult.cause().getMessage())
                        .put(DISCOVERY_ID, id)
                        .put(CREDENTIAL_ID, null)
                        .put(EVENT_TYPE, EVENT_DISCOVERY);

                    vertx.eventBus().send(STORAGE_RESULTS, failedResult);

                    var statusQuery = new JsonObject()
                        .put(QUERY, QueryConstant.UPDATE_DISCOVERY_PROFILE_STATUS)
                        .put(PARAMS, new JsonArray().add(DISCOVERY_STATUS_FAILED).add(id));

                    vertx.eventBus().send(DB_EXECUTE_QUERY, statusQuery);
                }

            });

    }

    private Future<List<String>> resolveIps(String ipInput)
    {
        return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
    }

    private Future<JsonArray> checkReach(List<String> ips, int port)
    {
        return vertx.executeBlocking(() -> checkReachability(ips, port), false);
    }

    private void handleConnection(JsonArray reachResults, JsonArray credentials, int port, long discoveryId)
    {
        var targets = new JsonArray();

        for (var i = 0; i < reachResults.size(); i++)
        {
            var obj = reachResults.getJsonObject(i);

            var up = obj.getBoolean("reachable");

            var open = obj.getBoolean("port_open");

            var ip = obj.getString(IP);

            if (up && open)
            {
                // One v2 target per (ip, credential): the engine tries a SINGLE credential per target,
                // so a discovery with multiple candidate credentials fans out. The request_id carries the
                // context (v2 result lines omit ip/discovery_id) so ResponseProcessor can store the result.
                for (var j = 0; j < credentials.size(); j++)
                {
                    var cred = credentials.getJsonObject(j);

                    var credential = new JsonObject()
                        .put(USERNAME, cred.getString(USERNAME))
                        .put(PASSWORD, cred.getString(PASSWORD));

                    var requestId = DiscoveryRequestId.encode(discoveryId, ip, port, cred.getLong(ID));

                    // plugin_type must be the engine's registry key (uppercase); Constant.LINUX is lowercase.
                    targets.add(PluginEnvelope.target(requestId, 0L, "LINUX", ip, port, credential, new JsonArray()));
                }
            }
            else
            {
                var errorMsg = up ? "Port closed" : "Device unreachable";

                var unreachableResult = new JsonObject()
                    .put(IP, ip)
                    .put(PORT, port)
                    .put(STATUS, FAILURE)
                    .put(RESULT, errorMsg)
                    .put(DISCOVERY_ID, discoveryId)
                    .put(CREDENTIAL_ID, null)
                    .put(EVENT_TYPE, EVENT_DISCOVERY);

                vertx.eventBus().send(STORAGE_RESULTS, unreachableResult);
            }
        }

        // v2 discovery envelope {version, event_type:"discovery", targets}. discovery_id is an extra
        // field the engine ignores (lenient JSON) but the EVENT_COMPLETION handler uses to mark the
        // profile COMPLETED once the run finishes.
        var envelope = PluginEnvelope.build(EVENT_DISCOVERY, targets)
            .put(DISCOVERY_ID, discoveryId);

        LOGGER.info("Dispatching discovery envelope for ID {} with {} target(s)", discoveryId, targets.size());

        vertx.eventBus().send(PLUGIN_EXECUTE, envelope);

    }
}
