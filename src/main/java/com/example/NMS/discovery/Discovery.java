package com.example.NMS.discovery;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.events.DiscoveryEvents;
import com.example.NMS.plugin.PluginEnvelope;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Set;

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

        DiscoveryEvents.state(vertx.eventBus(), id, DISCOVERY_STATUS_RUNNING, null);

        var ipInput = profile.getString(IP);

        var port = profile.getInteger(PORT);

        var pluginType = profile.getString(PLUGIN_TYPE, PLUGIN_LINUX);

        var credentials = profile.getJsonArray("credential");

        LOGGER.info("Discovery {} ({}): {}", id, pluginType, ipInput);

        resolveIps(ipInput)
            .compose(ips ->
            {
                DiscoveryEvents.targets(vertx.eventBus(), id, ips);

                return vertx.<Set<String>>executeBlocking(() -> pingCheck(ips), false)
                    .map(alive -> publishPingStage(id, pluginType, port, ips, alive));
            })
            .compose(alive ->
            {
                if (DiscoveryStages.skipsPortCheck(pluginType))
                {
                    return Future.succeededFuture(alive);
                }

                return vertx.<Set<String>>executeBlocking(() -> portCheck(alive, port), false)
                    .map(open -> publishPortStage(id, port, alive, open));
            })
            .onComplete(asyncResult ->
            {
                if (asyncResult.succeeded())
                {
                    handleConnection(asyncResult.result(), credentials, pluginType, port, id);
                }
                else
                {
                    LOGGER.error("Discovery {} failed during resolve/reachability: {}", id, asyncResult.cause().getMessage());

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

                    DiscoveryEvents.state(vertx.eventBus(), id, DISCOVERY_STATUS_FAILED, asyncResult.cause().getMessage());
                }
            });
    }

    private Future<List<String>> resolveIps(String ipInput)
    {
        return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
    }

    /**
     * Publishes per-IP PING outcomes and short-circuits dead IPs straight to
     * storage as FAILED (they never reach the port check or the engine).
     *
     * @return the alive subset, input to the next stage.
     */
    private Set<String> publishPingStage(long id, String pluginType, int port, List<String> ips, Set<String> alive)
    {
        for (var ip : ips)
        {
            if (alive.contains(ip))
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PING",
                    DiscoveryStages.pingProgress(pluginType), "ok", null);
            }
            else
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PING", 100.0, "failed", "ping failed");

                storeShortCircuitFailure(id, ip, port, "Device unreachable");
            }
        }

        return alive;
    }

    /** Publishes per-IP PORT outcomes; closed-port IPs short-circuit to storage as FAILED. */
    private Set<String> publishPortStage(long id, int port, Set<String> alive, Set<String> open)
    {
        for (var ip : alive)
        {
            if (open.contains(ip))
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PORT", 66.66, "ok", null);
            }
            else
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PORT", 100.0, "failed", "port not reachable");

                storeShortCircuitFailure(id, ip, port, "Port closed");
            }
        }

        return open;
    }

    private void storeShortCircuitFailure(long id, String ip, int port, String reason)
    {
        vertx.eventBus().send(STORAGE_RESULTS, new JsonObject()
            .put(IP, ip)
            .put(PORT, port)
            .put(STATUS, FAILURE)
            .put(RESULT, reason)
            .put(DISCOVERY_ID, id)
            .put(CREDENTIAL_ID, null)
            .put(EVENT_TYPE, EVENT_DISCOVERY));
    }

    private void handleConnection(Set<String> readyIps, JsonArray credentials, String pluginType, int port, long discoveryId)
    {
        var targets = new JsonArray();

        for (var ip : readyIps)
        {
            // One v2 target per (ip, credential): the engine tries a SINGLE credential per
            // target. request_id carries the correlation context for ResponseProcessor.
            for (var j = 0; j < credentials.size(); j++)
            {
                var cred = credentials.getJsonObject(j);

                var requestId = DiscoveryRequestId.encode(discoveryId, ip, port, cred.getLong(ID));

                targets.add(PluginEnvelope.target(requestId, 0L, pluginType, ip, port,
                    PluginEnvelope.credential(pluginType, cred), new JsonArray()));
            }
        }

        var envelope = PluginEnvelope.build(EVENT_DISCOVERY, targets)
            .put(DISCOVERY_ID, discoveryId);

        LOGGER.info("Dispatching discovery envelope for ID {} with {} target(s)", discoveryId, targets.size());

        vertx.eventBus().send(PLUGIN_EXECUTE, envelope);
    }
}
