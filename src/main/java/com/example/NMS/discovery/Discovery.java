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

        var credentialProfiles = new JsonArray();

        for (int j = 0; j < credentials.size(); j++)
        {
            var cred = credentials.getJsonObject(j);

            credentialProfiles.add(new JsonObject()
                .put(USERNAME, cred.getString(USERNAME))
                .put(PASSWORD, cred.getString(PASSWORD))
                .put(ID, cred.getLong(ID)));
        }

        for (var i = 0; i < reachResults.size(); i++)
        {
            var obj = reachResults.getJsonObject(i);

            var up = obj.getBoolean("reachable");

            var open = obj.getBoolean("port_open");

            if (up && open)
            {
                var target = new JsonObject()
                    .put(IP, obj.getString(IP))
                    .put(PORT, port)
                    .put(CREDENTIAL_PROFILES, credentialProfiles)
                    .put(PLUGIN_TYPE, LINUX)
                    .put(DISCOVERY_ID, discoveryId);

                targets.add(target);
            }
            else
            {
                var errorMsg = up ? "Port closed" : "Device unreachable";

                var unreachableResult = new JsonObject()
                    .put(IP, obj.getString(IP))
                    .put(PORT, port)
                    .put(STATUS, FAILURE)
                    .put(RESULT, errorMsg)
                    .put(DISCOVERY_ID, discoveryId)
                    .put(CREDENTIAL_ID, null)
                    .put(REQUEST_TYPE, DISCOVERY);

                vertx.eventBus().send(STORAGE_RESULTS, unreachableResult);
            }
        }

        var pluginInput = new JsonObject()
            .put(REQUEST_TYPE, DISCOVERY)
            .put(DISCOVERY_ID, discoveryId)
            .put(TARGETS, targets);

        LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());

        vertx.eventBus().send(PLUGIN_EXECUTE, pluginInput);

    }
}
