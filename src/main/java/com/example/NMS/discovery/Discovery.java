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
import static com.example.NMS.service.QueryProcessor.executeBatchQuery;
import static com.example.NMS.service.QueryProcessor.executeQuery;
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
     * Run a discovery process for the given discovery ID.
     *
     * @param id The discovery profile ID
     * @return Future containing the reachability results
     */
    private Future<JsonArray> runDiscovery(long id)
    {
        return fetchDiscoveryProfile(id)
          .compose(body -> executeDiscovery(body, id))
          .compose(results -> setDiscoveryStatus(id).map(results))
          .recover(Future::failedFuture);
    }

    // after discovery run set it's status to true by default when user create discovery profile it is set to false
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

    // fetch all the credential and ip regarding the discovery profile
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

    private Future<JsonArray> executeDiscovery(JsonArray result, long id)
    {
        var profile = result.getJsonObject(0);

        var ipInput = profile.getString(IP);

        var port = profile.getInteger(PORT);

        var credentials = profile.getJsonArray("credential");

        LOGGER.info("Discovery profile: {}", ipInput);

        return resolveIps(ipInput)
          .compose(ips -> checkReach(ips, port))
          .compose(reachResults -> doSSH(reachResults, credentials, id, port))
          .compose(sshResult ->
          {
            var reachabilityResults = sshResult.getJsonArray("reachabilityResults");
            var discoveryResults = sshResult.getJsonArray("discoveryResults");
            return storeDiscoveryResults(discoveryResults, id)
              .map(reachabilityResults);
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

    private Future<JsonObject> doSSH(JsonArray reachResults, JsonArray credentials, long discoveryId, int port)
    {
        var reachableIps = new JsonArray();

        var discoveryResults = new JsonArray();

        var pluginInput = new JsonObject()
          .put("category", "discovery")
          .put("metric.type", "uname")
          .put("port", port)
          .put("dis.id", discoveryId)
          .put("ips", new JsonArray())
          .put("credentials", credentials);

        LOGGER.info(reachResults.encodePrettily());

        for (var i = 0; i < reachResults.size(); i++)
        {
            var obj = reachResults.getJsonObject(i);

            var up = obj.getBoolean("reachable");

            var open = obj.getBoolean("port_open");

            if (up && open)
            {
                pluginInput.getJsonArray("ips").add(obj.getString("ip"));
            }
        }

        LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());

        // Execute spawnPlugin in a blocking context
        return vertx.executeBlocking(() -> spawnPlugin(pluginInput), false)
          .compose(pluginResults -> {
            for (var i = 0; i < reachResults.size(); i++)
            {
                var obj = reachResults.getJsonObject(i);

                var ip = obj.getString(IP);

                var up = obj.getBoolean("reachable");

                var open = obj.getBoolean("port_open");

                Long successfulCredId = null;

                String uname = null;

                var errorMsg = up ? (open ? "SSH connection failed" : "Port closed") : "Device unreachable";

                if (up && open)
                {
                    for (var j = 0; j < pluginResults.size(); j++)
                    {
                        var res = pluginResults.getJsonObject(j);

                        var resIp = res.getString(IP);

                        var status = res.getString("status");

                        if (ip.equals(resIp))
                        {
                          if ("success".equals(status))
                          {
                              reachableIps.add(ip);
                              successfulCredId = res.getLong("credential_id");
                              uname = res.getString("uname");
                              errorMsg = null;
                              break;
                          }
                          else
                          {
                              errorMsg = res.getString("error");
                          }
                        }
                    }
                }

                discoveryResults.add(new JsonObject()
                  .put(IP, ip)
                  .put(PORT, port)
                  .put(RESULT, errorMsg == null ? "completed" : "failed")
                  .put(MESSAGE, errorMsg == null ? uname : errorMsg)
                  .put(CREDENTIAL_PROFILE_ID, successfulCredId));

                obj.put("uname", uname);
                obj.put("ssh_error", errorMsg);
                obj.put(CREDENTIAL_PROFILE_ID, successfulCredId);
            }

            return Future.succeededFuture(new JsonObject()
              .put("reachabilityResults", reachResults)
              .put("reachableIps", reachableIps)
              .put("discoveryResults", discoveryResults));
          });
      }

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
