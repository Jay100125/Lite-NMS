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


/**
 * Vert.x verticle for running network discovery in Lite NMS.
 * Consumes discovery requests from the event bus, fetches discovery profiles, resolves IP addresses,
 * checks reachability, executes an SSH plugin for discovery, and stores results in the database.
 */

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
            .compose(reachResults -> doSSH(reachResults, credentials, id, port))
            .compose(sshResult ->
            {
              var reachabilityResults = sshResult.getJsonArray("reachabilityResults");
              var discoveryResults = sshResult.getJsonArray("discoveryResults");
              return storeDiscoveryResults(discoveryResults, id)
                .map(reachabilityResults);
            });
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
     * @param discoveryId  The discovery profile ID.
     * @param port         The port for SSH connections.
     * @return A Future containing a JSON object with reachability and discovery results.
     */
      private Future<JsonObject> doSSH(JsonArray reachResults, JsonArray credentials, long discoveryId, int port)
      {
          var reachableIps = new JsonArray();

          var discoveryResults = new JsonArray();

          // Prepare plugin input for SSH discovery
          var pluginInput = new JsonObject()
            .put("category", "discovery")
            .put("metric.type", "uname")
            .put("port", port)
            .put("dis.id", discoveryId)
            .put("ips", new JsonArray())
            .put("credentials", credentials);

          LOGGER.info(reachResults.encodePrettily());

          // Process reachability and plugin results
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

          // Execute SSH plugin
          return vertx.executeBlocking(() -> spawnPlugin(pluginInput), false)
            .compose(pluginResults ->
            {
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
