package com.example.NMS.service;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.SUCCESS;
import static com.example.NMS.service.QueryProcessor.*;
import static com.example.NMS.utility.Utility.*;

public class DiscoveryService
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DiscoveryService.class);

  /**
   * Run a discovery process for the given discovery ID.
   *
   * @param id The discovery profile ID
   * @return Future containing the reachability results
   */
  public static Future<JsonArray> runDiscovery(long id)
  {
    var fetchQuery = new JsonObject()
      .put("query", QueryConstant.RUN_DISCOVERY)
      .put("params", new JsonArray().add(id));

    var setRunningQuery = new JsonObject()
      .put("query", QueryConstant.SET_DISCOVERY_STATUS)
      .put("params", new JsonArray().add(true).add(id));

    return executeQuery(setRunningQuery)
      .compose(v -> executeQuery(fetchQuery))
      .compose(body ->
      {
        var rows = body.getJsonArray("result");

        if (!SUCCESS.equals(body.getString("msg")) || rows.isEmpty())
        {
          LOGGER.info("Discovery profile not found");

          return Future.failedFuture("Discovery profile not found");
        }

        var ipInput = rows.getJsonObject(0).getString("ip");

        var port = rows.getJsonObject(0).getInteger("port");

        LOGGER.info("Discovery profile: {}", ipInput);

        // Process credentials
        var credentials = new JsonArray();

        Set<Long> seenCredentialIds = new HashSet<>();
        for (var i = 0; i < rows.size(); i++)
        {
          var row = rows.getJsonObject(i);

          var credentialProfileId = row.getLong("cpid");

          var credData = row.getJsonObject("cred_data");

          if (credentialProfileId != null && credData != null && seenCredentialIds.add(credentialProfileId))
          {
            credentials.add(new JsonObject()
              .put("credential_profile_id", credentialProfileId)
              .put("cred_data", credData));
          }
        }

        return resolveIps(ipInput)
          .compose(ips -> checkReach(ips, port))
          .compose(reachResults -> doSSH(reachResults, credentials, id, port))
          .compose(sshResult ->
          {
            var reachabilityResults = sshResult.getJsonArray("reachabilityResults");

            var discoveryResults = sshResult.getJsonArray("discoveryResults");

            return storeDiscoveryResults(discoveryResults, id)
              .map(reachabilityResults);
          })
          .compose(results ->
          {
            var resetStatusQuery = new JsonObject()
              .put("query", QueryConstant.SET_DISCOVERY_STATUS)
              .put("params", new JsonArray().add(true).add(id));

            return executeQuery(resetStatusQuery)
              .map(results);
          })
          .recover(err -> {

            var resetStatusQuery = new JsonObject()
              .put("query", QueryConstant.SET_DISCOVERY_STATUS)
              .put("params", new JsonArray().add(false).add(id));

            return executeQuery(resetStatusQuery)
              .compose(v -> Future.failedFuture(err));
          });
      });
  }

  private static Future<List<String>> resolveIps(String ipInput)
  {
    return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
  }

  private static Future<JsonArray> checkReach(List<String> ips, int port)
  {
    return vertx.executeBlocking(() -> checkReachability(ips, port), false);
  }

  private static Future<JsonObject> doSSH(JsonArray reachResults, JsonArray credentials, long discoveryId, int port)
  {
    return vertx.executeBlocking(() ->
    {
      var reachableIps = new JsonArray();

      var discoveryResults = new JsonArray();

      var pluginInput = new JsonObject()
        .put("category", "discovery")
        .put("metric.type", "linux")
        .put("port", port)
        .put("dis.id", discoveryId)
        .put("ips", new JsonArray())
        .put("credentials", new JsonArray());

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

      for (var i = 0; i < credentials.size(); i++)
      {
        var cred = credentials.getJsonObject(i);

        var credData = cred.getJsonObject("cred_data");

        pluginInput.getJsonArray("credentials").add(new JsonObject()
          .put("id", cred.getLong("credential_profile_id"))
          .put("username", credData.getString("user"))
          .put("password", credData.getString("password")));
      }

      LOGGER.info("Plugin input: {}", pluginInput.encodePrettily());

      var pluginResults = runSSHPlugin(pluginInput);

      for (var i = 0; i < reachResults.size(); i++)
      {
        var obj = reachResults.getJsonObject(i);

        var ip = obj.getString("ip");

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

            var resIp = res.getString("ip");

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
          .put("ip", ip)
          .put("port", port)
          .put("result", errorMsg == null ? "completed" : "failed")
          .put("msg", errorMsg == null ? uname : errorMsg)
          .put("credential_profile_id", successfulCredId));

        obj.put("uname", uname);

        obj.put("ssh_error", errorMsg);

        obj.put("credential_profile_id", successfulCredId);
      }

      return new JsonObject()
        .put("reachabilityResults", reachResults)
        .put("reachableIps", reachableIps)
        .put("discoveryResults", discoveryResults);
    }, false);
  }

  private static Future<Void> storeDiscoveryResults(JsonArray discoveryResults, long discoveryId)
  {
    var batchParams = new JsonArray();

    for (var i = 0; i < discoveryResults.size(); i++)
    {
      var result = discoveryResults.getJsonObject(i);

      batchParams.add(new JsonArray()
        .add(discoveryId)
        .add(result.getString("ip"))
        .add(result.getInteger("port"))
        .add(result.getString("result"))
        .add(result.getString("msg"))
        .add(result.getValue("credential_profile_id")));
    }

    var message = new JsonObject()
      .put("query", QueryConstant.INSERT_DISCOVERY_RESULT)
      .put("batchParams", batchParams);

    return executeBatchQuery(message)
      .compose(result ->
      {
        if (SUCCESS.equals(result.getString("msg")))
        {
          LOGGER.info("Successfully inserted/updated {} discovery results", batchParams.size());

          return Future.succeededFuture();
        }
        else
        {
          return Future.failedFuture("Batch insert failed: " + result.getString("ERROR"));
        }
      });
  }
}
