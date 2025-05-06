package com.example.NMS.service;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.Utility.*;

public class QueryProcessor
{
  public static final Logger logger = LoggerFactory.getLogger(QueryProcessor.class);

  /**
   * Execute a single database query and return a Future with the result.
   *
   * @param query The query JSON object with "query" and "params" fields
   * @return Future containing the query result
   */
  public static Future<JsonObject> executeQuery(JsonObject query)
  {
    return Future.future(promise ->
    {
      vertx.eventBus().request(EVENTBUS_ADDRESS, query, ar -> {
        if (ar.succeeded())
        {
          JsonObject result = (JsonObject) ar.result().body();
          logger.info("Database query executed: {}", query);

          logger.info("Database query result: {}", result);

          promise.complete(result);
        }
        else
        {
          logger.error("Database query failed: {}", ar.cause().getMessage());
          promise.fail(ar.cause());
        }
      });
    });
  }


  /**
   * Execute a batch database query and return a Future with the result.
   *
   * @param batchQuery The batch query JSON object with "query" and "batchParams" fields
   * @return Future containing the batch query result
   */
  public static Future<JsonObject> executeBatchQuery(JsonObject batchQuery)
  {
    return Future.future(promise ->
    {
      vertx.eventBus().request(EVENTBUS_BATCH_ADDRESS, batchQuery, ar ->
      {
        if (ar.succeeded())
        {
          var result = (JsonObject) ar.result().body();

          logger.info("Batch query executed: {}", batchQuery.getString("query"));

          logger.info("Batch query result: {}", result);

          promise.complete(result);
        }
        else
        {
          logger.error("Batch query failed: {}", ar.cause().getMessage());

          promise.fail(ar.cause());
        }
      });
    });
  }

  public static void runDiscovery(long id, RoutingContext context)
  {
    var fetchQuery = new JsonObject()
      .put(QUERY, QueryConstant.RUN_DISCOVERY)
      .put(PARAMS, new JsonArray().add(id));

    var setRunningQuery = new JsonObject()
      .put(QUERY, QueryConstant.SET_DISCOVERY_STATUS)
      .put(PARAMS, new JsonArray().add(true).add(id));

    executeQuery(setRunningQuery)
      .compose(v -> executeQuery(fetchQuery))
      .compose(body ->
      {
        var rows = body.getJsonArray("result");

        if (!SUCCESS.equals(body.getString("msg")) || rows.isEmpty())
        {
          logger.info("Discovery profile not found");

          return Future.failedFuture("Discovery profile not found");
        }

        var ipInput = rows.getJsonObject(0).getString("ip");

        var port = rows.getJsonObject(0).getInteger("port");

        logger.info("Discovery profile: {}", ipInput);

        // Simplified credential processing
        var credentials = new JsonArray();

        Set<Long> seenCredentialIds = new HashSet<>(); // For deduplication

        for (int i = 0; i < rows.size(); i++)
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
              .put(QUERY, QueryConstant.SET_DISCOVERY_STATUS)
              .put(PARAMS, new JsonArray().add(true).add(id));

            return executeQuery(resetStatusQuery)
              .map(results);
          });
      })
      .onSuccess(results ->
      {
        context.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put(MSG, SUCCESS)
            .put("results", results)
            .encodePrettily());
      })
      .onFailure(err -> {
        var resetStatusQuery = new JsonObject()
          .put(QUERY, QueryConstant.SET_DISCOVERY_STATUS)
          .put(PARAMS, new JsonArray().add(false).add(id));

        // Reset discovery status to false
        executeQuery(resetStatusQuery)
          .onComplete(v ->
          {
            logger.warn("Discovery flow failed");

            sendError(context, err instanceof NoSuchElementException ? 404 : 500, err.getMessage());
          });
      });
  }

  private static Future<List<String>> resolveIps(String ipInput)
  {
    // Callable-based executeBlocking returns a Future<List<String>>
    return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
  }

  private static Future<JsonArray> checkReach(List<String> ips, int port)
  {
    return vertx.executeBlocking(() -> checkReachability(ips, port), false);
  }

  private static Future<JsonObject> doSSH(JsonArray reachResults,
                                          JsonArray credentials,
                                          long discoveryId,
                                          int port
                                      )
  {
    return vertx.executeBlocking(() ->
    {
      var reachableIps = new JsonArray();

      var discoveryResults = new JsonArray();

      var pluginInput = new JsonObject()
        .put("category", "discovery")
        .put("metric.type", "linux")
        .put("port",port)
        .put("dis.id", discoveryId)
        .put("targets", new JsonArray());


      logger.info(reachResults.encodePrettily());

      for (var i = 0; i < reachResults.size(); i++)
      {
        var obj = reachResults.getJsonObject(i);

        var up = obj.getBoolean("reachable");

        var open = obj.getBoolean("port_open");

        if (up && open)
        {
          var ip = obj.getString("ip");

          logger.info(credentials.encodePrettily());

          for (var j = 0; j < credentials.size(); j++)
          {

            var cred = credentials.getJsonObject(j);

            var credentialProfileId = cred.getLong("credential_profile_id");

            // Only add if this credential is valid for this IP
            pluginInput.getJsonArray("targets").add(new JsonObject()
              .put("ip.address", ip)
              .put("user", cred.getJsonObject("cred_data").getString("user"))
              .put("password", cred.getJsonObject("cred_data").getString("password"))
              .put("port", port)
              .put("credential_profile_id", credentialProfileId)
            );
          }
        }
      }

      logger.info("Plugin input: {}", pluginInput.encodePrettily());

      var pluginResults = runSSHPlugin(pluginInput);

      for (var i = 0; i < reachResults.size(); i++)
      {
        var obj = reachResults.getJsonObject(i);

        var ip = obj.getString("ip");

        var up = obj.getBoolean("reachable");

        var open = obj.getBoolean("port_open");

        Long successfulCredId = null;

        String uname = null;

        String errorMsg = up ? (open ? "SSH connection failed" : "Port closed") : "Device unreachable";

        if (up && open)
        {
          for (int j = 0; j < pluginResults.size(); j++)
          {
            var res = pluginResults.getJsonObject(j);

            var resIp = res.getString("ip.address");

            var status = res.getString("status");

            if (ip.equals(resIp))
            {
              if ("success".equals(status))
              {
                reachableIps.add(ip);

                successfulCredId = res.getLong("credential_profile_id");

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
          .put("msg", errorMsg)
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

    for (int i = 0; i < discoveryResults.size(); i++)
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

    // Send batch request to EVENTBUS_BATCH_ADDRESS
    return executeBatchQuery(message)
      .compose(result ->
      {
        if (SUCCESS.equals(result.getString("msg")))
        {
          logger.info("Successfully inserted/updated {} discovery results", batchParams.size());

          return Future.succeededFuture();
        }
        else
        {
          return Future.failedFuture("Batch insert failed: " + result.getString("ERROR"));
        }
      });
  }


  public static void createProvisioningJobs(long discoveryId, JsonArray selectedIps, RoutingContext context)
  {
    if (selectedIps == null || selectedIps.isEmpty()) {
      sendError(context, 400, "No IPs provided for provisioning");
      return;
    }

    // Query to validate IPs against discovery results
    JsonObject validateQuery = new JsonObject()
      .put("query", "SELECT ip, result, credential_profile_id, port " +
        "FROM discovery_result WHERE discovery_id = $1 AND ip = ANY($2::varchar[])")
      .put("params", new JsonArray().add(discoveryId).add(selectedIps));

    logger.info(validateQuery.encodePrettily());

    executeQuery(validateQuery)
      .compose(result ->
      {
        if (!SUCCESS.equals(result.getString("msg")))
        {
          return Future.failedFuture("Failed to validate IPs");
        }

        var resultArray = result.getJsonArray("result");

        Map<String, JsonObject> discoveryResults = new HashMap<>();

        for (int i = 0; i < resultArray.size(); i++)
        {
          var row = resultArray.getJsonObject(i);

          discoveryResults.put(row.getString("ip"), row);
        }

        var validIps = new JsonArray();
        var invalidIps = new JsonArray();
        var batchParams = new JsonArray();

        for (int i = 0; i < selectedIps.size(); i++)
        {
          String ip = selectedIps.getString(i);

          JsonObject discoveryResult = discoveryResults.get(ip);

          if (discoveryResult == null)
          {
            invalidIps.add(new JsonObject().put("ip", ip).put("error", "IP not found in discovery results"));
          }
          else if (!"completed".equals(discoveryResult.getString("result")))
          {
            invalidIps.add(new JsonObject().put("ip", ip).put("error", "Discovery not completed"));
          }
          else
          {
            validIps.add(ip);

            batchParams.add(new JsonArray()
              .add(discoveryResult.getLong("credential_profile_id"))
              .add(ip)
              .add(discoveryResult.getInteger("port")));
          }
        }

        if (validIps.isEmpty())
        {
          return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
        }

        var batchQuery = new JsonObject()
          .put("query", QueryConstant.INSERT_PROVISIONING_JOB)
          .put("batchParams", batchParams);

        return executeBatchQuery(batchQuery)
          .compose(insertResult ->
          {
            if (!SUCCESS.equals(insertResult.getString("msg")))
            {
              var error = insertResult.getString("ERROR", "Batch insert failed");

              if (error.contains("provisioning_jobs_ip_unique"))
              {
                // Fallback for race conditions
                for (var i = 0; i < validIps.size(); i++)
                {
                  var ip = validIps.getString(i);

                  invalidIps.add(new JsonObject().put("ip", ip).put("error", "IP already provisioned"));
                }
                return Future.failedFuture("No valid IPs for provisioning: " + invalidIps.encodePrettily());
              }
              return Future.failedFuture(error);
            }

            var insertedIds = insertResult.getJsonArray("insertedIds");

            var metricsBatch = new JsonArray();

            String[] defaultMetrics = {"CPU", "MEMORY", "DISK"};

            var defaultInterval = 300;

            for (var i = 0; i < insertedIds.size(); i++)
            {
              var provisioningJobId = insertedIds.getLong(i);

              for (String metric : defaultMetrics)
              {
                metricsBatch.add(new JsonArray()
                  .add(provisioningJobId)
                  .add(metric)
                  .add(defaultInterval));
              }
            }

            var metricsQuery = new JsonObject()
              .put("query", QueryConstant.INSERT_DEFAULT_METRICS)
              .put("batchParams", metricsBatch);

            return executeBatchQuery(metricsQuery)
              .map(v ->
              {
                JsonArray insertedRecords = new JsonArray();

                for (var j = 0; j < validIps.size(); j++)
                {
                  var ip = validIps.getString(j);
                  insertedRecords.add(new JsonObject()
                    .put("ip", ip)
                    .put("status", "created")
                          .put("provisioning_job_id", insertedIds.getLong(j)));
                }
                return new JsonObject()
                  .put("validIps", validIps)
                  .put("invalidIps", invalidIps)
                  .put("insertedRecords", insertedRecords);
              });
          });
      })
      .onSuccess(result -> context.response()
        .setStatusCode(201)
        .putHeader("Content-Type", "application/json")
        .end(new JsonObject()
          .put("msg", "Success")
          .put("Provision_created ", result.getJsonArray("insertedRecords"))
          .put("invalid_ips", result.getJsonArray("invalidIps"))
          .encodePrettily()))
      .onFailure(err -> sendError(context, err.getMessage().contains("No valid IPs") ? 400 : 500, err.getMessage()));
  }

  private static void sendError(RoutingContext ctx, int status, String msg)
  {
    ctx.response()
      .setStatusCode(status)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("status", "failed")
        .put("error", msg)
        .encodePrettily());

  }
}

