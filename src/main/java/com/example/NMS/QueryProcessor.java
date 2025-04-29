package com.example.NMS;

import com.example.NMS.constant.Constant;
import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.Promise;
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

  public static void executeQuery(JsonObject query, RoutingContext context)
  {
    vertx.eventBus().request(Constant.EVENTBUS_ADDRESS, query, ar -> {

      if (ar.succeeded())
      {
        logger.info("Database query executed" + query);

        var result = (JsonObject) ar.result().body();

        var resultArray = result.getJsonArray("result");

        logger.info("Database query result: {}", result);

        if (Objects.equals(result.getString("msg"), Constant.SUCCESS))
        {
          var queryString = query.getString("query").toLowerCase();

          var isGetOrDelete = queryString.contains("select") || queryString.contains("delete");

          if (isGetOrDelete && resultArray.isEmpty())
          {
            context.response()
              .setStatusCode(404)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put("status", "failed")
                .put("error", "Resource not found")
                .encodePrettily());
            return;
          }
          context.response()
            .setStatusCode(201)
            .end(result.encodePrettily());
        }
        else
        {
          context.response()
            .setStatusCode(500)
            .end(new JsonObject()
              .put("ERROR", result.getString("ERROR"))
              .encodePrettily());
        }
      }
      else
      {
        context.response()
          .setStatusCode(500)
          .end(new JsonObject()
            .put("ERROR", "DBService failed: " + ar.cause().getMessage())
            .encodePrettily());
      }
    });
  }

  public static void runDiscovery(long id, RoutingContext context)
  {
    String fetchSql = QueryConstant.RUN_DISCOVERY;

    JsonObject fetchQuery = new JsonObject()
      .put("query", fetchSql)
      .put("params", new JsonArray().add(id));

    vertx.eventBus().request(EVENTBUS_ADDRESS, fetchQuery, ar -> {
      if (ar.failed())
      {
        sendError(context, 500, "Database query failed: " + ar.cause().getMessage());

        return;
      }

      JsonObject body = (JsonObject) ar.result().body();

      JsonArray rows = body.getJsonArray("result");

      if (!SUCCESS.equals(body.getString("msg")) || rows.isEmpty())
      {
        sendError(context, 404, "Discovery profile not found");

        return;
      }

      String ipInput = rows.getJsonObject(0).getString("ip");

      int port = rows.getJsonObject(0).getInteger("port");

      logger.info("Discovery profile: {}", ipInput);
      logger.info("Discovery profile rows: {}", rows);

//      JsonArray credentials = new JsonArray();
//
//      for (int i = 0; i < rows.size(); i++) {
//        JsonObject row = rows.getJsonObject(i);
//
//        JsonArray credentialProfileIds = row.getJsonArray("credential_profile_id");
//        JsonObject credData = row.getJsonObject("cred_data");
//
//        for (int j = 0; j < credentialProfileIds.size(); j++) {
//          Long credentialProfileId = credentialProfileIds.getLong(j);
//          credentials.add(new JsonObject()
//            .put("credential_profile_id", credentialProfileId)
//            .put("cred_data", credData)
//          );
//        }
//      }
      JsonArray credentials = new JsonArray();
      Map<Long, JsonObject> credDataMap = new HashMap<>();

      // Build a map of credential_profile_id to cred_data
      for (int i = 0; i < rows.size(); i++) {
        JsonObject row = rows.getJsonObject(i);
        Long credentialProfileId = row.getLong("cpid"); // Use the unnested cpid from query
        JsonObject credData = row.getJsonObject("cred_data");
        credDataMap.put(credentialProfileId, credData);
      }

    // Create credentials array with unique credential_profile_id and corresponding cred_data
      for (Long credentialProfileId : credDataMap.keySet()) {
        credentials.add(new JsonObject()
          .put("credential_profile_id", credentialProfileId)
          .put("cred_data", credDataMap.get(credentialProfileId))
        );
      }



      // Compose blocking tasks using Callable-based executeBlocking
      resolveIps(ipInput)
        .compose(ips -> {
//          if (ips.size() > 256) {
//            return Future.failedFuture("Too many IPs to process: " + ips.size());
//          }
          return checkReach(ips, port);
        })
        .compose(reachResults -> doSSH(reachResults, credentials, id, port))
        .compose(sshResult -> {
          JsonArray reachabilityResults = sshResult.getJsonArray("reachabilityResults");
          JsonArray discoveryResults = sshResult.getJsonArray("discoveryResults");
          return storeDiscoveryResults(discoveryResults, id)
            .map(reachabilityResults);
        })
        .onSuccess(results -> context.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put("msg", "Success")
            .put("results", results)
            .encodePrettily()))
        .onFailure(err -> {
          logger.error("Discovery flow failed", err);
          sendError(context, 500, err.getMessage());
        });
    });
  }

  private static Future<List<String>> resolveIps(String ipInput) {
    // Callable-based executeBlocking returns a Future<List<String>>
    return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
  }

  private static Future<JsonArray> checkReach(List<String> ips, int port) {
    return vertx.executeBlocking(() -> checkReachability(ips, port), false);
  }

  private static Future<JsonObject> doSSH(JsonArray reachResults,
                                          JsonArray credentials,
                                          long discoveryId,
                                          int port) {
    return vertx.executeBlocking(() -> {
      JsonArray reachableIps = new JsonArray();
      JsonArray discoveryResults = new JsonArray();
      JsonObject pluginInput = new JsonObject()
        .put("category", "discovery")
        .put("metric.type", "linux")
        .put("port", port)
        .put("dis.id", discoveryId)
        .put("targets", new JsonArray());


//      for (int i = 0; i < reachResults.size(); i++) {
//        JsonObject obj = reachResults.getJsonObject(i);
//        boolean up = obj.getBoolean("reachable");
//        boolean open = obj.getBoolean("port_open");
//        if (up && open) {
//          String ip = obj.getString("ip");
//          for (int j = 0; j < credentials.size(); j++) {
//            JsonObject cred = credentials.getJsonObject(j);
//            pluginInput.getJsonArray("targets").add(new JsonObject()
//              .put("ip.address", ip)
//              .put("user", cred.getJsonObject("cred_data").getString("user"))
//              .put("password", cred.getJsonObject("cred_data").getString("password"))
//              .put("credential_profile_id", cred.getLong("credential_profile_id")));
//          }
//        }
//      }

      logger.info(reachResults.encodePrettily());
      for (int i = 0; i < reachResults.size(); i++) {
        JsonObject obj = reachResults.getJsonObject(i);
        boolean up = obj.getBoolean("reachable");
        boolean open = obj.getBoolean("port_open");
        if (up && open) {
          String ip = obj.getString("ip");
          logger.info(credentials.encodePrettily());
          for (int j = 0; j < credentials.size(); j++) {
            JsonObject cred = credentials.getJsonObject(j);
            Long credentialProfileId = cred.getLong("credential_profile_id");

            // Only add if this credential is valid for this IP
            pluginInput.getJsonArray("targets").add(new JsonObject()
              .put("ip.address", ip)
              .put("user", cred.getJsonObject("cred_data").getString("user"))
              .put("password", cred.getJsonObject("cred_data").getString("password"))
              .put("credential_profile_id", credentialProfileId)
            );
          }
        }
      }

      JsonArray pluginResults = runSSHPlugin(pluginInput);

      for (int i = 0; i < reachResults.size(); i++) {
        JsonObject obj = reachResults.getJsonObject(i);
        String ip = obj.getString("ip");
        boolean up = obj.getBoolean("reachable");
        boolean open = obj.getBoolean("port_open");
        Long successfulCredId = null;
        String uname = null;
        String errorMsg = up ? (open ? "SSH connection failed" : "Port closed") : "Device unreachable";

        if (up && open) {
          for (int j = 0; j < pluginResults.size(); j++) {
            JsonObject res = pluginResults.getJsonObject(j);
            String resIp = res.getString("ip.address");
            String status = res.getString("status");

            if (ip.equals(resIp)) {
              if ("success".equals(status)) {
                reachableIps.add(ip);
                successfulCredId = res.getLong("credential_profile_id");
                uname = res.getString("uname");
                errorMsg = null;
                break;
              } else {
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

  private static Future<Void> storeDiscoveryResults(JsonArray discoveryResults, long discoveryId) {
    Promise<Void> promise = Promise.promise();
    JsonArray batchParams = new JsonArray();
    for (int i = 0; i < discoveryResults.size(); i++) {
      JsonObject result = discoveryResults.getJsonObject(i);
      batchParams.add(new JsonArray()
        .add(discoveryId)
        .add(result.getString("ip"))
        .add(result.getInteger("port"))
        .add(result.getString("result"))
        .add(result.getString("msg")) // Nullable
        .add(result.getValue("credential_profile_id"))); // Nullable
    }

    // Send batch request to EVENTBUS_BATCH_ADDRESS
    JsonObject message = new JsonObject()
      .put("query", QueryConstant.INSERT_DISCOVERY_RESULT)
      .put("batchParams", batchParams);

    vertx.eventBus().request(EVENTBUS_BATCH_ADDRESS, message, ar -> {
      if (ar.succeeded()) {
        JsonObject result = (JsonObject) ar.result().body();
        if ("Success".equals(result.getString("msg"))) {
          logger.info("Successfully inserted {} discovery results", batchParams.size());
          promise.complete();
        } else {
          logger.error("Batch insert failed: {}", result.getString("ERROR"));
          promise.fail("Batch insert failed: " + result.getString("ERROR"));
        }
      } else {
        logger.error("Batch insert request failed: {}", ar.cause().getMessage());
        promise.fail("Batch insert request failed: " + ar.cause().getMessage());
      }
    });

    return promise.future();
  }


  private static void sendError(RoutingContext ctx, int status, String msg) {
    ctx.response()
      .setStatusCode(status)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put("status", "failed")
        .put("error", msg)
        .encodePrettily());
  }


  public static void createProvisioningJobs(long discoveryId, JsonArray selectedIps, RoutingContext context)
  {
    var queryStr = "SELECT reachable_ip, credential_profile_id, port " +
      "FROM discovery_profiles WHERE id = $1";
    var query = new JsonObject()
      .put("query", queryStr)
      .put("params", new JsonArray().add(discoveryId));

    vertx.eventBus().request("database", query, ar -> {
      if (ar.succeeded())
      {
        JsonObject result = (JsonObject) ar.result().body();

        JsonArray resultArray = result.getJsonArray("result");

        if (!result.getString("msg").equals("Success") || resultArray.isEmpty())
        {
          context.response()
            .setStatusCode(404)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
              .put("status", "failed")
              .put("error", "Discovery profile not found")
              .encodePrettily());
          return;
        }

        var row = resultArray.getJsonObject(0);
        var reachableIps = row.getJsonArray("reachable_ip");
        var credentialProfileId = row.getLong("credential_profile_id");
        var port = row.getInteger("port");

        // Validate selected IPs against reachable_ip
        if (reachableIps == null || reachableIps.isEmpty())
        {
          context.response()
            .setStatusCode(400)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
              .put("status", "failed")
              .put("error", "No reachable IPs in discovery profile")
              .encodePrettily());
          return;
        }

        List<String> reachableIpList = new ArrayList<>();
        for (var i = 0; i < reachableIps.size(); i++)
        {
          reachableIpList.add(reachableIps.getString(i));
        }

        var validIps = new JsonArray();

        var invalidIps = new JsonArray();
        for (int i = 0; i < selectedIps.size(); i++)
        {
          var ip = selectedIps.getString(i);

          if (reachableIpList.contains(ip))
          {
            validIps.add(ip);
          }
          else
          {
            invalidIps.add(ip);
          }
        }

        if (validIps.isEmpty())
        {
          context.response()
            .setStatusCode(400)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
              .put("status", "failed")
              .put("error", "None of the selected IPs are in reachable_ip: " + invalidIps.encode())
              .encodePrettily());
          return;
        }

        if (!invalidIps.isEmpty())
        {
          logger.warn("Some selected IPs are not in reachable_ip: {}", invalidIps.encode());
        }

        var insertQueryStr = "INSERT INTO provisioning_jobs (credential_profile_id, ip, port) " +
          "VALUES ($1, $2, $3) RETURNING id";

        var insertQueries = new JsonArray();

        for (var i = 0; i < validIps.size(); i++)
        {
          var ip = validIps.getString(i);

          insertQueries.add(new JsonObject()
            .put("query", insertQueryStr)
            .put("params", new JsonArray()
              .add(credentialProfileId)
              .add(ip)
              .add(port)));
        }

        // Execute batch insert
        vertx.eventBus().request("database", new JsonObject().put("queries", insertQueries), insertAr -> {
          if (insertAr.succeeded())
          {
            var insertResult = (JsonObject) insertAr.result().body();

            var insertedIds = insertResult.getJsonArray("results", new JsonArray());

            var insertedRecords = new JsonArray();

            for (var i = 0; i < validIps.size(); i++)
            {
              insertedRecords.add(new JsonObject()
                .put("ip", validIps.getString(i))
                .put("status", "created"));
            }

            context.response()
              .setStatusCode(201)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put("msg", "Success")
                .put("inserted", insertedRecords)
                .put("invalid_ips", invalidIps)
                .encodePrettily());
          }
          else
          {
            logger.error("Failed to insert provisioning jobs: {}", insertAr.cause().getMessage());

            context.response()
              .setStatusCode(500)
              .putHeader("Content-Type", "application/json")
              .end(new JsonObject()
                .put("status", "failed")
                .put("error", "Failed to create provisioning jobs: " + insertAr.cause().getMessage())
                .encodePrettily());
          }
        });
      }
      else
      {
        logger.error("Database query failed: {}", ar.cause().getMessage());

        context.response()
          .setStatusCode(500)
          .putHeader("Content-Type", "application/json")
          .end(new JsonObject()
            .put("status", "failed")
            .put("error", "Database query failed: " + ar.cause().getMessage())
            .encodePrettily());
      }
    });
  }
}


