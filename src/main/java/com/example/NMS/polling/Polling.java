//package com.example.NMS.polling;
//
//import io.vertx.core.AbstractVerticle;
//import io.vertx.core.json.JsonArray;
//import io.vertx.core.json.JsonObject;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static com.example.NMS.Main.vertx;
//import static com.example.NMS.utility.Utility.runSSHPlugin;
//
//public class Polling extends AbstractVerticle {
//  public static final Logger logger = LoggerFactory.getLogger(Polling.class);
//
//  private static final long POLLING_INTERVAL = 10000; // 5 minutes in milliseconds
//
//  @Override
//  public void start() {
//    vertx.setPeriodic(POLLING_INTERVAL, timerId -> {
//      logger.info("Starting polling cycle");
//      pollDevices();
//    });
//  }
//
//  private void pollDevices() {
//    // Query to fetch all provisioned jobs
//    String queryStr = "SELECT pj.id, pj.ip, pj.port, cp.cred_data, cp.system_type " +
//      "FROM provisioning_jobs pj " +
//      "JOIN credential_profile cp ON pj.credential_profile_id = cp.id";
//    JsonObject query = new JsonObject()
//      .put("query", queryStr);
//
//    vertx.eventBus().request("database", query, ar -> {
//      if (ar.succeeded()) {
//        JsonObject result = (JsonObject) ar.result().body();
//        JsonArray resultArray = result.getJsonArray("result");
//
//        if (result.getString("msg").equals("Success")) {
//          for (int i = 0; i < resultArray.size(); i++) {
//            JsonObject job = resultArray.getJsonObject(i);
//            long jobId = job.getLong("id");
//            String ip = job.getString("ip");
//            int port = job.getInteger("port");
//            JsonObject credData = job.getJsonObject("cred_data");
//            String systemType = job.getString("system_type");
//
//
//            String username = credData.getString("user");
//            String password = credData.getString("password");
//
//            // Prepare plugin input for polling uptime
//            JsonObject pluginInput = new JsonObject()
//              .put("category", "polling")
//              .put("metric_type", "uptime")
//              .put("ip.address", ip)
//              .put("port", port)
//              .put("user", "jay")
//              .put("password", password)
//              .put("dis.id", jobId);
//
//            // Run SSH plugin
//            JsonObject pluginResult = runSSHPlugin(pluginInput);
//            if ("success".equals(pluginResult.getString("status"))) {
//              String data = pluginResult.getString("data", "");
//              JsonObject metricData = new JsonObject().put("uptime", data.trim());
//              logger.info("Uptime data for job {}: {}", jobId, metricData);
//              // Insert into polled_data
//              String insertQueryStr = "INSERT INTO polled_data (job_id, metric_type, data, polled_at) " +
//                "VALUES ($1, $2, $3, to_timestamp($4))";
//              JsonObject insertQuery = new JsonObject()
//                .put("query", insertQueryStr)
//                .put("params", new JsonArray()
//                  .add(jobId)
//                  .add("uptime")
//                  .add(metricData)
//                  .add(System.currentTimeMillis() / 1000L)); // Unix timestamp in seconds
//
//              vertx.eventBus().send("database", insertQuery);
//            } else {
//              logger.error("Failed to poll uptime for job {}: {}", jobId, pluginResult.getString("error", "Unknown error"));
//            }
//          }
//        } else {
//          logger.error("Failed to fetch provisioned jobs: {}", result.getString("ERROR", "Unknown error"));
//        }
//      } else {
//        logger.error("Database query failed: {}", ar.cause().getMessage());
//      }
//    });
//  }
//}
