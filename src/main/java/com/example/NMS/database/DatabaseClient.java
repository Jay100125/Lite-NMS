//package com.example.NMS.database;
//
//import io.vertx.core.Vertx;
//import io.vertx.pgclient.PgBuilder;
//import io.vertx.pgclient.PgConnectOptions;
//import io.vertx.sqlclient.PoolOptions;
//import io.vertx.sqlclient.SqlClient;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import static com.example.NMS.constant.Constant.*;
//
//public class DatabaseClient
//{
//  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);
//
//  private static SqlClient client;
//
//  public static synchronized SqlClient getClient(Vertx vertx)
//  {
//    if (client == null)
//    {
//      LOGGER.info("Initializing database connection pool...");
//
//      var connectOptions = new PgConnectOptions()
//        .setHost(DB_HOST)
//        .setPort(DB_PORT)
//        .setDatabase(DB_NAME)
//        .setUser(DB_USER)
//        .setPassword(DB_PASSWORD)
//        .setReconnectAttempts(5)
//        .setReconnectInterval(1000);
//
//      PoolOptions poolOptions = new PoolOptions()
//        .setMaxSize(10)
//        .setIdleTimeout(30) // seconds
//        .setConnectionTimeout(10); // seconds
//
//      client = PgBuilder.client()
//        .with(poolOptions)
//        .connectingTo(connectOptions)
//        .using(vertx)
//        .build();
//
//      LOGGER.info("Database connection pool initialized");
//    }
//    return client;
//  }
//
//  public static synchronized void close() {
//    if (client != null) {
//      client.close();
//      client = null;
//      LOGGER.info("Database connection pool closed");
//    }
//  }
//}
package com.example.NMS.database;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgBuilder;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.sqlclient.PoolOptions;
import io.vertx.sqlclient.SqlClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.Main.vertx;
import static com.example.NMS.constant.Constant.*;

public class DatabaseClient
{
  private static final Logger LOGGER = LoggerFactory.getLogger(DatabaseClient.class);

  // Private constructor to prevent instantiation
  private DatabaseClient()
  {
    throw new AssertionError("Utility class, not meant to be instantiated");
  }

  // Static inner class for lazy initialization
  private static class ClientHolder
  {
    private static final SqlClient CLIENT;

    static
    {
      LOGGER.info("Initializing database connection pool...");

      var connectOptions = new PgConnectOptions()
        .setHost(DB_HOST)
        .setPort(DB_PORT)
        .setDatabase(DB_NAME)
        .setUser(DB_USER)
        .setPassword(DB_PASSWORD)
        .setReconnectAttempts(5)
        .setReconnectInterval(1000);

      var poolOptions = new PoolOptions()
        .setMaxSize(10)
        .setIdleTimeout(30) // seconds
        .setConnectionTimeout(10); // seconds

     // Vertx instance is passed at runtime via getClient
      CLIENT = PgBuilder.client()
        .with(poolOptions)
        .connectingTo(connectOptions)
        .using(vertx)
        .build();

      LOGGER.info("Database connection pool initialized");
    }
  }

  public static SqlClient getClient(Vertx vertx)
  {
    if (vertx == null) {
      LOGGER.error("Vertx instance is null");
      throw new IllegalArgumentException("Vertx instance cannot be null");
    }
    return ClientHolder.CLIENT;
  }

  public static void close()
  {
    var client = ClientHolder.CLIENT;

    if (client != null) {
      client.close();
      LOGGER.info("Database connection pool closed");
    }
  }
}
