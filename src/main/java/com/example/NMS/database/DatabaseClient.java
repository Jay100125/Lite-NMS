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

  private static DatabaseClient instance;

  private static SqlClient client;

  // Private constructor to prevent instantiation
  private DatabaseClient(Vertx vertx)
  {
    if (client == null)
    {
      LOGGER.info("Initializing database connection pool...");

      var connectOptions = new PgConnectOptions()
        .setHost(DB_HOST)
        .setPort(DB_PORT)
        .setDatabase(DB_NAME)
        .setUser(DB_USER)
        .setPassword(DB_PASSWORD);

      var poolOptions = new PoolOptions()
        .setMaxSize(10)
        .setIdleTimeout(30);

      client = PgBuilder.client()
        .with(poolOptions)
        .connectingTo(connectOptions)
        .using(vertx)
        .build();
    }
  }

  public static DatabaseClient getInstance(Vertx vertx)
  {
    if (instance == null)
    {
      instance = new DatabaseClient(vertx);
    }
    return instance;
  }

  public SqlClient getClient()
  {

    return client;
  }

  public void close()
  {
    if (client != null)
    {
      client.close();

      client = null;

      instance = null;

      LOGGER.info("Database connection pool closed");
    }
  }
}
