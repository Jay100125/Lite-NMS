package com.example.NMS;

import com.example.NMS.database.Database;
import com.example.NMS.polling.PollingVerticle;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

  public static Vertx vertx = Vertx.vertx();

  public static void main(String[] args)
  {
    LOGGER.info("Starting NMS");

    vertx.deployVerticle(new ApiServer())
      .compose(res ->
      {
        LOGGER.info("HTTP server verticle deployed");

        return vertx.deployVerticle(Database.class.getName());
      })
      .compose(res ->
      {
        LOGGER.info("database verticle is deployed");

        return vertx.deployVerticle(PollingVerticle.class.getName()).onComplete(apiRes -> LOGGER.info("polling verticle deployed"));
      })
      .onComplete(handler -> {
      if (handler.succeeded())
      {
        LOGGER.info("Application started");
      }
      else
      {
        LOGGER.error("Application failed to start {}", String.valueOf(handler.cause()));
      }
    });
  }

}
