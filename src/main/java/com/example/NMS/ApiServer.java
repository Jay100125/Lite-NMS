package com.example.NMS;

import com.example.NMS.api.Credential;
import com.example.NMS.api.Discovery;
import com.example.NMS.api.Provision;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ApiServer extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(ApiServer.class);

  @Override
  public void start(Promise<Void> startPromise)
  {
    var router = Router.router(vertx);

    var discoveryRoute = Router.router(vertx);

    var credentialRoute = Router.router(vertx);

    var provisionRoute = Router.router(vertx);

    router.route("/api/*").handler(BodyHandler.create());

    router.route().subRouter(credentialRoute);

    router.route().subRouter(discoveryRoute);

    router.route().subRouter(provisionRoute);

    new Credential().init(credentialRoute);

    new Discovery().init(discoveryRoute);

    new Provision().init(provisionRoute);

    vertx.createHttpServer().requestHandler(router)
      .listen(8080)
      .onComplete(handler ->
      {
        if (handler.succeeded())
        {
          LOGGER.info("Server Created on port 8080");

          startPromise.complete();
        }
        else
        {
          LOGGER.error("Server Failed");

          startPromise.fail(handler.cause());
        }
      });
  }
}
