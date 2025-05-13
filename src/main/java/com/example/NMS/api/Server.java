package com.example.NMS.api;

import com.example.NMS.api.handlers.Auth;
import com.example.NMS.api.handlers.Credential;
import com.example.NMS.api.handlers.Discovery;
import com.example.NMS.api.handlers.Provision;
import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.handler.BodyHandler;
import io.vertx.ext.web.handler.JWTAuthHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.JWT_SECRET;


public class Server extends AbstractVerticle
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  @Override
  public void start(Promise<Void> startPromise)
  {

    var jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
                                    .addPubSecKey(new PubSecKeyOptions()
                                                        .setAlgorithm("HS256")
                                                       .setBuffer(JWT_SECRET)));


    var router = Router.router(vertx);

    var authRoute = Router.router(vertx);

    var discoveryRoute = Router.router(vertx);

    var credentialRoute = Router.router(vertx);

    var provisionRoute = Router.router(vertx);

    router.route("/api/*").handler(BodyHandler.create());

    router.route("/api/*").handler(ctx ->
    {
      String path = ctx.normalizedPath();
      if (path.endsWith("/register") || path.endsWith("/login"))
      {
        ctx.next();
      }
      else
      {
        JWTAuthHandler.create(jwtAuth).handle(ctx);
      }
    });

    router.route().subRouter(authRoute);

    router.route().subRouter(credentialRoute);

    router.route().subRouter(discoveryRoute);

    router.route().subRouter(provisionRoute);

    new Auth(jwtAuth).init(authRoute);

    new Credential().init(credentialRoute);

    new Discovery().init(discoveryRoute);

    new Provision().init(provisionRoute);

    router.errorHandler(401, ctx -> {
      ctx.response()
        .setStatusCode(401)
        .putHeader("Content-Type", "application/json")
        .end(new JsonObject()
          .put("error", "Unauthorized")
          .put("message", "Invalid or missing JWT token")
          .encode());
    });

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
