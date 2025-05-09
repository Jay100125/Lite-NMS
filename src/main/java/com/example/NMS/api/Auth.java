package com.example.NMS.api;

import com.example.NMS.service.UserService;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Auth
{
  private static final Logger logger = LoggerFactory.getLogger(Auth.class);

  private final JWTAuth jwtAuth;

  public Auth(JWTAuth jwtAuth) {
    this.jwtAuth = jwtAuth;
  }

  public void init(Router router) {
    router.post("/register").handler(this::handleRegister);
    router.post("/login").handler(this::handleLogin);
  }

  private void handleRegister(RoutingContext ctx)
  {
    var body = ctx.body().asJsonObject();

    if (body == null) {
      sendError(ctx, 400, "Missing request body");
      return;
    }

    var username = body.getString("username");

    var password = body.getString("password");

    UserService.register(username, password)
      .onSuccess(userId -> ctx.response()
        .setStatusCode(201)
        .putHeader("Content-Type", "application/json")
        .end(new JsonObject()
          .put("msg", "Success")
          .put("user_id", userId)
          .encodePrettily()))
      .onFailure(err -> {
        var status = err.getMessage().contains("Username already exists") ? 409 : 400;
        sendError(ctx, status, err.getMessage());
      });
  }

  private void handleLogin(RoutingContext ctx)
  {
    var body = ctx.body().asJsonObject();

    if (body == null) {
      sendError(ctx, 400, "Missing request body");
      return;
    }

    var username = body.getString("username");

    var password = body.getString("password");

    UserService.login(username, password, jwtAuth)
      .onSuccess(token -> ctx.response()
        .setStatusCode(200)
        .putHeader("Content-Type", "application/json")
        .end(new JsonObject()
          .put("msg", "Success")
          .put("token", token)
          .encodePrettily()))
      .onFailure(err -> sendError(ctx, 401, err.getMessage()));
  }

  private void sendError(RoutingContext ctx, int statusCode, String errorMessage)
  {
    logger.info(errorMessage);

    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(new JsonObject()
        .put(statusCode == 400 || statusCode == 409 ? "msg" : "status", "failed")
        .put("error", errorMessage)
        .encode());
  }
}
