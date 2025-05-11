//package com.example.NMS.api;
//
//import com.example.NMS.service.UserService;
//import io.vertx.core.json.JsonObject;
//import io.vertx.ext.auth.jwt.JWTAuth;
//import io.vertx.ext.web.Router;
//import io.vertx.ext.web.RoutingContext;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//public class Auth
//{
//  private static final Logger LOGGER = LoggerFactory.getLogger(Auth.class);
//
//  private final JWTAuth jwtAuth;
//
//  public Auth(JWTAuth jwtAuth) {
//    this.jwtAuth = jwtAuth;
//  }
//
//  public void init(Router router) {
//    router.post("/register").handler(this::register);
//    router.post("/login").handler(this::login);
//  }
//
//  private void register(RoutingContext ctx)
//  {
//    var body = ctx.body().asJsonObject();
//
//    if (body == null) {
//      sendError(ctx, 400, "Missing request body");
//      return;
//    }
//
//    var username = body.getString("username");
//
//    var password = body.getString("password");
//
//    UserService.register(username, password)
//      .onSuccess(userId -> ctx.response()
//        .setStatusCode(201)
//        .putHeader("Content-Type", "application/json")
//        .end(new JsonObject()
//          .put("msg", "Success")
//          .put("user_id", userId)
//          .encodePrettily()))
//      .onFailure(err -> {
//        var status = err.getMessage().contains("Username already exists") ? 409 : 400;
//        sendError(ctx, status, err.getMessage());
//      });
//  }
//
//  private void login(RoutingContext ctx)
//  {
//    var body = ctx.body().asJsonObject();
//
//    if (body == null) {
//      sendError(ctx, 400, "Missing request body");
//      return;
//    }
//
//    var username = body.getString("username");
//
//    var password = body.getString("password");
//
//    UserService.login(username, password, jwtAuth)
//      .onSuccess(token -> ctx.response()
//        .setStatusCode(200)
//        .putHeader("Content-Type", "application/json")
//        .end(new JsonObject()
//          .put("msg", "Success")
//          .put("token", token)
//          .encodePrettily()))
//      .onFailure(err -> sendError(ctx, 401, err.getMessage()));
//  }
//
//  private void sendError(RoutingContext ctx, int statusCode, String errorMessage)
//  {
//    LOGGER.info(errorMessage);
//
//    ctx.response()
//      .setStatusCode(statusCode)
//      .putHeader("Content-Type", "application/json")
//      .end(new JsonObject()
//        .put(statusCode == 400 || statusCode == 409 ? "msg" : "status", "failed")
//        .put("error", errorMessage)
//        .encode());
//  }
//}

package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.SUCCESS;
import static com.example.NMS.service.QueryProcessor.executeQuery;

public class Auth {
  private static final Logger LOGGER = LoggerFactory.getLogger(Auth.class);
  private final JWTAuth jwtAuth;
  private final JsonObject query = new JsonObject();
  private final JsonArray params = new JsonArray();
  private final JsonObject response = new JsonObject();

  public Auth(JWTAuth jwtAuth) {
    this.jwtAuth = jwtAuth;
  }

  public void init(Router router) {
    router.post("/api/register").handler(this::register);
    router.post("/api/login").handler(this::login);
  }

  private void register(RoutingContext ctx) {
    var body = ctx.body().asJsonObject();
    if (body == null) {
      sendError(ctx, 400, "Missing request body");
      return;
    }

    var username = body.getString("username");
    var password = body.getString("password");

    registerUser(username, password)
      .onSuccess(userId -> {
        response.clear();
        ctx.response()
          .setStatusCode(201)
          .putHeader("Content-Type", "application/json")
          .end(response
            .put("msg", "Success")
            .put("user_id", userId)
            .encodePrettily());
      })
      .onFailure(err -> {
        var status = err.getMessage().contains("Username already exists") ? 409 : 400;
        sendError(ctx, status, err.getMessage());
      });
  }

  private Future<Long> registerUser(String username, String password) {
    if (username == null || username.trim().isEmpty() || password == null || password.length() < 8) {
      return Future.failedFuture("Invalid username or password (minimum 8 characters)");
    }

    String hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt());

    query.clear();
    params.clear();
    query.put("query", QueryConstant.REGISTER_USER);
    query.put("params", params.add(username).add(hashedPassword));

    return executeQuery(query)
      .compose(result -> {
        if (SUCCESS.equals(result.getString("msg"))) {
          Long userId = result.getLong("insertedId");
          LOGGER.info("User registered: {}", username);
          return Future.succeededFuture(userId);
        } else {
          String error = result.getString("ERROR", "Failed to register user");
          if (error.contains("users_username_key")) {
            return Future.failedFuture("Username already exists");
          }
          return Future.failedFuture(error);
        }
      });
  }

  private void login(RoutingContext ctx) {
    var body = ctx.body().asJsonObject();
    if (body == null) {
      sendError(ctx, 400, "Missing request body");
      return;
    }

    var username = body.getString("username");
    var password = body.getString("password");

    loginUser(username, password)
      .onSuccess(token -> {
        response.clear();
        ctx.response()
          .setStatusCode(200)
          .putHeader("Content-Type", "application/json")
          .end(response
            .put("msg", "Success")
            .put("token", token)
            .encodePrettily());
      })
      .onFailure(err -> sendError(ctx, 401, err.getMessage()));
  }

  private Future<String> loginUser(String username, String password) {
    if (username == null || password == null) {
      return Future.failedFuture("Username and password are required");
    }

    query.clear();
    params.clear();
    query.put("query", QueryConstant.GET_USER_BY_USERNAME);
    query.put("params", params.add(username));

    return executeQuery(query)
      .compose(result -> {
        if (!SUCCESS.equals(result.getString("msg")) || result.getJsonArray("result").isEmpty()) {
          return Future.failedFuture("Invalid username or password");
        }

        var user = result.getJsonArray("result").getJsonObject(0);
        var storedHash = user.getString("password");

        if (!BCrypt.checkpw(password, storedHash)) {
          LOGGER.warn("Failed login attempt for username: {}", username);
          return Future.failedFuture("Invalid username or password");
        }

        // Set token expiration to 60 minutes
        long currentTimeSeconds = System.currentTimeMillis() / 1000;
        long expiryTimeSeconds = currentTimeSeconds + 60 * 60; // 60 minutes
        var claims = new JsonObject()
          .put("sub", username)
          .put("exp", expiryTimeSeconds);

        var token = jwtAuth.generateToken(claims);

        LOGGER.info("User logged in: {}", username);
        return Future.succeededFuture(token);
      });
  }

  private void sendError(RoutingContext ctx, int statusCode, String errorMessage) {
    LOGGER.info(errorMessage);
    response.clear();
    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(response
        .put(statusCode == 400 || statusCode == 409 ? "msg" : "status", "failed")
        .put("error", errorMessage)
        .encode());
  }
}
