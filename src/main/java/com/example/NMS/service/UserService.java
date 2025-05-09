package com.example.NMS.service;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.Future;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.SUCCESS;
import static com.example.NMS.service.QueryProcessor.*;

public class UserService
{
  private static final Logger logger = LoggerFactory.getLogger(UserService.class);

  /**
   * Register a new user with username, password.
   *
   * @param username The username
   * @param password The plain-text password
   * @return Future containing the user ID
   */
  public static Future<Long> register(String username, String password)
  {
    if (username == null || username.trim().isEmpty() || password == null || password.length() < 8)
    {
      return Future.failedFuture("Invalid username or password (minimum 8 characters)");
    }

    String hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt());

    JsonObject query = new JsonObject()
      .put("query", QueryConstant.REGISTER_USER)
      .put("params", new JsonArray()
        .add(username)
        .add(hashedPassword));

    return executeQuery(query)
      .compose(result -> {
        if (SUCCESS.equals(result.getString("msg"))) {
          Long userId = result.getLong("insertedId");
          logger.info("User registered: {}", username);
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

  /**
   * Authenticate a user and generate a JWT token.
   *
   * @param username The username
   * @param password The plain-text password
   * @param jwtAuth  The JWTAuth provider
   * @return Future containing the JWT token
   */
  public static Future<String> login(String username, String password, JWTAuth jwtAuth) {
    if (username == null || password == null) {
      return Future.failedFuture("Username and password are required");
    }

    var query = new JsonObject()
      .put("query", QueryConstant.GET_USER_BY_USERNAME)
      .put("params", new JsonArray().add(username));

    return executeQuery(query)
      .compose(result -> {
        if (!SUCCESS.equals(result.getString("msg")) || result.getJsonArray("result").isEmpty())
        {
          return Future.failedFuture("Invalid username or password");
        }

        var user = result.getJsonArray("result").getJsonObject(0);

        var storedHash = user.getString("password");

        if (!BCrypt.checkpw(password, storedHash))
        {
          return Future.failedFuture("Invalid username or password");
        }

        // Generate JWT token
        var claims = new JsonObject()
          .put("sub", username);

        var token = jwtAuth.generateToken(claims);

        logger.info("User logged in: {}", username);

        return Future.succeededFuture(token);
      });
  }
}
