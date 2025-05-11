package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.service.QueryProcessor.executeQuery;


/**
 * Handles user authentication for Lite NMS, including registration and login with JWT token generation.
 */
public class Auth
{
  private static final Logger LOGGER = LoggerFactory.getLogger(Auth.class);

  private final JWTAuth jwtAuth;

  // Class member JsonObjects, similar to Credential.java
  // These should be cleared before use in each handler to prevent state leakage if the Auth instance is reused unexpectedly.
  private final JsonObject query = new JsonObject();

  private final JsonArray params = new JsonArray();

  private final JsonObject response = new JsonObject();

  /**
   * Constructor initializing the JWTAuth provider.
   *
   * @param jwtAuth The JWT authentication provider for token generation.
   */
  public Auth(JWTAuth jwtAuth)
  {
    this.jwtAuth = jwtAuth;
  }

  public void init(Router router)
  {
    router.post("/api/register").handler(this::register);

    router.post("/api/login").handler(this::login);
  }


  /**
   * Handles user registration by validating input, hashing the password, and storing the user in the database.
   *
   * @param ctx The routing context containing the HTTP request.
   */

  private void register(RoutingContext ctx)
  {
    // Clear shared JsonObjects at the beginning of the handler
    query.clear();
    params.clear();
    response.clear();

    try
    {
      var body = ctx.body().asJsonObject();

      if (body == null)
      {
        sendError(ctx, 400, "Missing request body");

        return;
      }

      var username = body.getString("username");

      var password = body.getString("password");

      if (username == null || username.trim().isEmpty() || password == null || password.length() < 8)
      {
        sendError(ctx, 400, "Invalid username or password (minimum 8 characters for password)");

        return;
      }

      // Hash the password using BCrypt for secure storage.
      var hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt());

      // Prepare query to register the user.
      query.put(QUERY, QueryConstant.REGISTER_USER);

      params.add(username).add(hashedPassword);

      query.put(PARAMS, params);

      executeQuery(query)
        .onSuccess(dbResult ->
        {
          if (SUCCESS.equals(dbResult.getString(MSG)))
          {
            var resultArray = dbResult.getJsonArray("result");

            if (resultArray != null && !resultArray.isEmpty())
            {
              var userId = resultArray.getJsonObject(0).getLong(ID);

              LOGGER.info("User registered: {} with ID: {}", username, userId);

              response.clear();

              ctx.response()
                .setStatusCode(201)
                .putHeader("Content-Type", "application/json")
                .end(response
                  .put(MSG, SUCCESS)
                  .put("user_id", userId)
                  .encodePrettily());
            }
            else
            {
              sendError(ctx, 500, "Failed to register user: No ID returned from database.");
            }
          }
          else
          {

            var error = dbResult.getString("ERROR", "Failed to register user due to a database error.");

            if (error.contains("users_username_key"))
            { // Check for unique constraint violation on username
              sendError(ctx, 409, "Username already exists");
            }
            else
            {
              LOGGER.error("User registration failed for {}. DB Error: {}", username, error);

              sendError(ctx, 500, error);
            }
          }
        })
        .onFailure(err ->
        {
          LOGGER.error("User registration query execution failed for username {}: {}", username, err.getMessage(), err);

          sendError(ctx, 500, "Failed to register user: " + err.getMessage());
        });

    }
    catch (Exception e)
    {
      LOGGER.error("Unexpected error during registration: {}", e.getMessage(), e);

      sendError(ctx, 500, "An unexpected error occurred during registration.");
    }
  }


  /**
   * Handles user login by verifying credentials and issuing a JWT token upon successful authentication.
   *
   * @param ctx The routing context containing the HTTP request.
   */

  private void login(RoutingContext ctx)
  {
    // Clear shared JsonObjects at the beginning of the handler
    query.clear();
    params.clear();
    response.clear();

    try
    {
      var body = ctx.body().asJsonObject();

      if (body == null)
      {
        sendError(ctx, 400, "Missing request body");

        return;
      }

      var username = body.getString("username");

      var password = body.getString("password");

      if (username == null || username.trim().isEmpty() || password == null || password.isEmpty())
      {
        sendError(ctx, 400, "Username and password are required");

        return;
      }

      // Query the database for the user by username.
      query.put(QUERY, QueryConstant.GET_USER_BY_USERNAME);
      params.add(username);
      query.put(PARAMS, params);

      executeQuery(query)
        .onSuccess(dbResult ->
        {
          if (SUCCESS.equals(dbResult.getString(MSG)) && dbResult.getJsonArray("result") != null && !dbResult.getJsonArray("result").isEmpty())
          {
            var user = dbResult.getJsonArray("result").getJsonObject(0);

            var storedHash = user.getString("password");

            // Verify password against stored hash.
            if (BCrypt.checkpw(password, storedHash))
            {
              // Set token expiration to 60 minutes
              long currentTimeSeconds = System.currentTimeMillis() / 1000;

              long expiryTimeSeconds = currentTimeSeconds + (60 * 60); // 60 minutes

              var claims = new JsonObject()
                .put("sub", username)
                .put("exp", expiryTimeSeconds);

              var token = jwtAuth.generateToken(claims);

              LOGGER.info("User logged in: {}", username);

              response.clear(); // Ensure response is clean

              ctx.response()
                .setStatusCode(200)
                .putHeader("Content-Type", "application/json")
                .end(response
                  .put(MSG, SUCCESS) // Using MSG constant
                  .put("token", token)
                  .encodePrettily());
            }
            else
            {
              LOGGER.warn("Failed login attempt for username: {} (Incorrect password)", username);

              sendError(ctx, 401, "Invalid username or password");
            }
          }
          else
          {
            // User not found or DB error reported where msg != SUCCESS
            LOGGER.warn("Failed login attempt for username: {} (User not found or DB issue reported by QueryProcessor)", username);

            sendError(ctx, 401, "Invalid username or password");
          }
        })
        .onFailure(err ->
        {
          // This block handles failures in the executeQuery Future itself
          LOGGER.error("User login query execution failed for username {}: {}", username, err.getMessage(), err);

          sendError(ctx, 500, "Login failed due to a server error: " + err.getMessage());
        });
    }
    catch (Exception e)
    {
      LOGGER.error("Unexpected error during login: {}", e.getMessage(), e);

      sendError(ctx, 500, "An unexpected error occurred during login.");
    }
  }

  private void sendError(RoutingContext ctx, int statusCode, String errorMessage)
  {
    response.clear();

    LOGGER.info("Sending error response: Status {}, Message: {}", statusCode, errorMessage); // Changed to info for less verbose general logging

    ctx.response()
      .setStatusCode(statusCode)
      .putHeader("Content-Type", "application/json")
      .end(response // Reusing the class member 'response' JsonObject
        .put(statusCode == 400 || statusCode == 409 || statusCode == 401 ? MSG : "status", "failed") // Use MSG for client errors, "status" for server
        .put(ERROR, errorMessage) // ERROR constant for "error" key
        .encode());
  }
}
