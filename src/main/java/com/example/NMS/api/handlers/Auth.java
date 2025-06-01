package com.example.NMS.api.handlers;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.utility.APIUtils;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;
import org.mindrot.jbcrypt.BCrypt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.example.NMS.constant.Constant.*;
import static com.example.NMS.utility.DBUtils.executeQuery;

/**
 * Handles user authentication for Lite NMS, including registration and login with JWT token generation.
 * This class provides endpoints for user registration and login, ensuring secure password storage
 * using BCrypt and generating JWT tokens for authenticated users.
 */
public class Auth
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Auth.class);

    private final JWTAuth jwtAuth;

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
     * Ensures the username is unique and the password meets security requirements (minimum 8 characters).
     * Responds with a success message and user details on successful registration, or an error if validation fails
     * or the username is already taken.
     *
     * @param context The routing context containing the HTTP request with user registration data.
     */
    private void register(RoutingContext context)
    {
        try
        {
            // Extract JSON body from the request
            var body = context.body().asJsonObject();

            // validating the request body
            if (body == null)
            {
                APIUtils.sendError(context, 400, "Missing request body");

                return;
            }

            // Extract username and password from the request body
            var username = body.getString(USERNAME);

            var password = body.getString(PASSWORD);

            // Validate username and password
            if (username == null || username.trim().isEmpty() || password == null || password.length() < 8)
            {
                APIUtils.sendError(context, 400, "Invalid username or password (minimum 8 characters for password)");

                return;
            }

            // Hash the password using BCrypt for secure storage.
            var hashedPassword = BCrypt.hashpw(password, BCrypt.gensalt());

            // Prepare query to register the user.
            var query = new JsonObject()
              .put(QUERY, QueryConstant.REGISTER_USER)
              .put(PARAMS, new JsonArray().add(username).add(hashedPassword));

            // Execute the registration query
            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result != null && !result.isEmpty())
                        {
                            var userId = result.getJsonObject(0).getLong(ID);

                            LOGGER.info("User registered successfully: username={}, userId={}", username, userId);

                            APIUtils.sendSuccess(context,201, "user registered successfully", result);
                        }
                        else
                        {
                            APIUtils.sendError(context, 500, "Failed to register user: No ID returned from database.");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        // Handle case where username is already taken
                        if(error.getMessage().contains("users_username_key"))
                        {
                            LOGGER.warn("Registration failed for username={}: Username already exists", username);

                            APIUtils.sendError(context, 409, "Username already exists");
                        }
                        else
                        {
                            LOGGER.error("User registration failed for {}. Database Error: {}", username, error.getMessage());

                            APIUtils.sendError(context, 500, "Failed to register user: " + error.getMessage());
                        }
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error during registration: {}", exception.getMessage(), exception);

            APIUtils.sendError(context, 500, "An unexpected error occurred during registration.");
        }
    }

  /**
   * Handles user login by verifying credentials and issuing a JWT token upon successful authentication.
   * Validates the username and password, checks them against the stored credentials in the database,
   * and generates a JWT token with a 24-hour expiration if authentication is successful.
   *
   * @param context The routing context containing the HTTP request with login credentials.
   */
    private void login(RoutingContext context)
    {
        try
        {
            // Extract JSON body from the request
            var body = context.body().asJsonObject();

            if (body == null)
            {
                APIUtils.sendError(context, 400, "Missing request body");

                return;
            }

            var username = body.getString(USERNAME);

            var password = body.getString(PASSWORD);

            if (username == null || username.trim().isEmpty() || password == null || password.isEmpty())
            {
                APIUtils.sendError(context, 400, "Username and password are required");

                return;
            }

            // Query the database for the user by username.
            var query = new JsonObject()
              .put(QUERY, QueryConstant.GET_USER_BY_USERNAME)
              .put(PARAMS, new JsonArray().add(username));

            // Execute the user lookup query
            executeQuery(query)
                .onComplete(queryResult ->
                {
                    if (queryResult.succeeded())
                    {
                        var result = queryResult.result();

                        if (result != null && !result.isEmpty())
                        {
                          // get the user from the result
                            var user = result.getJsonObject(0);

                            var storedHash = user.getString(PASSWORD);

                            // Verify password against stored hash.
                            if (BCrypt.checkpw(password, storedHash))
                            {
                              // Set token expiration to 24 hours
                                var currentTimeSeconds = System.currentTimeMillis() / 1000;

                                var expiryTimeSeconds = currentTimeSeconds + (24 * 60 * 60); // 24 hours

                                // Create JWT claims (Token)
                                var claims = new JsonObject()
                                  .put("sub", username)
                                  .put("exp", expiryTimeSeconds);

                                var token = jwtAuth.generateToken(claims);

                                LOGGER.info("User logged in: {}", username);

                                // sending the response
                                APIUtils.sendSuccess(context,200,"Login successful", new JsonArray().add(token));

                            }
                            else
                            {
                                LOGGER.warn("Failed login attempt for username: {} (Incorrect password)", username);

                                APIUtils.sendError(context, 401, "Invalid username or password");
                            }
                        }
                        else
                        {
                            LOGGER.warn("Failed login attempt for username: {} (User not found)", username);

                            APIUtils.sendError(context, 401, "Invalid username or password");
                        }
                    }
                    else
                    {
                        var error = queryResult.cause();

                        LOGGER.error("User login query execution failed for username {}: {}", username, error.getMessage());

                        APIUtils.sendError(context, 500, "Login failed due to a server error: " + error.getMessage());
                    }
                });
        }
        catch (Exception exception)
        {
            LOGGER.error("Unexpected error during login: {}", exception.getMessage());

            APIUtils.sendError(context, 500, "An unexpected error occurred during login.");
        }
    }
}
