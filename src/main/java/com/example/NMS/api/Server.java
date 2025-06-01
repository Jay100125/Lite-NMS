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

import static com.example.NMS.constant.Constant.*;

/**
 * Main Vert.x verticle for the Lite NMS API server.
 * Configures JWT authentication, sets up API routing, and initializes handlers for authentication,
 * credential management, discovery profiles, and provisioning jobs. Starts an HTTP server to handle
 * incoming requests on the configured port.
 */
public class Server extends AbstractVerticle
{
    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

  /**
   * Starts the Vert.x verticle and initializes the API server.
   * Configures JWT authentication, sets up routing with sub-routers for each handler,
   * and starts the HTTP server on the specified port.
   *
   * @param startPromise The promise to complete or fail based on server startup success.
   */
    @Override
    public void start(Promise<Void> startPromise)
    {
        // Initialize JWT authentication with HS256 algorithm and secret key
        var jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
                                        .addPubSecKey(new PubSecKeyOptions()
                                                            .setAlgorithm("HS256")
                                                           .setBuffer(JWT_SECRET)));

        // Create the main router for the application
        var router = Router.router(vertx);

        // Create sub-routers for specific handlers
        var authRoute = Router.router(vertx);

        var discoveryRoute = Router.router(vertx);

        var credentialRoute = Router.router(vertx);

        var provisionRoute = Router.router(vertx);

        router.route("/api/*").handler(BodyHandler.create());

        // Configure JWT authentication for protected routes
        router.route("/api/*").handler(ctx ->
        {
            var path = ctx.normalizedPath();

            // Bypass JWT authentication for register and login endpoints
            if (path.endsWith("/register") || path.endsWith("/login"))
            {
                ctx.next();
            }
            else
            {
                // Apply JWT authentication for all other API routes
                JWTAuthHandler.create(jwtAuth).handle(ctx);
            }
        });

        // Mount sub-routers to the main router
        router.route().subRouter(authRoute);

        router.route().subRouter(credentialRoute);

        router.route().subRouter(discoveryRoute);

        router.route().subRouter(provisionRoute);


        // Initialize handlers with their respective sub-routers
        new Auth(jwtAuth).init(authRoute);

        new Credential().init(credentialRoute);

        new Discovery().init(discoveryRoute);

        new Provision().init(provisionRoute);


        // Configure error handler for unauthorized (401) responses
        router.errorHandler(401, context ->
          context.response()
            .setStatusCode(401)
            .putHeader("Content-Type", "application/json")
            .end(new JsonObject()
              .put(ERROR, "Unauthorized")
              .put(MESSAGE, "Invalid or missing JWT token")
              .encode()));


        // Start the HTTP server
        vertx.createHttpServer().requestHandler(router)
          .listen(SERVER_PORT)
          .onComplete(handler ->
          {
              if (handler.succeeded())
              {
                  LOGGER.info("API server started successfully on port {}", SERVER_PORT);

                  startPromise.complete();
              }
              else
              {
                  LOGGER.error("Failed to start API server on port {}: {}", SERVER_PORT, handler.cause().getMessage());

                  startPromise.fail(handler.cause());
              }
          });
    }
}
