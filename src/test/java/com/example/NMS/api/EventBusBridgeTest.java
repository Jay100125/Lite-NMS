package com.example.NMS.api;

import com.example.NMS.config.AppConfig;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.SERVER_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The SockJS bridge at /eventbus must reject connections without a valid JWT
 * (SockJS cannot send an Authorization header, so the token rides the
 * access_token query param) and serve the SockJS /info handshake with one.
 */
class EventBusBridgeTest
{
    private static Vertx vertx;

    private static String validToken;

    @BeforeAll
    static void deployServer() throws Exception
    {
        vertx = Vertx.vertx();

        vertx.deployVerticle(new Server()).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        var jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
            .addPubSecKey(new PubSecKeyOptions().setAlgorithm("HS256").setBuffer(AppConfig.jwtSecret())));

        validToken = jwtAuth.generateToken(new JsonObject().put("sub", "test"), new JWTOptions().setExpiresInMinutes(5));
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private int statusOf(String uri) throws Exception
    {
        var client = vertx.createHttpClient();

        try
        {
            return client.request(HttpMethod.GET, SERVER_PORT, "127.0.0.1", uri)
                .compose(req -> req.send())
                .map(resp -> resp.statusCode())
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    void rejectsMissingAndInvalidToken() throws Exception
    {
        assertEquals(401, statusOf("/eventbus/info"));

        assertEquals(401, statusOf("/eventbus/info?access_token=not-a-jwt"));
    }

    @Test
    void servesSockJsInfoWithValidToken() throws Exception
    {
        assertEquals(200, statusOf("/eventbus/info?access_token=" + validToken));
    }
}
