package com.example.NMS.api;

import io.vertx.core.Vertx;
import io.vertx.core.VertxOptions;
import io.vertx.core.http.HttpClientRequest;
import io.vertx.core.http.HttpClientResponse;
import io.vertx.core.http.HttpMethod;
import io.vertx.core.json.JsonObject;
import io.vertx.micrometer.MicrometerMetricsOptions;
import io.vertx.micrometer.VertxPrometheusOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.AbstractMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.SERVER_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies the self-observability endpoints added in Task 10: a plain /health
 * liveness probe and a Prometheus /metrics scrape endpoint backed by Micrometer.
 * Both routes are exercised against a Vertx instance with Micrometer metrics
 * enabled (mirroring how Main.java wires the production Vertx instance), since
 * PrometheusScrapingHandler only has data to report when metrics are enabled
 * on the same Vertx instance that serves the route.
 */
class HealthMetricsTest
{
    private static Vertx vertx;

    @BeforeAll
    static void deployServer() throws Exception
    {
        vertx = Vertx.vertx(new VertxOptions()
            .setMetricsOptions(new MicrometerMetricsOptions()
                .setPrometheusOptions(new VertxPrometheusOptions().setEnabled(true))
                .setEnabled(true)));

        vertx.deployVerticle(new Server())
            .toCompletionStage()
            .toCompletableFuture()
            .get(30, TimeUnit.SECONDS);
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        if (vertx != null)
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
        }
    }

    /**
     * Issues a GET request and resolves the status code together with the fully
     * read body. The body() call is chained inside the same Vert.x future
     * composition (rather than fetched after a separate blocking wait on the
     * response object) so the body collector is attached before any data can
     * arrive and be missed.
     */
    private static Map.Entry<Integer, String> get(String path) throws Exception
    {
        var client = vertx.createHttpClient();

        try
        {
            return client.request(HttpMethod.GET, SERVER_PORT, "localhost", path)
                .compose(HttpClientRequest::send)
                .compose(response -> response.body()
                    .map(buffer -> new AbstractMap.SimpleEntry<>(response.statusCode(), buffer.toString())))
                .toCompletionStage()
                .toCompletableFuture()
                .get(10, TimeUnit.SECONDS);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    void healthReturnsUp() throws Exception
    {
        var result = get("/health");

        assertEquals(200, result.getKey());

        assertEquals("UP", new JsonObject(result.getValue()).getString("status"));
    }

    @Test
    void metricsReturnsPrometheusExposition() throws Exception
    {
        var result = get("/metrics");

        assertEquals(200, result.getKey());

        assertTrue(result.getValue().contains("vertx_"));
    }
}
