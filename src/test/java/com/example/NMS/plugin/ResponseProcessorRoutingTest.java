package com.example.NMS.plugin;

import com.example.NMS.discovery.DiscoveryRequestId;
import com.example.NMS.events.DiscoveryEvents;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

class ResponseProcessorRoutingTest {
    @Test
    void routesByEventType() {
        assertEquals("poll", ResponseProcessor.classify(
            new JsonObject().put("event_type", "poll").put("status", "success")));
        assertEquals("discovery", ResponseProcessor.classify(
            new JsonObject().put("event_type", "discovery")));
        assertEquals("unknown", ResponseProcessor.classify(new JsonObject()));
    }

    @Test
    void engineDiscoveryResultPublishesPluginProgress() throws Exception
    {
        var vertx = Vertx.vertx();

        try
        {
            vertx.deployVerticle(new ResponseProcessor()).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            var got = new CompletableFuture<JsonObject>();

            vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(42L), msg -> got.complete(msg.body()));

            var requestId = DiscoveryRequestId.encode(42L, "10.0.0.5", 22, 3L);

            vertx.eventBus().send(STORAGE_RESULTS, new JsonObject()
                .put(REQUEST_ID, requestId)
                .put(EVENT_TYPE, EVENT_DISCOVERY)
                .put(STATUS, SUCCESS));

            var event = got.get(5, TimeUnit.SECONDS);

            assertEquals("progress", event.getString("type"));
            assertEquals("10.0.0.5", event.getString("ip"));
            assertEquals("PLUGIN", event.getString("stage"));
            assertEquals(100.0, event.getDouble("progress"));
            assertEquals("COMPLETED", event.getString("status"));
        }
        finally
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void discoveryCompletionPublishesCompletedState() throws Exception
    {
        var vertx = Vertx.vertx();

        try
        {
            vertx.deployVerticle(new ResponseProcessor()).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            var got = new CompletableFuture<JsonObject>();

            vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(42L), msg -> got.complete(msg.body()));

            vertx.eventBus().send(EVENT_COMPLETION, new JsonObject()
                .put(EVENT_TYPE, EVENT_DISCOVERY)
                .put(DISCOVERY_ID, 42));

            var event = got.get(5, TimeUnit.SECONDS);

            assertEquals("state", event.getString("type"));
            assertEquals("COMPLETED", event.getString("status"));
        }
        finally
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
}
