package com.example.NMS.events;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** DiscoveryEvents publishes UI progress payloads to the per-run address nms.discovery.<id>. */
class DiscoveryEventsTest
{
    private static Vertx vertx;

    @BeforeAll
    static void up() { vertx = Vertx.vertx(); }

    @AfterAll
    static void down() throws Exception { vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS); }

    private CompletableFuture<JsonObject> nextEvent(long id)
    {
        var future = new CompletableFuture<JsonObject>();

        vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(id), msg -> future.complete(msg.body()));

        return future;
    }

    @Test
    void publishesProgressWithStagePayload() throws Exception
    {
        var got = nextEvent(42L);

        DiscoveryEvents.progress(vertx.eventBus(), 42L, "10.0.0.5", "PING", 33.33, "ok", null);

        var event = got.get(5, TimeUnit.SECONDS);

        assertEquals("progress", event.getString("type"));
        assertEquals("10.0.0.5", event.getString("ip"));
        assertEquals("PING", event.getString("stage"));
        assertEquals(33.33, event.getDouble("progress"));
        assertEquals("ok", event.getString("status"));
        assertFalse(event.containsKey("message"));
    }

    @Test
    void publishesStateAndTargets() throws Exception
    {
        var gotState = nextEvent(7L);

        DiscoveryEvents.state(vertx.eventBus(), 7L, "RUNNING", null);

        var state = gotState.get(5, TimeUnit.SECONDS);

        assertEquals("state", state.getString("type"));
        assertEquals("RUNNING", state.getString("status"));

        var gotTargets = nextEvent(8L);

        DiscoveryEvents.targets(vertx.eventBus(), 8L, List.of("10.0.0.1", "10.0.0.2"));

        var targets = gotTargets.get(5, TimeUnit.SECONDS);

        assertEquals("targets", targets.getString("type"));
        assertEquals(2, (int) targets.getInteger("total"));
        assertEquals("10.0.0.1", targets.getJsonArray("ips").getString(0));
    }
}
