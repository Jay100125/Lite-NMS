package com.example.NMS.plugin;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
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
}
