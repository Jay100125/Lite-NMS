package com.example.NMS.plugin;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import static org.junit.jupiter.api.Assertions.*;

class PluginRunnerTest {
    @Test
    void decodesBase64ResultLine() {
        String json = "{\"request_id\":\"r1\",\"event_type\":\"poll\",\"status\":\"success\"}";
        String line = Base64.getEncoder().encodeToString(json.getBytes());
        JsonObject obj = Plugin.decodeResultLine(line);
        assertEquals("poll", obj.getString("event_type"));
        assertEquals("success", obj.getString("status"));
    }

    @Test
    void badLineReturnsNull() {
        assertNull(Plugin.decodeResultLine("not-base64-json"));
    }
}
