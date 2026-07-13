package com.example.NMS.plugin;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PluginEnvelopeTest {
    @Test
    void buildsSpecShape() {
        JsonObject t = PluginEnvelope.target("r1", 7L, "LINUX", "10.0.0.5", 22,
            new JsonObject().put("username", "u").put("password", "p"),
            new JsonArray().add("CPU"));
        JsonObject env = PluginEnvelope.build("poll", new JsonArray().add(t));

        assertEquals(1, env.getInteger("version"));
        assertEquals("poll", env.getString("event_type"));
        JsonObject t0 = env.getJsonArray("targets").getJsonObject(0);
        assertEquals("r1", t0.getString("request_id"));
        assertEquals(7L, (long) t0.getLong("job_id"));
        assertEquals("LINUX", t0.getString("plugin_type"));
        assertEquals("CPU", t0.getJsonArray("metrics").getString(0));
    }
}
