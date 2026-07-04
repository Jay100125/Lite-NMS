package com.example.NMS.plugin;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** Builds the v2 engine envelope (spec §4). Field names are contractual. */
public final class PluginEnvelope {
    private PluginEnvelope() {}

    public static final int VERSION = 1;

    public static JsonObject build(String eventType, JsonArray targets) {
        return new JsonObject()
            .put("version", VERSION)
            .put("event_type", eventType)
            .put("targets", targets);
    }

    public static JsonObject target(String requestId, long jobId, String pluginType,
                                    String ip, int port, JsonObject credential, JsonArray metrics) {
        return new JsonObject()
            .put("request_id", requestId)
            .put("job_id", jobId)
            .put("plugin_type", pluginType)
            .put("ip", ip)
            .put("port", port)
            .put("credential", credential)
            .put("metrics", metrics);
    }
}
