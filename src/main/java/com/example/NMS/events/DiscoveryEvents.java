package com.example.NMS.events;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

import static com.example.NMS.constant.Constant.DISCOVERY_EVENT_ADDRESS_PREFIX;

/**
 * Publishes discovery progress events to the per-run event-bus address
 * {@code nms.discovery.<discoveryId>}, exposed outbound-only over the SockJS
 * bridge. Fire-and-forget: publishing must never fail a discovery run.
 */
public final class DiscoveryEvents
{
    private DiscoveryEvents() {}

    public static String address(long discoveryId)
    {
        return DISCOVERY_EVENT_ADDRESS_PREFIX + discoveryId;
    }

    /** Run lifecycle: status RUNNING | COMPLETED | FAILED, message optional. */
    public static void state(EventBus bus, long discoveryId, String status, String message)
    {
        var payload = new JsonObject().put("type", "state").put("status", status);

        if (message != null)
        {
            payload.put("message", message);
        }

        bus.publish(address(discoveryId), payload);
    }

    /** The expanded target list, published once after IP resolution. */
    public static void targets(EventBus bus, long discoveryId, List<String> ips)
    {
        bus.publish(address(discoveryId), new JsonObject()
            .put("type", "targets")
            .put("total", ips.size())
            .put("ips", new JsonArray(ips)));
    }

    /** Per-IP stage progress: stage PING|PORT|PLUGIN, status ok|failed (PLUGIN: COMPLETED|FAILED). */
    public static void progress(EventBus bus, long discoveryId, String ip, String stage,
                                double progress, String status, String message)
    {
        var payload = new JsonObject()
            .put("type", "progress")
            .put("ip", ip)
            .put("stage", stage)
            .put("progress", progress)
            .put("status", status);

        if (message != null)
        {
            payload.put("message", message);
        }

        bus.publish(address(discoveryId), payload);
    }
}
