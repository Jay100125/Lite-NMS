package com.example.NMS.discovery;

import io.vertx.core.json.JsonObject;

/**
 * Encodes discovery correlation context into the engine {@code request_id}.
 *
 * <p>The v2 engine echoes {@code request_id} verbatim on every result line, but a result line carries
 * no {@code ip}/{@code discovery_id} — so we round-trip the (discovery_id, ip, port, credential_id)
 * needed to store a {@code discovery_result} row through the request_id. This keeps the discovery
 * result path stateless (no in-flight map shared across verticles).
 */
public final class DiscoveryRequestId
{
    private DiscoveryRequestId() {}

    private static final String PREFIX = "disc";

    private static final String SEP = "|";

    /** Builds the request_id for one (ip, credential) discovery target. */
    public static String encode(long discoveryId, String ip, int port, long credentialId)
    {
        return String.join(SEP, PREFIX, String.valueOf(discoveryId), ip, String.valueOf(port), String.valueOf(credentialId));
    }

    /** Decodes a discovery request_id into its context, or returns null if it is not one (e.g. a poll UUID). */
    public static JsonObject decode(String requestId)
    {
        if (requestId == null)
        {
            return null;
        }

        var parts = requestId.split("\\" + SEP);

        if (parts.length != 5 || !PREFIX.equals(parts[0]))
        {
            return null;
        }

        try
        {
            return new JsonObject()
                .put("discovery_id", Long.parseLong(parts[1]))
                .put("ip", parts[2])
                .put("port", Integer.parseInt(parts[3]))
                .put("credential_id", Long.parseLong(parts[4]));
        }
        catch (NumberFormatException exception)
        {
            return null;
        }
    }
}
