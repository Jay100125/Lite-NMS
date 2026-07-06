package com.example.NMS.discovery;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class DiscoveryRequestIdTest
{
    @Test
    void roundTripsContext()
    {
        var rid = DiscoveryRequestId.encode(7L, "10.20.41.163", 22, 3L);

        var decoded = DiscoveryRequestId.decode(rid);

        assertNotNull(decoded);
        assertEquals(7L, (long) decoded.getLong("discovery_id"));
        assertEquals("10.20.41.163", decoded.getString("ip"));
        assertEquals(22, (int) decoded.getInteger("port"));
        assertEquals(3L, (long) decoded.getLong("credential_id"));
    }

    @Test
    void returnsNullForNonDiscoveryRequestId()
    {
        assertNull(DiscoveryRequestId.decode("a-plain-uuid"));
        assertNull(DiscoveryRequestId.decode(null));
        assertNull(DiscoveryRequestId.decode("disc|notanumber|ip|22|3"));
    }
}
