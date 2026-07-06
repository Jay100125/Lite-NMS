package com.example.NMS.api;

import com.example.NMS.api.handlers.Discovery;
import com.example.NMS.util.CryptoUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * The discovery run path reads credentials whose {@code cred_data} is now encrypted at rest.
 * {@code Discovery.resolveCredentials} must decrypt each row into the plaintext
 * username/password the discovery engine expects, and skip rows without cred_data.
 */
class DiscoveryCredentialsTest
{
    @Test
    void decryptsCredData()
    {
        // stored plaintext uses keys "user"/"password" (see Credential.create)
        var cipher = CryptoUtil.encrypt(
            new JsonObject().put(USER, "admin").put(PASSWORD, "s3cr3t").encode());

        var rows = new JsonArray().add(new JsonObject()
            .put(ID, 5L)
            .put(PROTOCOL, "LINUX")
            .put(CRED_DATA, cipher));

        var resolved = Discovery.resolveCredentials(rows);

        assertEquals(1, resolved.size());
        var cred = resolved.getJsonObject(0);
        assertEquals("admin", cred.getString(USERNAME));
        assertEquals("s3cr3t", cred.getString(PASSWORD));
        assertEquals(5L, (long) cred.getLong(ID));
    }

    @Test
    void skipsRowsWithoutCredData()
    {
        // a profile with no mapped credentials aggregates to a row with null cred_data
        var rows = new JsonArray().add(new JsonObject().putNull(CRED_DATA));

        assertTrue(Discovery.resolveCredentials(rows).isEmpty());
        assertTrue(Discovery.resolveCredentials(null).isEmpty());
    }
}
