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
 * {@code Discovery.resolveCredentials} must decrypt each row into its stored plaintext shape
 * (passed through untouched — {@code PluginEnvelope.credential} remaps per plugin type later)
 * plus {@code id}, and skip rows without cred_data.
 */
class DiscoveryCredentialsTest
{
    @Test
    void decryptsCredData()
    {
        // stored plaintext uses keys "user"/"password" (see Credential.create)
        var cipher = CryptoUtil.encrypt(new JsonObject().put(USER, "root").put(PASSWORD, "pw").encode());

        var resolved = Discovery.resolveCredentials(new JsonArray()
            .add(new JsonObject().put(CRED_DATA, cipher).put(ID, 5L)));

        assertEquals(1, resolved.size());
        assertEquals("root", resolved.getJsonObject(0).getString(USER));
        assertEquals("pw", resolved.getJsonObject(0).getString(PASSWORD));
        assertEquals(5L, (long) resolved.getJsonObject(0).getLong(ID));
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
