package com.example.NMS.polling;

import com.example.NMS.util.CryptoUtil;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit test for the pure {@code Polling.buildEnvelope} contract: due jobs carrying an
 * encrypted {@code cred_data} become a v2 "poll" envelope with one target per job,
 * decrypted credentials, and metrics preserved.
 */
class PollingEnvelopeTest
{
    @Test
    void buildsPollEnvelopeWithDecryptedCreds()
    {
        // cred_data is stored with the {"user","password"} shape (see Credential create
        // validation); buildEnvelope remaps it to the engine's {"username","password"}.
        var credCipher = CryptoUtil.encrypt(
            new JsonObject().put(USER, "admin").put(PASSWORD, "s3cr3t").encode());

        var job = new JsonObject()
            .put(JOB_ID, 7L)
            .put(PLUGIN_TYPE, "LINUX")
            .put(IP, "10.0.0.5")
            .put(PORT, 22)
            .put(CRED_DATA, credCipher)
            .put("metrics", new JsonArray().add("CPU").add("MEMORY"));

        var envelope = Polling.buildEnvelope(new JsonArray().add(job));

        assertEquals(EVENT_POLL, envelope.getString(EVENT_TYPE));

        var targets = envelope.getJsonArray(TARGETS);
        assertEquals(1, targets.size());

        var target = targets.getJsonObject(0);
        assertEquals(7L, (long) target.getLong(JOB_ID));
        assertEquals("LINUX", target.getString(PLUGIN_TYPE));
        assertEquals("10.0.0.5", target.getString(IP));

        // credential is decrypted back to plaintext username/password
        var credential = target.getJsonObject("credential");
        assertEquals("admin", credential.getString(USERNAME));
        assertEquals("s3cr3t", credential.getString(PASSWORD));

        // metrics are preserved verbatim
        assertEquals(new JsonArray().add("CPU").add("MEMORY"), target.getJsonArray("metrics"));
    }

    @Test
    void oneTargetPerJob()
    {
        var cipher = CryptoUtil.encrypt(new JsonObject().put(USER, "u").put(PASSWORD, "p").encode());

        var jobs = new JsonArray();
        for (int i = 0; i < 3; i++)
        {
            jobs.add(new JsonObject()
                .put(JOB_ID, (long) i)
                .put(PLUGIN_TYPE, "SNMP")
                .put(IP, "10.0.0." + i)
                .put(PORT, 161)
                .put(CRED_DATA, cipher)
                .put("metrics", new JsonArray().add("CPU")));
        }

        var envelope = Polling.buildEnvelope(jobs);
        assertEquals(3, envelope.getJsonArray(TARGETS).size());
    }
}
