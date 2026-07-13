package com.example.NMS.api;

import com.example.NMS.api.handlers.Credential;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the per-system_type cred_data contract (spec: multi-protocol section). */
class CredentialCredDataTest
{
    @Test
    void linuxAndWinrmRequireUserAndPassword()
    {
        var valid = new JsonObject().put(USER, "root").put(PASSWORD, "pw");

        assertNull(Credential.credDataError(PLUGIN_LINUX, valid));
        assertNull(Credential.credDataError(PLUGIN_WINRM, valid));

        assertNotNull(Credential.credDataError(PLUGIN_LINUX, new JsonObject().put(USER, "root")));
        assertNotNull(Credential.credDataError(PLUGIN_WINRM, new JsonObject().put(USER, "").put(PASSWORD, "pw")));
        assertNotNull(Credential.credDataError(PLUGIN_LINUX, new JsonObject().put(COMMUNITY, "public")));
    }

    @Test
    void snmpRequiresCommunity()
    {
        assertNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "public")));

        assertNotNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(USER, "root").put(PASSWORD, "pw")));
        assertNotNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "")));
    }

    @Test
    void snmpNormalizationPinsVersion2c()
    {
        var stored = Credential.normalizeCredData(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "public"));

        assertEquals(SNMP_V2C, stored.getString(SNMP_VERSION));
        assertEquals("public", stored.getString(COMMUNITY));

        // LINUX/WINRM pass through untouched
        var linux = new JsonObject().put(USER, "root").put(PASSWORD, "pw");
        assertEquals(linux, Credential.normalizeCredData(PLUGIN_LINUX, linux));
    }
}
