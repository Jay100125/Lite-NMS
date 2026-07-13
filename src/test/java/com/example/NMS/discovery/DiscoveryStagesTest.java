package com.example.NMS.discovery;

import com.example.NMS.plugin.PluginEnvelope;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/** Stage math and per-type credential shaping (spec: staged progress + multi-protocol). */
class DiscoveryStagesTest
{
    @Test
    void snmpSkipsPortCheckAndJumpsToFifty()
    {
        assertEquals(50.0, DiscoveryStages.pingProgress(PLUGIN_SNMP));
        assertTrue(DiscoveryStages.skipsPortCheck(PLUGIN_SNMP));

        assertEquals(33.33, DiscoveryStages.pingProgress(PLUGIN_LINUX));
        assertFalse(DiscoveryStages.skipsPortCheck(PLUGIN_LINUX));
        assertEquals(33.33, DiscoveryStages.pingProgress(PLUGIN_WINRM));
        assertFalse(DiscoveryStages.skipsPortCheck(PLUGIN_WINRM));
    }

    @Test
    void credentialShapePerType()
    {
        var stored = new JsonObject().put(USER, "root").put(PASSWORD, "pw");

        var ssh = PluginEnvelope.credential(PLUGIN_LINUX, stored);

        assertEquals("root", ssh.getString(USERNAME));
        assertEquals("pw", ssh.getString(PASSWORD));

        var winrm = PluginEnvelope.credential(PLUGIN_WINRM, stored);

        assertEquals("root", winrm.getString(USERNAME));

        var snmpStored = new JsonObject().put(SNMP_VERSION, SNMP_V2C).put(COMMUNITY, "public");

        var snmp = PluginEnvelope.credential(PLUGIN_SNMP, snmpStored);

        assertEquals(SNMP_V2C, snmp.getString(SNMP_VERSION));
        assertEquals("public", snmp.getString(COMMUNITY));
        assertFalse(snmp.containsKey(USERNAME));
    }
}
