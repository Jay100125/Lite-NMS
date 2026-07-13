package com.example.NMS.discovery;

import static com.example.NMS.constant.Constant.PLUGIN_SNMP;

/**
 * Per-plugin-type stage rules for discovery progress. SNMP is UDP, so the TCP
 * port check is meaningless and skipped — ping completes half the run (50%)
 * instead of a third (33.33%). Mirrors the reference NMS stage percentages.
 */
public final class DiscoveryStages
{
    private DiscoveryStages() {}

    public static double pingProgress(String pluginType)
    {
        return PLUGIN_SNMP.equals(pluginType) ? 50.0 : 33.33;
    }

    public static boolean skipsPortCheck(String pluginType)
    {
        return PLUGIN_SNMP.equals(pluginType);
    }
}
