package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.support.PgTestBase;
import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;

import static org.junit.jupiter.api.Assertions.*;

/** Provisioning must only create default metric rows the plugin type supports. */
class ProvisionDefaultMetricsDbTest extends PgTestBase
{
    private long seedCompletedDiscovery(String suffix, String systemType, String ip) throws Exception
    {
        var credId = pool.preparedQuery(
                "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, 'x') RETURNING id")
            .execute(Tuple.of("pm-cred-" + suffix, systemType))
            .toCompletionStage().toCompletableFuture().get().iterator().next().getLong("id");

        var discoveryId = pool.preparedQuery(
                "INSERT INTO discovery_profiles (discovery_profile_name, ip, port, plugin_type) VALUES ($1, $2, 161, $3) RETURNING id")
            .execute(Tuple.of("pm-disc-" + suffix, ip, systemType))
            .toCompletionStage().toCompletableFuture().get().iterator().next().getLong("id");

        pool.preparedQuery(
                "INSERT INTO discovery_result (discovery_id, ip, port, msg, credential_profile_id, result) VALUES ($1, $2, 161, 'ok', $3, 'COMPLETED')")
            .execute(Tuple.of(discoveryId, ip, credId))
            .toCompletionStage().toCompletableFuture().get();

        return discoveryId;
    }

    private java.util.List<String> provisionAndListMetrics(long discoveryId, String ip) throws Exception
    {
        pool.preparedQuery(QueryConstant.INSERT_PROVISIONING_AND_METRICS)
            .execute(Tuple.of(discoveryId, new JsonArray().add(ip).encode()))
            .toCompletionStage().toCompletableFuture().get();

        var rs = pool.preparedQuery(
                "SELECT m.name::text AS name FROM metrics m JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id WHERE pj.ip = $1 ORDER BY 1")
            .execute(Tuple.of(ip))
            .toCompletionStage().toCompletableFuture().get();

        var names = new ArrayList<String>();

        rs.forEach(row -> names.add(row.getString("name")));

        return names;
    }

    @Test
    void snmpDeviceGetsOnlyUptime() throws Exception
    {
        var discoveryId = seedCompletedDiscovery("snmp", "SNMP", "10.99.0.1");

        assertEquals(java.util.List.of("UPTIME"), provisionAndListMetrics(discoveryId, "10.99.0.1"));
    }

    @Test
    void winrmDeviceGetsCpuMemoryDisk() throws Exception
    {
        var discoveryId = seedCompletedDiscovery("winrm", "WINRM", "10.99.0.2");

        assertEquals(java.util.List.of("CPU", "DISK", "MEMORY"), provisionAndListMetrics(discoveryId, "10.99.0.2"));
    }

    @Test
    void linuxDeviceGetsFullSet() throws Exception
    {
        var discoveryId = seedCompletedDiscovery("linux", "LINUX", "10.99.0.3");

        assertEquals(java.util.List.of("CPU", "DISK", "MEMORY", "NETWORK", "PROCESS", "UPTIME"),
            provisionAndListMetrics(discoveryId, "10.99.0.3"));
    }
}
