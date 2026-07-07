package com.example.NMS.api;

import com.example.NMS.constant.QueryConstant;
import com.example.NMS.support.PgTestBase;
import io.vertx.core.json.JsonArray;
import io.vertx.sqlclient.Tuple;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Verifies the credential/system_type mismatch guard used by discovery create/update. */
class DiscoveryPluginTypeDbTest extends PgTestBase
{
    private long insertCredential(String name, String type) throws Exception
    {
        var rs = pool.preparedQuery(
                "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, 'x') RETURNING id")
            .execute(Tuple.of(name, type))
            .toCompletionStage().toCompletableFuture().get();

        return rs.iterator().next().getLong("id");
    }

    @Test
    void countsCredentialsWhoseTypeDiffers() throws Exception
    {
        var linuxId = insertCredential("dt-linux", "LINUX");

        var snmpId = insertCredential("dt-snmp", "SNMP");

        var ids = new JsonArray().add(linuxId).add(snmpId).encode();

        var rs = pool.preparedQuery(QueryConstant.COUNT_MISMATCHED_CREDENTIALS)
            .execute(Tuple.of(ids, "LINUX"))
            .toCompletionStage().toCompletableFuture().get();

        assertEquals(1, (int) rs.iterator().next().getInteger("mismatched"));

        var rsAllMatch = pool.preparedQuery(QueryConstant.COUNT_MISMATCHED_CREDENTIALS)
            .execute(Tuple.of(new JsonArray().add(linuxId).encode(), "LINUX"))
            .toCompletionStage().toCompletableFuture().get();

        assertEquals(0, (int) rsAllMatch.iterator().next().getInteger("mismatched"));
    }
}
