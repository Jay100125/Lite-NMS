package com.example.NMS.api;

import com.example.NMS.support.PgTestBase;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CredentialApiTest extends PgTestBase {
    @Test
    void credDataStoredEncrypted() throws Exception {
        pool.preparedQuery("INSERT INTO credential_profile(credential_name,system_type,cred_data) VALUES($1,$2,$3)")
            .execute(io.vertx.sqlclient.Tuple.of("c1", "LINUX",
                com.example.NMS.util.CryptoUtil.encrypt(new JsonObject().put("username","u").put("password","p").encode())))
            .toCompletionStage().toCompletableFuture().get();

        var rs = pool.query("SELECT cred_data FROM credential_profile WHERE credential_name='c1'")
            .execute().toCompletionStage().toCompletableFuture().get();
        String stored = rs.iterator().next().getString("cred_data");
        assertFalse(stored.contains("password"));                 // not plaintext
        String back = com.example.NMS.util.CryptoUtil.decrypt(stored);
        assertTrue(back.contains("password"));                    // decryptable
    }

    @Test
    void credDataUpdatedEncrypted() throws Exception {
        // seed a row
        pool.preparedQuery("INSERT INTO credential_profile(credential_name,system_type,cred_data) VALUES($1,$2,$3)")
            .execute(io.vertx.sqlclient.Tuple.of("c_upd", "LINUX",
                com.example.NMS.util.CryptoUtil.encrypt(new io.vertx.core.json.JsonObject().put("user","u").put("password","old").encode())))
            .toCompletionStage().toCompletableFuture().get();

        // update via the real UPDATE_CREDENTIAL query with an encrypted new value (as the handler now does)
        var id = pool.query("SELECT id FROM credential_profile WHERE credential_name='c_upd'")
            .execute().toCompletionStage().toCompletableFuture().get().iterator().next().getInteger("id");
        String newEnc = com.example.NMS.util.CryptoUtil.encrypt(new io.vertx.core.json.JsonObject().put("user","u").put("password","new").encode());
        pool.preparedQuery(com.example.NMS.constant.QueryConstant.UPDATE_CREDENTIAL)
            .execute(io.vertx.sqlclient.Tuple.of(null, null, newEnc, id))
            .toCompletionStage().toCompletableFuture().get();

        var stored = pool.query("SELECT cred_data FROM credential_profile WHERE id=" + id)
            .execute().toCompletionStage().toCompletableFuture().get().iterator().next().getString("cred_data");
        org.junit.jupiter.api.Assertions.assertFalse(stored.contains("password"));           // not plaintext
        org.junit.jupiter.api.Assertions.assertTrue(com.example.NMS.util.CryptoUtil.decrypt(stored).contains("new")); // updated value, decryptable
    }
}
