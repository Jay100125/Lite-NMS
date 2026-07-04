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
}
