package com.example.NMS.util;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CryptoUtilTest {
    @Test
    void roundTrip() {
        String secret = "{\"username\":\"admin\",\"password\":\"s3cr3t\"}";
        String token = CryptoUtil.encrypt(secret);
        assertNotEquals(secret, token);            // stored form is not plaintext
        assertEquals(secret, CryptoUtil.decrypt(token));
    }

    @Test
    void differentIVsProduceDifferentCiphertext() {
        String a = CryptoUtil.encrypt("same");
        String b = CryptoUtil.encrypt("same");
        assertNotEquals(a, b);                     // random IV per encryption
    }
}
