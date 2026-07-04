package com.example.NMS.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {
    @Test
    void readsFromEnvWithDefaults() {
        // host/port/name have safe defaults
        assertNotNull(AppConfig.dbHost());
        assertTrue(AppConfig.dbPort() > 0);
        // secrets fall back to a documented dev default only when env is unset
        assertNotNull(AppConfig.jwtSecret());
        assertNotNull(AppConfig.credEncryptionKey());
    }
}
