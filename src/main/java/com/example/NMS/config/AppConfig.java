package com.example.NMS.config;

/** Central configuration sourced from environment variables. */
public final class AppConfig {
    private AppConfig() {}

    private static String env(String key, String def) {
        String v = System.getenv(key);
        return (v == null || v.isBlank()) ? def : v;
    }

    public static String dbHost()  { return env("NMS_DB_HOST", "localhost"); }
    public static int    dbPort()  { return Integer.parseInt(env("NMS_DB_PORT", "5432")); }
    public static String dbName()  { return env("NMS_DB_NAME", "nms"); }
    public static String dbUser()  { return env("NMS_DB_USER", "nms"); }
    public static String dbPassword() { return env("NMS_DB_PASSWORD", "nms"); }

    /** 32+ char signing key. Override in every real environment. */
    public static String jwtSecret() {
        return env("NMS_JWT_SECRET", "dev-only-change-me-32byteminimum-key!");
    }

    /** Base64 AES-256 key (32 bytes). Override in every real environment. */
    public static String credEncryptionKey() {
        return env("NMS_CRED_KEY", "ZGV2LW9ubHktMzJieXRlLWtleS1jaGFuZ2UtbWUtISE=");
    }
}
