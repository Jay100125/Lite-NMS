package com.example.NMS.support;

import io.vertx.core.Vertx;
import io.vertx.pgclient.PgConnectOptions;
import io.vertx.pgclient.PgPool;
import io.vertx.sqlclient.PoolOptions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

/**
 * Base for DB integration tests. Creates a uniquely-named throwaway database on a
 * local PostgreSQL (env-configurable), applies schema.sql, and drops it after.
 * No Docker required. Subclasses use the static {@code pool}.
 */
public abstract class PgTestBase {
    private static String env(String k, String d) {
        String v = System.getenv(k);
        return (v == null || v.isBlank()) ? d : v;
    }

    private static final String HOST = env("NMS_TEST_DB_HOST", "127.0.0.1");
    private static final int PORT = Integer.parseInt(env("NMS_TEST_DB_PORT", "5432"));
    private static final String ADMIN_DB = env("NMS_TEST_DB_ADMIN", "nms_test");
    private static final String USER = env("NMS_TEST_DB_USER", "nms_test");
    private static final String PASSWORD = env("NMS_TEST_DB_PASSWORD", "nms_test");

    protected static Vertx vertx;
    protected static PgPool pool;
    private static String dbName;

    private static PgConnectOptions opts(String database) {
        return new PgConnectOptions().setHost(HOST).setPort(PORT)
            .setDatabase(database).setUser(USER).setPassword(PASSWORD);
    }

    @BeforeAll
    static void startDb() throws Exception {
        vertx = Vertx.vertx();
        dbName = "nms_test_" + UUID.randomUUID().toString().replace("-", "");

        // Create the throwaway database via a short-lived admin pool (simple-query protocol).
        PgPool admin = PgPool.pool(vertx, opts(ADMIN_DB), new PoolOptions().setMaxSize(1));
        admin.query("CREATE DATABASE \"" + dbName + "\"").execute()
            .toCompletionStage().toCompletableFuture().get();
        admin.close();

        pool = PgPool.pool(vertx, opts(dbName), new PoolOptions().setMaxSize(4));

        String ddl = Files.readString(Path.of("src/main/resources/schema.sql"));
        for (String stmt : ddl.split(";")) {
            String s = stmt.trim();
            if (!s.isEmpty()) {
                pool.query(s).execute().toCompletionStage().toCompletableFuture().get();
            }
        }
    }

    @AfterAll
    static void stopDb() throws Exception {
        if (pool != null) pool.close();
        PgPool admin = PgPool.pool(vertx, opts(ADMIN_DB), new PoolOptions().setMaxSize(1));
        admin.query("DROP DATABASE IF EXISTS \"" + dbName + "\" WITH (FORCE)").execute()
            .toCompletionStage().toCompletableFuture().get();
        admin.close();
        if (vertx != null) vertx.close();
    }
}
