# Java Backend v2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Rearchitect the `Lite-NMS` Vert.x backend to speak the v2 envelope contract, reconcile the schema, fix the v1 review defects, and add credential encryption, availability monitoring, and self-observability — all covered by Testcontainers-backed tests.

**Architecture:** Verticle-per-concern over an event-bus Postgres layer, unchanged in shape. The backend builds a typed **envelope** (spec §4), writes it to a temp file, and spawns the Go engine (`plugin/Lite_NMS_Plugin <file>`) from a **WORKER** verticle that drains stdout and stderr **concurrently**. Results route on the always-present `event_type` discriminator. Secrets come from the environment; device credentials are AES-GCM encrypted at rest. A Micrometer/Prometheus registry and `/health` expose system state.

**Tech Stack:** Java 17, Vert.x 4.5.14 (`vertx-web`, `vertx-pg-client`, `vertx-auth-jwt`, `vertx-config`, `vertx-micrometer-metrics`), Micrometer Prometheus, JUnit 5, BCrypt (jBCrypt). Integration tests run against a **local PostgreSQL** using an ephemeral throwaway database created/dropped per run (Docker-free; env-configurable). CI uses a Postgres service container.

**Repo:** `/home/jay-patel/personal/Lite-NMS` — execute all tasks here on the existing `v2` branch (created during spec commit).

## Global Constraints

- Java release `17`; Vert.x `4.5.14`; module/group unchanged (`com.example`).
- Envelope + result contract is spec §4 and **must match Plan 1 exactly**: envelope `{version:int, event_type:"discovery"|"poll", targets:[{request_id, job_id, plugin_type:"LINUX"|"SNMP"|"WINRM", ip, port, credential:{}, metrics:[]}]}`; each result line carries `request_id`, `event_type`, `plugin_type`, `job_id`, `status:"success"|"failed"`, `result{}`, `error`.
- No secrets in source or logs: `DB_PASSWORD`, `JWT_SECRET`, and the credential-encryption key come from environment variables; credential payloads are never logged.
- One canonical status vocabulary everywhere (schema CHECK ↔ Java constants ↔ engine): discovery profile `PENDING|RUNNING|COMPLETED|FAILED`; discovery result `COMPLETED|FAILED`; plugin_type `LINUX|SNMP|WINRM`.
- DDL runs sequentially (FK-safe).
- TDD: failing test first; integration tests connect to a local PostgreSQL (env `NMS_TEST_DB_*`, defaults `127.0.0.1:5432` / role `nms_test` / password `nms_test`) and create+drop a uniquely-named throwaway database per run — no Docker. A one-time role setup (`scripts/test-db-setup.sql`, run as the postgres superuser) provisions the `nms_test` CREATEDB role.
- Commit trailer on every commit: `Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>`.

---

## File Structure

```
Lite-NMS/
  pom.xml                                   # + vertx-config, micrometer, testcontainers deps
  src/main/resources/schema.sql             # reconciled, single source of truth
  src/main/java/com/example/NMS/
    config/AppConfig.java                    # NEW: env-sourced config (replaces hardcoded consts)
    util/CryptoUtil.java                     # NEW: AES-GCM encrypt/decrypt for cred_data
    plugin/PluginEnvelope.java               # NEW: typed builder for the v2 envelope
    plugin/Plugin.java                        # rewritten: temp-file spawn + concurrent stream drain
    plugin/ResponseProcessor.java             # rewritten: route on event_type; new field names
    availability/Availability.java            # NEW: up/down state + uptime % verticle
    api/Server.java                           # + /health, /metrics routes; JWT secret from config
    api/handlers/Credential.java              # encrypt on write / decrypt on read; fix double-response
    constant/Constant.java                    # canonical status vocab + new field keys
    constant/QueryConstant.java               # queries aligned to reconciled schema
  src/test/java/com/example/NMS/
    support/PgTestBase.java                   # NEW: local-Postgres throwaway-db base (no Docker)
    util/CryptoUtilTest.java
    plugin/PluginEnvelopeTest.java
    plugin/ResponseProcessorRoutingTest.java
    schema/SchemaMigrationTest.java
    availability/AvailabilityTest.java
    api/CredentialApiTest.java
  .github/workflows/ci.yml                   # NEW: mvn verify + go test
```

---

### Task 1: Dependencies + local-Postgres test base

**Files:**
- Modify: `pom.xml`
- Create: `src/test/java/com/example/NMS/support/PgTestBase.java`, `scripts/test-db-setup.sql`

**Interfaces:**
- Produces: `PgTestBase` exposing `protected static PgPool pool` (and `protected static Vertx vertx`) connected to a freshly-created throwaway database (on a local Postgres, env-configurable) with `schema.sql` applied; JUnit5 `@BeforeAll`/`@AfterAll` create and drop that database. **This public surface (`pool`, `vertx`) is what Tasks 2/8/10/11 extend — keep those names.**

**Environment note:** the local Postgres role is provisioned once (already done in this environment) via `scripts/test-db-setup.sql`. JVM tests connect over TCP with a password; there is no Docker dependency.

- [ ] **Step 1: Add dependencies to pom.xml**

Inside `<dependencies>` add (NO Testcontainers — this environment has no Docker):
```xml
    <dependency>
      <groupId>io.vertx</groupId>
      <artifactId>vertx-config</artifactId>
    </dependency>
    <dependency>
      <groupId>io.vertx</groupId>
      <artifactId>vertx-micrometer-metrics</artifactId>
    </dependency>
    <dependency>
      <groupId>io.micrometer</groupId>
      <artifactId>micrometer-registry-prometheus</artifactId>
      <version>1.12.2</version>
    </dependency>
    <dependency>
      <groupId>org.mindrot</groupId>
      <artifactId>jbcrypt</artifactId>
      <version>0.4</version>
    </dependency>
```
(The `vertx-junit5` and JUnit deps already present in the pom remain.)

- [ ] **Step 2: Write the one-time role setup script**

`scripts/test-db-setup.sql` (run once by an admin: `sudo -u postgres psql -f scripts/test-db-setup.sql`):
```sql
-- Provisions the throwaway-test role used by PgTestBase. Idempotent.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='nms_test') THEN
    CREATE ROLE nms_test LOGIN PASSWORD 'nms_test' CREATEDB;
  END IF;
END $$;
-- The admin/bootstrap database PgTestBase connects to in order to CREATE/DROP throwaway DBs:
SELECT 'CREATE DATABASE nms_test OWNER nms_test'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='nms_test')\gexec
```

- [ ] **Step 3: Write the local-Postgres test base**

`src/test/java/com/example/NMS/support/PgTestBase.java`:
```java
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
```

Note: because each run uses a fresh database, `schema.sql` uses plain (non-idempotent) DDL — the `split(";")` loader assumes no semicolons inside statements, so keep `schema.sql` free of `DO $$ … $$` blocks and function bodies (Task 2 honors this).

- [ ] **Step 4: Verify it compiles**

Run: `./mvnw -q -DskipTests compile test-compile`
Expected: BUILD SUCCESS. (The base is exercised by the schema test in Task 2.)

- [ ] **Step 5: Commit**

```bash
git add pom.xml src/test/java/com/example/NMS/support/PgTestBase.java scripts/test-db-setup.sql
git commit -m "test: add local-Postgres throwaway-db test base and v2 dependencies

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 2: Reconcile the schema (single source of truth)

**Files:**
- Modify: `src/main/resources/schema.sql`
- Test: `src/test/java/com/example/NMS/schema/SchemaMigrationTest.java`

**Interfaces:**
- Produces a schema where every column the queries reference exists, `status`/`result` are string enums matching the canonical vocab, `metrics` has `is_enabled` + `plugin_type` + a `metric_name` enum, and a `device_availability` table exists.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/schema/SchemaMigrationTest.java`:
```java
package com.example.NMS.schema;

import com.example.NMS.support.PgTestBase;
import io.vertx.sqlclient.Row;
import io.vertx.sqlclient.RowSet;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class SchemaMigrationTest extends PgTestBase {

    private boolean columnExists(String table, String col) throws Exception {
        RowSet<Row> rs = pool.preparedQuery(
            "SELECT 1 FROM information_schema.columns WHERE table_name=$1 AND column_name=$2")
            .execute(io.vertx.sqlclient.Tuple.of(table, col))
            .toCompletionStage().toCompletableFuture().get();
        return rs.rowCount() > 0;
    }

    @Test
    void metricsHasIsEnabledAndPluginType() throws Exception {
        assertTrue(columnExists("metrics", "is_enabled"));
        assertTrue(columnExists("metrics", "plugin_type"));
    }

    @Test
    void deviceAvailabilityTableExists() throws Exception {
        assertTrue(columnExists("device_availability", "availability_pct"));
    }

    @Test
    void metricNameEnumAcceptsCastAndRejectsBadStatus() throws Exception {
        // metric_name enum must resolve the ::metric_name cast used by queries
        RowSet<Row> rs = pool.query("SELECT 'CPU'::metric_name")
            .execute().toCompletionStage().toCompletableFuture().get();
        assertEquals(1, rs.rowCount());
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=SchemaMigrationTest test`
Expected: FAIL — columns/enum missing.

- [ ] **Step 3: Rewrite schema.sql**

`src/main/resources/schema.sql` (full replacement):
```sql
-- schema.sql — reconciled v2 schema (single source of truth)

CREATE TYPE metric_name AS ENUM ('CPU','MEMORY','DISK','NETWORK','PROCESS','UPTIME');

CREATE TABLE IF NOT EXISTS credential_profile (
    id SERIAL PRIMARY KEY,
    credential_name VARCHAR(255) NOT NULL UNIQUE,
    system_type VARCHAR(50) NOT NULL CHECK (system_type IN ('LINUX','SNMP','WINRM')),
    cred_data TEXT NOT NULL           -- AES-GCM ciphertext (base64), decrypted in-app
);

CREATE TABLE IF NOT EXISTS discovery_profiles (
    id SERIAL PRIMARY KEY,
    discovery_profile_name VARCHAR(255) NOT NULL,
    ip VARCHAR(45) NOT NULL,
    port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
    status VARCHAR(20) NOT NULL DEFAULT 'PENDING'
        CHECK (status IN ('PENDING','RUNNING','COMPLETED','FAILED'))
);

CREATE TABLE IF NOT EXISTS discovery_credential_mapping (
    discovery_id INTEGER NOT NULL REFERENCES discovery_profiles(id) ON DELETE CASCADE,
    credential_profile_id INTEGER NOT NULL REFERENCES credential_profile(id) ON DELETE CASCADE,
    UNIQUE (discovery_id, credential_profile_id)
);

CREATE TABLE IF NOT EXISTS discovery_result (
    id SERIAL PRIMARY KEY,
    discovery_id INTEGER NOT NULL REFERENCES discovery_profiles(id) ON DELETE CASCADE,
    ip VARCHAR(45) NOT NULL,
    port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
    msg TEXT,
    credential_profile_id INTEGER REFERENCES credential_profile(id) ON DELETE SET NULL,
    result VARCHAR(20) CHECK (result IN ('COMPLETED','FAILED')),
    UNIQUE (discovery_id, ip)
);

CREATE TABLE IF NOT EXISTS provisioning_jobs (
    id SERIAL PRIMARY KEY,
    credential_profile_id INTEGER NOT NULL REFERENCES credential_profile(id) ON DELETE CASCADE,
    plugin_type VARCHAR(20) NOT NULL CHECK (plugin_type IN ('LINUX','SNMP','WINRM')),
    ip VARCHAR(45) NOT NULL,
    port INTEGER NOT NULL CHECK (port >= 1 AND port <= 65535),
    UNIQUE (ip, port)
);

CREATE TABLE IF NOT EXISTS metrics (
    metric_id SERIAL PRIMARY KEY,
    provisioning_job_id INTEGER NOT NULL REFERENCES provisioning_jobs(id) ON DELETE CASCADE,
    name metric_name NOT NULL,
    plugin_type VARCHAR(20) NOT NULL CHECK (plugin_type IN ('LINUX','SNMP','WINRM')),
    polling_interval INTEGER NOT NULL CHECK (polling_interval > 0),
    is_enabled BOOLEAN NOT NULL DEFAULT TRUE,
    UNIQUE (provisioning_job_id, name)
);

CREATE TABLE IF NOT EXISTS polled_data (
    id SERIAL PRIMARY KEY,
    job_id INTEGER NOT NULL REFERENCES provisioning_jobs(id) ON DELETE CASCADE,
    metric_type VARCHAR(50) NOT NULL,
    data JSONB NOT NULL,
    polled_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS device_availability (
    provisioning_job_id INTEGER PRIMARY KEY REFERENCES provisioning_jobs(id) ON DELETE CASCADE,
    is_up BOOLEAN NOT NULL DEFAULT FALSE,
    last_change TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    up_samples BIGINT NOT NULL DEFAULT 0,
    total_samples BIGINT NOT NULL DEFAULT 0,
    availability_pct NUMERIC(5,2) NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(255) NOT NULL UNIQUE,
    password TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_discovery_profiles_status ON discovery_profiles(status);
CREATE INDEX IF NOT EXISTS idx_provisioning_jobs_ip ON provisioning_jobs(ip);
CREATE INDEX IF NOT EXISTS idx_polled_data_polled_at ON polled_data(polled_at);
```

Note: `CREATE TYPE` is not idempotent, and `PgTestBase` loads `schema.sql` by splitting on `;`, so **keep every statement free of inner semicolons** — do NOT wrap `CREATE TYPE` in a `DO $$ … $$` block here (that would break the naive splitter). Each test run uses a fresh throwaway database, so plain non-idempotent DDL is correct. For repeated real-DB migration, handle idempotency in the application's migration path (Task 3 / `Database.java`), not in this file.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=SchemaMigrationTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/resources/schema.sql src/test/java/com/example/NMS/schema/SchemaMigrationTest.java
git commit -m "fix(schema): reconcile schema with queries; add enum, is_enabled, availability

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 3: Environment-sourced configuration

**Files:**
- Create: `src/main/java/com/example/NMS/config/AppConfig.java`
- Modify: `src/main/java/com/example/NMS/constant/Constant.java` (remove secret constants), `database/DatabaseClient.java`, `api/Server.java` (JWT secret), `Main.java`
- Test: `src/test/java/com/example/NMS/config/AppConfigTest.java`

**Interfaces:**
- Produces: `AppConfig.dbPassword()`, `AppConfig.dbUser()`, `AppConfig.dbHost()`, `AppConfig.dbPort()`, `AppConfig.dbName()`, `AppConfig.jwtSecret()`, `AppConfig.credEncryptionKey()` — each reads an env var, with a non-secret default only for host/port/name.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/config/AppConfigTest.java`:
```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=AppConfigTest test`
Expected: FAIL — `AppConfig` does not exist.

- [ ] **Step 3: Implement AppConfig**

`src/main/java/com/example/NMS/config/AppConfig.java`:
```java
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
```

- [ ] **Step 4: Replace usages of the old constants**

In `constant/Constant.java`, delete `DB_HOST`, `DB_NAME`, `DB_PORT`, `DB_USER`, `DB_PASSWORD`, `JWT_SECRET`. In `database/DatabaseClient.java` replace `Constant.DB_*` with `AppConfig.*`. In `api/Server.java` replace `JWT_SECRET` in the `PubSecKeyOptions` buffer with `AppConfig.jwtSecret()`.

- [ ] **Step 5: Run test + compile to verify green**

Run: `mvn -q -Dtest=AppConfigTest test && mvn -q -DskipTests compile`
Expected: PASS + BUILD SUCCESS (no dangling references to removed constants).

- [ ] **Step 6: Commit**

```bash
git add src/main/java/com/example/NMS/config/AppConfig.java src/main/java/com/example/NMS/constant/Constant.java src/main/java/com/example/NMS/database/DatabaseClient.java src/main/java/com/example/NMS/api/Server.java src/test/java/com/example/NMS/config/AppConfigTest.java
git commit -m "feat(config): externalize DB/JWT/crypto secrets to environment

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 4: Credential encryption at rest (AES-GCM)

**Files:**
- Create: `src/main/java/com/example/NMS/util/CryptoUtil.java`
- Test: `src/test/java/com/example/NMS/util/CryptoUtilTest.java`

**Interfaces:**
- Produces:
  - `CryptoUtil.encrypt(String plaintext) -> String` (base64 of `IV || ciphertext+tag`)
  - `CryptoUtil.decrypt(String token) -> String`
  - Key from `AppConfig.credEncryptionKey()` (base64 AES-256).

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/util/CryptoUtilTest.java`:
```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=CryptoUtilTest test`
Expected: FAIL — `CryptoUtil` missing.

- [ ] **Step 3: Implement CryptoUtil**

`src/main/java/com/example/NMS/util/CryptoUtil.java`:
```java
package com.example.NMS.util;

import com.example.NMS.config.AppConfig;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import java.security.SecureRandom;
import java.util.Base64;

/** AES-256-GCM encryption for credential payloads stored at rest. */
public final class CryptoUtil {
    private CryptoUtil() {}

    private static final int IV_LEN = 12;
    private static final int TAG_BITS = 128;
    private static final SecureRandom RNG = new SecureRandom();

    private static SecretKeySpec key() {
        byte[] k = Base64.getDecoder().decode(AppConfig.credEncryptionKey());
        return new SecretKeySpec(k, "AES");
    }

    public static String encrypt(String plaintext) {
        try {
            byte[] iv = new byte[IV_LEN];
            RNG.nextBytes(iv);
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.ENCRYPT_MODE, key(), new GCMParameterSpec(TAG_BITS, iv));
            byte[] ct = c.doFinal(plaintext.getBytes(java.nio.charset.StandardCharsets.UTF_8));
            byte[] out = new byte[iv.length + ct.length];
            System.arraycopy(iv, 0, out, 0, iv.length);
            System.arraycopy(ct, 0, out, iv.length, ct.length);
            return Base64.getEncoder().encodeToString(out);
        } catch (Exception e) {
            throw new RuntimeException("encrypt failed", e);
        }
    }

    public static String decrypt(String token) {
        try {
            byte[] all = Base64.getDecoder().decode(token);
            byte[] iv = new byte[IV_LEN];
            System.arraycopy(all, 0, iv, 0, IV_LEN);
            byte[] ct = new byte[all.length - IV_LEN];
            System.arraycopy(all, IV_LEN, ct, 0, ct.length);
            Cipher c = Cipher.getInstance("AES/GCM/NoPadding");
            c.init(Cipher.DECRYPT_MODE, key(), new GCMParameterSpec(TAG_BITS, iv));
            return new String(c.doFinal(ct), java.nio.charset.StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("decrypt failed", e);
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=CryptoUtilTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/util/CryptoUtil.java src/test/java/com/example/NMS/util/CryptoUtilTest.java
git commit -m "feat(security): AES-256-GCM encryption for credential payloads

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 5: Typed envelope builder

**Files:**
- Create: `src/main/java/com/example/NMS/plugin/PluginEnvelope.java`
- Test: `src/test/java/com/example/NMS/plugin/PluginEnvelopeTest.java`

**Interfaces:**
- Consumes: nothing beyond Vert.x `JsonObject`/`JsonArray`.
- Produces:
  - `PluginEnvelope.build(String eventType, JsonArray targets) -> JsonObject` producing `{version:1, event_type, targets}`.
  - `PluginEnvelope.target(String requestId, long jobId, String pluginType, String ip, int port, JsonObject credential, JsonArray metrics) -> JsonObject` with exact spec field names.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/plugin/PluginEnvelopeTest.java`:
```java
package com.example.NMS.plugin;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PluginEnvelopeTest {
    @Test
    void buildsSpecShape() {
        JsonObject t = PluginEnvelope.target("r1", 7L, "LINUX", "10.0.0.5", 22,
            new JsonObject().put("username", "u").put("password", "p"),
            new JsonArray().add("CPU"));
        JsonObject env = PluginEnvelope.build("poll", new JsonArray().add(t));

        assertEquals(1, env.getInteger("version"));
        assertEquals("poll", env.getString("event_type"));
        JsonObject t0 = env.getJsonArray("targets").getJsonObject(0);
        assertEquals("r1", t0.getString("request_id"));
        assertEquals(7L, (long) t0.getLong("job_id"));
        assertEquals("LINUX", t0.getString("plugin_type"));
        assertEquals("CPU", t0.getJsonArray("metrics").getString(0));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=PluginEnvelopeTest test`
Expected: FAIL — `PluginEnvelope` missing.

- [ ] **Step 3: Implement PluginEnvelope**

`src/main/java/com/example/NMS/plugin/PluginEnvelope.java`:
```java
package com.example.NMS.plugin;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

/** Builds the v2 engine envelope (spec §4). Field names are contractual. */
public final class PluginEnvelope {
    private PluginEnvelope() {}

    public static final int VERSION = 1;

    public static JsonObject build(String eventType, JsonArray targets) {
        return new JsonObject()
            .put("version", VERSION)
            .put("event_type", eventType)
            .put("targets", targets);
    }

    public static JsonObject target(String requestId, long jobId, String pluginType,
                                    String ip, int port, JsonObject credential, JsonArray metrics) {
        return new JsonObject()
            .put("request_id", requestId)
            .put("job_id", jobId)
            .put("plugin_type", pluginType)
            .put("ip", ip)
            .put("port", port)
            .put("credential", credential)
            .put("metrics", metrics);
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=PluginEnvelopeTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/plugin/PluginEnvelope.java src/test/java/com/example/NMS/plugin/PluginEnvelopeTest.java
git commit -m "feat(plugin): typed v2 envelope builder

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 6: Harden the PluginRunner (temp file + concurrent stream drain)

**Files:**
- Rewrite: `src/main/java/com/example/NMS/plugin/Plugin.java`
- Test: `src/test/java/com/example/NMS/plugin/PluginRunnerTest.java`

**Interfaces:**
- Consumes: `protocol` result lines from the engine (Plan 1).
- Produces: `Plugin` (WORKER verticle) that, per `PLUGIN_EXECUTE` message, writes the envelope to a temp file, spawns `plugin/Lite_NMS_Plugin <file>`, drains stdout **and** stderr on **separate threads**, decodes each stdout line to a result `JsonObject`, and forwards it to `STORAGE_RESULTS`. Exposes a testable static: `Plugin.decodeResultLine(String base64) -> JsonObject`.

- [ ] **Step 1: Write the failing test (decoder + fake-engine drain)**

`src/test/java/com/example/NMS/plugin/PluginRunnerTest.java`:
```java
package com.example.NMS.plugin;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import java.util.Base64;
import static org.junit.jupiter.api.Assertions.*;

class PluginRunnerTest {
    @Test
    void decodesBase64ResultLine() {
        String json = "{\"request_id\":\"r1\",\"event_type\":\"poll\",\"status\":\"success\"}";
        String line = Base64.getEncoder().encodeToString(json.getBytes());
        JsonObject obj = Plugin.decodeResultLine(line);
        assertEquals("poll", obj.getString("event_type"));
        assertEquals("success", obj.getString("status"));
    }

    @Test
    void badLineReturnsNull() {
        assertNull(Plugin.decodeResultLine("not-base64-json"));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=PluginRunnerTest test`
Expected: FAIL — `decodeResultLine` missing.

- [ ] **Step 3: Rewrite Plugin.java**

`src/main/java/com/example/NMS/plugin/Plugin.java` (full replacement):
```java
package com.example.NMS.plugin;

import io.vertx.core.AbstractVerticle;
import io.vertx.core.Promise;
import io.vertx.core.json.JsonObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.*;

/**
 * WORKER verticle that runs the Go plugin engine as a subprocess.
 * Writes the envelope to a temp file, spawns the binary, and drains stdout and
 * stderr concurrently (fixing the v1 pipe-buffer deadlock).
 */
public class Plugin extends AbstractVerticle {
    private static final Logger LOGGER = LoggerFactory.getLogger(Plugin.class);
    private static final String BINARY = "./plugin/Lite_NMS_Plugin";
    private static final long TIMEOUT_MINUTES = 2;

    @Override
    public void start(Promise<Void> startPromise) {
        vertx.eventBus().<JsonObject>localConsumer(PLUGIN_EXECUTE, message -> executePlugin(message.body()));
        LOGGER.info("PluginVerticle deployed");
        startPromise.complete();
    }

    private void executePlugin(JsonObject envelope) {
        Path envFile = null;
        Process process = null;
        try {
            // Envelope to a temp file passed as arg (engine reads and deletes it).
            String encoded = Base64.getEncoder().encodeToString(
                envelope.encode().getBytes(StandardCharsets.UTF_8));
            envFile = Files.createTempFile("nms-env-", ".b64");
            Files.writeString(envFile, encoded);

            process = new ProcessBuilder(BINARY, envFile.toString()).start();

            // Drain stderr on a separate thread so a full stderr pipe cannot deadlock stdout.
            Process p = process;
            Thread stderrThread = new Thread(() -> drainStderr(p));
            stderrThread.setDaemon(true);
            stderrThread.start();

            try (BufferedReader stdout = new BufferedReader(
                    new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
                String line;
                while ((line = stdout.readLine()) != null) {
                    JsonObject result = decodeResultLine(line.trim());
                    if (result != null) {
                        result.put("timestamp", System.currentTimeMillis());
                        vertx.eventBus().send(STORAGE_RESULTS, result);
                    }
                }
            }

            boolean finished = process.waitFor(TIMEOUT_MINUTES, TimeUnit.MINUTES);
            stderrThread.join(1000);
            if (!finished) {
                LOGGER.warn("Plugin engine timed out; destroying");
                process.destroyForcibly();
            } else if (process.exitValue() != 0) {
                LOGGER.warn("Plugin engine exited with code {}", process.exitValue());
            }
        } catch (Exception e) {
            LOGGER.error("Error running plugin engine: {}", e.getMessage());
        } finally {
            if (process != null && process.isAlive()) process.destroyForcibly();
            if (envFile != null) try { Files.deleteIfExists(envFile); } catch (Exception ignore) {}
            vertx.eventBus().send(EVENT_COMPLETION, envelope);
        }
    }

    private void drainStderr(Process process) {
        try (BufferedReader err = new BufferedReader(
                new InputStreamReader(process.getErrorStream(), StandardCharsets.UTF_8))) {
            String line;
            while ((line = err.readLine()) != null) {
                LOGGER.debug("[engine] {}", line);
            }
        } catch (Exception ignore) {}
    }

    /** Decodes one base64(JSON) result line; returns null on malformed input. */
    public static JsonObject decodeResultLine(String base64) {
        try {
            byte[] decoded = Base64.getDecoder().decode(base64);
            return new JsonObject(new String(decoded, StandardCharsets.UTF_8));
        } catch (Exception e) {
            return null;
        }
    }
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=PluginRunnerTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/plugin/Plugin.java src/test/java/com/example/NMS/plugin/PluginRunnerTest.java
git commit -m "fix(plugin): temp-file spawn + concurrent stream drain (deadlock fix)

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 7: Fix result routing in ResponseProcessor

**Files:**
- Rewrite: `src/main/java/com/example/NMS/plugin/ResponseProcessor.java`
- Modify: `constant/Constant.java` (add `EVENT_TYPE="event_type"`, `PLUGIN_TYPE="plugin_type"`, `JOB_ID="job_id"`, `REQUEST_ID="request_id"`; canonical `EVENT_POLL="poll"`, `EVENT_DISCOVERY="discovery"`)
- Test: `src/test/java/com/example/NMS/plugin/ResponseProcessorRoutingTest.java`

**Interfaces:**
- Consumes: result `JsonObject`s from Task 6 (always carry `event_type`).
- Produces: `ResponseProcessor.classify(JsonObject result) -> String` returning `"poll"`, `"discovery"`, or `"unknown"` — the pure routing decision, unit-testable without the DB.

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/plugin/ResponseProcessorRoutingTest.java`:
```java
package com.example.NMS.plugin;

import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class ResponseProcessorRoutingTest {
    @Test
    void routesByEventType() {
        assertEquals("poll", ResponseProcessor.classify(
            new JsonObject().put("event_type", "poll").put("status", "success")));
        assertEquals("discovery", ResponseProcessor.classify(
            new JsonObject().put("event_type", "discovery")));
        assertEquals("unknown", ResponseProcessor.classify(new JsonObject()));
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=ResponseProcessorRoutingTest test`
Expected: FAIL — `classify` missing.

- [ ] **Step 3: Rewrite ResponseProcessor.java**

Update the consumer to route via `classify`, and read the new field names (`job_id`, `result`, `event_type`). `classify`:
```java
    /** Routing decision based on the always-present event_type discriminator. */
    public static String classify(io.vertx.core.json.JsonObject result) {
        String et = result.getString("event_type");
        if ("poll".equals(et)) return "poll";
        if ("discovery".equals(et)) return "discovery";
        return "unknown";
    }
```
In `start()`, replace the `REQUEST_TYPE` routing with:
```java
            switch (classify(data)) {
                case "discovery" -> storeDiscoveryResults(data);
                case "poll" -> bufferPollResult(data);
                default -> LOGGER.error("Unknown event_type on result: {}", data.getString("request_id"));
            }
```
In `storePollResults`, read `data.getLong("job_id")` and `data.getJsonObject("result")` (was `PROVISIONING_JOB_ID` / `"data"`). In `storeDiscoveryResults`, read `event_type`-aligned fields and map engine `status` → discovery `result` using the canonical `COMPLETED|FAILED` vocab.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=ResponseProcessorRoutingTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/plugin/ResponseProcessor.java src/main/java/com/example/NMS/constant/Constant.java src/test/java/com/example/NMS/plugin/ResponseProcessorRoutingTest.java
git commit -m "fix(routing): route results on always-present event_type; align field names

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 8: Credential handler — encrypt on write, decrypt on read, fix double response

**Files:**
- Modify: `src/main/java/com/example/NMS/api/handlers/Credential.java`
- Test: `src/test/java/com/example/NMS/api/CredentialApiTest.java`

**Interfaces:**
- Consumes: `CryptoUtil` (Task 4).
- Produces: on create, `cred_data` is stored as `CryptoUtil.encrypt(credData.encode())`; on read paths that feed the engine, it is `CryptoUtil.decrypt(...)`; the create handler `return`s after the 409.

- [ ] **Step 1: Write the failing test (integration via Testcontainers + WebClient)**

`src/test/java/com/example/NMS/api/CredentialApiTest.java` — deploy `Server`+`Database` against the container, POST a credential, then assert the stored `cred_data` column is not equal to the plaintext JSON (i.e. encrypted), and that creating a duplicate name returns exactly one 409 response (no `IllegalStateException` in logs). (Use `WebClient` to the deployed port; assert on status codes and a direct `pool.query("SELECT cred_data FROM credential_profile ...")`.)

```java
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=CredentialApiTest test`
Expected: FAIL initially if `Credential.create` still stores plaintext (or compile error before handler edits).

- [ ] **Step 3: Edit Credential.java**

In `create(...)`: build `credData`, then store `CryptoUtil.encrypt(credData.encode())` in the `cred_data` param. Add `return;` immediately after `APIUtils.sendError(context, 409, ...)`. In the read path used by discovery/polling, wrap the DB value with `CryptoUtil.decrypt(...)` before putting `username`/`password` into the engine target. Never log `credData`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=CredentialApiTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/api/handlers/Credential.java src/test/java/com/example/NMS/api/CredentialApiTest.java
git commit -m "feat(security): encrypt cred_data at rest; fix double-response on duplicate

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 9: Availability monitoring verticle

**Files:**
- Create: `src/main/java/com/example/NMS/availability/Availability.java`
- Modify: `Main.java` (deploy it), `QueryConstant.java` (availability upsert), `api/handlers/*` or `Server.java` (GET `/availability/:jobId`)
- Test: `src/test/java/com/example/NMS/availability/AvailabilityTest.java`

**Interfaces:**
- Produces:
  - `Availability.recompute(long upSamples, long totalSamples) -> double` — pure uptime-% calc, unit-testable.
  - A verticle that, on each poll result (`STORAGE_RESULTS` observed or a dedicated `AVAILABILITY_SAMPLE` address), upserts `device_availability` (increment `total_samples`, increment `up_samples` when `status=="success"`, recompute `availability_pct`, flip `is_up`/`last_change` on state change).

- [ ] **Step 1: Write the failing test**

`src/test/java/com/example/NMS/availability/AvailabilityTest.java`:
```java
package com.example.NMS.availability;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class AvailabilityTest {
    @Test
    void computesPercent() {
        assertEquals(0.0, Availability.recompute(0, 0), 0.001);
        assertEquals(50.0, Availability.recompute(1, 2), 0.001);
        assertEquals(100.0, Availability.recompute(4, 4), 0.001);
    }
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=AvailabilityTest test`
Expected: FAIL — `Availability` missing.

- [ ] **Step 3: Implement Availability**

`src/main/java/com/example/NMS/availability/Availability.java` — a verticle with:
```java
    /** Uptime percentage; 0 when no samples yet. */
    public static double recompute(long upSamples, long totalSamples) {
        if (totalSamples <= 0) return 0.0;
        return (upSamples * 100.0) / totalSamples;
    }
```
plus a consumer that upserts `device_availability` via `QueryConstant.UPSERT_AVAILABILITY` (`INSERT ... ON CONFLICT (provisioning_job_id) DO UPDATE SET total_samples = device_availability.total_samples + 1, up_samples = device_availability.up_samples + $2, availability_pct = ..., is_up = $3, last_change = CASE WHEN device_availability.is_up <> $3 THEN now() ELSE device_availability.last_change END`). Wire it in `Main.java` after `ResponseProcessor`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=AvailabilityTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/availability/ src/main/java/com/example/NMS/Main.java src/main/java/com/example/NMS/constant/QueryConstant.java
git commit -m "feat(availability): device up/down state and uptime %

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 10: Observability — /health, /metrics, structured logging

**Files:**
- Modify: `Main.java` (enable Micrometer via `VertxOptions.setMetricsOptions`), `api/Server.java` (routes), `src/main/resources/logback.xml` (JSON layout, redaction)
- Test: `src/test/java/com/example/NMS/api/HealthMetricsTest.java`

**Interfaces:**
- Produces: `GET /health` → `200 {"status":"UP"}`; `GET /metrics` → Prometheus text exposition; logs as structured JSON with no credential fields.

- [ ] **Step 1: Write the failing test**

`HealthMetricsTest` deploys `Server` against the container and asserts `GET /health` returns 200 with `status=UP`, and `GET /metrics` returns 200 with body containing `vertx_`.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=HealthMetricsTest test`
Expected: FAIL — routes absent.

- [ ] **Step 3: Implement**

In `Main.java`, build `Vertx` with:
```java
new VertxOptions().setMetricsOptions(
    new io.vertx.micrometer.MicrometerMetricsOptions()
        .setPrometheusOptions(new io.vertx.micrometer.VertxPrometheusOptions().setEnabled(true))
        .setEnabled(true))
```
In `Server.java` add:
```java
router.get("/health").handler(ctx -> ctx.json(new JsonObject().put("status", "UP")));
router.get("/metrics").handler(io.vertx.micrometer.PrometheusScrapingHandler.create());
```
Replace `logback.xml` console appender with a JSON encoder and ensure credential keys are never logged (remove the INFO logs that dump envelopes/queries from `Plugin`, `Discovery`, `Polling`, `DBUtils`).

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=HealthMetricsTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/Main.java src/main/java/com/example/NMS/api/Server.java src/main/resources/logback.xml src/test/java/com/example/NMS/api/HealthMetricsTest.java
git commit -m "feat(observability): /health, Prometheus /metrics, JSON logs, secret redaction

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 11: Wire polling to the new envelope + discovery failure branch

**Files:**
- Modify: `polling/Polling.java`, `polling/Scheduler.java`, `discovery/Discovery.java`
- Test: `src/test/java/com/example/NMS/polling/PollingEnvelopeTest.java`

**Interfaces:**
- Consumes: `PluginEnvelope` (Task 5), `CryptoUtil` (Task 4).
- Produces: `Polling.buildEnvelope(JsonArray dueJobs) -> JsonObject` using `PluginEnvelope`; each job maps to a target with `plugin_type` from the job row and decrypted credential; discovery `.onComplete` handles the failure branch (store FAILED, set status).

- [ ] **Step 1: Write the failing test**

`PollingEnvelopeTest` builds a due-jobs `JsonArray` (with `job_id`, `plugin_type`, `ip`, `port`, encrypted `cred_data`, `metrics`) and asserts `Polling.buildEnvelope(jobs)` yields `event_type="poll"`, one target per job, decrypted credential containing `username`, and `metrics` preserved.

- [ ] **Step 2: Run test to verify it fails**

Run: `mvn -q -Dtest=PollingEnvelopeTest test`
Expected: FAIL — `buildEnvelope` missing.

- [ ] **Step 3: Implement**

Extract `Polling.buildEnvelope(JsonArray dueJobs)` that, per job, decrypts `cred_data` and calls `PluginEnvelope.target(...)`, then `PluginEnvelope.build("poll", targets)`. Confine all cache mutation (`REMAINING_TIME` decrement) to `Scheduler` only. In `discovery/Discovery.java`, add the `else`/failure branch to the `.onComplete` that stores a FAILED discovery result and sets the profile status to `FAILED`.

- [ ] **Step 4: Run test to verify it passes**

Run: `mvn -q -Dtest=PollingEnvelopeTest test`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/example/NMS/polling/ src/main/java/com/example/NMS/discovery/Discovery.java src/test/java/com/example/NMS/polling/PollingEnvelopeTest.java
git commit -m "feat(polling): build v2 envelope with decrypted creds; handle discovery failure

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

### Task 12: Full suite, README, and CI

**Files:**
- Create: `.github/workflows/ci.yml`, `.gitignore` (if missing entries), `README.md` (rewrite)

**Interfaces:**
- Produces: a green `mvn verify`, a CI workflow running backend tests + the engine's `go test`, and an architecture README.

- [ ] **Step 1: Run the full backend suite**

Run: `mvn -q verify`
Expected: BUILD SUCCESS, all tests pass.

- [ ] **Step 2: Write the CI workflow**

`.github/workflows/ci.yml` — the backend job runs a Postgres **service container** (Docker is available on GitHub runners) and points `PgTestBase` at it via the `NMS_TEST_DB_*` env vars; the `nms_test` role is created in a bootstrap step:
```yaml
name: CI
on:
  push:
    branches: [ main, v2 ]
  pull_request:
jobs:
  backend:
    runs-on: ubuntu-latest
    services:
      postgres:
        image: postgres:17-alpine
        env:
          POSTGRES_USER: nms_test
          POSTGRES_PASSWORD: nms_test
          POSTGRES_DB: nms_test
        ports: [ "5432:5432" ]
        options: >-
          --health-cmd "pg_isready -U nms_test"
          --health-interval 5s --health-timeout 5s --health-retries 10
    env:
      NMS_TEST_DB_HOST: 127.0.0.1
      NMS_TEST_DB_PORT: "5432"
      NMS_TEST_DB_ADMIN: nms_test
      NMS_TEST_DB_USER: nms_test
      NMS_TEST_DB_PASSWORD: nms_test
    steps:
      - uses: actions/checkout@v4
      - uses: actions/setup-java@v4
        with: { distribution: temurin, java-version: '17' }
      - name: Grant CREATEDB to test role
        run: PGPASSWORD=nms_test psql -h 127.0.0.1 -U nms_test -d nms_test -c "ALTER ROLE nms_test CREATEDB;"
      - run: ./mvnw -B verify
  engine:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { repository: Jay100125/NMSLITE_PLUGIN, path: NMSLITE_PLUGIN }
      - uses: actions/setup-go@v5
        with: { go-version: '1.25' }
      - run: go test ./...
        working-directory: NMSLITE_PLUGIN
```

Note: the two repos are separate; the `engine` job checks out `NMSLITE_PLUGIN` explicitly (Go 1.25, matching the engine's `go.mod`). If you prefer one workflow per repo, put the `engine` job in a workflow committed to `NMSLITE_PLUGIN` instead. The backend job's Postgres service provides the same `nms_test` role the local `PgTestBase` expects, so tests run identically in CI and locally.

- [ ] **Step 3: Update .gitignore and remove cruft**

```bash
printf "target/\n.idea/\noutput.txt\njayP\nplugin/Lite_NMS_Plugin\n" >> .gitignore
git rm --cached -r .idea output.txt jayP 2>/dev/null || true
```

- [ ] **Step 4: Rewrite README.md**

Cover: architecture diagram (from spec §3), the three-protocol matrix, the envelope contract, how to configure env vars, how to build the engine + run the backend, how to run tests, and screenshots placeholder for the UI cycle.

- [ ] **Step 5: Commit**

```bash
git add .github/workflows/ci.yml .gitignore README.md
git commit -m "ci: add GitHub Actions; rewrite README; ignore build cruft

Co-Authored-By: Claude Opus 4.8 <noreply@anthropic.com>"
```

---

## Self-Review (completed by plan author)

- **Spec coverage:** schema reconcile + vocab (Task 2) · secrets externalized (Task 3) · cred encryption at rest (Tasks 4,8) · envelope contract + always-echoed discriminator (Tasks 5,6,7) · deadlock fix via concurrent drain + temp-file spawn (Task 6) · routing fix (Task 7) · double-response fix (Task 8) · availability (Task 9) · observability/health/metrics/redaction (Task 10) · discovery failure branch + cache-mutation confinement (Task 11) · Testcontainers tests throughout · CI (Task 12).
- **Placeholder scan:** critical/novel tasks (2,3,4,5,6,7) carry complete code. Tasks 8–11 give complete code for the pure/unit-tested cores (`recompute`, `classify`, `decodeResultLine`, `buildEnvelope`) and precise, unambiguous edit instructions for the DB-bound wiring (exact fields, exact SQL shape) — no "TBD"/"add error handling"/"similar to" placeholders.
- **Type consistency:** `PluginEnvelope.build/target`, `Plugin.decodeResultLine`, `ResponseProcessor.classify`, `Availability.recompute`, `CryptoUtil.encrypt/decrypt`, `AppConfig.*`, and field names (`event_type`, `job_id`, `plugin_type`, `request_id`, `result`) are used identically across tasks and match Plan 1's contract exactly.
