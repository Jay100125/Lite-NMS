# Discovery Realtime Backend Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Multi-protocol (LINUX/SNMP/WINRM) discovery with staged per-IP progress events published over a JWT-guarded SockJS event-bus bridge.

**Architecture:** Discovery keeps its existing pipeline (resolve IPs → fping → TCP port check → Go plugin) but publishes a progress event to `nms.discovery.<id>` at each stage boundary; a SockJS bridge at `/eventbus/*` exposes those addresses outbound-only. `plugin_type` moves from a hardcoded `"LINUX"` to a column on `discovery_profiles`, and credential payloads become type-shaped (`{username,password}` vs `{version,community}`).

**Tech Stack:** Java 21, Vert.x 4.5.14 (`vertx-web` already includes SockJS), PostgreSQL, JUnit 5, PgTestBase throwaway-DB tests.

**Spec:** `docs/superpowers/specs/2026-07-07-discovery-realtime-redesign-design.md`

## Global Constraints

- Commit as Jay Patel <jaypatel100125@gmail.com> (already the repo-local config).
- IP boundary: the Motadata repos under `/home/jay-patel/workspace/` are reference-only; never copy code from them.
- Tests bind :8080 — make sure no dev backend is running before `./mvnw verify`.
- Tests must stay hermetic: never invoke `fping` or open real sockets in tests — test the pure stage helpers instead.
- DB tests extend `src/test/java/com/example/NMS/support/PgTestBase.java` (local Postgres, role `nms_test`/`nms_test`).
- `schema.sql` is split on `;` and applied sequentially with duplicate-tolerance — every DDL statement must be single-statement and restart-safe.
- Run `./mvnw verify` (all tests green) before every commit.

---

### Task 1: `discovery_profiles.plugin_type` column + queries

**Files:**
- Modify: `src/main/resources/schema.sql` (after the `discovery_profiles` CREATE TABLE, ~line 19)
- Modify: `src/main/java/com/example/NMS/constant/QueryConstant.java:38-40,95-114`
- Modify: `src/main/java/com/example/NMS/constant/Constant.java`
- Test: `src/test/java/com/example/NMS/schema/SchemaMigrationTest.java`

**Interfaces:**
- Produces: `discovery_profiles.plugin_type` column (`'LINUX'|'SNMP'|'WINRM'`, default `'LINUX'`); `INSERT_DISCOVERY` takes params `(name, ip, port, plugin_type)`; `UPDATE_DISCOVERY` takes `(name, ip, port, plugin_type, id)`; `GET_BY_RUN_ID` row gains `plugin_type`; constants `PLUGIN_LINUX/PLUGIN_SNMP/PLUGIN_WINRM/COMMUNITY/SNMP_VERSION/SNMP_V2C`.

- [ ] **Step 1: Write the failing test** — add to `SchemaMigrationTest.java`:

```java
@Test
void discoveryProfilesHasPluginType() throws Exception {
    assertTrue(columnExists("discovery_profiles", "plugin_type"));
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw -q test -Dtest=SchemaMigrationTest`
Expected: FAIL — `discoveryProfilesHasPluginType` asserts false (column missing).

- [ ] **Step 3: Add the column to `schema.sql`** — insert this single statement right after the `discovery_profiles` CREATE TABLE block:

```sql
ALTER TABLE discovery_profiles ADD COLUMN IF NOT EXISTS plugin_type VARCHAR(20) NOT NULL DEFAULT 'LINUX' CHECK (plugin_type IN ('LINUX','SNMP','WINRM'));
```

(`IF NOT EXISTS` makes re-boots a no-op, so the inline CHECK is never re-added.)

- [ ] **Step 4: Update the three queries in `QueryConstant.java`:**

```java
public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, ip, port, plugin_type, status) VALUES ($1, $2, $3, $4, 'PENDING') RETURNING id";

public static final String UPDATE_DISCOVERY = "UPDATE discovery_profiles SET discovery_profile_name = $1, ip = $2, port = $3, plugin_type = $4, status = 'PENDING' WHERE id = $5 RETURNING id";
```

In `GET_BY_RUN_ID`, add `dp.plugin_type AS plugin_type,` after `dp.port AS port,` in the SELECT list, and append `, dp.plugin_type` to the trailing `GROUP BY`.

- [ ] **Step 5: Add constants to `Constant.java`:**

```java
public static final String PLUGIN_LINUX = "LINUX";

public static final String PLUGIN_SNMP = "SNMP";

public static final String PLUGIN_WINRM = "WINRM";

public static final String COMMUNITY = "community";

public static final String SNMP_VERSION = "version";

public static final String SNMP_V2C = "2c";
```

- [ ] **Step 6: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=SchemaMigrationTest`
Expected: PASS (all tests in class).

- [ ] **Step 7: Full verify + commit**

```bash
./mvnw -q verify
git add src/main/resources/schema.sql src/main/java/com/example/NMS/constant/QueryConstant.java src/main/java/com/example/NMS/constant/Constant.java src/test/java/com/example/NMS/schema/SchemaMigrationTest.java
git commit -m "feat(schema): add plugin_type to discovery_profiles and wire it into queries"
```

---

### Task 2: Per-type credential validation

**Files:**
- Modify: `src/main/java/com/example/NMS/api/handlers/Credential.java:62-137` (create), `:146-180` (update)
- Test: Create `src/test/java/com/example/NMS/api/CredentialCredDataTest.java`

**Interfaces:**
- Produces: `public static String Credential.credDataError(String systemType, JsonObject credData)` — returns an error message or null when valid; `public static JsonObject Credential.normalizeCredData(String systemType, JsonObject credData)` — returns the object to encrypt (SNMP gets `{version:"2c", community}`).
- Consumes: constants from Task 1.

- [ ] **Step 1: Write the failing test** — `CredentialCredDataTest.java`:

```java
package com.example.NMS.api;

import com.example.NMS.api.handlers.Credential;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/** Unit tests for the per-system_type cred_data contract (spec: multi-protocol section). */
class CredentialCredDataTest
{
    @Test
    void linuxAndWinrmRequireUserAndPassword()
    {
        var valid = new JsonObject().put(USER, "root").put(PASSWORD, "pw");

        assertNull(Credential.credDataError(PLUGIN_LINUX, valid));
        assertNull(Credential.credDataError(PLUGIN_WINRM, valid));

        assertNotNull(Credential.credDataError(PLUGIN_LINUX, new JsonObject().put(USER, "root")));
        assertNotNull(Credential.credDataError(PLUGIN_WINRM, new JsonObject().put(USER, "").put(PASSWORD, "pw")));
        assertNotNull(Credential.credDataError(PLUGIN_LINUX, new JsonObject().put(COMMUNITY, "public")));
    }

    @Test
    void snmpRequiresCommunity()
    {
        assertNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "public")));

        assertNotNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(USER, "root").put(PASSWORD, "pw")));
        assertNotNull(Credential.credDataError(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "")));
    }

    @Test
    void snmpNormalizationPinsVersion2c()
    {
        var stored = Credential.normalizeCredData(PLUGIN_SNMP, new JsonObject().put(COMMUNITY, "public"));

        assertEquals(SNMP_V2C, stored.getString(SNMP_VERSION));
        assertEquals("public", stored.getString(COMMUNITY));

        // LINUX/WINRM pass through untouched
        var linux = new JsonObject().put(USER, "root").put(PASSWORD, "pw");
        assertEquals(linux, Credential.normalizeCredData(PLUGIN_LINUX, linux));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw -q test -Dtest=CredentialCredDataTest`
Expected: FAIL — compile error, `credDataError` not defined.

- [ ] **Step 3: Implement in `Credential.java`** — add the two static methods:

```java
    /**
     * Validates cred_data against the credential's system_type contract:
     * LINUX/WINRM need non-empty user+password, SNMP needs a non-empty community.
     *
     * @return an error message, or null when the payload is valid.
     */
    public static String credDataError(String systemType, JsonObject credData)
    {
        if (credData == null)
        {
            return "cred_data is required";
        }

        if (PLUGIN_SNMP.equals(systemType))
        {
            var community = credData.getString(COMMUNITY, "");

            return community.isEmpty() ? "cred_data must contain community for SNMP" : null;
        }

        var user = credData.getString(USER, "");

        var password = credData.getString(PASSWORD, "");

        return (user.isEmpty() || password.isEmpty()) ? "cred_data must contain user and password" : null;
    }

    /** Shapes cred_data for storage: SNMP is pinned to {version:"2c", community}; others pass through. */
    public static JsonObject normalizeCredData(String systemType, JsonObject credData)
    {
        if (PLUGIN_SNMP.equals(systemType))
        {
            return new JsonObject()
                .put(SNMP_VERSION, SNMP_V2C)
                .put(COMMUNITY, credData.getString(COMMUNITY));
        }

        return credData;
    }
```

- [ ] **Step 4: Wire into `create`** — replace the hardcoded user/password check (lines 82-89) with:

```java
            if (!VALID_PLUGIN_TYPES.contains(protocol))
            {
                APIUtils.sendError(context, 400, "protocol must be one of LINUX, SNMP, WINRM");

                return;
            }

            var credError = credDataError(protocol, credentialData);

            if (credError != null)
            {
                LOGGER.warn("Create credential failed: {}", credError);

                APIUtils.sendError(context, 400, credError);

                return;
            }
```

and change the insert param to use the normalized shape:

```java
              .put(PARAMS, new JsonArray().add(credentialName).add(protocol).add(CryptoUtil.encrypt(normalizeCredData(protocol, credentialData).encode())));
```

Add the set near the top of the class:

```java
    public static final java.util.Set<String> VALID_PLUGIN_TYPES = java.util.Set.of(PLUGIN_LINUX, PLUGIN_SNMP, PLUGIN_WINRM);
```

- [ ] **Step 5: Wire into `update`** — replace the user/password presence check (lines 175-180) with a rule that cred_data updates must carry their protocol:

```java
            if (credentialData != null)
            {
                if (protocol == null)
                {
                    APIUtils.sendError(context, 400, "protocol is required when updating cred_data");

                    return;
                }

                var credError = credDataError(protocol, credentialData);

                if (credError != null)
                {
                    APIUtils.sendError(context, 400, credError);

                    return;
                }
            }

            if (protocol != null && !VALID_PLUGIN_TYPES.contains(protocol))
            {
                APIUtils.sendError(context, 400, "protocol must be one of LINUX, SNMP, WINRM");

                return;
            }
```

Where the update encrypts cred_data further down, encrypt `normalizeCredData(protocol, credentialData).encode()` instead of `credentialData.encode()`.

- [ ] **Step 6: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=CredentialCredDataTest`
Expected: PASS.

- [ ] **Step 7: Full verify + commit**

```bash
./mvnw -q verify
git add src/main/java/com/example/NMS/api/handlers/Credential.java src/test/java/com/example/NMS/api/CredentialCredDataTest.java
git commit -m "feat(credential): validate and shape cred_data per system_type (SNMP community)"
```

---

### Task 3: Discovery create/update accept + validate `plugin_type`

**Files:**
- Modify: `src/main/java/com/example/NMS/api/handlers/Discovery.java:62-143` (create), `:151-248` (update)
- Modify: `src/main/java/com/example/NMS/constant/QueryConstant.java` (new query)
- Test: Create `src/test/java/com/example/NMS/api/DiscoveryPluginTypeDbTest.java`

**Interfaces:**
- Consumes: `Credential.VALID_PLUGIN_TYPES` (Task 2), `INSERT_DISCOVERY`/`UPDATE_DISCOVERY` param order (Task 1).
- Produces: `POST/PUT /api/discovery` require `plugin_type` in the body and reject credentials whose `system_type` differs; query `COUNT_MISMATCHED_CREDENTIALS`.

- [ ] **Step 1: Add the query to `QueryConstant.java`:**

```java
    // $1 = jsonb array of credential ids, $2 = the discovery profile's plugin_type.
    // > 0 means at least one selected credential has a different system_type.
    public static final String COUNT_MISMATCHED_CREDENTIALS =
        "SELECT COUNT(*)::int AS mismatched FROM credential_profile " +
            "WHERE id IN (SELECT jsonb_array_elements_text($1::jsonb)::int) AND system_type <> $2";
```

- [ ] **Step 2: Write the failing DB test** — `DiscoveryPluginTypeDbTest.java`:

```java
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
```

- [ ] **Step 3: Run it** — `./mvnw -q test -Dtest=DiscoveryPluginTypeDbTest`
Expected: PASS already (query added in Step 1); if it fails, fix the SQL before moving on. The failing-first discipline here is on Step 4's handler change, guarded by compile.

- [ ] **Step 4: Update `create` in `api/handlers/Discovery.java`** — require and validate the new field, then thread it into the insert. Change the fields array:

```java
            var fields = new String[]{DISCOVERY_PROFILE_NAME, CREDENTIAL_PROFILE_ID, IP_ADDRESS, PORT, PLUGIN_TYPE};
```

after the existing extraction block add:

```java
            var pluginType = body.getString(PLUGIN_TYPE);

            if (!Credential.VALID_PLUGIN_TYPES.contains(pluginType))
            {
                APIUtils.sendError(context, 400, "plugin_type must be one of LINUX, SNMP, WINRM");

                return;
            }
```

then replace the direct insert with a mismatch pre-check composed in front:

```java
            var mismatchQuery = new JsonObject()
                .put(QUERY, QueryConstant.COUNT_MISMATCHED_CREDENTIALS)
                .put(PARAMS, new JsonArray().add(credentialIds.encode()).add(pluginType));

            var query = new JsonObject()
                .put(QUERY, QueryConstant.INSERT_DISCOVERY)
                .put(PARAMS, new JsonArray().add(discoveryName).add(ip).add(port).add(pluginType));

            executeQuery(mismatchQuery)
                .compose(check ->
                {
                    if (check.getJsonObject(0).getInteger("mismatched") > 0)
                    {
                        return Future.failedFuture("credential type mismatch");
                    }

                    return executeQuery(query);
                })
                .compose(result ->
                {
                    // ... existing body from the current .compose(result -> { ... }) unchanged ...
                })
```

and in the `.onComplete` failure branch, map the new error to a 400:

```java
                        if ("credential type mismatch".equals(queryResult.cause().getMessage()))
                        {
                            APIUtils.sendError(context, 400, "all credentials must have system_type " + pluginType);
                        }
                        else
                        {
                            APIUtils.sendError(context, 500, "Failed to create discovery profile: " + queryResult.cause().getMessage());
                        }
```

- [ ] **Step 5: Update `update` the same way** — add `PLUGIN_TYPE` to the fields array, validate the value, add the same `mismatchQuery` as the first `compose` link before the exists-check, and change the update params to `new JsonArray().add(discoveryName).add(ip).add(port).add(pluginType).add(id)`. Map the `"credential type mismatch"` failure to a 400 in `onComplete` exactly as in create.

- [ ] **Step 6: Full verify + commit**

```bash
./mvnw -q verify
git add src/main/java/com/example/NMS/api/handlers/Discovery.java src/main/java/com/example/NMS/constant/QueryConstant.java src/test/java/com/example/NMS/api/DiscoveryPluginTypeDbTest.java
git commit -m "feat(discovery): accept plugin_type and enforce credential system_type match"
```

---

### Task 4: `DiscoveryEvents` publisher

**Files:**
- Create: `src/main/java/com/example/NMS/events/DiscoveryEvents.java`
- Modify: `src/main/java/com/example/NMS/constant/Constant.java`
- Test: Create `src/test/java/com/example/NMS/events/DiscoveryEventsTest.java`

**Interfaces:**
- Produces:
  - `Constant.DISCOVERY_EVENT_ADDRESS_PREFIX = "nms.discovery."`
  - `DiscoveryEvents.address(long discoveryId)` → `"nms.discovery.<id>"`
  - `DiscoveryEvents.state(EventBus bus, long id, String status, String message)` → publishes `{type:"state", status, message?}`
  - `DiscoveryEvents.targets(EventBus bus, long id, List<String> ips)` → `{type:"targets", total, ips}`
  - `DiscoveryEvents.progress(EventBus bus, long id, String ip, String stage, double progress, String status, String message)` → `{type:"progress", ip, stage, progress, status, message?}`
  - Stage names: `"PING"`, `"PORT"`, `"PLUGIN"`. Statuses: `"ok"`/`"failed"` for PING/PORT, `"COMPLETED"`/`"FAILED"` for PLUGIN.

- [ ] **Step 1: Write the failing test** — `DiscoveryEventsTest.java`:

```java
package com.example.NMS.events;

import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/** DiscoveryEvents publishes UI progress payloads to the per-run address nms.discovery.&lt;id&gt;. */
class DiscoveryEventsTest
{
    private static Vertx vertx;

    @BeforeAll
    static void up() { vertx = Vertx.vertx(); }

    @AfterAll
    static void down() throws Exception { vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS); }

    private CompletableFuture<JsonObject> nextEvent(long id)
    {
        var future = new CompletableFuture<JsonObject>();

        vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(id), msg -> future.complete(msg.body()));

        return future;
    }

    @Test
    void publishesProgressWithStagePayload() throws Exception
    {
        var got = nextEvent(42L);

        DiscoveryEvents.progress(vertx.eventBus(), 42L, "10.0.0.5", "PING", 33.33, "ok", null);

        var event = got.get(5, TimeUnit.SECONDS);

        assertEquals("progress", event.getString("type"));
        assertEquals("10.0.0.5", event.getString("ip"));
        assertEquals("PING", event.getString("stage"));
        assertEquals(33.33, event.getDouble("progress"));
        assertEquals("ok", event.getString("status"));
        assertFalse(event.containsKey("message"));
    }

    @Test
    void publishesStateAndTargets() throws Exception
    {
        var gotState = nextEvent(7L);

        DiscoveryEvents.state(vertx.eventBus(), 7L, "RUNNING", null);

        var state = gotState.get(5, TimeUnit.SECONDS);

        assertEquals("state", state.getString("type"));
        assertEquals("RUNNING", state.getString("status"));

        var gotTargets = nextEvent(8L);

        DiscoveryEvents.targets(vertx.eventBus(), 8L, List.of("10.0.0.1", "10.0.0.2"));

        var targets = gotTargets.get(5, TimeUnit.SECONDS);

        assertEquals("targets", targets.getString("type"));
        assertEquals(2, (int) targets.getInteger("total"));
        assertEquals("10.0.0.1", targets.getJsonArray("ips").getString(0));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw -q test -Dtest=DiscoveryEventsTest`
Expected: FAIL — compile error, class missing.

- [ ] **Step 3: Implement `DiscoveryEvents.java`:**

```java
package com.example.NMS.events;

import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;

import java.util.List;

import static com.example.NMS.constant.Constant.DISCOVERY_EVENT_ADDRESS_PREFIX;

/**
 * Publishes discovery progress events to the per-run event-bus address
 * {@code nms.discovery.<discoveryId>}, exposed outbound-only over the SockJS
 * bridge. Fire-and-forget: publishing must never fail a discovery run.
 */
public final class DiscoveryEvents
{
    private DiscoveryEvents() {}

    public static String address(long discoveryId)
    {
        return DISCOVERY_EVENT_ADDRESS_PREFIX + discoveryId;
    }

    /** Run lifecycle: status RUNNING | COMPLETED | FAILED, message optional. */
    public static void state(EventBus bus, long discoveryId, String status, String message)
    {
        var payload = new JsonObject().put("type", "state").put("status", status);

        if (message != null)
        {
            payload.put("message", message);
        }

        bus.publish(address(discoveryId), payload);
    }

    /** The expanded target list, published once after IP resolution. */
    public static void targets(EventBus bus, long discoveryId, List<String> ips)
    {
        bus.publish(address(discoveryId), new JsonObject()
            .put("type", "targets")
            .put("total", ips.size())
            .put("ips", new JsonArray(ips)));
    }

    /** Per-IP stage progress: stage PING|PORT|PLUGIN, status ok|failed (PLUGIN: COMPLETED|FAILED). */
    public static void progress(EventBus bus, long discoveryId, String ip, String stage,
                                double progress, String status, String message)
    {
        var payload = new JsonObject()
            .put("type", "progress")
            .put("ip", ip)
            .put("stage", stage)
            .put("progress", progress)
            .put("status", status);

        if (message != null)
        {
            payload.put("message", message);
        }

        bus.publish(address(discoveryId), payload);
    }
}
```

Add to `Constant.java`:

```java
    /** Per-run discovery progress address prefix; full address is nms.discovery.<discoveryId>. */
    public static final String DISCOVERY_EVENT_ADDRESS_PREFIX = "nms.discovery.";
```

- [ ] **Step 4: Run test to verify it passes**

Run: `./mvnw -q test -Dtest=DiscoveryEventsTest`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
./mvnw -q verify
git add src/main/java/com/example/NMS/events/DiscoveryEvents.java src/main/java/com/example/NMS/constant/Constant.java src/test/java/com/example/NMS/events/DiscoveryEventsTest.java
git commit -m "feat(events): DiscoveryEvents publisher for per-run progress addresses"
```

---

### Task 5: Stage helpers + ping/port split in `Utility`

**Files:**
- Create: `src/main/java/com/example/NMS/discovery/DiscoveryStages.java`
- Modify: `src/main/java/com/example/NMS/utility/Utility.java:166-296`
- Modify: `src/main/java/com/example/NMS/plugin/PluginEnvelope.java`
- Test: Create `src/test/java/com/example/NMS/discovery/DiscoveryStagesTest.java`

**Interfaces:**
- Produces:
  - `DiscoveryStages.pingProgress(String pluginType)` → `50.0` for SNMP else `33.33`
  - `DiscoveryStages.skipsPortCheck(String pluginType)` → true for SNMP (UDP)
  - `PluginEnvelope.credential(String pluginType, JsonObject plain)` → engine credential: SNMP passes `{version, community}` through; others map stored `{user,password}` → `{username,password}`
  - `Utility.pingCheck(List<String> ips)` → `Set<String>` alive (fping, unchanged behavior)
  - `Utility.portCheck(Collection<String> ips, int port)` → `Set<String>` with the TCP port open
  - `Utility.checkReachability` is DELETED (Discovery verticle is its only caller and is rewired in Task 6).
- Consumes: constants from Task 1.

- [ ] **Step 1: Write the failing test** — `DiscoveryStagesTest.java`:

```java
package com.example.NMS.discovery;

import com.example.NMS.plugin.PluginEnvelope;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.Test;

import static com.example.NMS.constant.Constant.*;
import static org.junit.jupiter.api.Assertions.*;

/** Stage math and per-type credential shaping (spec: staged progress + multi-protocol). */
class DiscoveryStagesTest
{
    @Test
    void snmpSkipsPortCheckAndJumpsToFifty()
    {
        assertEquals(50.0, DiscoveryStages.pingProgress(PLUGIN_SNMP));
        assertTrue(DiscoveryStages.skipsPortCheck(PLUGIN_SNMP));

        assertEquals(33.33, DiscoveryStages.pingProgress(PLUGIN_LINUX));
        assertFalse(DiscoveryStages.skipsPortCheck(PLUGIN_LINUX));
        assertEquals(33.33, DiscoveryStages.pingProgress(PLUGIN_WINRM));
        assertFalse(DiscoveryStages.skipsPortCheck(PLUGIN_WINRM));
    }

    @Test
    void credentialShapePerType()
    {
        var stored = new JsonObject().put(USER, "root").put(PASSWORD, "pw");

        var ssh = PluginEnvelope.credential(PLUGIN_LINUX, stored);

        assertEquals("root", ssh.getString(USERNAME));
        assertEquals("pw", ssh.getString(PASSWORD));

        var winrm = PluginEnvelope.credential(PLUGIN_WINRM, stored);

        assertEquals("root", winrm.getString(USERNAME));

        var snmpStored = new JsonObject().put(SNMP_VERSION, SNMP_V2C).put(COMMUNITY, "public");

        var snmp = PluginEnvelope.credential(PLUGIN_SNMP, snmpStored);

        assertEquals(SNMP_V2C, snmp.getString(SNMP_VERSION));
        assertEquals("public", snmp.getString(COMMUNITY));
        assertFalse(snmp.containsKey(USERNAME));
    }
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `./mvnw -q test -Dtest=DiscoveryStagesTest`
Expected: FAIL — compile errors (`DiscoveryStages` missing, `PluginEnvelope.credential` missing).

- [ ] **Step 3: Implement `DiscoveryStages.java`:**

```java
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
```

- [ ] **Step 4: Add `credential` to `PluginEnvelope.java`:**

```java
    /**
     * Shapes a decrypted stored credential for the engine: SNMP passes
     * {version, community} through; SSH/WinRM map stored "user" to the
     * engine's "username" key.
     */
    public static JsonObject credential(String pluginType, JsonObject plain) {
        if ("SNMP".equals(pluginType)) {
            return new JsonObject()
                .put("version", plain.getString("version", "2c"))
                .put("community", plain.getString("community"));
        }
        return new JsonObject()
            .put("username", plain.getString("user"))
            .put("password", plain.getString("password"));
    }
```

- [ ] **Step 5: Split `Utility.checkReachability`** — replace it (and keep `isPortOpen` private as-is) with:

```java
  /**
   * Bulk liveness check via {@code fping -a -q -c 1 -t 1000}. Returns the subset
   * of {@code ipAddresses} that answered. (Extracted from the old checkReachability;
   * fping output parsing is unchanged.)
   */
    public static Set<String> pingCheck(List<String> ipAddresses)
    {
        var aliveIps = new HashSet<String>();

        try
        {
            var command = new ArrayList<String>();

            command.add("fping");
            command.add("-a");
            command.add("-q");
            command.add("-c");
            command.add("1");
            command.add("-t");
            command.add("1000");
            command.addAll(ipAddresses);

            var process = new ProcessBuilder(command).start();

            LOGGER.info("fping command: {}", String.join(" ", command));

            var reader = new BufferedReader(new InputStreamReader(process.getErrorStream()));

            String line;

            while ((line = reader.readLine()) != null)
            {
                if (!line.contains("100%"))
                {
                    aliveIps.add(line.split(":")[0].trim());
                }
            }

            var exitCode = process.waitFor();

            if (exitCode != 0 && aliveIps.isEmpty())
            {
                LOGGER.warn("fping exited with code {} and no alive IPs", exitCode);
            }
        }
        catch (Exception exception)
        {
            LOGGER.error("Error running fping: {}", exception.getMessage());
        }

        LOGGER.info("fping alive IPs: {}", aliveIps);

        return aliveIps;
    }

  /** TCP-connect check (1s timeout) for each candidate; returns the IPs with the port open. */
    public static Set<String> portCheck(Collection<String> ipAddresses, int port)
    {
        var open = new HashSet<String>();

        for (var ip : ipAddresses)
        {
            if (isPortOpen(ip, port))
            {
                open.add(ip);
            }
        }

        return open;
    }
```

Delete `checkReachability` entirely. Add `import java.util.Collection;` and `import java.util.Set;` if missing. `Discovery.java` still calls `checkReachability` — it will not compile until Task 6; do Tasks 5 and 6 as one commit if you prefer, but the intended flow is: complete Task 6 Step 3 before running the full suite. To keep every commit green, **do NOT commit at the end of this task** — Task 6 commits both.

- [ ] **Step 6: Run only the new unit tests**

Run: `./mvnw -q test -Dtest=DiscoveryStagesTest`
Expected: PASS (compile of main sources will fail if Discovery.java was already edited — it hasn't been yet; if compilation of the verticle fails here, proceed straight to Task 6 Step 3).

---

### Task 6: Discovery verticle — staged publishing + per-type flow

**Files:**
- Modify: `src/main/java/com/example/NMS/discovery/Discovery.java` (whole `runDiscovery`/`handleConnection` region)
- Modify: `src/main/java/com/example/NMS/api/handlers/Discovery.java:324-353` (`resolveCredentials`)
- Modify: `src/test/java/com/example/NMS/api/DiscoveryCredentialsTest.java` (contract change)
- Test: `src/test/java/com/example/NMS/api/DiscoveryCredentialsTest.java`

**Interfaces:**
- Consumes: `DiscoveryEvents` (Task 4), `DiscoveryStages` + `Utility.pingCheck`/`portCheck` + `PluginEnvelope.credential` (Task 5), `GET_BY_RUN_ID` now returning `plugin_type` (Task 1).
- Produces: the staged event sequence on `nms.discovery.<id>`; envelope targets carry the profile's `plugin_type` and per-type credential. `resolveCredentials` now returns the decrypted stored fields plus `id` (no username remap — that happens in `PluginEnvelope.credential`).

- [ ] **Step 1: Change `resolveCredentials`** in `api/handlers/Discovery.java` — replace the remapping body with a decrypt-and-pass-through:

```java
        for (var entry : credentials)
        {
            var cred = (JsonObject) entry;

            var cipher = cred.getString(CRED_DATA);

            if (cipher == null || cipher.isBlank())
            {
                continue;
            }

            // Decrypted stored shape passes through untouched ({user,password} or
            // {version,community}); PluginEnvelope.credential shapes it per plugin type
            // at envelope-build time.
            resolved.add(new JsonObject(CryptoUtil.decrypt(cipher)).put(ID, cred.getLong(ID)));
        }
```

Update the Javadoc above it accordingly (returns decrypted stored fields + id).

- [ ] **Step 2: Update `DiscoveryCredentialsTest`** — it asserts the old `{username,password,id}` contract. Change assertions to the stored-shape contract: a LINUX credential decrypts to `{user, password, id}`. Example replacement for the success-path test body:

```java
        var cipher = CryptoUtil.encrypt(new JsonObject().put(USER, "root").put(PASSWORD, "pw").encode());

        var resolved = Discovery.resolveCredentials(new JsonArray()
            .add(new JsonObject().put(CRED_DATA, cipher).put(ID, 5L)));

        assertEquals(1, resolved.size());
        assertEquals("root", resolved.getJsonObject(0).getString(USER));
        assertEquals("pw", resolved.getJsonObject(0).getString(PASSWORD));
        assertEquals(5L, (long) resolved.getJsonObject(0).getLong(ID));
```

(Keep the skip-blank-cred_data test as-is.)

- [ ] **Step 3: Rewire the verticle `discovery/Discovery.java`** — replace `runDiscovery`, `checkReach`, and `handleConnection` with the staged flow:

```java
    private void runDiscovery(long id, JsonObject profile)
    {
        var query = new JsonObject()
            .put(QUERY, QueryConstant.UPDATE_DISCOVERY_PROFILE_STATUS)
            .put(PARAMS, new JsonArray().add(DISCOVERY_STATUS_RUNNING).add(id));

        vertx.eventBus().send(DB_EXECUTE_QUERY, query);

        DiscoveryEvents.state(vertx.eventBus(), id, DISCOVERY_STATUS_RUNNING, null);

        var ipInput = profile.getString(IP);

        var port = profile.getInteger(PORT);

        var pluginType = profile.getString(PLUGIN_TYPE, PLUGIN_LINUX);

        var credentials = profile.getJsonArray("credential");

        LOGGER.info("Discovery {} ({}): {}", id, pluginType, ipInput);

        resolveIps(ipInput)
            .compose(ips ->
            {
                DiscoveryEvents.targets(vertx.eventBus(), id, ips);

                return vertx.<Set<String>>executeBlocking(() -> pingCheck(ips), false)
                    .map(alive -> publishPingStage(id, pluginType, port, ips, alive));
            })
            .compose(alive ->
            {
                if (DiscoveryStages.skipsPortCheck(pluginType))
                {
                    return Future.succeededFuture(alive);
                }

                return vertx.<Set<String>>executeBlocking(() -> portCheck(alive, port), false)
                    .map(open -> publishPortStage(id, port, alive, open));
            })
            .onComplete(asyncResult ->
            {
                if (asyncResult.succeeded())
                {
                    handleConnection(asyncResult.result(), credentials, pluginType, port, id);
                }
                else
                {
                    LOGGER.error("Discovery {} failed during resolve/reachability: {}", id, asyncResult.cause().getMessage());

                    var failedResult = new JsonObject()
                        .put(IP, ipInput)
                        .put(PORT, port)
                        .put(STATUS, FAILURE)
                        .put(RESULT, asyncResult.cause().getMessage())
                        .put(DISCOVERY_ID, id)
                        .put(CREDENTIAL_ID, null)
                        .put(EVENT_TYPE, EVENT_DISCOVERY);

                    vertx.eventBus().send(STORAGE_RESULTS, failedResult);

                    var statusQuery = new JsonObject()
                        .put(QUERY, QueryConstant.UPDATE_DISCOVERY_PROFILE_STATUS)
                        .put(PARAMS, new JsonArray().add(DISCOVERY_STATUS_FAILED).add(id));

                    vertx.eventBus().send(DB_EXECUTE_QUERY, statusQuery);

                    DiscoveryEvents.state(vertx.eventBus(), id, DISCOVERY_STATUS_FAILED, asyncResult.cause().getMessage());
                }
            });
    }

    private Future<List<String>> resolveIps(String ipInput)
    {
        return vertx.executeBlocking(() -> resolveIpAddresses(ipInput), false);
    }

    /**
     * Publishes per-IP PING outcomes and short-circuits dead IPs straight to
     * storage as FAILED (they never reach the port check or the engine).
     *
     * @return the alive subset, input to the next stage.
     */
    private Set<String> publishPingStage(long id, String pluginType, int port, List<String> ips, Set<String> alive)
    {
        for (var ip : ips)
        {
            if (alive.contains(ip))
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PING",
                    DiscoveryStages.pingProgress(pluginType), "ok", null);
            }
            else
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PING", 100.0, "failed", "ping failed");

                storeShortCircuitFailure(id, ip, port, "Device unreachable");
            }
        }

        return alive;
    }

    /** Publishes per-IP PORT outcomes; closed-port IPs short-circuit to storage as FAILED. */
    private Set<String> publishPortStage(long id, int port, Set<String> alive, Set<String> open)
    {
        for (var ip : alive)
        {
            if (open.contains(ip))
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PORT", 66.66, "ok", null);
            }
            else
            {
                DiscoveryEvents.progress(vertx.eventBus(), id, ip, "PORT", 100.0, "failed", "port not reachable");

                storeShortCircuitFailure(id, ip, port, "Port closed");
            }
        }

        return open;
    }

    private void storeShortCircuitFailure(long id, String ip, int port, String reason)
    {
        vertx.eventBus().send(STORAGE_RESULTS, new JsonObject()
            .put(IP, ip)
            .put(PORT, port)
            .put(STATUS, FAILURE)
            .put(RESULT, reason)
            .put(DISCOVERY_ID, id)
            .put(CREDENTIAL_ID, null)
            .put(EVENT_TYPE, EVENT_DISCOVERY));
    }

    private void handleConnection(Set<String> readyIps, JsonArray credentials, String pluginType, int port, long discoveryId)
    {
        var targets = new JsonArray();

        for (var ip : readyIps)
        {
            // One v2 target per (ip, credential): the engine tries a SINGLE credential per
            // target. request_id carries the correlation context for ResponseProcessor.
            for (var j = 0; j < credentials.size(); j++)
            {
                var cred = credentials.getJsonObject(j);

                var requestId = DiscoveryRequestId.encode(discoveryId, ip, port, cred.getLong(ID));

                targets.add(PluginEnvelope.target(requestId, 0L, pluginType, ip, port,
                    PluginEnvelope.credential(pluginType, cred), new JsonArray()));
            }
        }

        var envelope = PluginEnvelope.build(EVENT_DISCOVERY, targets)
            .put(DISCOVERY_ID, discoveryId);

        LOGGER.info("Dispatching discovery envelope for ID {} with {} target(s)", discoveryId, targets.size());

        vertx.eventBus().send(PLUGIN_EXECUTE, envelope);
    }
```

New imports needed in the verticle: `com.example.NMS.events.DiscoveryEvents`, `java.util.Set`. The static import `com.example.NMS.utility.Utility.*` already covers `pingCheck`/`portCheck`/`resolveIpAddresses`.

**Edge kept from today's behavior:** when `readyIps` is empty the envelope still dispatches with zero targets, so `EVENT_COMPLETION` still fires and marks the profile COMPLETED (and Task 7 publishes the state event).

- [ ] **Step 4: Full verify**

Run: `./mvnw -q verify`
Expected: BUILD SUCCESS, all tests green (including the Task 5 unit tests and the updated `DiscoveryCredentialsTest`).

- [ ] **Step 5: Commit Tasks 5+6 together**

```bash
git add src/main/java/com/example/NMS/discovery/ src/main/java/com/example/NMS/utility/Utility.java src/main/java/com/example/NMS/plugin/PluginEnvelope.java src/main/java/com/example/NMS/api/handlers/Discovery.java src/test/java/com/example/NMS/discovery/DiscoveryStagesTest.java src/test/java/com/example/NMS/api/DiscoveryCredentialsTest.java
git commit -m "feat(discovery): staged ping/port/plugin progress events and per-type envelopes"
```

---

### Task 7: ResponseProcessor — PLUGIN progress + completion state

**Files:**
- Modify: `src/main/java/com/example/NMS/plugin/ResponseProcessor.java:66-82,182-232`
- Test: `src/test/java/com/example/NMS/plugin/ResponseProcessorRoutingTest.java` (add cases)

**Interfaces:**
- Consumes: `DiscoveryEvents` (Task 4), `DiscoveryRequestId.encode/decode`.
- Produces: engine result lines publish `{type:"progress", ip, stage:"PLUGIN", progress:100, status:"COMPLETED"|"FAILED", message}`; `EVENT_COMPLETION` for discovery publishes `{type:"state", status:"COMPLETED"}`. Short-circuited results (no request_id) do NOT re-publish (the Discovery verticle already published their PING/PORT failure).

- [ ] **Step 1: Write the failing test** — add to `ResponseProcessorRoutingTest.java` (follow the file's existing deploy/consume pattern; if it is pure-unit only, add a new nested Vertx-based test class in the same file):

```java
    @Test
    void engineDiscoveryResultPublishesPluginProgress() throws Exception
    {
        var vertx = Vertx.vertx();

        try
        {
            vertx.deployVerticle(new ResponseProcessor()).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            var got = new CompletableFuture<JsonObject>();

            vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(42L), msg -> got.complete(msg.body()));

            var requestId = DiscoveryRequestId.encode(42L, "10.0.0.5", 22, 3L);

            vertx.eventBus().send(STORAGE_RESULTS, new JsonObject()
                .put(REQUEST_ID, requestId)
                .put(EVENT_TYPE, EVENT_DISCOVERY)
                .put(STATUS, SUCCESS));

            var event = got.get(5, TimeUnit.SECONDS);

            assertEquals("progress", event.getString("type"));
            assertEquals("10.0.0.5", event.getString("ip"));
            assertEquals("PLUGIN", event.getString("stage"));
            assertEquals(100.0, event.getDouble("progress"));
            assertEquals("COMPLETED", event.getString("status"));
        }
        finally
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }

    @Test
    void discoveryCompletionPublishesCompletedState() throws Exception
    {
        var vertx = Vertx.vertx();

        try
        {
            vertx.deployVerticle(new ResponseProcessor()).toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);

            var got = new CompletableFuture<JsonObject>();

            vertx.eventBus().<JsonObject>consumer(DiscoveryEvents.address(42L), msg -> got.complete(msg.body()));

            vertx.eventBus().send(EVENT_COMPLETION, new JsonObject()
                .put(EVENT_TYPE, EVENT_DISCOVERY)
                .put(DISCOVERY_ID, 42));

            var event = got.get(5, TimeUnit.SECONDS);

            assertEquals("state", event.getString("type"));
            assertEquals("COMPLETED", event.getString("status"));
        }
        finally
        {
            vertx.close().toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
    }
```

Required imports if missing: `io.vertx.core.Vertx`, `io.vertx.core.json.JsonObject`, `java.util.concurrent.CompletableFuture`, `java.util.concurrent.TimeUnit`, `com.example.NMS.events.DiscoveryEvents`, `com.example.NMS.discovery.DiscoveryRequestId`, static `com.example.NMS.constant.Constant.*`.

(The `DB_EXECUTE_QUERY` sends inside the verticle have no consumer in this test — that is fine, `send` without a reply handler just logs a delivery failure.)

- [ ] **Step 2: Run to verify both fail**

Run: `./mvnw -q test -Dtest=ResponseProcessorRoutingTest`
Expected: FAIL — both new tests time out (no events published yet).

- [ ] **Step 3: Implement.** In `storeDiscoveryResults`, inside the `if (context != null)` branch (engine results only), after the existing field extraction add:

```java
            // Live progress for the UI: only engine results publish here; short-circuited
            // ping/port failures were already published by the Discovery verticle.
            DiscoveryEvents.progress(vertx.eventBus(), discoveryId, ip, "PLUGIN", 100.0,
                SUCCESS.equals(status) ? "COMPLETED" : "FAILED", msg);
```

In the `EVENT_COMPLETION` consumer, after the status-update send add:

```java
                    DiscoveryEvents.state(vertx.eventBus(), discoveryId, DISCOVERY_STATUS_COMPLETED, null);
```

Import `com.example.NMS.events.DiscoveryEvents`.

- [ ] **Step 4: Run to verify pass, then full verify + commit**

```bash
./mvnw -q test -Dtest=ResponseProcessorRoutingTest
./mvnw -q verify
git add src/main/java/com/example/NMS/plugin/ResponseProcessor.java src/test/java/com/example/NMS/plugin/ResponseProcessorRoutingTest.java
git commit -m "feat(events): publish plugin-stage progress and completion state from ResponseProcessor"
```

---

### Task 8: Polling — per-type credential in the poll envelope

**Files:**
- Modify: `src/main/java/com/example/NMS/polling/Polling.java:224-252` (`buildEnvelope`)
- Test: `src/test/java/com/example/NMS/polling/PollingEnvelopeTest.java` (add SNMP case)

**Interfaces:**
- Consumes: `PluginEnvelope.credential` (Task 5).
- Produces: poll targets for SNMP jobs carry `{version, community}`; LINUX/WINRM unchanged `{username, password}`.

- [ ] **Step 1: Write the failing test** — add to `PollingEnvelopeTest.java`:

```java
    @Test
    void snmpJobsCarryCommunityCredential()
    {
        var cipher = CryptoUtil.encrypt(
            new JsonObject().put(SNMP_VERSION, SNMP_V2C).put(COMMUNITY, "public").encode());

        var job = new JsonObject()
            .put(JOB_ID, 9L)
            .put(PLUGIN_TYPE, PLUGIN_SNMP)
            .put(IP, "10.0.0.9")
            .put(PORT, 161)
            .put(CRED_DATA, cipher)
            .put("metrics", new JsonArray().add("UPTIME"));

        var envelope = Polling.buildEnvelope(new JsonArray().add(job));

        var credential = envelope.getJsonArray(TARGETS).getJsonObject(0).getJsonObject("credential");

        assertEquals(SNMP_V2C, credential.getString(SNMP_VERSION));
        assertEquals("public", credential.getString(COMMUNITY));
        assertFalse(credential.containsKey(USERNAME));
    }
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw -q test -Dtest=PollingEnvelopeTest`
Expected: FAIL — credential contains `username=null`, no `community`.

- [ ] **Step 3: Implement** — in `buildEnvelope`, replace the manual remap:

```java
            var plain = new JsonObject(CryptoUtil.decrypt(job.getString(CRED_DATA)));

            var credential = PluginEnvelope.credential(job.getString(PLUGIN_TYPE), plain);
```

(delete the old `.put(USERNAME, plain.getString(USER))...` block and update the comment above to say the shaping is per plugin type in `PluginEnvelope.credential`).

- [ ] **Step 4: Run to verify pass, then full verify + commit**

```bash
./mvnw -q test -Dtest=PollingEnvelopeTest
./mvnw -q verify
git add src/main/java/com/example/NMS/polling/Polling.java src/test/java/com/example/NMS/polling/PollingEnvelopeTest.java
git commit -m "fix(polling): shape poll credentials per plugin type (SNMP community passthrough)"
```

---

### Task 9: Per-type default metrics on provision

**Files:**
- Modify: `src/main/java/com/example/NMS/constant/QueryConstant.java:118-213` (`INSERT_PROVISIONING_AND_METRICS`)
- Test: Create `src/test/java/com/example/NMS/api/ProvisionDefaultMetricsDbTest.java`

**Interfaces:**
- Produces: provisioning a device creates only the metrics its engine plugin supports — LINUX: CPU, MEMORY, DISK, UPTIME, NETWORK, PROCESS; WINRM: CPU, MEMORY, DISK; SNMP: UPTIME.

- [ ] **Step 1: Write the failing DB test** — `ProvisionDefaultMetricsDbTest.java`:

```java
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
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw -q test -Dtest=ProvisionDefaultMetricsDbTest`
Expected: FAIL — SNMP/WINRM cases get all six metrics.

- [ ] **Step 3: Implement** — in `INSERT_PROVISIONING_AND_METRICS`, replace the `metric_names` CTE and the `inserted_metrics` join:

```sql
    metric_names AS (
        SELECT name, plugin_types
        FROM (VALUES
            ('CPU'::metric_name,     ARRAY['LINUX','WINRM']),
            ('MEMORY'::metric_name,  ARRAY['LINUX','WINRM']),
            ('DISK'::metric_name,    ARRAY['LINUX','WINRM']),
            ('UPTIME'::metric_name,  ARRAY['LINUX','SNMP']),
            ('NETWORK'::metric_name, ARRAY['LINUX']),
            ('PROCESS'::metric_name, ARRAY['LINUX'])
        ) AS metrics (name, plugin_types)
    ),
    inserted_metrics AS (
        INSERT INTO metrics (provisioning_job_id, name, plugin_type, polling_interval, is_enabled)
        SELECT
            pj.provisioning_job_id,
            mn.name,
            pj.plugin_type,
            300,
            TRUE
        FROM inserted_provisioning_jobs pj
        JOIN metric_names mn ON pj.plugin_type = ANY(mn.plugin_types)
        RETURNING metric_id, provisioning_job_id, name
    )
```

(The `CROSS JOIN metric_names mn` line is replaced by the `JOIN ... ON pj.plugin_type = ANY(mn.plugin_types)`.)

- [ ] **Step 4: Run to verify pass, then full verify + commit**

```bash
./mvnw -q test -Dtest=ProvisionDefaultMetricsDbTest
./mvnw -q verify
git add src/main/java/com/example/NMS/constant/QueryConstant.java src/test/java/com/example/NMS/api/ProvisionDefaultMetricsDbTest.java
git commit -m "feat(provisioning): default metric set per plugin type"
```

---

### Task 10: SockJS event-bus bridge with JWT query-param auth

**Files:**
- Modify: `src/main/java/com/example/NMS/api/Server.java:104-113` (mount before the sub-routers)
- Test: Create `src/test/java/com/example/NMS/api/EventBusBridgeTest.java`

**Interfaces:**
- Consumes: `Constant.DISCOVERY_EVENT_ADDRESS_PREFIX` (Task 4), `AppConfig.jwtSecret()`.
- Produces: `/eventbus/*` SockJS endpoint; outbound-only bridge for addresses matching `nms\.discovery\.[0-9]+`; connections require a valid `?access_token=<jwt>`.

- [ ] **Step 1: Write the failing test** — `EventBusBridgeTest.java` (modeled on `HealthMetricsTest`; reuse its Vertx bootstrap without Micrometer):

```java
package com.example.NMS.api;

import com.example.NMS.config.AppConfig;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.auth.JWTOptions;
import io.vertx.ext.auth.PubSecKeyOptions;
import io.vertx.ext.auth.jwt.JWTAuth;
import io.vertx.ext.auth.jwt.JWTAuthOptions;
import io.vertx.core.json.JsonObject;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.concurrent.TimeUnit;

import static com.example.NMS.constant.Constant.SERVER_PORT;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * The SockJS bridge at /eventbus must reject connections without a valid JWT
 * (SockJS cannot send an Authorization header, so the token rides the
 * access_token query param) and serve the SockJS /info handshake with one.
 */
class EventBusBridgeTest
{
    private static Vertx vertx;

    private static String validToken;

    @BeforeAll
    static void deployServer() throws Exception
    {
        vertx = Vertx.vertx();

        vertx.deployVerticle(new Server()).toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);

        var jwtAuth = JWTAuth.create(vertx, new JWTAuthOptions()
            .addPubSecKey(new PubSecKeyOptions().setAlgorithm("HS256").setBuffer(AppConfig.jwtSecret())));

        validToken = jwtAuth.generateToken(new JsonObject().put("sub", "test"), new JWTOptions().setExpiresInMinutes(5));
    }

    @AfterAll
    static void tearDown() throws Exception
    {
        vertx.close().toCompletionStage().toCompletableFuture().get(30, TimeUnit.SECONDS);
    }

    private int statusOf(String uri) throws Exception
    {
        var client = vertx.createHttpClient();

        try
        {
            return client.request(HttpMethod.GET, SERVER_PORT, "127.0.0.1", uri)
                .compose(req -> req.send())
                .map(resp -> resp.statusCode())
                .toCompletionStage().toCompletableFuture().get(10, TimeUnit.SECONDS);
        }
        finally
        {
            client.close();
        }
    }

    @Test
    void rejectsMissingAndInvalidToken() throws Exception
    {
        assertEquals(401, statusOf("/eventbus/info"));

        assertEquals(401, statusOf("/eventbus/info?access_token=not-a-jwt"));
    }

    @Test
    void servesSockJsInfoWithValidToken() throws Exception
    {
        assertEquals(200, statusOf("/eventbus/info?access_token=" + validToken));
    }
}
```

- [ ] **Step 2: Run to verify it fails**

Run: `./mvnw -q test -Dtest=EventBusBridgeTest`
Expected: FAIL — `/eventbus/info` currently 404s (no route), so the 401 assertions fail.

- [ ] **Step 3: Implement in `Server.java`** — after the JWT `/api/*` handler block (line 103) and before mounting sub-routers, add:

```java
        // SockJS cannot send an Authorization header; the bridge authenticates via
        // ?access_token=<jwt> validated against the same JWTAuth provider as /api.
        router.route("/eventbus/*").handler(context ->
        {
            var token = context.request().getParam("access_token");

            if (token == null || token.isBlank())
            {
                context.fail(401);

                return;
            }

            jwtAuth.authenticate(new TokenCredentials(token))
                .onSuccess(user ->
                {
                    context.setUser(user);

                    context.next();
                })
                .onFailure(err -> context.fail(401));
        });

        // Outbound-only bridge: the UI may subscribe to per-run discovery progress
        // addresses; no inbound addresses are permitted at all.
        var bridgeOptions = new SockJSBridgeOptions()
            .addOutboundPermitted(new PermittedOptions().setAddressRegex("nms\\.discovery\\.[0-9]+"));

        router.route("/eventbus/*").subRouter(SockJSHandler.create(vertx).bridge(bridgeOptions));
```

Imports to add:

```java
import io.vertx.ext.auth.authentication.TokenCredentials;
import io.vertx.ext.bridge.PermittedOptions;
import io.vertx.ext.web.handler.sockjs.SockJSBridgeOptions;
import io.vertx.ext.web.handler.sockjs.SockJSHandler;
```

(No CORS handler on `/eventbus` — the dev UI reaches it through the Vite proxy, same-origin.)

- [ ] **Step 4: Run to verify pass, then full verify + commit**

```bash
./mvnw -q test -Dtest=EventBusBridgeTest
./mvnw -q verify
git add src/main/java/com/example/NMS/api/Server.java src/test/java/com/example/NMS/api/EventBusBridgeTest.java
git commit -m "feat(api): JWT-guarded SockJS event-bus bridge at /eventbus"
```

---

## Final verification (whole plan)

- [ ] `./mvnw -q verify` — all tests green.
- [ ] Manual smoke (optional but recommended, needs the Go plugin binary at `./plugin/Lite_NMS_Plugin` and a reachable device): run the backend per `README`, create a LINUX discovery with a CIDR, run it, and watch `websocat` or a quick SockJS client on `/eventbus?access_token=<jwt>` print PING → PORT → PLUGIN events; confirm `discovery_result` rows and profile `COMPLETED`.
