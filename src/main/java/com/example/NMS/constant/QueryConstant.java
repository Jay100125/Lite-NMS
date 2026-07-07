package com.example.NMS.constant;

public class QueryConstant
{
    public static final String GET_ALL_CREDENTIALS = "SELECT * FROM credential_profile";

    public static final String GET_CREDENTIAL_BY_ID = "SELECT * FROM credential_profile WHERE id = $1";

    public static final String INSERT_CREDENTIAL = "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) returning id";

    public static final String UPDATE_CREDENTIAL = """
      UPDATE credential_profile
      SET credential_name = COALESCE($1, credential_name),
          system_type = COALESCE($2, system_type),
          cred_data = COALESCE($3::text, cred_data)
      WHERE id = $4
      RETURNING *""";

    public static final String DELETE_CREDENTIAL = "DELETE FROM credential_profile WHERE id = $1 returning id";

    public static final String GET_ALL_DISCOVERIES = "SELECT dp.*, array_agg(dcm.credential_profile_id) AS credential_profile_ids " +
        "FROM discovery_profiles dp " +
        "LEFT JOIN discovery_credential_mapping dcm ON dp.id = dcm.discovery_id " +
        "GROUP BY dp.id";

    public static final String GET_DISCOVERY_BY_ID = "SELECT dp.*, array_agg(dcm.credential_profile_id) AS credential_profile_ids " +
        "FROM discovery_profiles dp " +
        "LEFT JOIN discovery_credential_mapping dcm ON dp.id = dcm.discovery_id " +
        "WHERE dp.id = $1 " +
        "GROUP BY dp.id";

    public static final String DELETE_DISCOVERY = "DELETE FROM discovery_profiles WHERE id = $1 RETURNING id";

    public static final String INSERT_DISCOVERY_CREDENTIAL = "INSERT INTO discovery_credential_mapping (discovery_id, credential_profile_id) VALUES ($1, $2) RETURNING discovery_id as id";

    public static final String DELETE_DISCOVERY_CREDENTIALS = "DELETE FROM discovery_credential_mapping WHERE discovery_id = $1";

    public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, ip, port, plugin_type, status) VALUES ($1, $2, $3, $4, 'PENDING') RETURNING id";

    public static final String UPDATE_DISCOVERY = "UPDATE discovery_profiles SET discovery_profile_name = $1, ip = $2, port = $3, plugin_type = $4, status = 'PENDING' WHERE id = $5 RETURNING id";

    public static final String SET_DISCOVERY_STATUS = "UPDATE discovery_profiles SET status = $1 WHERE id = $2 RETURNING id";

    // A discovery can try several credentials per IP → one result row per (ip, credential). Aggregate by IP
    // with "COMPLETED wins": once an IP is COMPLETED (a credential worked), a later FAILED must not downgrade it.
    public static final String INSERT_DISCOVERY_RESULT = "INSERT INTO discovery_result (discovery_id, ip, port, result, msg, credential_profile_id) " +
        "VALUES ($1, $2, $3, $4, $5, $6) " +
        "ON CONFLICT (discovery_id, ip) DO UPDATE " +
        "SET port = EXCLUDED.port, " +
        "    result = CASE WHEN discovery_result.result = 'COMPLETED' THEN 'COMPLETED' ELSE EXCLUDED.result END, " +
        "    msg = CASE WHEN discovery_result.result = 'COMPLETED' THEN discovery_result.msg ELSE EXCLUDED.msg END, " +
        "    credential_profile_id = CASE WHEN discovery_result.result = 'COMPLETED' THEN discovery_result.credential_profile_id ELSE EXCLUDED.credential_profile_id END " +
        "RETURNING id";

    // metrics.plugin_type is NOT NULL; on first insert derive it from the parent job so a metric-config
    // edit for a not-yet-persisted metric row does not violate the constraint.
    public static final String UPSERT_METRICS =
        "INSERT INTO metrics (provisioning_job_id, name, plugin_type, polling_interval, is_enabled) " +
            "VALUES ($1, $2, (SELECT plugin_type FROM provisioning_jobs WHERE id = $1), COALESCE($3, 300), $4) " +
            "ON CONFLICT (provisioning_job_id, name) " +
            "DO UPDATE SET polling_interval = COALESCE(EXCLUDED.polling_interval, metrics.polling_interval), " +
            "is_enabled = EXCLUDED.is_enabled " +
            "RETURNING metric_id as id";

    // $4 is the engine's epoch-millis timestamp (a bigint); polled_at is `timestamp without time zone`,
    // so convert in SQL rather than passing a raw Long the pg client cannot coerce to a temporal.
    public static final String INSERT_POLLED_DATA =
        "INSERT INTO polled_data (job_id, metric_type, data, polled_at) " +
            "VALUES ($1, $2, $3::jsonb, to_timestamp($4 / 1000.0)) returning id";

    public static final String GET_ALL_PROVISIONING_JOBS =
        "SELECT pj.*, cp.credential_name, cp.system_type " +
            "FROM provisioning_jobs pj " +
            "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
            "ORDER BY pj.id DESC";

    public static final String DELETE_PROVISIONING_JOB =
        "DELETE FROM provisioning_jobs WHERE id = $1 RETURNING id";

    public static final String GET_ALL_POLLED_DATA = """
            SELECT id, job_id, metric_type, data, polled_at
            FROM polled_data""";

    public static final String GET_POLLED_DATA_BY_JOB_ID = """
            SELECT id, job_id, metric_type, data, polled_at
            FROM polled_data
            WHERE job_id = $1
            ORDER BY polled_at DESC""";


    public static final String REGISTER_USER = "INSERT INTO users (username, password) VALUES ($1, $2) RETURNING id";

    public static final String GET_USER_BY_USERNAME = "SELECT id, username, password FROM users WHERE username = $1";

    public static final String GET_BY_RUN_ID =
        """
                      SELECT
                      dp.id AS id,
                      dp.discovery_profile_name AS name,
                      dp.ip AS ip,
                      dp.status AS status,
                      dp.port AS port,
                      dp.plugin_type AS plugin_type,
                      ARRAY_AGG(
                          JSON_BUILD_OBJECT(
                              'id', cp.id,
                              'protocol', cp.system_type,
                              'cred_data', cp.cred_data
                          )
                      ) AS credential
                  FROM discovery_profiles dp
                  LEFT JOIN discovery_credential_mapping dc ON dp.id = dc.discovery_id
                  LEFT JOIN credential_profile cp ON dc.credential_profile_id = cp.id
                  WHERE dp.id = $1
                  GROUP BY dp.id, dp.discovery_profile_name, dp.ip, dp.status, dp.port, dp.plugin_type;""";

    public static final String GET_DISCOVERY_RESULTS = "SELECT * FROM discovery_result WHERE discovery_id = $1";

    public static final String INSERT_PROVISIONING_AND_METRICS = """
    WITH input_ips AS (
        SELECT jsonb_array_elements_text($2::jsonb) AS ip
    ),
    discovery_validation AS (
        SELECT
            dr.ip,
            dr.credential_profile_id,
            dr.port,
            cp.system_type AS plugin_type
        FROM discovery_result dr
        JOIN input_ips i ON dr.ip = i.ip
        JOIN credential_profile cp ON cp.id = dr.credential_profile_id
        WHERE dr.discovery_id = $1
        AND dr.result = 'COMPLETED'
    ),
    invalid_ips AS (
        SELECT
            i.ip,
            CASE
                WHEN dr.ip IS NULL THEN 'IP not found in discovery results'
                ELSE 'Discovery not completed'
            END AS error
        FROM input_ips i
        LEFT JOIN discovery_result dr ON dr.ip = i.ip AND dr.discovery_id = $1
        WHERE dr.ip IS NULL OR dr.result != 'COMPLETED'
    ),
    inserted_provisioning_jobs AS (
        INSERT INTO provisioning_jobs (credential_profile_id, plugin_type, ip, port)
        SELECT
            dv.credential_profile_id,
            dv.plugin_type,
            dv.ip,
            dv.port
        FROM discovery_validation dv
        ON CONFLICT (ip, port) DO NOTHING
        RETURNING id AS provisioning_job_id, credential_profile_id, plugin_type, ip, port
    ),
    metric_names AS (
        SELECT name
        FROM (VALUES
            ('CPU'::metric_name),
            ('MEMORY'::metric_name),
            ('DISK'::metric_name),
            ('UPTIME'::metric_name),
            ('NETWORK'::metric_name),
            ('PROCESS'::metric_name)
        ) AS metrics (name)
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
        CROSS JOIN metric_names mn
        RETURNING metric_id, provisioning_job_id, name
    )
    SELECT
        COALESCE(
            (SELECT json_agg(
                json_build_object(
                    'ip', pj.ip,
                    'provisioning_job_id', pj.provisioning_job_id,
                    'credential_profile_id', pj.credential_profile_id,
                    'port', pj.port,
                    'protocol', pj.plugin_type,
                    'metric_id', m.metric_id,
                    'metric_name', m.name,
                    'cred_data', cp.cred_data
                )
            )
            FROM inserted_provisioning_jobs pj
            JOIN inserted_metrics m ON m.provisioning_job_id = pj.provisioning_job_id
            JOIN credential_profile cp ON pj.credential_profile_id = cp.id),
            '[]'::json
        ) AS records,
        COALESCE(
            (SELECT json_agg(pj.ip)
             FROM inserted_provisioning_jobs pj),
            '[]'::json
        ) AS valid_ips,
        COALESCE(
            (SELECT json_agg(
                json_build_object(
                    'ip', iv.ip,
                    'error', iv.error
                )
            )
            FROM invalid_ips iv),
            '[]'::json
        ) AS invalid_ips
    """;

    public static final String GET_PROVISIONING_JOB_AND_METRICS = """
    SELECT
        pj.ip,
        pj.port,
        cp.system_type AS protocol,
        cp.cred_data,
        m.metric_id,
        m.name AS metric_name,
        m.polling_interval,
        m.is_enabled
    FROM provisioning_jobs pj
    LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id
    JOIN metrics m ON m.provisioning_job_id = pj.id
    WHERE pj.id = $1
    AND m.name IN (
        SELECT jsonb_array_elements_text($2::jsonb)::metric_name
    )
    """;
    public static final String GET_ACTIVE_METRIC_JOBS = """
            SELECT
                m.metric_id,
                m.provisioning_job_id,
                m.name AS metric_name,
                m.polling_interval,
                m.is_enabled,
                pj.ip,
                pj.port,
                cp.cred_data,
                cp.system_type AS protocol
            FROM metrics m
            JOIN provisioning_jobs pj ON m.provisioning_job_id = pj.id
            JOIN credential_profile cp ON pj.credential_profile_id = cp.id
            WHERE m.is_enabled = true
            """;

    public static final String GET_PROVISIONING_JOB_BY_ID = """
        SELECT
            pj.id,
            pj.ip AS ip,
            pj.port,
            json_agg(
                json_build_object(
                    'metric_name', m.name,
                    'polling_interval', m.polling_interval,
                    'is_enabled', m.is_enabled
                )
            ) AS metrics
        FROM provisioning_jobs pj
        LEFT JOIN metrics m ON pj.id = m.provisioning_job_id
        WHERE pj.id = $1
        GROUP BY pj.id, pj.ip, pj.port;
    """;


    public static final String UPDATE_DISCOVERY_PROFILE_STATUS = "UPDATE discovery_profiles SET status = $1 WHERE id = $2";

    // $1 = provisioning_job_id, $2 = up increment (1 if sample succeeded else 0), $3 = is_up (boolean of current sample)
    public static final String UPSERT_AVAILABILITY = """
            INSERT INTO device_availability (provisioning_job_id, is_up, up_samples, total_samples, availability_pct)
            VALUES ($1, $3, $2, 1, CASE WHEN $3 THEN 100.0 ELSE 0.0 END)
            ON CONFLICT (provisioning_job_id) DO UPDATE
            SET total_samples = device_availability.total_samples + 1,
                up_samples = device_availability.up_samples + $2,
                availability_pct = ROUND(((device_availability.up_samples + $2) * 100.0) / (device_availability.total_samples + 1), 2),
                is_up = $3,
                last_change = CASE WHEN device_availability.is_up <> $3 THEN now() ELSE device_availability.last_change END""";

    public static final String GET_AVAILABILITY_BY_JOB = "SELECT * FROM device_availability WHERE provisioning_job_id = $1";
}
