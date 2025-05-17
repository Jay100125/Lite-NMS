package com.example.NMS.constant;

public class QueryConstant
{
  public static final String GET_ALL_CREDENTIALS = "SELECT * FROM credential_profile";

  public static final String GET_CREDENTIAL_BY_ID = "SELECT * FROM credential_profile WHERE id = $1";

  public static final String GET_CREDENTIAL_DATA = "SELECT cred_data FROM credential_profile WHERE id = $1";

  public static final String INSERT_CREDENTIAL = "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) returning id";

    public static final String UPDATE_CREDENTIAL = "UPDATE credential_profile\n" +
      "SET credential_name = COALESCE($1, credential_name),\n" +
      "    system_type = COALESCE($2, system_type),\n" +
      "    cred_data = COALESCE($3, cred_data)\n" +
      "WHERE id = $4\n" +
      "RETURNING *";

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

  public static final String RUN_DISCOVERY = "SELECT dp.ip, dp.port, dcm.credential_profile_id AS cpid, cp.cred_data \n" +
    "FROM discovery_profiles dp \n" +
    "JOIN discovery_credential_mapping dcm ON dp.id = dcm.discovery_id \n" +
    "JOIN credential_profile cp ON dcm.credential_profile_id = cp.id \n" +
    "WHERE dp.id = $1";

  public static final String INSERT_DISCOVERY_CREDENTIAL = "INSERT INTO discovery_credential_mapping (discovery_id, credential_profile_id) VALUES ($1, $2) RETURNING discovery_id as id";

  public static final String DELETE_DISCOVERY_CREDENTIALS = "DELETE FROM discovery_credential_mapping WHERE discovery_id = $1";

  public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, ip, port, status) VALUES ($1, $2, $3, FALSE) RETURNING id";

  public static final String UPDATE_DISCOVERY = "UPDATE discovery_profiles SET discovery_profile_name = $1, ip = $2, port = $3, status = FALSE WHERE id = $4 RETURNING id";

  public static final String SET_DISCOVERY_STATUS = "UPDATE discovery_profiles SET status = $1 WHERE id = $2 RETURNING id";

  public static final String INSERT_DISCOVERY_RESULT = "INSERT INTO discovery_result (discovery_id, ip, port, result, msg, credential_profile_id) " +
    "VALUES ($1, $2, $3, $4, $5, $6) " +
    "ON CONFLICT (discovery_id, ip) DO UPDATE " +
    "SET port = EXCLUDED.port, result = EXCLUDED.result, msg = EXCLUDED.msg, credential_profile_id = EXCLUDED.credential_profile_id " +
    "RETURNING id";


  public static final String INSERT_PROVISIONING_JOB = "INSERT INTO provisioning_jobs (credential_profile_id, ip, port) " +
    "VALUES ($1, $2, $3) RETURNING id";

  public static final String INSERT_DEFAULT_METRICS =
    "INSERT INTO metrics (provisioning_job_id, name, polling_interval, is_enabled) " +
      "VALUES ($1, $2, $3, $4) RETURNING metric_id as id";

  public static final String UPSERT_METRICS =
    "INSERT INTO metrics (provisioning_job_id, name, polling_interval, is_enabled) " +
      "VALUES ($1, $2, COALESCE($3, 300), $4) " +
      "ON CONFLICT (provisioning_job_id, name) " +
      "DO UPDATE SET polling_interval = COALESCE(EXCLUDED.polling_interval, metrics.polling_interval), " +
      "is_enabled = EXCLUDED.is_enabled " +
      "RETURNING metric_id as id";

  public static final String INSERT_POLLED_DATA =
    "INSERT INTO polled_data (job_id, metric_type, data) " +
      "VALUES ($1, $2, $3::jsonb) returning id";

  public static final String GET_ALL_PROVISIONING_JOBS =
    "SELECT pj.*, cp.credential_name, cp.system_type " +
      "FROM provisioning_jobs pj " +
      "LEFT JOIN credential_profile cp ON pj.credential_profile_id = cp.id " +
      "ORDER BY pj.id DESC";

  public static final String DELETE_PROVISIONING_JOB =
    "DELETE FROM provisioning_jobs WHERE id = $1 RETURNING id";

  public static final String GET_ALL_POLLED_DATA = """
            SELECT id, job_id, metric_type, data, TO_CHAR(polled_at, 'YYYY-MM-DD"T"HH24:MI:SS') AS polled_at
            FROM polled_data""";


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
                  ARRAY_AGG(
                      JSON_BUILD_OBJECT(
                          'id', cp.id,
                          'username', cp.cred_data->>'user',
                          'password', cp.cred_data->>'password'
                      )
                  ) AS credential
              FROM discovery_profiles dp
              LEFT JOIN discovery_credential_mapping dc ON dp.id = dc.discovery_id
              LEFT JOIN credential_profile cp ON dc.credential_profile_id = cp.id
              WHERE dp.id = $1
              GROUP BY dp.id, dp.discovery_profile_name, dp.ip, dp.status, dp.port;""";

  public static final String GET_DISCOVERY_RESULTS = "SELECT * FROM discovery_result WHERE discovery_id = $1";

  public static final String INSERT_PROVISIONING_AND_METRICS = """
       WITH input_ips AS (
            SELECT unnest($2::varchar[]) AS ip
        ),
        discovery_validation AS (
            SELECT
                dr.ip,
                dr.credential_profile_id,
                dr.port
            FROM discovery_result dr
            JOIN input_ips i ON dr.ip = i.ip
            WHERE dr.discovery_id = $1
            AND dr.result = 'completed'
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
            WHERE dr.ip IS NULL OR dr.result != 'completed'
        ),
        inserted_provisioning_jobs AS (
            INSERT INTO provisioning_jobs (credential_profile_id, ip, port)
            SELECT
                dv.credential_profile_id,
                dv.ip,
                dv.port
            FROM discovery_validation dv
            ON CONFLICT (ip) DO NOTHING
            RETURNING id AS provisioning_job_id, credential_profile_id, ip, port
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
            INSERT INTO metrics (provisioning_job_id, name, polling_interval, is_enabled)
            SELECT
                pj.provisioning_job_id,
                mn.name,
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
                        'metrics', (
                            SELECT json_agg(
                                json_build_object(
                                    'metric_id', m.metric_id,
                                    'name', m.name
                                )
                            )
                            FROM inserted_metrics m
                            WHERE m.provisioning_job_id = pj.provisioning_job_id
                        ),
                        'cred_data', cp.cred_data
                    )
                )
                FROM inserted_provisioning_jobs pj
                JOIN credential_profile cp ON pj.credential_profile_id = cp.id),
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
}
