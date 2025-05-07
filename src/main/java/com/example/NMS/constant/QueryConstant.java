package com.example.NMS.constant;

public class QueryConstant
{
  public static final String GET_ALL_CREDENTIALS = "SELECT * FROM credential_profile";

  public static final String GET_CREDENTIAL_BY_ID = "SELECT * FROM credential_profile WHERE id = $1";

  public static final String INSERT_CREDENTIAL = "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) returning id";

    public static final String UPDATE_CREDENTIAL = "UPDATE credential_profile\n" +
      "SET credential_name = COALESCE($1, credential_name),\n" +
      "    system_type = COALESCE($2, system_type),\n" +
      "    cred_data = COALESCE($3, cred_data)\n" +
      "WHERE id = $4\n" +
      "RETURNING id";

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

  public static final String INSERT_DISCOVERY_CREDENTIAL = "INSERT INTO discovery_credential_mapping (discovery_id, credential_profile_id) VALUES ($1, $2)";

  public static final String DELETE_DISCOVERY_CREDENTIALS = "DELETE FROM discovery_credential_mapping WHERE discovery_id = $1";

  public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, ip, port, status) VALUES ($1, $2, $3, FALSE) RETURNING id";

  public static final String UPDATE_DISCOVERY = "UPDATE discovery_profiles SET discovery_profile_name = $1, ip = $2, port = $3, status = FALSE WHERE id = $4 RETURNING id";

  public static final String SET_DISCOVERY_STATUS = "UPDATE discovery_profiles SET status = $1 WHERE id = $2 RETURNING id";

  public static final String INSERT_DISCOVERY_RESULT = "INSERT INTO discovery_result (discovery_id, ip, port, result, msg, credential_profile_id) " +
    "VALUES ($1, $2, $3, $4, $5, $6) " +
    "ON CONFLICT (discovery_id, ip) DO UPDATE " +
    "SET port = EXCLUDED.port, result = EXCLUDED.result, msg = EXCLUDED.msg, credential_profile_id = EXCLUDED.credential_profile_id " +
    "RETURNING id";

  public static final String VALIDATE_DISCOVERY_FROM_RESULT = "SELECT ip, result, credential_profile_id, port" +
    "FROM discovery_result WHERE discovery_id = $1 AND ip = ANY($2::varchar[])";

  public static final String INSERT_PROVISIONING_JOB = "INSERT INTO provisioning_jobs (credential_profile_id, ip, port) " +
    "VALUES ($1, $2, $3) RETURNING id";

  public static final String INSERT_DEFAULT_METRICS = "INSERT INTO metrics (provisioning_job_id, name, polling_interval) " +
    "VALUES ($1, $2, $3) returning metric_id as id";

  public static final String GET_METRICS_BY_PROVISIONING_JOB = "SELECT * FROM metrics WHERE provisioning_job_id = $1";

  public static final String INSERT_POLLING_RESULT = "INSERT INTO polling_results (provisioning_job_id, name, value) " +
    "VALUES ($1, $2, $3) RETURNING id";

  public static final String DELETE_STALE_METRICS =
    "DELETE FROM metrics " +
      "WHERE provisioning_job_id = $1 " +
      "AND name NOT IN (SELECT UNNEST($2::varchar[])::metric_name) returning provisioning_job_id as id";

  public static final String UPSERT_METRICS = """
            INSERT INTO metrics (provisioning_job_id, name, polling_interval)
            VALUES ($1, $2, $3)
            ON CONFLICT (provisioning_job_id, name)
            DO UPDATE SET polling_interval = EXCLUDED.polling_interval
            RETURNING metric_id as id""";

  public static final String VALIDATE_PROVISIONING_JOB =
    "SELECT id FROM provisioning_jobs WHERE id = $1";

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

}
