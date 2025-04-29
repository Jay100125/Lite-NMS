package com.example.NMS.constant;

public class QueryConstant
{
  public static final String GET_ALL_CREDENTIALS = "SELECT * FROM credential_profile";

  public static final String GET_CREDENTIAL_BY_ID = "SELECT * FROM credential_profile WHERE id = $1";

  public static final String INSERT_CREDENTIAL = "INSERT INTO credential_profile (credential_name, system_type, cred_data) VALUES ($1, $2, $3) returning id";

  public static final String UPDATE_CREDENTIAL = "UPDATE credential_profile SET credential_name = $1, system_type = $2, cred_data = $3 WHERE id = $4 returning id";

  public static final String DELETE_CREDENTIAL = "DELETE FROM credential_profile WHERE id = $1 returning id";


//  public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, credential_profile_id, ip, port) VALUES ($1, $2, $3, $4) RETURNING id";

  public static final String INSERT_DISCOVERY = "INSERT INTO discovery_profiles (discovery_profile_name, credential_profile_id, ip, port) VALUES ($1, $2, $3, $4) RETURNING id";

  public static final String UPDATE_DISCOVERY = "UPDATE discovery_profiles SET discovery_profile_name = $1, credential_profile_id = $2, ip = $3, port = $4 WHERE id = $5 RETURNING id";

  public static final String GET_ALL_DISCOVERIES = "SELECT * FROM discovery_profiles";

  public static final String GET_DISCOVERY_BY_ID = "SELECT * FROM discovery_profiles WHERE id = $1";

  public static final String DELETE_DISCOVERY = "DELETE FROM discovery_profiles WHERE id = $1 RETURNING id";

  public static final String RUN_DISCOVERY = "SELECT dp.ip, dp.port, cpid, cp.cred_data \n" +
    "FROM discovery_profiles dp, unnest(dp.credential_profile_id) AS cpid \n" +
    "JOIN credential_profile cp ON cp.id = cpid \n" +
    "WHERE dp.id = $1";

  public static final String INSERT_DISCOVERY_RESULT = "INSERT INTO discovery_result (discovery_id, ip, port, result, msg, credential_profile_id) " +
    "VALUES ($1, $2, $3, $4, $5, $6) RETURNING id";

}
