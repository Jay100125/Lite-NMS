package com.example.NMS.constant;

public class Constant {

    public static final int SERVER_PORT = 8080;

    public static final int MAX_WORKER_EXECUTION_TIME = 900;

    public static final int TIMER_INTERVAL_SECONDS = 10;

    public static final String DB_EXECUTE_QUERY = "db.execute.query";

    public static final String DB_EXECUTE_BATCH_QUERY = "db.execute.batch.query";

    public static final String DISCOVERY_RUN = "discovery.run";

    public static final String DISCOVERY_STATUS_PENDING = "PENDING";

    public static final String DISCOVERY_STATUS_RUNNING = "RUNNING";

    public static final String DISCOVERY_STATUS_COMPLETED = "COMPLETED";

    public static final String DISCOVERY_STATUS_FAILED = "FAILED";

    public static final String EVENT_COMPLETION = "event.completion";


    public static final String POLLING_BATCH_PROCESS = "polling.batch.process";

    public static final String SUCCESS = "success";

    public static final String FAILURE = "failure";

    public static final String STATUS_CODE = "status.code";

    public static final String USERNAME = "username";

    public static final String RESULT = "result";

    public static final String CREDENTIAL_NAME = "credential_name";

    public static final String SYSTEM_TYPE = "system_type";

    public static final String CRED_DATA = "cred_data";

    public static final String USER = "user";

    public static final String PASSWORD = "password";

    public static final String QUERY = "query";

    public static final String PARAMS = "params";

    public static final String BATCHPARAMS = "batchParams";

    public static final String IP_ADDRESS = "ip.address";

    public static final String MESSAGE = "message";

    public static final String STATUS = "status";

    public static final String ERROR = "error";

    public static final String ID = "id";

    public static final String JOB_ID_PATH_PARAM = "jobId";

    public static final String PROTOCOL = "protocol";

    public static final String LINUX = "linux";

    public static final String PLUGIN_TYPE = "plugin_type";

    public static final String DISCOVERY_PROFILE_NAME = "discovery_profile_name";

    public static final String CREDENTIAL_PROFILE_ID = "credential_profile_id";

    public static final String PORT = "port";

    public static final String SELECTED_IPS = "selected_ips";

    public static final String PROVISIONING_JOB_ID = "provisioning_job_id";

    public static final String METRIC_ID = "metric_id";

    public static final String METRIC_NAME = "metric_name";

    public static final String POLLING_INTERVAL = "polling_interval";

    public static final String ORIGINAL_INTERVAL = "original_interval";

    public static final String REMAINING_TIME = "remaining_time";

    public static final String DISCOVERY_ID = "discovery_id";

    public static final String CREDENTIAL_ID = "credential_id";

    public static final String IP = "ip";

    public static final String IS_ENABLED = "is_enabled";

    public static final String REQUEST_TYPE = "request.type";

    /** Per-run discovery progress address prefix; full address is nms.discovery.<discoveryId>. */
    public static final String DISCOVERY_EVENT_ADDRESS_PREFIX = "nms.discovery.";

    /** Always-present discriminator on engine result lines (v2 spec §4.2). */
    public static final String EVENT_TYPE = "event_type";

    public static final String JOB_ID = "job_id";

    public static final String REQUEST_ID = "request_id";

    public static final String EVENT_POLL = "poll";

    public static final String EVENT_DISCOVERY = "discovery";

    public static final String DISCOVERY = "discovery";

    public static final String POLLING = "polling";

    public static final String CREDENTIAL_PROFILES = "credential_profiles";

    public static final String TARGETS = "targets";

    public static final String STORAGE_POLL_RESULTS = "storage.poll.results";

    public static final String PLUGIN_EXECUTE = "plugin.execute";

    public static final String STORAGE_DISCOVERY_RESULTS = "storage.discovery.results";

    public static final String STORAGE_RESULTS = "storage.results";

    /** Dedicated address for availability samples (one per device per poll cycle, emitted from Polling's ping gate). */
    public static final String AVAILABILITY_SAMPLE = "availability.sample";

    /** A device flips Up→Down only after this many consecutive failed ping samples (flap damping); one success resets the count. */
    public static final int AVAILABILITY_DOWN_THRESHOLD = 3;

    public static final int BATCH_SIZE = 25;

    public static final String PLUGIN_LINUX = "LINUX";

    public static final String PLUGIN_SNMP = "SNMP";

    public static final String PLUGIN_WINRM = "WINRM";

    public static final String COMMUNITY = "community";

    public static final String SNMP_VERSION = "version";

    public static final String SNMP_V2C = "2c";

}


