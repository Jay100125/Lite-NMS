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
