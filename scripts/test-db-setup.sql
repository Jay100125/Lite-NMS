-- Provisions the throwaway-test role used by PgTestBase. Idempotent.
DO $$ BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname='nms_test') THEN
    CREATE ROLE nms_test LOGIN PASSWORD 'nms_test' CREATEDB;
  END IF;
END $$;
-- The admin/bootstrap database PgTestBase connects to in order to CREATE/DROP throwaway DBs:
SELECT 'CREATE DATABASE nms_test OWNER nms_test'
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname='nms_test')\gexec
