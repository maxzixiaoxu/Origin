-- Reverse of 000001_init.
--
-- Dropped in dependency order. Triggers and indexes go with their tables, so
-- only the tables, the shared trigger function, and the enum need explicit
-- statements.

BEGIN;

DROP TRIGGER IF EXISTS jobs_set_updated_at ON jobs;
DROP TRIGGER IF EXISTS queues_set_updated_at ON queues;

DROP TABLE IF EXISTS queue_stats_minute;
DROP TABLE IF EXISTS job_attempts;
DROP TABLE IF EXISTS jobs;
DROP TABLE IF EXISTS queues;

DROP FUNCTION IF EXISTS set_updated_at();

DROP TYPE IF EXISTS job_status;

COMMIT;
