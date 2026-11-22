CREATE TABLE IF NOT EXISTS runs (
 run_id text PRIMARY KEY, spec_hash text NOT NULL, spec jsonb NOT NULL, config jsonb NOT NULL,
 manifest_hash text NOT NULL, discovery jsonb NOT NULL, expected_images bigint NOT NULL CHECK(expected_images>0),
 state text NOT NULL DEFAULT 'PENDING', created_at timestamptz NOT NULL DEFAULT clock_timestamp()
);
CREATE TABLE IF NOT EXISTS tasks (
 run_id text NOT NULL REFERENCES runs, task_id text NOT NULL, spec_hash text NOT NULL,
 manifest_uri text NOT NULL, manifest_sha256 text NOT NULL, expected_images bigint NOT NULL CHECK(expected_images>0),
 state text NOT NULL CHECK(state IN ('PLANNED','QUEUED','RUNNING','RETRY_WAIT','SUCCEEDED','FAILED')),
 state_version bigint NOT NULL DEFAULT 0, attempt_count integer NOT NULL DEFAULT 0,
 available_at timestamptz NOT NULL DEFAULT clock_timestamp(), attempt_id text, lease_token text,
 lease_owner text, lease_expires_at timestamptz, deadline_at timestamptz,
 PRIMARY KEY(run_id,task_id)
);
CREATE INDEX IF NOT EXISTS tasks_queue ON tasks(run_id,state,available_at);
CREATE TABLE IF NOT EXISTS task_attempts (
 run_id text NOT NULL, task_id text NOT NULL, attempt_id text NOT NULL, worker text NOT NULL,
 lease_token text NOT NULL, state text NOT NULL DEFAULT 'RUNNING', environment jsonb,
 started_at timestamptz NOT NULL DEFAULT clock_timestamp(), finished_at timestamptz,
 error_code text, candidate_output jsonb, metrics jsonb,
 PRIMARY KEY(run_id,task_id,attempt_id), FOREIGN KEY(run_id,task_id) REFERENCES tasks
);
CREATE TABLE IF NOT EXISTS inbox_messages (
 kafka_log_generation text NOT NULL, topic text NOT NULL, partition_id integer NOT NULL,
 offset_id bigint NOT NULL, event_id text UNIQUE, digest text NOT NULL, outcome text NOT NULL,
 processed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(kafka_log_generation,topic,partition_id,offset_id)
);
CREATE TABLE IF NOT EXISTS outbox_events (
 event_id text PRIMARY KEY, run_id text, task_id text, topic text NOT NULL, payload jsonb NOT NULL,
 created_at timestamptz NOT NULL DEFAULT clock_timestamp(), acknowledged_at timestamptz,
 retry_count integer NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS outbox_pending ON outbox_events(created_at) WHERE acknowledged_at IS NULL;
CREATE TABLE IF NOT EXISTS committed_outputs (
 run_id text NOT NULL, task_id text NOT NULL, attempt_id text NOT NULL, uri text NOT NULL,
 sha256 text NOT NULL, row_count bigint NOT NULL, success_count bigint NOT NULL,
 error_count bigint NOT NULL, committed_at timestamptz NOT NULL DEFAULT clock_timestamp(),
 PRIMARY KEY(run_id,task_id), FOREIGN KEY(run_id,task_id,attempt_id) REFERENCES task_attempts,
 CHECK(row_count=success_count+error_count)
);
