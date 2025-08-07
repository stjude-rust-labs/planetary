-- An enumeration for task statuses.
CREATE TYPE task_state AS ENUM(
    'UNKNOWN',
    'QUEUED',
    'INITIALIZING',
    'RUNNING',
    'PAUSED',
    'COMPLETE',
    'EXECUTOR_ERROR',
    'SYSTEM_ERROR',
    'CANCELING',
    'CANCELED',
    'PREEMPTED'
);

-- The tasks table.
CREATE TABLE tasks (
    id SERIAL PRIMARY KEY,
    tes_id TEXT NOT NULL,
    state task_state NOT NULL DEFAULT 'UNKNOWN',
    name TEXT NULL,
    description TEXT NULL,
    inputs JSONB NULL,
    outputs JSONB NULL,
    cpu_cores INTEGER NULL,
    preemptible BOOLEAN NULL,
    ram_gb DOUBLE PRECISION NULL,
    disk_gb DOUBLE PRECISION NULL,
    zones TEXT[] NULL,
    backend_parameters JSONB NULL,
    backend_parameters_strict BOOLEAN NULL,
    executors JSONB NOT NULL,
    volumes TEXT[] NULL,
    tags JSONB NULL,
    output_files JSONB NULL,
    system_logs TEXT[] NULL,
    creation_time TIMESTAMPTZ NOT NULL DEFAULT (now() at time zone 'utc'),
    CONSTRAINT tasks_tes_id_unique UNIQUE (tes_id)
);

-- Create an index on the `name` column for list operations.
CREATE INDEX idx_tasks_name ON tasks (name text_pattern_ops);

-- Create an index on the `state` column for list operations.
CREATE INDEX idx_tasks_state ON tasks (state);

-- Create an index on the `tags` column for list operations.
CREATE INDEX idx_tasks_tags ON tasks USING gin (tags);

-- An enumeration for pod kind.
CREATE TYPE pod_kind AS ENUM(
    'INPUTS',
    'EXECUTOR',
    'OUTPUTS'
);

-- An enumeration for pod state.
CREATE TYPE pod_state AS ENUM(
    'UNKNOWN',
    'WAITING',
    'INITIALIZING',
    'RUNNING',
    'SUCCEEDED',
    'FAILED',
    'IMAGE_PULL_ERROR'
);

-- The pods table.
-- This table keeps track of the pods that run as part of a task.
CREATE TABLE pods (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    kind pod_kind NOT NULL,
    state pod_state NOT NULL,
    executor_index INTEGER NULL,
    start_time TIMESTAMPTZ NULL,
    end_time TIMESTAMPTZ NULL,
    stdout TEXT NULL,
    stderr TEXT NULL,
    exit_code INTEGER NULL,
    creation_time TIMESTAMPTZ NOT NULL DEFAULT (now() at time zone 'utc'),
    CONSTRAINT pods_name_unique UNIQUE (name)
);

-- Create an index on the `state` column for list operations.
CREATE INDEX idx_pods_state ON pods (state);
