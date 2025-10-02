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

-- An enumeration for container kind.
CREATE TYPE container_kind AS ENUM(
    'INPUTS',
    'EXECUTOR',
    'OUTPUTS'
);

-- The containers table.
-- This table keeps track of the individual containers used to run a task.
-- An entry is inserted into this table when the container has completed.
CREATE TABLE containers (
    id SERIAL PRIMARY KEY,
    task_id INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    kind container_kind NOT NULL,
    executor_index INTEGER NULL,
    start_time TIMESTAMPTZ NOT NULL,
    end_time TIMESTAMPTZ NOT NULL,
    stdout TEXT NULL,
    stderr TEXT NULL,
    exit_code INTEGER NOT NULL,
    creation_time TIMESTAMPTZ NOT NULL DEFAULT (now() at time zone 'utc')
);

-- Create an index of the `task id` column.
CREATE INDEX idx_containers_task_id ON containers (task_id);

-- Table for internal system errors encountered.
CREATE TABLE errors (
    id SERIAL PRIMARY KEY,
    source TEXT NOT NULL,
    task_id INTEGER NULL REFERENCES tasks(id) ON DELETE CASCADE,
    message TEXT NOT NULL,
    creation_time TIMESTAMPTZ NOT NULL DEFAULT (now() at time zone 'utc')
);

-- Create an index on the `source` column for errors.
CREATE INDEX idx_errors_task_id ON errors (task_id);
CREATE INDEX idx_errors_source ON errors (source);
