-- Aggregated resource usage for each task container.
CREATE TABLE task_container_usage (
    task_id             INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    container_name      TEXT NOT NULL,
    peak_memory_bytes   BIGINT NULL,
    memory_total_bytes  BIGINT NULL,
    memory_sample_count BIGINT NULL,
    cpu_seconds         DOUBLE PRECISION NULL,
    PRIMARY KEY (task_id, container_name)
);

-- Durable CPU counter baselines for idempotent delta accounting.
CREATE TABLE task_container_baseline (
    task_id            INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    pod_name           TEXT NOT NULL,
    container_name     TEXT NOT NULL,
    start_time_seconds DOUBLE PRECISION NULL,
    cpu_seconds        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (task_id, pod_name, container_name)
);
