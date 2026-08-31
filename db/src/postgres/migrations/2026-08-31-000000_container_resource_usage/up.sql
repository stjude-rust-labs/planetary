-- Per-container aggregation of sampled task resource usage.
--
-- Usage is sampled per container (the input transporter, each executor, and
-- the output transporter) and folded in the database so that aggregation
-- survives monitor restarts: the peak is kept with `GREATEST` and the
-- average is derived from a running total and sample count.
--
-- Task-level usage is derived from the executor containers' rows at read
-- time; because a task pod's executors run sequentially, the task-level peak
-- is the greatest of the executor peaks.
--
-- Memory values are sampled Kubernetes working set memory, in bytes.
CREATE TABLE task_container_usage (
    task_id             INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    container_name      TEXT NOT NULL,
    peak_memory_bytes   BIGINT NULL,
    memory_total_bytes  BIGINT NULL,
    memory_sample_count BIGINT NULL,
    cpu_time_ms         BIGINT NULL,
    PRIMARY KEY (task_id, container_name)
);
