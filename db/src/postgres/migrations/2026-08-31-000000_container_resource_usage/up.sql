-- Per-container aggregation of sampled task resource usage.
--
-- Usage is sampled per container (the input transporter, each executor, and
-- the output transporter) and folded in the database: the peak is kept with
-- `GREATEST` and the average is derived from a running total and sample
-- count.
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

-- The CPU accounting baseline of each observed container instance.
--
-- The monitor samples cumulative per-container CPU counters from kubelets
-- and records the observed counter values; the database computes each
-- counter's delta against the stored baseline and advances the baseline
-- atomically with the aggregate update in `task_container_usage`. Keeping
-- the baseline durable with the aggregate makes recording idempotent:
-- re-recording an observation whose write already committed (e.g. after an
-- ambiguous commit outcome) yields a zero delta, and monitor restarts
-- continue accounting from the stored baseline without loss.
--
-- Baselines are keyed by pod as well, so that multiple pods carrying the
-- same task label are accounted independently.
CREATE TABLE task_container_baseline (
    task_id            INTEGER NOT NULL REFERENCES tasks(id) ON DELETE CASCADE,
    pod_name           TEXT NOT NULL,
    container_name     TEXT NOT NULL,
    start_time_seconds DOUBLE PRECISION NULL,
    cpu_seconds        DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (task_id, pod_name, container_name)
);
