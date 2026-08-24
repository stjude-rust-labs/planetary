-- Columns for aggregating sampled task resource usage.
--
-- Usage is folded in the database so that aggregation survives monitor
-- restarts: the peak is kept with `GREATEST` and the average is derived from
-- a running total and sample count.
--
-- Memory values are sampled Kubernetes working set memory, in bytes.
ALTER TABLE tasks
    ADD COLUMN peak_memory_bytes BIGINT NULL,
    ADD COLUMN memory_total_bytes BIGINT NULL,
    ADD COLUMN memory_sample_count BIGINT NULL,
    ADD COLUMN cpu_time_ms BIGINT NULL;
