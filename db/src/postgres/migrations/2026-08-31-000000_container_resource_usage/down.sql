ALTER TABLE tasks
    ADD COLUMN peak_memory_bytes BIGINT NULL,
    ADD COLUMN memory_total_bytes BIGINT NULL,
    ADD COLUMN memory_sample_count BIGINT NULL,
    ADD COLUMN cpu_time_ms BIGINT NULL;

DROP TABLE task_container_usage;
