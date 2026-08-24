ALTER TABLE tasks
    DROP COLUMN peak_memory_bytes,
    DROP COLUMN memory_total_bytes,
    DROP COLUMN memory_sample_count,
    DROP COLUMN cpu_time_ms;
