ALTER TABLE tasks
    DROP COLUMN peak_rss_bytes,
    DROP COLUMN rss_total_bytes,
    DROP COLUMN rss_sample_count,
    DROP COLUMN cpu_time_ms;
