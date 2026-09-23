//! TES task log metadata built from per-container resource usage.

use super::models::ContainerUsage;

/// The name prefix of executor containers within a task pod.
///
/// This must match the container naming used by the orchestrator's task pod
/// template (`executor-N` for the task's Nth executor).
const EXECUTOR_CONTAINER_PREFIX: &str = "executor-";

/// Returns a finite, non-negative CPU observation with negative zero
/// normalized to zero.
pub(super) fn normalize_cpu_seconds(cpu_seconds: f64) -> Option<f64> {
    if !cpu_seconds.is_finite() || cpu_seconds < 0.0 {
        None
    } else if cpu_seconds == 0.0 {
        Some(0.0)
    } else {
        Some(cpu_seconds)
    }
}

/// Builds a usage metadata entry from aggregate values.
///
/// Values are encoded as strings, as the TES specification types
/// `TaskLog.metadata` as a map of strings; returns `None` if the aggregate
/// carries no measurements.
fn usage_entry(
    peak_memory_bytes: Option<i64>,
    memory_total_bytes: Option<i64>,
    memory_sample_count: Option<i64>,
    cpu_seconds: Option<f64>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut entry = serde_json::Map::new();

    if let Some(peak) = peak_memory_bytes {
        entry.insert("peak_memory_bytes".to_string(), peak.to_string().into());
    }

    if let (Some(total), Some(count)) = (memory_total_bytes, memory_sample_count)
        && count > 0
    {
        entry.insert(
            "avg_memory_bytes".to_string(),
            (total / count).to_string().into(),
        );
    }

    if let Some(cpu) = cpu_seconds.and_then(normalize_cpu_seconds) {
        entry.insert(
            "cpu_time_ms".to_string(),
            (cpu * 1000.0).round().to_string().into(),
        );
    }

    if entry.is_empty() { None } else { Some(entry) }
}

/// Builds the task log metadata object from per-container aggregated
/// resource usage.
///
/// The task-level `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms`
/// keys cover the task's executor containers only (excluding the input and
/// output transporter containers). Because a task pod's executors run
/// sequentially, the task-level peak is the greatest of the executor peaks
/// and the CPU time is the sum of the executor CPU times.
///
/// The `resource_usage` key carries the per-container breakdown, keyed by
/// container name (`inputs`, `executor-N`, `outputs`). A container absent
/// from the breakdown was never sampled (e.g. it completed within a single
/// sampling interval), which is not the same as zero usage.
///
/// Returns `None` if no usage was recorded.
pub(super) fn resource_usage_metadata(usage: &[ContainerUsage]) -> Option<serde_json::Value> {
    let mut metadata = serde_json::Map::new();

    // Fold the executor containers' aggregates into the task-level keys
    let mut peak: Option<i64> = None;
    let mut total: Option<i64> = None;
    let mut count: Option<i64> = None;
    let mut cpu_seconds: Option<f64> = None;
    for container in usage {
        if !container
            .container_name
            .starts_with(EXECUTOR_CONTAINER_PREFIX)
        {
            continue;
        }

        if let Some(p) = container.peak_memory_bytes {
            peak = Some(peak.unwrap_or(0).max(p));
        }

        if let Some(t) = container.memory_total_bytes {
            total = Some(total.unwrap_or(0) + t);
        }

        if let Some(c) = container.memory_sample_count {
            count = Some(count.unwrap_or(0) + c);
        }

        if let Some(seconds) = container.cpu_seconds.and_then(normalize_cpu_seconds) {
            cpu_seconds = Some(cpu_seconds.unwrap_or(0.0) + seconds);
        }
    }

    if let Some(entry) = usage_entry(peak, total, count, cpu_seconds) {
        metadata.extend(entry);
    }

    // Add the per-container breakdown
    let mut breakdown = serde_json::Map::new();
    for container in usage {
        if let Some(entry) = usage_entry(
            container.peak_memory_bytes,
            container.memory_total_bytes,
            container.memory_sample_count,
            container.cpu_seconds,
        ) {
            breakdown.insert(container.container_name.clone(), entry.into());
        }
    }

    if !breakdown.is_empty() {
        metadata.insert("resource_usage".to_string(), breakdown.into());
    }

    if metadata.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(metadata))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a container usage row for tests.
    fn usage(
        name: &str,
        peak: Option<i64>,
        total: Option<i64>,
        count: Option<i64>,
        cpu: Option<f64>,
    ) -> ContainerUsage {
        ContainerUsage {
            task_id: 1,
            container_name: name.to_string(),
            peak_memory_bytes: peak,
            memory_total_bytes: total,
            memory_sample_count: count,
            cpu_seconds: cpu,
        }
    }

    #[test]
    fn metadata_is_none_without_usage() {
        assert_eq!(resource_usage_metadata(&[]), None);
        assert_eq!(
            resource_usage_metadata(&[usage("executor-0", None, None, None, None)]),
            None
        );
    }

    #[test]
    fn task_level_keys_cover_executors_only() {
        let metadata = resource_usage_metadata(&[
            usage("inputs", Some(500), Some(1000), Some(2), Some(0.05)),
            usage("executor-0", Some(100), Some(150), Some(2), Some(1.0)),
            usage("executor-1", Some(300), Some(300), Some(1), Some(2.0)),
            usage("outputs", Some(400), Some(400), Some(1), Some(0.025)),
        ])
        .expect("should have metadata");

        // Peak is the greatest executor peak; the average and CPU time fold
        // across the executors; the transporters are excluded
        assert_eq!(metadata["peak_memory_bytes"], "300");
        assert_eq!(metadata["avg_memory_bytes"], "150");
        assert_eq!(metadata["cpu_time_ms"], "3000");

        // The breakdown carries every sampled container
        let breakdown = &metadata["resource_usage"];
        assert_eq!(breakdown["inputs"]["peak_memory_bytes"], "500");
        assert_eq!(breakdown["inputs"]["avg_memory_bytes"], "500");
        assert_eq!(breakdown["inputs"]["cpu_time_ms"], "50");
        assert_eq!(breakdown["executor-0"]["cpu_time_ms"], "1000");
        assert_eq!(breakdown["executor-1"]["peak_memory_bytes"], "300");
        assert_eq!(breakdown["outputs"]["cpu_time_ms"], "25");
    }

    #[test]
    fn transporter_only_usage_omits_task_level_keys() {
        let metadata =
            resource_usage_metadata(&[usage("inputs", Some(500), Some(500), Some(1), Some(0.05))])
                .expect("should have metadata");

        assert!(metadata.get("peak_memory_bytes").is_none());
        assert!(metadata.get("avg_memory_bytes").is_none());
        assert!(metadata.get("cpu_time_ms").is_none());
        assert_eq!(metadata["resource_usage"]["inputs"]["cpu_time_ms"], "50");
    }

    #[test]
    fn partial_dimensions_are_omitted() {
        let metadata =
            resource_usage_metadata(&[usage("executor-0", None, None, Some(0), Some(0.123))])
                .expect("should have metadata");

        assert!(metadata.get("peak_memory_bytes").is_none());
        assert!(metadata.get("avg_memory_bytes").is_none());
        assert_eq!(metadata["cpu_time_ms"], "123");
    }

    #[test]
    fn cpu_milliseconds_are_rounded_after_aggregation() {
        let single_container_metadata =
            resource_usage_metadata(&[usage("executor-0", None, None, None, Some(0.0008))])
                .expect("should have metadata");

        assert_eq!(single_container_metadata["cpu_time_ms"], "1");
        assert_eq!(
            single_container_metadata["resource_usage"]["executor-0"]["cpu_time_ms"],
            "1"
        );

        let multi_container_metadata = resource_usage_metadata(&[
            usage("executor-0", None, None, None, Some(0.0004)),
            usage("executor-1", None, None, None, Some(0.0004)),
        ])
        .expect("should have metadata");

        assert_eq!(multi_container_metadata["cpu_time_ms"], "1");
        assert_eq!(
            multi_container_metadata["resource_usage"]["executor-0"]["cpu_time_ms"],
            "0"
        );
        assert_eq!(
            multi_container_metadata["resource_usage"]["executor-1"]["cpu_time_ms"],
            "0"
        );
    }

    #[test]
    fn invalid_cpu_values_are_omitted() {
        for cpu in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY, -0.001] {
            let metadata =
                resource_usage_metadata(&[usage("executor-0", Some(1), None, None, Some(cpu))])
                    .expect("memory should produce metadata");

            assert!(metadata.get("cpu_time_ms").is_none());
            assert!(
                metadata["resource_usage"]["executor-0"]
                    .get("cpu_time_ms")
                    .is_none()
            );
        }
    }

    #[test]
    fn negative_zero_cpu_is_normalized() {
        let metadata =
            resource_usage_metadata(&[usage("executor-0", None, None, None, Some(-0.0))])
                .expect("zero CPU should produce metadata");

        assert_eq!(metadata["cpu_time_ms"], "0");
        assert_eq!(metadata["resource_usage"]["executor-0"]["cpu_time_ms"], "0");
    }
}
