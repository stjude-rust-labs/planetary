//! Sampling of task pod resource usage from the Kubernetes metrics API.
//!
//! When enabled, the monitor periodically queries `metrics.k8s.io/v1beta1`
//! for the metrics of pods labeled as task pods and folds each sample into
//! the task's aggregate resource usage in the database.
//!
//! The aggregate is reported through the TES API as task log metadata using
//! the `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms` keys.

use std::collections::HashMap;
use std::time::Instant;

use anyhow::Context;
use anyhow::Result;
use kube::Api;
use kube::Client;
use kube::api::ApiResource;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use kube::api::ListParams;
use planetary_db::ResourceUsageSample;

/// The task id label.
const TASK_LABEL: &str = "planetary/task";

/// Returns the [`ApiResource`] for `PodMetrics` from the Kubernetes metrics
/// API (`metrics.k8s.io/v1beta1`).
pub fn pod_metrics_resource() -> ApiResource {
    ApiResource::from_gvk_with_plural(
        &GroupVersionKind::gvk("metrics.k8s.io", "v1beta1", "PodMetrics"),
        "pods",
    )
}

/// Creates a namespaced [`Api`] for querying pod metrics.
pub fn pod_metrics_api(client: Client, namespace: &str) -> Api<DynamicObject> {
    Api::namespaced_with(client, namespace, &pod_metrics_resource())
}

/// The sampling baseline for a task's pod.
#[derive(Debug, Clone)]
struct PodBaseline {
    /// The pod's identity, used to detect pod replacement.
    identity: String,
    /// The last time the pod was sampled.
    last: Instant,
}

/// Tracks per-pod sampling state used to convert instantaneous CPU rates into
/// accumulated CPU time.
#[derive(Debug, Default)]
pub struct UsageSampler {
    /// The sampling baseline of each task's pod.
    baselines: HashMap<String, PodBaseline>,
}

/// Extracts an identity for a pod from its metrics object's metadata.
///
/// The UID is used when the metrics API populates it; a pod that is replaced
/// (e.g. recreated after an eviction) then yields a different identity, which
/// resets its CPU baseline so that a new pod's usage rate is never integrated
/// over a stale interval.
///
/// The reference Kubernetes metrics-server leaves the UID empty (and stamps
/// `creationTimestamp` with the response time, so it cannot serve as a
/// fallback identity); in that case the identity is a constant and the
/// baseline is effectively keyed by task alone, with any pod-replacement
/// error bounded to at most one sampling interval by the elapsed-time cap.
fn pod_identity(metrics: &DynamicObject) -> String {
    metrics.metadata.uid.clone().unwrap_or_default()
}

impl UsageSampler {
    /// Creates a new usage sampler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds a list of pod metrics into per-task resource usage samples.
    ///
    /// CPU time is accumulated by integrating the sampled CPU usage rate
    /// over the time elapsed since the pod's previous sample. No CPU time is
    /// recorded for a pod's first sample (there is no observed period to
    /// integrate over), and the elapsed time is capped at `sample_interval`
    /// so that outages of the metrics API are not extrapolated from a single
    /// instantaneous rate. A change in pod identity (e.g. a pod recreated
    /// for the same task) also resets the CPU baseline.
    ///
    /// Returns pairs of TES task id and the sample to record; samples
    /// carrying no measurements are omitted.
    pub fn fold(
        &mut self,
        metrics: impl IntoIterator<Item = DynamicObject>,
        sample_interval: std::time::Duration,
    ) -> Vec<(String, ResourceUsageSample)> {
        let now = Instant::now();
        let mut samples = Vec::new();
        let mut seen = HashMap::new();

        for metrics in metrics {
            let Some(tes_id) = metrics
                .metadata
                .labels
                .as_ref()
                .and_then(|labels| labels.get(TASK_LABEL))
                .cloned()
            else {
                continue;
            };

            let Some(usage) = sum_container_usage(&metrics.data) else {
                continue;
            };

            let identity = pod_identity(&metrics);
            let elapsed = self
                .baselines
                .get(&tes_id)
                .filter(|baseline| baseline.identity == identity)
                .map(|baseline| now.duration_since(baseline.last).min(sample_interval));
            seen.insert(
                tes_id.clone(),
                PodBaseline {
                    identity,
                    last: now,
                },
            );

            let sample = ResourceUsageSample {
                memory_bytes: usage.memory_bytes.map(|bytes| bytes as i64),
                cpu_time_delta_ms: match (usage.cpu_cores, elapsed) {
                    (Some(cores), Some(elapsed)) => {
                        Some((cores * elapsed.as_millis() as f64) as i64)
                    }
                    _ => None,
                },
            };

            if !sample.is_empty() {
                samples.push((tes_id, sample));
            }
        }

        // Retain only the pods seen in this sampling round so that state for
        // completed tasks is dropped
        self.baselines = seen;

        samples
    }
}

/// The summed resource usage of a `PodMetrics` object's containers.
#[derive(Debug, Default, Clone, Copy)]
struct ContainerUsage {
    /// The summed CPU usage rate, in cores, if any container reported CPU.
    cpu_cores: Option<f64>,
    /// The summed working set memory, in bytes, if any container reported
    /// memory.
    memory_bytes: Option<u64>,
}

/// Sums the CPU (in cores) and memory (in bytes) usage across the containers
/// of a `PodMetrics` object.
///
/// Each dimension is present only if at least one container reported a
/// parseable quantity for it, so that a partially reported sample cannot
/// contribute fabricated zeroes to an aggregate. Returns `None` if the
/// object carries no container usage at all.
fn sum_container_usage(data: &serde_json::Value) -> Option<ContainerUsage> {
    let containers = data.get("containers")?.as_array()?;

    let mut summed = ContainerUsage::default();

    for container in containers {
        let Some(usage) = container.get("usage") else {
            continue;
        };

        if let Some(cpu) = usage.get("cpu").and_then(|v| v.as_str())
            && let Some(cores) = parse_quantity(cpu)
        {
            summed.cpu_cores = Some(summed.cpu_cores.unwrap_or(0.0) + cores);
        }

        if let Some(memory) = usage.get("memory").and_then(|v| v.as_str())
            && let Some(bytes) = parse_quantity(memory)
        {
            summed.memory_bytes = Some(summed.memory_bytes.unwrap_or(0) + bytes as u64);
        }
    }

    if summed.cpu_cores.is_some() || summed.memory_bytes.is_some() {
        Some(summed)
    } else {
        None
    }
}

/// Parses a Kubernetes resource quantity into its base unit (cores for CPU,
/// bytes for memory).
///
/// Supports the decimal (`n`, `u`, `m`, `k`, `M`, `G`, `T`, `P`, `E`) and
/// binary (`Ki`, `Mi`, `Gi`, `Ti`, `Pi`, `Ei`) suffixes as well as plain
/// numbers; returns `None` for unrecognized input.
pub fn parse_quantity(quantity: &str) -> Option<f64> {
    let quantity = quantity.trim();
    let split = quantity
        .find(|c: char| !c.is_ascii_digit() && c != '.' && c != '-' && c != '+' && c != 'e')
        .unwrap_or(quantity.len());

    // A trailing `e` is an exponent only if followed by digits; Kubernetes
    // also uses `E` (exa) as a suffix, which is handled below
    let (number, suffix) = quantity.split_at(split);
    let value: f64 = number.parse().ok()?;

    let multiplier = match suffix {
        "" => 1.0,
        "n" => 1e-9,
        "u" => 1e-6,
        "m" => 1e-3,
        "k" => 1e3,
        "M" => 1e6,
        "G" => 1e9,
        "T" => 1e12,
        "P" => 1e15,
        "E" => 1e18,
        "Ki" => 1024.0,
        "Mi" => 1024.0 * 1024.0,
        "Gi" => 1024.0 * 1024.0 * 1024.0,
        "Ti" => 1024.0f64.powi(4),
        "Pi" => 1024.0f64.powi(5),
        "Ei" => 1024.0f64.powi(6),
        _ => return None,
    };

    Some(value * multiplier)
}

/// Lists all pod metrics for task pods in the given namespace.
pub async fn list_task_pod_metrics(api: &Api<DynamicObject>) -> Result<Vec<DynamicObject>> {
    let params = ListParams::default().labels(TASK_LABEL);
    let list = api
        .list(&params)
        .await
        .context("failed to list pod metrics; is the Kubernetes metrics server installed?")?;
    Ok(list.items)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn quantities_parse() {
        assert_eq!(parse_quantity("128974848"), Some(128974848.0));
        assert_eq!(parse_quantity("129e6"), Some(129e6));
        assert_eq!(parse_quantity("123Mi"), Some(123.0 * 1024.0 * 1024.0));
        assert_eq!(parse_quantity("1Gi"), Some(1024.0f64.powi(3)));
        assert_eq!(parse_quantity("100m"), Some(0.1));
        assert_eq!(parse_quantity("1234567n"), Some(1234567e-9));
        assert_eq!(parse_quantity("250u"), Some(250e-6));
        assert_eq!(parse_quantity("129M"), Some(129e6));
        assert_eq!(parse_quantity("1k"), Some(1000.0));
        assert_eq!(parse_quantity("bogus"), None);
        assert_eq!(parse_quantity("1x"), None);
    }

    /// Builds a `PodMetrics`-shaped dynamic object for tests.
    fn pod_metrics(tes_id: &str, containers: serde_json::Value) -> DynamicObject {
        let mut object = DynamicObject::new(tes_id, &pod_metrics_resource());
        object.metadata.labels = Some(
            [(TASK_LABEL.to_string(), tes_id.to_string())]
                .into_iter()
                .collect(),
        );
        object.data = serde_json::json!({ "containers": containers });
        object
    }

    /// The containers used by the folding tests.
    fn containers() -> serde_json::Value {
        serde_json::json!([
            { "name": "executor-0", "usage": { "cpu": "500m", "memory": "1Gi" } },
            { "name": "sidecar", "usage": { "cpu": "100m", "memory": "512Mi" } },
        ])
    }

    #[test]
    fn samples_fold_across_containers() {
        let mut sampler = UsageSampler::new();
        let interval = Duration::from_secs(10);

        // The first sample carries memory only: there is no observed period
        // to integrate the CPU rate over yet
        let samples = sampler.fold([pod_metrics("task-1234", containers())], interval);
        assert_eq!(samples.len(), 1);

        let (tes_id, sample) = &samples[0];
        assert_eq!(tes_id, "task-1234");
        // 1 GiB + 512 MiB
        assert_eq!(
            sample.memory_bytes,
            Some(1024 * 1024 * 1024 + 512 * 1024 * 1024)
        );
        assert_eq!(sample.cpu_time_delta_ms, None);

        // The second sample also carries CPU time; the elapsed time between
        // the samples is negligible in this test, so the delta is ~0, and —
        // importantly — bounded by the sampling interval
        let samples = sampler.fold([pod_metrics("task-1234", containers())], interval);
        assert_eq!(samples.len(), 1);

        let (_, sample) = &samples[0];
        let delta = sample.cpu_time_delta_ms.expect("should have CPU time");
        // 0.6 cores over at most 10 seconds
        assert!((0..=6000).contains(&delta));
    }

    #[test]
    fn partial_samples_omit_missing_dimensions() {
        let mut sampler = UsageSampler::new();

        // CPU reported but no memory: the memory dimension must be absent
        // rather than a fabricated zero
        let metrics = pod_metrics(
            "task-1234",
            serde_json::json!([{ "name": "executor-0", "usage": { "cpu": "500m" } }]),
        );

        // First sample carries neither memory (not reported) nor CPU (first
        // sighting), so it is omitted entirely
        let samples = sampler.fold([metrics.clone()], Duration::from_secs(10));
        assert!(samples.is_empty());

        // Second sample carries CPU only
        let samples = sampler.fold([metrics], Duration::from_secs(10));
        assert_eq!(samples.len(), 1);
        let (_, sample) = &samples[0];
        assert_eq!(sample.memory_bytes, None);
        assert!(sample.cpu_time_delta_ms.is_some());
    }

    #[test]
    fn uid_less_metrics_keep_the_task_baseline() {
        let mut sampler = UsageSampler::new();
        let interval = Duration::from_secs(10);

        // The reference metrics-server leaves the UID empty and stamps
        // `creationTimestamp` with the response time; a changing timestamp
        // must not reset the baseline, or CPU time would never be recorded
        fn without_uid(timestamp: &str) -> DynamicObject {
            let mut metrics = pod_metrics("task-1234", containers());
            metrics.metadata.uid = None;
            metrics.metadata.creation_timestamp =
                serde_json::from_str(&format!("\"{timestamp}\"")).ok();
            metrics
        }

        let samples = sampler.fold([without_uid("2026-08-26T00:00:00Z")], interval);
        assert_eq!(samples[0].1.cpu_time_delta_ms, None);

        // Second round with a different response timestamp: CPU is recorded
        let samples = sampler.fold([without_uid("2026-08-26T00:00:10Z")], interval);
        assert!(samples[0].1.cpu_time_delta_ms.is_some());
    }

    #[test]
    fn pod_replacement_resets_the_cpu_baseline() {
        let mut sampler = UsageSampler::new();
        let interval = Duration::from_secs(10);

        /// Builds pod metrics with the given pod UID.
        fn with_uid(uid: &str) -> DynamicObject {
            let mut metrics = pod_metrics("task-1234", containers());
            metrics.metadata.uid = Some(uid.to_string());
            metrics
        }

        // First sighting of the pod: no CPU time
        let samples = sampler.fold([with_uid("pod-a")], interval);
        assert_eq!(samples[0].1.cpu_time_delta_ms, None);

        // Same pod: CPU time is now integrated
        let samples = sampler.fold([with_uid("pod-a")], interval);
        assert!(samples[0].1.cpu_time_delta_ms.is_some());

        // The pod was replaced (same task, new UID): the CPU baseline resets
        // so the new pod's rate is not integrated over a stale interval
        let samples = sampler.fold([with_uid("pod-b")], interval);
        assert_eq!(samples[0].1.cpu_time_delta_ms, None);

        // And resumes on the replacement pod's second sample
        let samples = sampler.fold([with_uid("pod-b")], interval);
        assert!(samples[0].1.cpu_time_delta_ms.is_some());
    }

    #[test]
    fn unlabeled_and_empty_metrics_are_skipped() {
        let mut sampler = UsageSampler::new();

        let mut unlabeled = DynamicObject::new("other", &pod_metrics_resource());
        unlabeled.data = serde_json::json!({
            "containers": [{ "name": "c", "usage": { "cpu": "1", "memory": "1Ki" } }]
        });

        let empty = pod_metrics("task-5678", serde_json::json!([]));

        let samples = sampler.fold([unlabeled, empty], Duration::from_secs(10));
        assert!(samples.is_empty());
    }
}
