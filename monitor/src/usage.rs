//! Sampling of task pod resource usage from the Kubernetes metrics API.
//!
//! When enabled, the monitor periodically queries `metrics.k8s.io/v1beta1`
//! for the metrics of pods labeled as task pods and folds each sample into
//! the task's aggregate resource usage in the database.
//!
//! The aggregate is reported through the TES API as task log metadata using
//! the `peak_rss_bytes`, `avg_rss_bytes`, and `cpu_time_ms` keys.

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

/// Tracks per-pod sampling state used to convert instantaneous CPU rates into
/// accumulated CPU time.
#[derive(Debug, Default)]
pub struct UsageSampler {
    /// The last time each pod was sampled.
    last_sampled: HashMap<String, Instant>,
}

impl UsageSampler {
    /// Creates a new usage sampler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Folds a list of pod metrics into per-task resource usage samples.
    ///
    /// The `default_elapsed` duration is used to convert the CPU usage rate
    /// of a pod seen for the first time into CPU time; subsequently, the
    /// actual elapsed time between samples is used.
    ///
    /// Returns pairs of TES task id and the sample to record.
    pub fn fold(
        &mut self,
        metrics: impl IntoIterator<Item = DynamicObject>,
        default_elapsed: std::time::Duration,
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

            let Some((cpu_cores, memory_bytes)) = sum_container_usage(&metrics.data) else {
                continue;
            };

            let elapsed = self
                .last_sampled
                .get(&tes_id)
                .map(|last| now.duration_since(*last))
                .unwrap_or(default_elapsed);
            seen.insert(tes_id.clone(), now);

            samples.push((
                tes_id,
                ResourceUsageSample {
                    rss_bytes: memory_bytes as i64,
                    cpu_time_delta_ms: (cpu_cores * elapsed.as_millis() as f64) as i64,
                },
            ));
        }

        // Retain only the pods seen in this sampling round so that state for
        // completed tasks is dropped
        self.last_sampled = seen;

        samples
    }
}

/// Sums the CPU (in cores) and memory (in bytes) usage across the containers
/// of a `PodMetrics` object.
///
/// Returns `None` if the object carries no container usage.
fn sum_container_usage(data: &serde_json::Value) -> Option<(f64, u64)> {
    let containers = data.get("containers")?.as_array()?;

    let mut cpu_cores = 0.0;
    let mut memory_bytes = 0u64;
    let mut any = false;

    for container in containers {
        let Some(usage) = container.get("usage") else {
            continue;
        };

        if let Some(cpu) = usage.get("cpu").and_then(|v| v.as_str())
            && let Some(cores) = parse_quantity(cpu)
        {
            cpu_cores += cores;
            any = true;
        }

        if let Some(memory) = usage.get("memory").and_then(|v| v.as_str())
            && let Some(bytes) = parse_quantity(memory)
        {
            memory_bytes += bytes as u64;
            any = true;
        }
    }

    if any {
        Some((cpu_cores, memory_bytes))
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

    #[test]
    fn samples_fold_across_containers() {
        let mut sampler = UsageSampler::new();

        let metrics = pod_metrics(
            "task-1234",
            serde_json::json!([
                { "name": "executor-0", "usage": { "cpu": "500m", "memory": "1Gi" } },
                { "name": "sidecar", "usage": { "cpu": "100m", "memory": "512Mi" } },
            ]),
        );

        let samples = sampler.fold([metrics], Duration::from_secs(10));
        assert_eq!(samples.len(), 1);

        let (tes_id, sample) = &samples[0];
        assert_eq!(tes_id, "task-1234");
        // 1 GiB + 512 MiB
        assert_eq!(sample.rss_bytes, 1024 * 1024 * 1024 + 512 * 1024 * 1024);
        // 0.6 cores over 10 seconds = 6000 ms of CPU time
        assert_eq!(sample.cpu_time_delta_ms, 6000);
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
