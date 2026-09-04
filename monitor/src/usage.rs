//! Sampling of task pod resource usage from kubelet resource metrics.
//!
//! When enabled, the monitor periodically reads the `/metrics/resource`
//! endpoint of the kubelets hosting task pods (through the Kubernetes API
//! server's node proxy) and records each per-container observation in the
//! database, which folds it into the task's aggregate resource usage.
//!
//! The kubelet is read directly — rather than through the `metrics.k8s.io`
//! API — for two reasons:
//!
//! * The reference Kubernetes metrics-server only serves metrics for pods in
//!   the `Running` phase; task pods execute their work in *init* containers and
//!   therefore remain in the `Pending` phase while executing, so their usage is
//!   never visible through `metrics.k8s.io`.
//!
//! * The metrics-server documentation itself states that it is meant only
//!   for autoscaling purposes and that monitoring consumers should "collect
//!   metrics from Kubelet `/metrics/resource` endpoint directly":
//!   <https://github.com/kubernetes-sigs/metrics-server#use-cases>
//!
//! The kubelet reports cumulative CPU time and instantaneous working set
//! memory per container, keyed by namespace, pod, and container name, for
//! all running containers (including init containers).
//!
//! Observations carry the *cumulative* CPU counter values; the database
//! computes each counter's delta against a stored per-container baseline
//! and advances the baseline atomically with the aggregate (see
//! [`planetary_db::Database::add_task_resource_usage_samples`]). Keeping the
//! accounting state durable with the write makes recording idempotent and
//! exactly-once for observed counter movement: skipped or failed rounds are
//! spanned by the next successful observation's delta, ambiguous database
//! commit outcomes resolve to a zero delta on the next observation, and
//! monitor restarts continue accounting from the stored baseline. The
//! sampling client itself is stateless.
//!
//! The aggregate usage is reported through the TES API as task log metadata:
//! the `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms` keys carry
//! the usage of the task's executor containers, and the `resource_usage` key
//! carries the per-container breakdown.

use std::collections::HashMap;
use std::collections::HashSet;

use anyhow::Context;
use anyhow::Result;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::api::ListParams;
use planetary_db::ContainerUsageSample;
use tracing::warn;

/// The task id label.
const TASK_LABEL: &str = "planetary/task";

/// The kubelet metric for cumulative container CPU time, in seconds.
const CPU_METRIC: &str = "container_cpu_usage_seconds_total";

/// The kubelet metric for container working set memory, in bytes.
const MEMORY_METRIC: &str = "container_memory_working_set_bytes";

/// The kubelet metric for the container start time, in seconds since the
/// Unix epoch.
const START_TIME_METRIC: &str = "container_start_time_seconds";

/// A task pod for which resource usage is sampled.
#[derive(Debug, Clone)]
pub struct TaskPod {
    /// The name of the pod.
    pub name: String,
    /// The TES identifier of the pod's task.
    pub tes_id: String,
    /// The name of the node hosting the pod.
    pub node: String,
}

/// Lists the task pods to sample, together with the nodes hosting them.
///
/// Pods that are not yet scheduled to a node are omitted.
pub async fn list_task_pods(api: &Api<Pod>) -> Result<Vec<TaskPod>> {
    let params = ListParams::default().labels(TASK_LABEL);
    let pods = api
        .list(&params)
        .await
        .context("failed to list task pods")?;

    Ok(pods
        .items
        .into_iter()
        .filter_map(|pod| {
            let metadata = pod.metadata;
            let name = metadata.name?;
            let tes_id = metadata.labels?.get(TASK_LABEL)?.clone();
            let node = pod.spec?.node_name?;
            Some(TaskPod { name, tes_id, node })
        })
        .collect())
}

/// The timeout for fetching a node's resource metrics.
///
/// Bounds the impact of an unresponsive kubelet: a fetch that exceeds the
/// timeout is treated like any other failed fetch, so the node's pods miss
/// the round (losslessly — the next successful observation's counter delta
/// spans the gap) instead of stalling the sampling of other nodes or
/// delaying monitor shutdown.
const NODE_METRICS_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(15);

/// Fetches the resource metrics of a node's kubelet through the API server's
/// node proxy.
///
/// The fetch is bounded by [`NODE_METRICS_TIMEOUT`].
pub async fn fetch_node_metrics(client: &Client, node: &str) -> Result<String> {
    let request = http::Request::get(format!("/api/v1/nodes/{node}/proxy/metrics/resource"))
        .body(Vec::new())
        .context("failed to build node metrics request")?;

    tokio::time::timeout(NODE_METRICS_TIMEOUT, client.request_text(request))
        .await
        .map_err(|_| {
            anyhow::anyhow!(
                "request timed out after {timeout} seconds",
                timeout = NODE_METRICS_TIMEOUT.as_secs()
            )
        })
        .and_then(|result| result.map_err(Into::into))
        .with_context(|| format!("failed to fetch resource metrics from node `{node}`"))
}

/// A single container's metrics point parsed from a kubelet's resource
/// metrics.
#[derive(Debug, Clone, Copy, Default, PartialEq)]
pub struct ContainerMetrics {
    /// The cumulative CPU time of the container, in seconds.
    pub cpu_seconds: Option<f64>,
    /// The working set memory of the container, in bytes.
    pub memory_bytes: Option<u64>,
    /// The container's start time, in seconds since the Unix epoch.
    pub start_time_seconds: Option<f64>,
}

/// Parses the labels of a Prometheus text format series.
///
/// Returns the `namespace`, `pod`, and `container` label values; kubelet
/// resource metric label values do not contain escapes or commas.
fn parse_labels(labels: &str) -> Option<(&str, &str, &str)> {
    let mut namespace = None;
    let mut pod = None;
    let mut container = None;

    for label in labels.split(',') {
        let (key, value) = label.split_once('=')?;
        let value = value.trim_matches('"');
        match key.trim() {
            "namespace" => namespace = Some(value),
            "pod" => pod = Some(value),
            "container" => container = Some(value),
            _ => {}
        }
    }

    Some((namespace?, pod?, container?))
}

/// Parses kubelet resource metrics in the Prometheus text format into
/// per-container metrics points.
///
/// Only the containers of pods in the given namespace are returned; the
/// result is keyed by pod name and container name.
pub fn parse_node_metrics(
    text: &str,
    namespace: &str,
) -> HashMap<(String, String), ContainerMetrics> {
    let mut containers: HashMap<(String, String), ContainerMetrics> = HashMap::new();

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }

        // Split the series into `name{labels}` and `value [timestamp]`
        let Some(labels_start) = line.find('{') else {
            continue;
        };
        let name = &line[..labels_start];
        if name != CPU_METRIC && name != MEMORY_METRIC && name != START_TIME_METRIC {
            continue;
        }

        let Some(labels_end) = line[labels_start..].find('}') else {
            continue;
        };
        let labels = &line[labels_start + 1..labels_start + labels_end];
        let rest = line[labels_start + labels_end + 1..].trim();

        let Some((series_namespace, pod, container)) = parse_labels(labels) else {
            continue;
        };
        if series_namespace != namespace {
            continue;
        }

        // The value is followed by an optional timestamp
        let Some(value) = rest
            .split_ascii_whitespace()
            .next()
            .and_then(|value| value.parse::<f64>().ok())
        else {
            continue;
        };

        let metrics = containers
            .entry((pod.to_string(), container.to_string()))
            .or_default();
        match name {
            CPU_METRIC => metrics.cpu_seconds = Some(value),
            MEMORY_METRIC => metrics.memory_bytes = Some(value as u64),
            START_TIME_METRIC => metrics.start_time_seconds = Some(value),
            _ => unreachable!(),
        }
    }

    containers
}

/// Builds resource usage observations from a round of per-container metrics
/// points.
///
/// Points for pods that are not task pods, and points carrying no
/// measurements, are omitted. Observations are per pod container: if
/// multiple pods carry the same task label, a round yields multiple
/// observations for the same task and container name, which the database
/// accounts independently via per-pod baselines.
pub fn build_samples(
    pods: &[TaskPod],
    metrics: &HashMap<(String, String), ContainerMetrics>,
) -> Vec<ContainerUsageSample> {
    let tasks: HashMap<&str, &str> = pods
        .iter()
        .map(|pod| (pod.name.as_str(), pod.tes_id.as_str()))
        .collect();

    let mut samples = Vec::new();

    for ((pod, container), point) in metrics {
        let Some(tes_id) = tasks.get(pod.as_str()) else {
            continue;
        };

        let sample = ContainerUsageSample {
            tes_id: tes_id.to_string(),
            pod_name: pod.clone(),
            container_name: container.clone(),
            memory_bytes: point.memory_bytes.map(|bytes| bytes as i64),
            cpu_seconds: point.cpu_seconds,
            start_time_seconds: point.start_time_seconds,
        };

        if !sample.is_empty() {
            samples.push(sample);
        }
    }

    samples
}

/// Samples the resource usage of all task pods.
///
/// Lists the task pods, fetches the kubelet resource metrics of each hosting
/// node through the API server's node proxy, and builds the per-container
/// resource usage observations for the database to record.
///
/// A node whose metrics cannot be fetched is skipped (its pods simply miss a
/// sampling round, losslessly); an error is returned only if the task pods
/// cannot be listed.
pub async fn sample_task_pods(
    client: &Client,
    pods_api: &Api<Pod>,
    namespace: &str,
) -> Result<Vec<ContainerUsageSample>> {
    let pods = list_task_pods(pods_api).await?;

    let nodes: HashSet<&str> = pods.iter().map(|pod| pod.node.as_str()).collect();

    let mut metrics = HashMap::new();
    for node in nodes {
        match fetch_node_metrics(client, node).await {
            Ok(text) => metrics.extend(parse_node_metrics(&text, namespace)),
            Err(e) => {
                warn!("failed to sample resource metrics from node `{node}`: {e:#}");
            }
        }
    }

    Ok(build_samples(&pods, &metrics))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a task pod for tests.
    fn task_pod(name: &str, tes_id: &str) -> TaskPod {
        TaskPod {
            name: name.to_string(),
            tes_id: tes_id.to_string(),
            node: "node-1".to_string(),
        }
    }

    /// Builds a metrics key for tests.
    fn key(pod: &str, container: &str) -> (String, String) {
        (pod.to_string(), container.to_string())
    }

    #[test]
    fn node_metrics_parse() {
        let text = r#"
# HELP container_cpu_usage_seconds_total [STABLE] Cumulative cpu time consumed by the container in core-seconds
# TYPE container_cpu_usage_seconds_total counter
container_cpu_usage_seconds_total{container="executor-0",namespace="planetary-tasks",pod="task-pod"} 1.5 1787946022174
container_cpu_usage_seconds_total{container="other",namespace="other-ns",pod="other-pod"} 9.0 1787946022174
container_memory_working_set_bytes{container="executor-0",namespace="planetary-tasks",pod="task-pod"} 380928 1787946022174
container_start_time_seconds{container="executor-0",namespace="planetary-tasks",pod="task-pod"} 1.7879459339754386e+09
container_swap_usage_bytes{container="executor-0",namespace="planetary-tasks",pod="task-pod"} 0 1787946022174
pod_cpu_usage_seconds_total{namespace="planetary-tasks",pod="task-pod"} 2.0 1787946024684
node_cpu_usage_seconds_total 100.0
"#;

        let metrics = parse_node_metrics(text, "planetary-tasks");
        assert_eq!(metrics.len(), 1);

        let point = &metrics[&key("task-pod", "executor-0")];
        assert_eq!(point.cpu_seconds, Some(1.5));
        assert_eq!(point.memory_bytes, Some(380928));
        assert_eq!(point.start_time_seconds, Some(1.7879459339754386e+09));
    }

    #[test]
    fn samples_carry_cumulative_observations() {
        let pods = [task_pod("task-pod", "task-1234")];

        let metrics = [(
            key("task-pod", "executor-0"),
            ContainerMetrics {
                cpu_seconds: Some(1.5),
                memory_bytes: Some(1024),
                start_time_seconds: Some(1000.0),
            },
        )]
        .into();

        let samples = build_samples(&pods, &metrics);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].tes_id, "task-1234");
        assert_eq!(samples[0].pod_name, "task-pod");
        assert_eq!(samples[0].container_name, "executor-0");
        assert_eq!(samples[0].memory_bytes, Some(1024));
        // The cumulative counter and start time are passed through for the
        // database to compute the delta against its stored baseline
        assert_eq!(samples[0].cpu_seconds, Some(1.5));
        assert_eq!(samples[0].start_time_seconds, Some(1000.0));
    }

    #[test]
    fn containers_yield_independent_samples() {
        let pods = [task_pod("task-pod", "task-1234")];

        let metrics = [
            (
                key("task-pod", "inputs"),
                ContainerMetrics {
                    cpu_seconds: Some(0.5),
                    memory_bytes: Some(100),
                    start_time_seconds: Some(1000.0),
                },
            ),
            (
                key("task-pod", "executor-0"),
                ContainerMetrics {
                    cpu_seconds: Some(2.0),
                    memory_bytes: Some(200),
                    start_time_seconds: Some(1010.0),
                },
            ),
        ]
        .into();

        let mut samples = build_samples(&pods, &metrics);
        samples.sort_by(|a, b| a.container_name.cmp(&b.container_name));

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].container_name, "executor-0");
        assert_eq!(samples[0].cpu_seconds, Some(2.0));
        assert_eq!(samples[0].memory_bytes, Some(200));
        assert_eq!(samples[1].container_name, "inputs");
        assert_eq!(samples[1].cpu_seconds, Some(0.5));
        assert_eq!(samples[1].memory_bytes, Some(100));
    }

    #[test]
    fn multiple_pods_for_a_task_yield_per_pod_samples() {
        let pods = [
            task_pod("task-pod-a", "task-1234"),
            task_pod("task-pod-b", "task-1234"),
        ];

        let point = ContainerMetrics {
            cpu_seconds: Some(1.0),
            memory_bytes: None,
            start_time_seconds: Some(1000.0),
        };
        let metrics = [
            (key("task-pod-a", "executor-0"), point),
            (key("task-pod-b", "executor-0"), point),
        ]
        .into();

        let mut samples = build_samples(&pods, &metrics);
        samples.sort_by(|a, b| a.pod_name.cmp(&b.pod_name));

        // The same task and container name from different pods yields
        // distinct observations, accounted independently via per-pod
        // baselines in the database
        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].pod_name, "task-pod-a");
        assert_eq!(samples[1].pod_name, "task-pod-b");
        assert!(samples.iter().all(|s| s.tes_id == "task-1234"));
        assert!(samples.iter().all(|s| s.container_name == "executor-0"));
    }

    #[test]
    fn unknown_pods_and_empty_points_are_skipped() {
        let pods = [task_pod("task-pod", "task-1234")];

        let metrics = [
            // Not a task pod
            (
                key("other-pod", "app"),
                ContainerMetrics {
                    cpu_seconds: Some(1.0),
                    memory_bytes: Some(100),
                    start_time_seconds: None,
                },
            ),
            // A task pod container with no measurements
            (key("task-pod", "executor-0"), ContainerMetrics::default()),
        ]
        .into();

        let samples = build_samples(&pods, &metrics);
        assert!(samples.is_empty());
    }
}
