//! Sampling of task pod resource usage from kubelet resource metrics.
//!
//! When enabled, the monitor periodically reads the `/metrics/resource`
//! endpoint of the kubelets hosting task pods (through the Kubernetes API
//! server's node proxy) and folds each per-container sample into the task's
//! aggregate resource usage in the database.
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

/// Fetches the resource metrics of a node's kubelet through the API server's
/// node proxy.
pub async fn fetch_node_metrics(client: &Client, node: &str) -> Result<String> {
    let request = http::Request::get(format!("/api/v1/nodes/{node}/proxy/metrics/resource"))
        .body(Vec::new())
        .context("failed to build node metrics request")?;

    client
        .request_text(request)
        .await
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

/// The CPU sampling baseline of a container.
#[derive(Debug, Clone, Copy)]
struct ContainerBaseline {
    /// The container's start time when last sampled, if reported.
    start_time_seconds: Option<f64>,
    /// The container's cumulative CPU time when last sampled, in seconds.
    cpu_seconds: f64,
}

/// Tracks per-container sampling state used to convert cumulative CPU
/// counters into per-round CPU time deltas.
#[derive(Debug)]
pub struct UsageSampler {
    /// The time the sampler was created, in seconds since the Unix epoch.
    ///
    /// Used to decide whether a newly observed container instance started
    /// under observation (see [`UsageSampler::fold`]).
    start_time_seconds: f64,
    /// The sampling baseline of each container, keyed by pod name and
    /// container name.
    baselines: HashMap<(String, String), ContainerBaseline>,
}

impl Default for UsageSampler {
    fn default() -> Self {
        Self::with_start(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|elapsed| elapsed.as_secs_f64())
                .unwrap_or_default(),
        )
    }
}

impl UsageSampler {
    /// Creates a new usage sampler.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a new usage sampler with the given start time, in seconds
    /// since the Unix epoch.
    fn with_start(start_time_seconds: f64) -> Self {
        Self {
            start_time_seconds,
            baselines: HashMap::new(),
        }
    }

    /// Whether a container instance started while this sampler was
    /// observing, according to the instance's reported start time.
    ///
    /// A container without a reported start time is conservatively treated
    /// as predating the sampler.
    fn born_under_observation(&self, point: &ContainerMetrics) -> bool {
        point
            .start_time_seconds
            .is_some_and(|start| start >= self.start_time_seconds)
    }

    /// Folds a round of per-container metrics points into per-container
    /// resource usage samples.
    ///
    /// The kubelet reports CPU time as a cumulative counter since container
    /// start. To guarantee that CPU time is never recorded twice — the
    /// aggregate in the database survives monitor restarts, while this
    /// sampler's baselines do not — attribution is at-most-once:
    ///
    /// * A previously observed container instance attributes the counter delta
    ///   since its previous observation. A skipped observation (e.g. a failed
    ///   node metrics fetch) loses nothing, as the next delta spans the gap.
    ///
    /// * A newly observed instance (first sight, or a counter reset detected by
    ///   a decreasing counter or a changed start time) attributes its full
    ///   counter value only if it started under this sampler's observation;
    ///   otherwise — a monitor restart, where the instance's prior usage may
    ///   already be recorded — it only establishes a baseline, forgoing the CPU
    ///   time consumed while unobserved.
    ///
    /// Baselines are retained for as long as a container's pod remains in
    /// the given task pod list, and samples carrying no measurements are
    /// omitted.
    pub fn fold(
        &mut self,
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

            let key = (pod.clone(), container.clone());
            let cpu_time_delta_ms = point.cpu_seconds.map(|cpu| {
                let delta = match self.baselines.get(&key) {
                    // Same container instance with a monotonic counter:
                    // attribute the delta since the previous observation
                    Some(baseline)
                        if baseline.start_time_seconds == point.start_time_seconds
                            && cpu >= baseline.cpu_seconds =>
                    {
                        cpu - baseline.cpu_seconds
                    }
                    // A newly observed container instance: attribute the
                    // full counter value only if the instance started under
                    // observation, so that a monitor restart cannot record
                    // already-recorded CPU time twice
                    _ if self.born_under_observation(point) => cpu,
                    _ => 0.0,
                };

                self.baselines.insert(
                    key.clone(),
                    ContainerBaseline {
                        start_time_seconds: point.start_time_seconds,
                        cpu_seconds: cpu,
                    },
                );

                (delta * 1000.0) as i64
            });

            let sample = ContainerUsageSample {
                tes_id: tes_id.to_string(),
                container_name: container.clone(),
                memory_bytes: point.memory_bytes.map(|bytes| bytes as i64),
                cpu_time_delta_ms,
            };

            if !sample.is_empty() {
                samples.push(sample);
            }
        }

        // Retain the baselines of containers whose pods still exist, so that
        // a round missing a container's metrics (e.g. a failed node metrics
        // fetch) does not restart the container's CPU attribution; state is
        // dropped once the pod itself is gone
        let pod_names: HashSet<&str> = pods.iter().map(|pod| pod.name.as_str()).collect();
        self.baselines
            .retain(|(pod, _), _| pod_names.contains(pod.as_str()));

        samples
    }
}

/// Samples the resource usage of all task pods.
///
/// Lists the task pods, fetches the kubelet resource metrics of each hosting
/// node through the API server's node proxy, and folds the per-container
/// metrics into resource usage samples.
///
/// A node whose metrics cannot be fetched is skipped (its pods simply miss a
/// sampling round); an error is returned only if the task pods cannot be
/// listed.
pub async fn sample_task_pods(
    client: &Client,
    pods_api: &Api<Pod>,
    namespace: &str,
    sampler: &mut UsageSampler,
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

    Ok(sampler.fold(&pods, &metrics))
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
    fn first_observation_attributes_cpu_since_start() {
        let mut sampler = UsageSampler::with_start(0.0);

        let metrics = [(
            key("task-pod", "executor-0"),
            ContainerMetrics {
                cpu_seconds: Some(1.5),
                memory_bytes: Some(1024),
                start_time_seconds: Some(1000.0),
            },
        )]
        .into();

        let samples = sampler.fold(&[task_pod("task-pod", "task-1234")], &metrics);
        assert_eq!(samples.len(), 1);
        assert_eq!(samples[0].tes_id, "task-1234");
        assert_eq!(samples[0].container_name, "executor-0");
        assert_eq!(samples[0].memory_bytes, Some(1024));
        // The container started under the sampler's observation, so its full
        // counter value is attributed on first observation and CPU time
        // consumed before the first sample is not lost
        assert_eq!(samples[0].cpu_time_delta_ms, Some(1500));
    }

    #[test]
    fn subsequent_observations_attribute_the_delta() {
        let mut sampler = UsageSampler::with_start(0.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = |cpu: f64| ContainerMetrics {
            cpu_seconds: Some(cpu),
            memory_bytes: Some(2048),
            start_time_seconds: Some(1000.0),
        };

        sampler.fold(&pods, &[(key("task-pod", "executor-0"), point(1.5))].into());
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(2.25))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(750));

        // An unchanged counter yields a zero delta
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(2.25))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(0));
    }

    #[test]
    fn counter_resets_restart_attribution() {
        let mut sampler = UsageSampler::with_start(0.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = |cpu: f64, start: f64| ContainerMetrics {
            cpu_seconds: Some(cpu),
            memory_bytes: None,
            start_time_seconds: Some(start),
        };

        sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(10.0, 1000.0))].into(),
        );

        // The container restarted: new start time and a lower counter; the
        // new counter value is attributed rather than a negative delta
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(0.5, 2000.0))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(500));

        // A decreasing counter without a start time change is also a reset
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(0.25, 2000.0))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(250));
    }

    #[test]
    fn containers_are_tracked_independently() {
        let mut sampler = UsageSampler::with_start(0.0);
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

        let mut samples = sampler.fold(&pods, &metrics);
        samples.sort_by(|a, b| a.container_name.cmp(&b.container_name));

        assert_eq!(samples.len(), 2);
        assert_eq!(samples[0].container_name, "executor-0");
        assert_eq!(samples[0].cpu_time_delta_ms, Some(2000));
        assert_eq!(samples[0].memory_bytes, Some(200));
        assert_eq!(samples[1].container_name, "inputs");
        assert_eq!(samples[1].cpu_time_delta_ms, Some(500));
        assert_eq!(samples[1].memory_bytes, Some(100));
    }

    #[test]
    fn unknown_pods_and_empty_points_are_skipped() {
        let mut sampler = UsageSampler::with_start(0.0);
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

        let samples = sampler.fold(&pods, &metrics);
        assert!(samples.is_empty());
    }

    #[test]
    fn baselines_are_dropped_with_their_pod() {
        let mut sampler = UsageSampler::with_start(0.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = ContainerMetrics {
            cpu_seconds: Some(1.0),
            memory_bytes: None,
            start_time_seconds: Some(1000.0),
        };

        sampler.fold(&pods, &[(key("task-pod", "inputs"), point)].into());
        assert_eq!(sampler.baselines.len(), 1);

        // The inputs container completed but its pod still exists: the
        // baseline is retained alongside the executor's
        sampler.fold(&pods, &[(key("task-pod", "executor-0"), point)].into());
        assert_eq!(sampler.baselines.len(), 2);

        // The pod is gone: all of its baselines are dropped
        sampler.fold(&[], &HashMap::new());
        assert!(sampler.baselines.is_empty());
    }

    #[test]
    fn a_skipped_round_does_not_double_count() {
        let mut sampler = UsageSampler::with_start(0.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = |cpu: f64| ContainerMetrics {
            cpu_seconds: Some(cpu),
            memory_bytes: None,
            start_time_seconds: Some(1000.0),
        };

        let samples = sampler.fold(&pods, &[(key("task-pod", "executor-0"), point(2.0))].into());
        assert_eq!(samples[0].cpu_time_delta_ms, Some(2000));

        // The node metrics fetch failed for a round: no metrics for the
        // container, but its pod still exists
        let samples = sampler.fold(&pods, &HashMap::new());
        assert!(samples.is_empty());

        // The next observation attributes only the delta spanning the gap,
        // not the full counter again
        let samples = sampler.fold(&pods, &[(key("task-pod", "executor-0"), point(3.5))].into());
        assert_eq!(samples[0].cpu_time_delta_ms, Some(1500));
    }

    #[test]
    fn preexisting_containers_only_establish_a_baseline() {
        // The sampler starts after the container did (e.g. a monitor
        // restart), so the container's counter may already be recorded
        let mut sampler = UsageSampler::with_start(5000.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = |cpu: f64| ContainerMetrics {
            cpu_seconds: Some(cpu),
            memory_bytes: Some(1024),
            start_time_seconds: Some(1000.0),
        };

        // The first observation must not re-attribute the counter
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(60.0))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(0));
        // Memory is unaffected by the CPU attribution gate
        assert_eq!(samples[0].memory_bytes, Some(1024));

        // Subsequent observations attribute deltas as usual
        let samples = sampler.fold(
            &pods,
            &[(key("task-pod", "executor-0"), point(61.5))].into(),
        );
        assert_eq!(samples[0].cpu_time_delta_ms, Some(1500));
    }

    #[test]
    fn containers_without_start_times_are_treated_as_preexisting() {
        let mut sampler = UsageSampler::with_start(0.0);
        let pods = [task_pod("task-pod", "task-1234")];

        let point = |cpu: f64| ContainerMetrics {
            cpu_seconds: Some(cpu),
            memory_bytes: None,
            start_time_seconds: None,
        };

        // Without a start time the instance cannot be proven to have started
        // under observation, so only a baseline is established
        let samples = sampler.fold(&pods, &[(key("task-pod", "executor-0"), point(2.0))].into());
        assert_eq!(samples[0].cpu_time_delta_ms, Some(0));

        let samples = sampler.fold(&pods, &[(key("task-pod", "executor-0"), point(2.5))].into());
        assert_eq!(samples[0].cpu_time_delta_ms, Some(500));
    }
}
