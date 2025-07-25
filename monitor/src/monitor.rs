//! Implementation of the task monitor.
//!
//! The task monitor is responsible for monitoring the Kubernetes
//! cluster for orphaned task pods. An orphaned task pod is one which is not
//! associated with a running orchestrator.
//!
//! Additionally, the task monitor will monitor for task pods in the database
//! that do not have associated Kubernetes resources; it will abort any running
//! tasks where a task pod has been deleted without an associated orchestrator.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use futures::FutureExt;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::ResourceExt;
use planetary_db::Database;
use planetary_db::ExecutingPod;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::interval;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use url::Url;

/// The default namespace for planetary services.
const PLANETARY_NAMESPACE: &str = "planetary";

/// The default namespace for planetary tasks.
const PLANETARY_TASKS_NAMESPACE: &str = "planetary-tasks";

/// The orchestrator id label.
const PLANETARY_ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The default interval for the monitoring service.
const MONITORING_INTERVAL: Duration = Duration::from_secs(60);

/// Represents the task monitor.
pub struct Monitor {
    /// The cancellation token for shutting down the service.
    shutdown: CancellationToken,
    /// The handle to the monitoring task.
    handle: JoinHandle<()>,
}

impl Monitor {
    /// Spawns a new task monitor.
    ///
    /// This method will spawn Tokio tasks for monitoring cluster state.
    pub async fn spawn(
        database: Arc<dyn Database>,
        orchestrator_url: Url,
        planetary_namespace: Option<String>,
        tasks_namespace: Option<String>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to get default Kubernetes client")?;

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(Self::monitor(
            shutdown.clone(),
            database,
            client,
            orchestrator_url.into(),
            planetary_namespace,
            tasks_namespace,
        ));
        Ok(Self { shutdown, handle })
    }

    /// Shuts down the service.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.handle.await.expect("failed to join task");
    }

    /// Implements the monitoring task.
    async fn monitor(
        shutdown: CancellationToken,
        database: Arc<dyn Database>,
        client: Client,
        orchestrator_url: Arc<Url>,
        planetary_namespace: Option<String>,
        tasks_namespace: Option<String>,
    ) {
        info!("task monitor has started");

        let planetary_pods: Api<Pod> = Api::namespaced(
            client.clone(),
            planetary_namespace
                .as_deref()
                .unwrap_or(PLANETARY_NAMESPACE),
        );
        let task_pods: Api<Pod> = Api::namespaced(
            client.clone(),
            tasks_namespace
                .as_deref()
                .unwrap_or(PLANETARY_TASKS_NAMESPACE),
        );

        let mut interval = interval(MONITORING_INTERVAL);
        let client = reqwest::Client::new();

        loop {
            select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    debug!("querying the database for the currently executing pods");

                    let client = client.clone();
                    let planetary_pods = planetary_pods.clone();
                    let task_pods = task_pods.clone();
                    let orchestrator_url = orchestrator_url.clone();

                    if let Err(e) = database.drain_executing_pods(Box::new(|executing| { async move {
                        Self::check_executing_pods(&client, &orchestrator_url, &planetary_pods, &task_pods, executing).await
                    }.boxed() })).await {
                        error!("failed to drain executing pods: {e:#}");
                    }
                }
            }
        }

        info!("task monitor has shut down");
    }

    /// Checks the executing pods for being "missing" or "orphaned".
    ///
    /// An executing pod is "missing" when the pod doesn't exist in the
    /// Kubernetes cluster.
    ///
    /// An executing pod is "orphaned" when the orchestrator managing the pod
    /// doesn't exist in the Kubernetes cluster.
    async fn check_executing_pods(
        client: &reqwest::Client,
        orchestrator_url: &Url,
        planetary_pods: &Api<Pod>,
        task_pods: &Api<Pod>,
        executing: Vec<ExecutingPod>,
    ) -> anyhow::Result<Vec<ExecutingPod>> {
        // The map of orchestrators we've checked for existence
        let mut orchestrators = HashMap::new();
        let mut drained = Vec::new();

        for pod in executing {
            // Check to see if the task pod still exists
            debug!(
                "checking for existence of task pod `{name}` (task `{tes_id}`)",
                name = pod.name(),
                tes_id = pod.tes_id(),
            );

            let Some(mut metadata) =
                task_pods
                    .get_metadata_opt(pod.name())
                    .await
                    .with_context(|| {
                        format!(
                            "failed to retrieve task pod `{name}` (task `{tes_id}`)",
                            name = pod.name(),
                            tes_id = pod.tes_id()
                        )
                    })?
            else {
                info!(
                    "aborting task pod `{name}` (task `{tes_id}`) because it no longer exists",
                    name = pod.name(),
                    tes_id = pod.tes_id(),
                );
                drained.push(pod);
                continue;
            };

            if let Some(id) = metadata.labels_mut().remove(PLANETARY_ORCHESTRATOR_LABEL) {
                // Check to see if the orchestrator pod exists
                let entry = match orchestrators.entry(id) {
                    Entry::Occupied(e) => e,
                    Entry::Vacant(e) => {
                        let exists = planetary_pods
                            .get_metadata_opt(e.key())
                            .await
                            .with_context(|| {
                                format!("failed to retrieve orchestrator pod `{id}`", id = e.key())
                            })?
                            .is_some();

                        e.insert_entry(exists)
                    }
                };

                if !*entry.get() {
                    info!(
                        "orchestrator pod `{id}` that managed task pod `{name}` (task `{tes_id}`) \
                         no longer exists: requesting another orchestrator to adopt the pod",
                        id = entry.key(),
                        name = pod.name(),
                        tes_id = pod.tes_id()
                    );

                    // Request that a running orchestrator adopt the pod
                    let response = client
                        .patch(
                            orchestrator_url
                                .join(&format!("/v1/pods/{name}", name = pod.name()))
                                .expect("URL should join"),
                        )
                        .send()
                        .await?;

                    response.error_for_status().with_context(|| {
                        format!(
                            "failed to adopt pod `{name}` (task `{tes_id}`)",
                            name = pod.name(),
                            tes_id = pod.tes_id()
                        )
                    })?;
                }
            }
        }

        Ok(drained)
    }
}
