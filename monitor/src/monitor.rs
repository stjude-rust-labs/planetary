//! Implementation of the task monitor.
//!
//! The task monitor is responsible for monitoring the Kubernetes
//! cluster for orphaned task pods. An orphaned task pod is one which is not
//! associated with a running orchestrator.
//!
//! Additionally, the task monitor will monitor for task pods in the database
//! that do not have associated Kubernetes resources; it will abort any running
//! tasks where a task pod has been deleted without an associated orchestrator.

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::core::v1::Pod;
use kube::Api;
use kube::Client;
use kube::api::DeleteParams;
use kube::api::ListParams;
use kube::api::ObjectMeta;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::core::ErrorResponse;
use kube::runtime::reflector::Lookup;
use planetary_db::Database;
use planetary_db::format_log_message;
use reqwest::header;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tes::v1::types::task::State;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
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

/// The error source for the monitor.
const MONITOR_ERROR_SOURCE: &str = "monitor";

/// A label applied to resources that are ready for garbage collection.
const PLANETARY_GC_LABEL: &str = "planetary/gc";

/// The amount of time after a task has been created for which we will consider
/// it to be in-progress.
///
/// Effectively, this is the maximum amount of time we're giving an orchestrator
/// to create a pod after a database entry for the task was inserted.
///
/// Setting this too low may cause the monitor to abort a task before it even
/// has a chance to start.
const TASK_CREATION_DELTA: Duration = Duration::from_secs(60);

/// Represents information about the orchestrator service.
pub struct OrchestratorServiceInfo {
    /// The URL of the orchestrator service.
    pub url: Url,
    /// The orchestrator service API key.
    pub api_key: SecretString,
}

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
        orchestrator: OrchestratorServiceInfo,
        planetary_namespace: Option<String>,
        tasks_namespace: Option<String>,
        monitoring_interval: Duration,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to get default Kubernetes client")?;

        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(Self::monitor(
            shutdown.clone(),
            database,
            client,
            orchestrator,
            planetary_namespace,
            tasks_namespace,
            monitoring_interval,
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
        orchestrator: OrchestratorServiceInfo,
        planetary_namespace: Option<String>,
        tasks_namespace: Option<String>,
        monitoring_interval: Duration,
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
        let task_pvcs: Api<PersistentVolumeClaim> = Api::namespaced(
            client.clone(),
            tasks_namespace
                .as_deref()
                .unwrap_or(PLANETARY_TASKS_NAMESPACE),
        );

        let client = reqwest::Client::new();
        let mut interval = interval(monitoring_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    // Start by getting the current pod map
                    match Self::get_task_pod_map(&task_pods).await {
                        Ok(pod_map) => {
                            // Check for orphaned tasks
                            if let Err(e) = Self::check_orphaned_tasks(&client, &orchestrator, &planetary_pods, &pod_map).await {
                                let message = format!("failed to check for orphaned pods: {e:#}");
                                error!("{message}");
                                let _ = database.insert_error(MONITOR_ERROR_SOURCE, None, &message).await;
                            }

                            // Check for missing task resources
                            if let Err(e) = Self::check_missing_resources(database.as_ref(), &task_pvcs, &pod_map).await {
                                let message = format!("failed to check for missing Kubernetes resources: {e:#}");
                                error!("{message}");
                                let _ = database.insert_error(MONITOR_ERROR_SOURCE, None, &message).await;
                            }
                        }
                        Err(e) => {
                            let message = format!("failed to get task pod map: {e:#}");
                            error!("{message}");
                            let _ = database.insert_error(MONITOR_ERROR_SOURCE, None, &message).await;
                        }
                    };

                    // Perform a GC
                    if let Err(e) = Self::gc(&task_pods, &task_pvcs).await {
                        let message = format!("failed to garbage collect Kubernetes resources: {e:#}");
                        error!("{message}");
                        let _ = database.insert_error(MONITOR_ERROR_SOURCE, None, &message).await;
                    }
                }
            }
        }

        info!("task monitor has shut down");
    }

    /// Gets the map of pod name (TES id) to pod.
    ///
    /// The key of the map is the TES identifier.
    async fn get_task_pod_map(task_pods: &Api<Pod>) -> Result<HashMap<String, Pod>> {
        let mut map = HashMap::new();
        for pod in task_pods
            .list(&ListParams::default().labels(&format!("{PLANETARY_GC_LABEL}!=true")))
            .await?
        {
            // Only include pods with names
            let Some(name) = pod.name() else {
                continue;
            };

            map.insert(name.into_owned(), pod);
        }

        Ok(map)
    }

    /// Checks for orphaned tasks.
    ///
    /// A task is "orphaned" when the orchestrator managing its pod no longer
    /// exists.
    async fn check_orphaned_tasks(
        client: &reqwest::Client,
        orchestrator: &OrchestratorServiceInfo,
        planetary_pods: &Api<Pod>,
        pod_map: &HashMap<String, Pod>,
    ) -> Result<()> {
        let mut orchestrators = HashMap::new();
        for (tes_id, pod) in pod_map {
            let orchestrator_id = pod
                .metadata
                .labels
                .as_ref()
                .and_then(|l| l.get(PLANETARY_ORCHESTRATOR_LABEL));

            if let Some(id) = orchestrator_id {
                // Check to see if the associated orchestrator pod exists
                let entry = match orchestrators.entry(id) {
                    Entry::Occupied(e) => e,
                    Entry::Vacant(e) => {
                        // Get the orchestrator's metadata; if we fail to get the metadata, assume
                        // the orchestrator exists for now
                        let exists = planetary_pods
                            .get_metadata_opt(e.key())
                            .await
                            .map(|m| m.is_some())
                            .unwrap_or(true);

                        e.insert_entry(exists)
                    }
                };

                // If the orchestrator doesn't exist, attempt to adopt it
                if !*entry.get() {
                    // SAFETY: we don't include pods in the map that do not have names
                    let name = pod.name().expect("missing pod name");

                    info!(
                        "orchestrator pod `{id}` that managed task pod `{name}` (task `{tes_id}`) \
                         no longer exists: requesting another orchestrator to adopt the pod",
                        id = entry.key(),
                    );

                    // Request that a running orchestrator adopt the pod
                    let response = client
                        .patch(
                            orchestrator
                                .url
                                .join(&format!("/v1/pods/{name}"))
                                .expect("URL should join"),
                        )
                        .header(
                            header::AUTHORIZATION,
                            format!(
                                "Bearer {token}",
                                token = orchestrator.api_key.expose_secret()
                            ),
                        )
                        .send()
                        .await?;

                    response.error_for_status().with_context(|| {
                        format!("failed to adopt pod `{name}` (task `{tes_id}`)")
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Checks for missing Kubernetes resources for "in-progress" tasks.
    async fn check_missing_resources(
        database: &dyn Database,
        task_pvcs: &Api<PersistentVolumeClaim>,
        pod_map: &HashMap<String, Pod>,
    ) -> Result<()> {
        debug!("checking for missing Kubernetes resources");

        // Query for ids for in-progress tasks that have existed since before the
        // creation delta
        let ids = database
            .get_in_progress_tasks(Utc::now() - TASK_CREATION_DELTA)
            .await?;

        for id in ids {
            if pod_map.contains_key(&id) {
                continue;
            }

            info!("task `{id}` does not have an associated pod and will be aborted");

            // Transition the task to a system error state
            if database
                .update_task_state(
                    &id,
                    State::SystemError,
                    &[&format_log_message!(
                        "task `{id}` was aborted by the system"
                    )],
                    None,
                )
                .await
                .with_context(|| format!("failed to update state for task `{id}`"))?
            {
                // Mark the PVC (if there is one) for GC
                match task_pvcs
                    .patch(
                        &id,
                        &PatchParams::default(),
                        &Patch::Merge(PersistentVolumeClaim {
                            metadata: ObjectMeta {
                                labels: Some(BTreeMap::from_iter([(
                                    PLANETARY_GC_LABEL.to_string(),
                                    "true".to_string(),
                                )])),
                                ..Default::default()
                            },
                            ..Default::default()
                        }),
                    )
                    .await
                {
                    Ok(_) => {}
                    Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
                    Err(e) => {
                        return Err(e).with_context(|| format!("failed to mark PVC `{id}` for GC"));
                    }
                }
            }
        }

        Ok(())
    }

    /// Performs a GC for task resources.
    async fn gc(pods: &Api<Pod>, pvcs: &Api<PersistentVolumeClaim>) -> Result<()> {
        debug!("performing garbage collection");

        let label = format!("{PLANETARY_GC_LABEL}=true");

        // Delete task pods
        pods.delete_collection(
            &DeleteParams::default(),
            &ListParams::default().labels(&label),
        )
        .await?;

        // Delete task PVCs
        pvcs.delete_collection(
            &DeleteParams::default(),
            &ListParams::default().labels(&label),
        )
        .await?;

        Ok(())
    }
}
