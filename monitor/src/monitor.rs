//! Implementation of the task monitor.
//!
//! The task monitor is responsible for monitoring the Kubernetes
//! cluster for orphaned task pods. An orphaned task pod is one which is not
//! associated with a running orchestrator.
//!
//! Additionally, the task monitor will search for task pods in the database
//! that do not have associated Kubernetes resources; it will abort any running
//! tasks where a task pod has been deleted without an associated orchestrator.
//!
//! The task monitor is also responsible for deleting task resources in the
//! cluster for any task that has entered a terminal state.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use chrono::Utc;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use kube::Api;
use kube::Client;
use kube::Discovery;
use kube::ResourceExt;
use kube::api::DeleteParams;
use kube::api::DynamicObject;
use kube::api::ListParams;
use kube::api::ObjectList;
use kube::discovery::Scope;
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
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use url::Url;

/// The task id label.
const TASK_LABEL: &str = "planetary/task";

/// The orchestrator id label.
const ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The amount of time after a task has been created for which we will consider
/// it to be in-progress.
///
/// Effectively, this is the maximum amount of time we're giving an orchestrator
/// to create task resources after a database entry for the task was inserted.
///
/// Setting this too low may cause the monitor to abort a task before it even
/// has a chance to start.
const TASK_CREATION_DELTA: Duration = Duration::from_secs(60);

/// Logs an error with the database.
///
/// The error is also emitted to stderr.
async fn log_error(database: &dyn Database, tes_id: Option<&str>, message: &str) {
    /// The error source for the monitor.
    const MONITOR_ERROR_SOURCE: &str = "monitor";

    error!("{message}");
    let _ = database
        .insert_error(MONITOR_ERROR_SOURCE, tes_id, message)
        .await;
}

/// Represents information about the orchestrator service.
pub struct OrchestratorServiceInfo {
    /// The URL of the orchestrator service.
    pub url: Url,
    /// The orchestrator service API key.
    pub api_key: SecretString,
}

/// Represents information about relevant Kubernetes namespaces.
pub struct Namespaces {
    /// The planetary namespace name.
    pub planetary: String,
    /// The tasks namespace name.
    pub tasks: String,
}

/// Represents information about monitoring intervals.
#[derive(Debug, Clone, Copy)]
pub struct Intervals {
    /// The interval for the check operation.
    pub check: Duration,
    /// The interval for the keeping Kubernetes resources after a task enters a
    /// terminal state.
    pub keep: Duration,
}

/// Represents the task monitor.
pub struct Monitor {
    /// The cancellation token for shutting down the service.
    shutdown: CancellationToken,
    /// The handle to the monitoring task.
    monitor: JoinHandle<()>,
    /// The handle to the GC task.
    gc: JoinHandle<()>,
}

impl Monitor {
    /// Spawns a new task monitor.
    ///
    /// This method will spawn Tokio tasks for monitoring cluster state.
    pub async fn spawn(
        database: Arc<dyn Database>,
        orchestrator: OrchestratorServiceInfo,
        namespaces: Namespaces,
        intervals: Intervals,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to get default Kubernetes client")?;

        let discovery = Discovery::new(client.clone())
            .run_aggregated()
            .await
            .context("failed to perform cluster resource discovery")?;

        let shutdown = CancellationToken::new();

        // Spawn the GC tokio task
        let gc = tokio::spawn(Self::gc(
            shutdown.clone(),
            database.clone(),
            client.clone(),
            discovery,
            namespaces.tasks.clone(),
            intervals,
        ));

        // Spawn the monitoring tokio task
        let monitor = tokio::spawn(Self::monitor(
            shutdown.clone(),
            database,
            client,
            orchestrator,
            namespaces,
            intervals.check,
        ));

        Ok(Self {
            shutdown,
            monitor,
            gc,
        })
    }

    /// Shuts down the service.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.monitor.await.expect("failed to join monitor task");
        self.gc.await.expect("failed to join gc task");
    }

    /// Implements the monitoring tokio task.
    async fn monitor(
        shutdown: CancellationToken,
        database: Arc<dyn Database>,
        client: Client,
        orchestrator: OrchestratorServiceInfo,
        namespaces: Namespaces,
        interval: Duration,
    ) {
        info!("task monitor has started");

        let planetary_pods: Api<Pod> = Api::namespaced(client.clone(), &namespaces.planetary);
        let task_pods: Api<Pod> = Api::namespaced(client.clone(), &namespaces.tasks);

        let http_client = reqwest::Client::new();
        let mut interval = tokio::time::interval(interval);
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
                            if let Err(e) = Self::check_orphaned_tasks(&http_client, &orchestrator, &planetary_pods, &pod_map).await {
                                log_error(database.as_ref(), None,  &format!("failed to check for orphaned pods: {e:#}")).await;
                            }

                            // Check for missing task resources
                            if let Err(e) = Self::check_missing_resources(database.as_ref(), &pod_map).await {
                                log_error(database.as_ref(), None,  &format!("failed to check for missing Kubernetes resources: {e:#}")).await;
                            }
                        }
                        Err(e) => {
                            log_error(database.as_ref(), None,  &format!("failed to get task pod map: {e:#}")).await;
                        }
                    };
                }
            }
        }

        info!("task monitor has shut down");
    }

    /// Implements the garbage collector tokio task.
    async fn gc(
        shutdown: CancellationToken,
        database: Arc<dyn Database>,
        client: Client,
        discovery: Discovery,
        task_namespace: String,
        intervals: Intervals,
    ) {
        info!("garbage collector has started");

        let task_pods: Api<Pod> = Api::namespaced(client.clone(), &task_namespace);

        let mut interval = tokio::time::interval(intervals.check);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                _ = shutdown.cancelled() => break,
                _ = interval.tick() => {
                    // Perform a GC
                    if let Err(e) = Self::collect_resources(database.as_ref(), &task_pods, &client, &discovery, &task_namespace, intervals.keep).await {
                        log_error(database.as_ref(), None,  &format!("failed to garbage collect Kubernetes resources: {e:#}")).await;
                    }
                }
            }
        }

        info!("garbage collector has shut down");
    }

    /// Gets the map of task identifier to pod.
    async fn get_task_pod_map(task_pods: &Api<Pod>) -> Result<HashMap<String, Pod>> {
        let mut map = HashMap::new();
        for pod in task_pods
            .list(&ListParams::default().labels(ORCHESTRATOR_LABEL))
            .await?
        {
            // Only include pods with names
            let Some(id) = pod.labels().get(TASK_LABEL) else {
                continue;
            };

            // Only include pods that haven't been deleted
            if pod.metadata.deletion_timestamp.is_some() {
                continue;
            }

            map.insert(id.clone(), pod);
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
                .and_then(|l| l.get(ORCHESTRATOR_LABEL));

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

            // Transition the task to a system error state
            if database
                .update_task_state(
                    &id,
                    State::SystemError,
                    &[&format_log_message!(
                        "task `{id}` was aborted by the system"
                    )],
                    None,
                    None,
                )
                .await
                .with_context(|| format!("failed to update state for task `{id}`"))?
            {
                info!("task `{id}` does not have an associated pod and was aborted");
            }
        }

        Ok(())
    }

    /// Performs a garbage collection for terminated tasks.
    async fn collect_resources(
        database: &dyn Database,
        task_pods: &Api<Pod>,
        client: &Client,
        discovery: &Discovery,
        tasks_namespace: &str,
        keep_interval: Duration,
    ) -> Result<()> {
        /// The maximum number of tasks to collect per iteration
        const MAX_TASKS: u32 = 100;

        /// Helper for mapping a pod to a task id.
        ///
        /// If the pod terminated within the keep interval, `None` is returned.
        ///
        /// If the pod is missing the task label, `None` is returned.
        fn map_task_id(pod: &Pod, keep_interval: Duration) -> Option<&str> {
            let status = pod.status.as_ref()?;

            // Look for the last container or init container status
            let status = if status
                .container_statuses
                .as_ref()
                .map(Vec::is_empty)
                .unwrap_or(true)
            {
                status.init_container_statuses.as_ref()?.last()?
            } else {
                status.container_statuses.as_ref()?.last()?
            };

            let state = status.state.as_ref()?;
            let terminated = state.terminated.as_ref()?;
            let finished_at = terminated.finished_at.as_ref()?;

            // Check to see if the pod's last container is within the keep interval
            if finished_at.0 >= (Timestamp::now() - keep_interval) {
                return None;
            }

            pod.labels().get(TASK_LABEL).map(String::as_str)
        }

        let mut token = None;

        loop {
            // Query all finished (succeeded or failed) task pods
            let ObjectList {
                metadata, items, ..
            } = task_pods
                .list(&ListParams {
                    label_selector: Some(TASK_LABEL.to_string()),
                    field_selector: Some("status.phase!=Running,status.phase!=Pending".to_string()),
                    limit: Some(MAX_TASKS),
                    continue_token: token,
                    ..Default::default()
                })
                .await
                .context("failed to query task pods")?;

            if items.is_empty() {
                return Ok(());
            }

            token = metadata.continue_;

            // Filter the pods to the associated task ids
            let ids: Vec<&str> = items
                .iter()
                .filter_map(|p| map_task_id(p, keep_interval))
                .collect();

            if ids.is_empty() {
                continue;
            }

            debug!("performing garbage collection for tasks: `{ids:?}`");

            // Delete all cluster resources associated with the tasks
            // Unfortunately there's no "generic" API for deleting all resources with a
            // label selector; we must make an API request for each defined resource
            for group in discovery.groups() {
                for version in group.versions() {
                    for (resource, capabilities) in group.versioned_resources(version) {
                        let api: Api<DynamicObject> = if capabilities.scope == Scope::Cluster {
                            Api::all_with(client.clone(), &resource)
                        } else {
                            Api::namespaced_with(client.clone(), tasks_namespace, &resource)
                        };

                        // Immediately perform a cascading delete of matching objects
                        if let Err(e) = api
                            .delete_collection(
                                &DeleteParams::foreground().grace_period(0),
                                &ListParams::default().labels(&format!(
                                    "{TASK_LABEL} in ({ids})",
                                    ids = ids.join(",")
                                )),
                            )
                            .await
                        {
                            match e {
                                // Ignore forbidden errors as the RBAC for the service may not
                                // permit delete operations on a
                                // resource
                                kube::Error::Api(e) if e.is_forbidden() => {}
                                _ => {
                                    log_error(
                                        database,
                                        None,
                                        &format!(
                                            "failed to delete resource collection for `{kind}` \
                                             ({api}): {e}",
                                            kind = resource.kind,
                                            api = resource.api_version
                                        ),
                                    )
                                    .await;
                                }
                            }
                        }
                    }
                }
            }

            // Delete the orchestrator storage for the task
            for id in ids {
                let task_dir = Path::new("/mnt/orchestrator").join(id);

                if task_dir.is_dir()
                    && let Err(e) = fs::remove_dir_all(&task_dir)
                {
                    log_error(
                        database,
                        Some(id),
                        &format!(
                            "failed to delete task directory `{task_dir}`: {e}",
                            task_dir = task_dir.display()
                        ),
                    )
                    .await;
                }
            }
        }
    }
}
