//! Implementation of the task monitor.
//!
//! The task monitor is responsible for the following:
//!
//! * Monitoring the Kubernetes cluster for orphaned task pods; an orphaned task
//!   pod is one which is not associated with a running orchestrator.
//!
//! * Monitoring for task resources that need garbage collection.
//!
//! * Monitoring for canceled tasks that need to need to be deleted.

use std::collections::HashMap;
use std::collections::hash_map::Entry;
use std::fs;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use chrono::Utc;
use futures::StreamExt as _;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use k8s_openapi::serde::Deserialize;
use kube::Api;
use kube::Client;
use kube::Discovery;
use kube::ResourceExt;
use kube::api::ApiResource;
use kube::api::DeleteParams;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use kube::api::ListParams;
use kube::api::ObjectList;
use kube::discovery::ApiCapabilities;
use kube::discovery::Scope;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector::Lookup;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use planetary_db::Database;
use planetary_db::format_log_message;
use reqwest::StatusCode;
use reqwest::header;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tera::Tera;
use tes::v1::types::task::State as TesState;
use tokio::pin;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use url::Url;

/// The expected name of the task resource template.
const TEMPLATE_NAME: &str = "task.yaml";

/// The task id label.
const TASK_LABEL: &str = "planetary/task";

/// The orchestrator id label.
const ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The cancellation label used to mark canceled task for garbage collection.
const CANCELED_LABEL: &str = "planetary/canceled";

/// The amount of time after a task has been created for which we will consider
/// it to be in-progress.
///
/// Effectively, this is the maximum amount of time we're giving an orchestrator
/// to create task resources after a database entry for the task was inserted.
///
/// Setting this too low may cause the monitor to abort a task before it even
/// has a chance to start.
const TASK_CREATION_DELTA: Duration = Duration::from_secs(60);

/// Creates a dummy template context for a TES task.
///
/// This is used to render a template for a task so that we can delete its
/// resources.
fn create_context(id: &str) -> Result<tera::Context> {
    /// Helper for inserting items into the context.
    fn insert(
        context: &mut tera::Map<String, tera::Value>,
        name: impl Into<String>,
        value: impl Into<tera::Value>,
    ) {
        context.insert(name.into(), value.into());
    }

    let mut context = tera::Map::new();
    insert(&mut context, "id", id);
    insert(&mut context, "preemptible", false);
    insert(&mut context, "cpu", 1);
    insert(&mut context, "memory", "1G");
    insert(&mut context, "disk", "1G");
    insert(&mut context, "inputs", Vec::<tera::Value>::new());
    insert(&mut context, "outputs", Vec::<tera::Value>::new());
    insert(&mut context, "volumes", Vec::<tera::Value>::new());
    insert(&mut context, "executors", Vec::<tera::Value>::new());
    tera::Context::from_value(context.into()).context("invalid template context")
}

/// Deserializes a Kubernetes object and returns its resolved API resources and
/// capabilities.
fn deserialize_object(
    state: &State,
    de: serde_yaml_ng::Deserializer<'_>,
) -> Result<(ApiResource, ApiCapabilities, DynamicObject)> {
    let mut object =
        DynamicObject::deserialize(de).context("failed to deserialize task resource template")?;

    let name = object
        .name()
        .context("task template contains a resource that has no name")?;

    let meta = object.types.as_ref().with_context(|| {
        format!("task resource `{name}` does not specify an object API version and kind")
    })?;

    let gvk = GroupVersionKind::try_from(meta).with_context(|| {
        format!(
            "task resource `{name}` has invalid kind: `{kind}` ({api})",
            kind = meta.kind,
            api = meta.api_version
        )
    })?;

    let (resource, capabilities) = state.discovery.resolve_gvk(&gvk).with_context(|| {
        format!(
            "task resource `{name}` has unknown resource kind `{kind}` ({api})",
            name = object.name().expect("object should have a name"),
            kind = meta.kind,
            api = meta.api_version
        )
    })?;

    if capabilities.scope == Scope::Cluster {
        object.metadata.namespace = None;
    } else {
        object.metadata.namespace = Some(state.namespaces.tasks.to_string());
    }

    Ok((resource, capabilities, object))
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

/// Represents state shared between different monitor tokio tasks.
struct State {
    /// The shutdown cancellation token.
    shutdown: CancellationToken,
    /// The Planetary database to use.
    database: Arc<dyn Database>,
    /// The K8s client to use.
    client: Client,
    /// The K8s discovery result.
    discovery: Discovery,
    /// The K8s namespaces.
    namespaces: Namespaces,
    /// The monitor intervals.
    intervals: Intervals,
    /// The templates for deleting task resources.
    templates: tera::Tera,
}

impl State {
    /// Constructs a new [`State`].
    async fn new(
        database: Arc<dyn Database>,
        templates_dir: PathBuf,
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

        let templates = Tera::new(templates_dir.join("**/*").to_str().with_context(|| {
            format!(
                "templates directory `{path}` is not valid UTF-8",
                path = templates_dir.display()
            )
        })?)?;

        if !templates.get_template_names().any(|n| n == TEMPLATE_NAME) {
            bail!(
                "templates directory `{path}` does not contain a template named `{TEMPLATE_NAME}`",
                path = templates_dir.display()
            );
        }

        Ok(Self {
            shutdown: CancellationToken::new(),
            database,
            client,
            discovery,
            namespaces,
            intervals,
            templates,
        })
    }

    /// Logs an error with the database.
    ///
    /// The error is also emitted to stderr.
    async fn log_error(&self, tes_id: Option<&str>, message: &str) {
        /// The error source for the monitor.
        const MONITOR_ERROR_SOURCE: &str = "monitor";

        error!("{message}");
        let _ = self
            .database
            .insert_error(MONITOR_ERROR_SOURCE, tes_id, message)
            .await;
    }
}

/// Represents the task monitor.
pub struct Monitor {
    /// The cancellation token for shutting down the service.
    shutdown: CancellationToken,
    /// The handle to the orphan monitoring tokio task.
    orphans: JoinHandle<()>,
    /// The handle to the garbage monitoring tokio task.
    garbage: JoinHandle<()>,
    /// The handle to the cancellation monitoring tokio task.
    cancellations: JoinHandle<()>,
}

impl Monitor {
    /// Spawns a new task monitor.
    ///
    /// This method will spawn Tokio tasks for monitoring cluster state.
    pub async fn spawn(
        database: Arc<dyn Database>,
        orchestrator: OrchestratorServiceInfo,
        namespaces: Namespaces,
        templates_dir: impl Into<PathBuf>,
        intervals: Intervals,
    ) -> Result<Self> {
        let state =
            Arc::new(State::new(database, templates_dir.into(), namespaces, intervals).await?);

        // Spawn the orphan monitoring tokio task
        let orphans = tokio::spawn(Self::monitor_orphans(state.clone(), orchestrator));

        // Spawn the garbage monitoring tokio task
        let garbage = tokio::spawn(Self::monitor_garbage(state.clone()));

        // Spawn the cancellations monitoring tokio task
        let cancellations = tokio::spawn(Self::monitor_cancellations(state.clone()));

        Ok(Self {
            shutdown: state.shutdown.clone(),
            orphans,
            garbage,
            cancellations,
        })
    }

    /// Shuts down the service.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.orphans
            .await
            .expect("failed to join orphan monitoring task");
        self.garbage
            .await
            .expect("failed to join garbage monitoring task");
        self.cancellations
            .await
            .expect("failed to join cancellations monitoring task");
    }

    /// Implements the orphan monitoring tokio task.
    async fn monitor_orphans(state: Arc<State>, orchestrator: OrchestratorServiceInfo) {
        info!("orphaned task monitor has started");

        let planetary_pods: Api<Pod> =
            Api::namespaced(state.client.clone(), &state.namespaces.planetary);
        let task_pods: Api<Pod> = Api::namespaced(state.client.clone(), &state.namespaces.tasks);

        let http_client = reqwest::Client::new();
        let mut interval = tokio::time::interval(state.intervals.check);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                _ = state.shutdown.cancelled() => break,
                _ = interval.tick() => {
                    // Start by getting the current pod map
                    match Self::get_task_pod_map(&task_pods).await {
                        Ok(pod_map) => {
                            // Check for orphaned tasks
                            if let Err(e) = Self::check_orphaned_tasks(&http_client, &orchestrator, &planetary_pods, &pod_map).await {
                                state.log_error(None,  &format!("failed to check for orphaned pods: {e:#}")).await;
                            }

                            // Check for missing task resources
                            if let Err(e) = Self::check_missing_resources(state.database.as_ref(), &pod_map).await {
                                state.log_error(None,  &format!("failed to check for missing Kubernetes resources: {e:#}")).await;
                            }
                        }
                        Err(e) => {
                            state.log_error(None,  &format!("failed to get task pod map: {e:#}")).await;
                        }
                    };
                }
            }
        }

        info!("orphaned task monitor has shut down");
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
                    TesState::SystemError,
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

    /// Implements the garbage monitoring tokio task.
    async fn monitor_garbage(state: Arc<State>) {
        info!("garbage monitor has started");

        let task_pods: Api<Pod> = Api::namespaced(state.client.clone(), &state.namespaces.tasks);

        let mut interval = tokio::time::interval(state.intervals.check);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        loop {
            select! {
                biased;
                _ = state.shutdown.cancelled() => break,
                _ = interval.tick() => {
                    // Perform a GC
                    if let Err(e) = Self::gc(&state, &task_pods).await {
                        state.log_error(None,  &format!("failed to garbage collect Kubernetes resources: {e:#}")).await;
                    }
                }
            }
        }

        info!("garbage monitor has shut down");
    }

    /// Performs a garbage collection for terminated tasks.
    async fn gc(state: &State, task_pods: &Api<Pod>) -> Result<()> {
        /// The maximum number of tasks to collect per iteration
        const MAX_TASKS: u32 = 100;

        /// Helper for filtering a pod for garbage collection.
        ///
        /// Returns the task id if the pod is outside of the keep interval and
        /// it has the task id label.
        ///
        /// Otherwise, `None` is returned.
        fn filter_pod(pod: &Pod, keep_interval: Duration) -> Option<&str> {
            // Only include pods that haven't been deleted
            if pod.metadata.deletion_timestamp.is_some() {
                return None;
            }

            let status = pod.status.as_ref()?;

            // Find the last terminated container for the pod
            let terminated = status
                .container_statuses
                .iter()
                .rev()
                .flatten()
                .chain(status.init_container_statuses.iter().rev().flatten())
                .filter_map(|s| s.state.as_ref()?.terminated.as_ref())
                .next()?;

            // Check to see if the pod is within the keep interval
            if terminated.finished_at.as_ref()?.0 >= (Timestamp::now() - keep_interval) {
                return None;
            }

            // Return the id label's value
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

            token = metadata.continue_;

            for pod in &items {
                let Some(id) = filter_pod(pod, state.intervals.keep) else {
                    continue;
                };

                if let Err(e) = Self::delete_resources(state, id).await {
                    state
                        .log_error(
                            Some(id),
                            &format!("failed to delete resources for task `{id}`: {e}"),
                        )
                        .await;
                }
            }

            // Check to see if there are no more pods in the list
            if token.is_none() {
                return Ok(());
            }
        }
    }

    /// Deletes the resources for a task.
    async fn delete_resources(state: &State, id: &str) -> Result<()> {
        debug!("performing garbage collection for task `{id}`");

        let resources = state
            .templates
            .render(TEMPLATE_NAME, &create_context(id)?)
            .context("failed to render task resource template")?;

        // Deserialize each object before deleting them
        let objects: Vec<_> = serde_yaml_ng::Deserializer::from_str(&resources)
            .map(|de| deserialize_object(state, de))
            .collect::<Result<_>>()?;

        // Delete the task's resources
        for (resource, capabilities, object) in objects {
            let api: Api<DynamicObject> = if capabilities.scope == Scope::Cluster {
                Api::all_with(state.client.clone(), &resource)
            } else {
                Api::namespaced_with(state.client.clone(), &state.namespaces.tasks, &resource)
            };

            match api
                .delete(
                    &object.name().expect("object should have a name"),
                    &DeleteParams::foreground().grace_period(0),
                )
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.is_not_found() => {}
                Err(e) => {
                    state
                        .log_error(
                            None,
                            &format!(
                                "failed to delete resource `{kind}` ({api}) for task `{id}`: {e}",
                                kind = resource.kind,
                                api = resource.api_version
                            ),
                        )
                        .await;
                }
            }
        }

        // Delete the orchestrator storage for the task
        let task_dir = Path::new("/mnt/orchestrator").join(id);
        if task_dir.is_dir()
            && let Err(e) = fs::remove_dir_all(&task_dir)
        {
            state
                .log_error(
                    Some(id),
                    &format!(
                        "failed to delete task directory `{task_dir}`: {e}",
                        task_dir = task_dir.display()
                    ),
                )
                .await;
        }

        Ok(())
    }

    /// Monitors Kubernetes pod events for the cancellation label to be applied.
    ///
    /// Responsible for immediately deleting pods and related resources for a
    /// canceled
    async fn monitor_cancellations(state: Arc<State>) {
        info!("canceled task monitor processing has started");

        let stream = watcher(
            Api::<Pod>::namespaced(state.client.clone(), &state.namespaces.tasks),
            watcher::Config {
                label_selector: Some(CANCELED_LABEL.to_string()),
                ..Default::default()
            },
        )
        .default_backoff();

        pin!(stream);

        loop {
            select! {
                biased;

                _ = state.shutdown.cancelled() => break,
                event = stream.next() => {
                    match event {
                        Some(Ok(Event::InitApply(pod) | Event::Apply(pod))) => {
                            let state = state.clone();
                            tokio::spawn(async move {
                                if let Some(id) = pod.labels().get(TASK_LABEL) &&
                                    let Err(e) = Self::delete_resources(&state, id).await {
                                        state
                                            .log_error(
                                                Some(id),
                                                &format!("failed to delete resources for task `{id}`: {e}"),
                                            )
                                            .await;

                                }
                            });
                        }
                        Some(Ok(Event::Init | Event::InitDone | Event::Delete(_))) => continue,
                        Some(Err(watcher::Error::WatchError(e))) if e.code == StatusCode::GONE => {
                            // This response happens when the initial resource version
                            // is too old. When this happens, the watcher will get a new
                            // resource version, so don't bother logging the error
                        }
                        Some(Err(e)) => {
                            state.log_error(None, &format!("error while streaming Kubernetes pod events: {e:#}")).await;
                        }
                        None => break,
                    }
                }
            }
        }

        info!("canceled task monitor has shut down");
    }
}
