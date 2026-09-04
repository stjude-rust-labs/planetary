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
use chrono::Utc;
use futures::StreamExt as _;
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
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector::Lookup;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use planetary_db::Database;
use planetary_db::format_log_message;
use planetary_server::templating::Template;
use reqwest::StatusCode;
use reqwest::header;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tes::v1::types::task::State as TesState;
use tokio::pin;
use tokio::select;
use tokio::task::JoinHandle;
use tokio::time::MissedTickBehavior;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;
use tracing::warn;
use url::Url;

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
    /// The interval for sampling task pod resource usage.
    ///
    /// `None` disables resource usage sampling.
    pub usage: Option<Duration>,
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
    /// The task template for deleting task resources.
    template: Template,
}

impl State {
    /// Constructs a new [`State`].
    async fn new(
        database: Arc<dyn Database>,
        templates_dir: impl Into<PathBuf>,
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

        let template = Template::new(templates_dir.into())?;

        // Do an up-front rendering with a dummy identifier to catch errors in
        // the template. Note: this will not catch an error in the
        // template that would result from a branch or loop not taken.
        template
            .render_id_only("validation", &discovery, &namespaces.tasks)
            .context("template failed initial validation")?;

        Ok(Self {
            shutdown: CancellationToken::new(),
            database,
            client,
            discovery,
            namespaces,
            intervals,
            template,
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
    /// The handle to the resource usage sampling tokio task, if enabled.
    usage: Option<JoinHandle<()>>,
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
        let state = Arc::new(State::new(database, templates_dir, namespaces, intervals).await?);

        // Spawn the orphan monitoring tokio task
        let orphans = tokio::spawn(Self::monitor_orphans(state.clone(), orchestrator));

        // Spawn the garbage monitoring tokio task
        let garbage = tokio::spawn(Self::monitor_garbage(state.clone()));

        // Spawn the cancellations monitoring tokio task
        let cancellations = tokio::spawn(Self::monitor_cancellations(state.clone()));

        // Spawn the resource usage sampling tokio task, if enabled; a zero
        // interval is normalized to disabled so that no caller can spawn a
        // sampler with an interval Tokio would panic on
        let usage = state
            .intervals
            .usage
            .filter(|interval| !interval.is_zero())
            .map(|interval| tokio::spawn(Self::monitor_usage(state.clone(), interval)));

        Ok(Self {
            shutdown: state.shutdown.clone(),
            orphans,
            garbage,
            cancellations,
            usage,
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
        if let Some(usage) = self.usage {
            usage
                .await
                .expect("failed to join resource usage sampling task");
        }
    }

    /// Implements the resource usage sampling tokio task.
    ///
    /// Samples task pod resource usage from the kubelets hosting task pods
    /// (through the API server's node proxy) at the given interval and
    /// records each round of per-container observations in the database,
    /// which folds them into the tasks' aggregate usage.
    async fn monitor_usage(state: Arc<State>, sample_interval: Duration) {
        info!("task resource usage sampler has started");

        let pods: Api<Pod> = Api::namespaced(state.client.clone(), &state.namespaces.tasks);

        let mut interval = tokio::time::interval(sample_interval);
        interval.set_missed_tick_behavior(MissedTickBehavior::Skip);

        // Whether the previous sampling attempt failed; used to log the
        // first failure and the recovery visibly without per-tick noise
        let mut failing = false;

        loop {
            select! {
                biased;
                _ = state.shutdown.cancelled() => break,
                _ = interval.tick() => {
                    // Racing the round against shutdown keeps shutdown from
                    // waiting on a slow node or database. Aborting the round
                    // is safe: observations carry cumulative counters and
                    // the database advances its accounting baselines
                    // atomically with each recorded round, so an aborted or
                    // failed round is simply spanned by the next successful
                    // observation's delta
                    let round = async {
                        match crate::usage::sample_task_pods(
                            &state.client,
                            &pods,
                            &state.namespaces.tasks,
                        )
                        .await
                        {
                            Ok(samples) => {
                                if failing {
                                    failing = false;
                                    info!("sampling task pod resource usage has recovered");
                                }

                                if let Err(e) = state
                                    .database
                                    .add_task_resource_usage_samples(&samples)
                                    .await
                                {
                                    // Recording is idempotent, so this is
                                    // self-healing regardless of whether the
                                    // write actually committed
                                    error!(
                                        "failed to record resource usage samples (the round \
                                         will be covered by the next successful sample): {e:#}"
                                    );
                                }
                            }
                            Err(e) => {
                                if failing {
                                    debug!("failed to sample task pod resource usage: {e:#}");
                                } else {
                                    failing = true;
                                    warn!(
                                        "failed to sample task pod resource usage (does the \
                                         monitor's service account have permission to list task \
                                         pods and proxy to nodes?): {e:#}"
                                    );
                                }
                            }
                        }
                    };

                    if state.shutdown.run_until_cancelled(round).await.is_none() {
                        break;
                    }
                }
            }
        }

        info!("task resource usage sampler has shut down");
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
                    // Racing the round against shutdown keeps shutdown from
                    // waiting on slow cluster or database requests. Aborting
                    // the round is safe: orphan detection is recomputed from
                    // cluster and database state every round, and each
                    // adoption request and task state transition is applied
                    // individually, so any remaining work is picked up by
                    // the next round
                    let round = async {
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
                        }
                    };

                    if state.shutdown.run_until_cancelled(round).await.is_none() {
                        break;
                    }
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
                        // Get the orchestrator's metadata; if we fail to get
                        // the metadata, assume
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
                    // SAFETY: we don't include pods in the map that do not have
                    // names
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

        // Query for ids for in-progress tasks that have existed since before
        // the creation delta
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
    ///
    /// Cancellation is cooperative rather than racing the whole collection
    /// against shutdown: the shutdown token is checked between pages and
    /// between tasks, so that an in-progress [`Self::delete_resources`] for
    /// a task is never abandoned partway (which could delete the task's pod
    /// — through which garbage is discovered — while leaking its other
    /// resources). Shutdown latency is therefore bounded by a single task's
    /// resource deletion; any remaining garbage is collected by the next
    /// monitor instance.
    async fn gc(state: &State, task_pods: &Api<Pod>) -> Result<()> {
        /// The maximum number of tasks to collect per iteration
        const MAX_TASKS: u32 = 100;

        /// Helper for filtering a pod for garbage collection.
        ///
        /// Returns the task id if the pod is outside of the keep interval and
        /// it has the task id label.
        ///
        /// Otherwise, `None` is returned.
        fn filter_pod(pod: &Pod, now: Timestamp, keep_interval: Duration) -> Option<&str> {
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
            if terminated.finished_at.as_ref()?.0 >= (now - keep_interval) {
                return None;
            }

            // Return the id label's value
            pod.labels().get(TASK_LABEL).map(String::as_str)
        }

        let mut token = None;
        let now = Timestamp::now();

        loop {
            // Stop between pages when shutting down
            if state.shutdown.is_cancelled() {
                return Ok(());
            }

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
                // Stop between tasks when shutting down
                if state.shutdown.is_cancelled() {
                    return Ok(());
                }

                let Some(id) = filter_pod(pod, now, state.intervals.keep) else {
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

        // Delete the task's resources
        for resource in
            state
                .template
                .render_id_only(id, &state.discovery, &state.namespaces.tasks)?
        {
            let api: Api<DynamicObject> = if resource.capabilities().scope == Scope::Cluster {
                Api::all_with(state.client.clone(), resource.api())
            } else {
                Api::namespaced_with(
                    state.client.clone(),
                    &state.namespaces.tasks,
                    resource.api(),
                )
            };

            let name = resource
                .object()
                .name()
                .context("object should have a name")?;

            match api
                .delete(&name, &DeleteParams::foreground().grace_period(0))
                .await
            {
                Ok(_) => {}
                Err(kube::Error::Api(e)) if e.is_not_found() => {}
                Err(e) => {
                    state
                        .log_error(
                            None,
                            &format!(
                                "failed to delete task resource `{name}` of kind `{kind}` and API \
                                 version `{api}`: {e}",
                                kind = resource.api().kind,
                                api = resource.api().api_version
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
                            // The deletion is deliberately detached: it is
                            // not joined on shutdown, so an in-flight
                            // deletion may be cut short by process exit
                            // (just as by a crash). This is recoverable, as
                            // the cancellation label persists on the pod and
                            // the next monitor instance's watcher re-observes
                            // it; joining with a timeout would instead delay
                            // shutdown behind slow deletions
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
