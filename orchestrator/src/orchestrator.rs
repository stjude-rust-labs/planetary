//! Implementation of the task orchestrator.
//!
//! The task orchestrator is creating Kubernetes resources for tasks.
//!
//! It watches for Kubernetes cluster events relating to the pods it is
//! orchestrating and updates the database accordingly.

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write;
use std::fs;
use std::io;
use std::io::BufReader;
use std::io::BufWriter;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context as _;
use anyhow::Result;
use anyhow::bail;
use axum::http::StatusCode;
use chrono::DateTime;
use futures::FutureExt;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::jiff::Timestamp;
use k8s_openapi::serde_json::from_reader;
use k8s_openapi::serde_json::to_writer;
use kube::Api;
use kube::Client;
use kube::Discovery;
use kube::api::ApiResource;
use kube::api::DeleteParams;
use kube::api::DynamicObject;
use kube::api::GroupVersionKind;
use kube::api::ListParams;
use kube::api::LogParams;
use kube::api::ObjectMeta;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::api::PostParams;
use kube::discovery::ApiCapabilities;
use kube::discovery::Scope;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector::Lookup;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use planetary_db::ContainerKind;
use planetary_db::Database;
use planetary_db::TerminatedContainer;
use planetary_db::format_log_message;
use serde::Deserialize;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tera::Tera;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::View;
use tes::v1::types::responses::Task;
use tes::v1::types::task::Executor;
use tes::v1::types::task::State;
use tokio::pin;
use tokio::select;
use tokio::task::JoinHandle;
use tokio_retry2::Retry;
use tokio_stream::StreamExt;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::into_retry_error;
use crate::notify_retry;
use crate::retry_durations;

/// The expected name of the task resource template.
const TEMPLATE_NAME: &str = "task.yaml";

/// The default task storage size, in gigabytes.
///
/// Uses a 1 GiB default.
const DEFAULT_STORAGE_SIZE: f64 = 1.07374182;

/// The default CPU request (in cores) for tasks.
const DEFAULT_CPU: i32 = 1;

/// The default memory request (in GB) for tasks.
///
/// Uses a 256 MiB default.
const DEFAULT_MEMORY: f64 = 0.268435455;

/// The orchestrator id label.
const ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The task id label.
const TASK_LABEL: &str = "planetary/task";

/// The mount point for the orchestrator volume.
///
/// Each task will have a sub-directory containing the following files:
///
/// * `inputs.json` - the task's inputs serialized as JSON; created by the
///   orchestrator.
/// * `outputs.json` - the task's outputs serialized as JSON; created by the
///   orchestrator.
/// * `uploaded.json` - the task's uploaded outputs serialized as JSON; created
///   by the transporter.
///
/// A task's directory will be deleted when the task transitions to a terminal
/// state.
const ORCHESTRATOR_MOUNT: &str = "/mnt/orchestrator";

/// The maximum number of lines to tail for an executor pod's logs.
///
/// This is 16 because there is always an extra line of output in the executor's
/// log to maybe contain the executors real exit code.
const MAX_EXECUTOR_LOG_LINES: i64 = 16;

/// The reason for an image pull backoff wait.
const IMAGE_PULL_BACKOFF_REASON: &str = "ImagePullBackOff";

/// A prefix that is used in a container's output when the a task executor
/// should ignore errors.
///
/// As executors run as init containers and init containers *must* return a zero
/// exit, an executor that should ignore errors is run in such a way that the
/// command's exit code is only printed to stdout rather than causing a non-zero
/// exit of the container.
///
/// Thus, in Kubernetes the container always has a zero exit, but in the TES
/// representation of the executor run by that container records the original
/// exit code.
const EXIT_PREFIX: &str = "exit: ";

/// Formats a container name given the container kind.
fn format_container_name(kind: ContainerKind, executor_index: Option<usize>) -> String {
    match kind {
        ContainerKind::Inputs => "inputs".into(),
        ContainerKind::Executor => format!(
            "executor-{index}",
            index = executor_index.expect("should have index")
        ),
        ContainerKind::Outputs => "outputs".into(),
    }
}

/// Gets the task directory path for a TES task.
fn task_directory_path(tes_id: &str) -> PathBuf {
    Path::new(ORCHESTRATOR_MOUNT).join(tes_id)
}

/// Gets the inputs file path for a TES task.
fn inputs_file_path(tes_id: &str) -> PathBuf {
    task_directory_path(tes_id).join("inputs.json")
}

/// Gets the outputs file path for a TES task.
fn outputs_file_path(tes_id: &str) -> PathBuf {
    task_directory_path(tes_id).join("outputs.json")
}

/// Gets the uploaded outputs file path for a TES task.
fn uploaded_outputs_file_path(tes_id: &str) -> PathBuf {
    task_directory_path(tes_id).join("uploaded.json")
}

/// Helper function for serializing an array of serializable items to a file.
fn serialize_items<T: Serialize>(path: impl AsRef<Path>, items: &[T]) -> anyhow::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create directory `{parent}`",
                parent = parent.display()
            )
        })?;
    }

    let file = fs::File::create(path)
        .with_context(|| format!("failed to create file `{path}`", path = path.display()))?;

    to_writer(BufWriter::new(file), items)
        .with_context(|| format!("failed to write file `{path}`", path = path.display()))?;

    Ok(())
}

/// Helper function for deserializing an array of deserializable items from a
/// file.
///
/// Returns `Ok(None)` if the file does not exist.
fn deserialize_items<T: DeserializeOwned>(
    path: impl AsRef<Path>,
) -> anyhow::Result<Option<Vec<T>>> {
    let path = path.as_ref();

    let file = match fs::File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            return Ok(None);
        }
        Err(e) => {
            return Err(e)
                .with_context(|| format!("failed to open file `{path}`", path = path.display()));
        }
    };

    Ok(Some(from_reader(BufReader::new(file)).with_context(
        || format!("failed to read file `{path}`", path = path.display()),
    )?))
}

/// Creates a template context for a TES task.
///
/// Returns an error if the task was invalid.
fn create_context(task: &Task) -> Result<tera::Context> {
    /// Helper for inserting items into the context.
    fn insert(
        context: &mut tera::Map<String, tera::Value>,
        name: impl Into<String>,
        value: impl Into<tera::Value>,
    ) {
        context.insert(name.into(), value.into());
    }

    let resources = task.resources.as_ref();

    let mut context = tera::Map::new();
    insert(
        &mut context,
        "id",
        task.id.as_deref().context("task should have id")?,
    );
    insert(
        &mut context,
        "preemptible",
        resources.and_then(|r| r.preemptible).unwrap_or(false),
    );
    insert(
        &mut context,
        "cpu",
        resources.and_then(|r| r.cpu_cores).unwrap_or(DEFAULT_CPU),
    );
    insert(
        &mut context,
        "memory",
        format!(
            "{memory}G",
            memory = resources
                .and_then(|r| r.ram_gb)
                .unwrap_or(DEFAULT_MEMORY)
                .ceil()
        ),
    );
    insert(
        &mut context,
        "disk",
        format!(
            "{disk}G",
            disk = resources
                .and_then(|r| r.disk_gb)
                .unwrap_or(0.0)
                .max(DEFAULT_STORAGE_SIZE)
        ),
    );

    let inputs: Vec<tera::Value> = task
        .inputs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|i| i.path.clone().into())
        .collect();
    insert(&mut context, "inputs", inputs);

    let outputs: Vec<tera::Value> = task
        .outputs
        .as_deref()
        .unwrap_or_default()
        .iter()
        .map(|o| o.path.clone().into())
        .collect();
    insert(&mut context, "outputs", outputs);

    insert(
        &mut context,
        "volumes",
        task.volumes.as_deref().unwrap_or_default(),
    );

    let executors: Vec<tera::Value> = task
        .executors
        .iter()
        .map(|e| {
            let mut executor = tera::Map::new();
            insert(&mut executor, "image", e.image.clone());
            insert(&mut executor, "script", format_executor_script(e)?);
            insert(
                &mut executor,
                "workdir",
                e.workdir.as_deref().unwrap_or_default(),
            );

            let mut env = tera::Map::new();
            if let Some(vars) = e.env.as_ref() {
                for (k, v) in vars {
                    insert(&mut env, k, v.clone());
                }
            }

            insert(&mut executor, "env", env);
            Ok(executor.into())
        })
        .collect::<Result<_>>()?;
    insert(&mut context, "executors", executors);

    tera::Context::from_value(context.into()).context("invalid template context")
}

/// Formats an executor script into a single line that can be used with `sh -c`.
///
/// It is expected that the arguments are already shell quoted.
fn format_executor_script(executor: &Executor) -> Result<String> {
    // Shell quote the stdin
    let stdin = executor
        .stdin
        .as_ref()
        .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stdin path `{p}`")))
        .transpose()?;

    // Shell quote the stdout
    let stdout = executor
        .stdout
        .as_ref()
        .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stdout path `{p}`")))
        .transpose()?;

    // Shell quote the stderr
    let stderr = executor
        .stderr
        .as_ref()
        .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stderr path `{p}`")))
        .transpose()?;

    // Shell join the command twice as we're going to nest its execution in yet
    // another `sh -c`.
    let command = shlex::try_join(executor.command.iter().map(AsRef::as_ref))
        .map_err(|_| Error::System("executor command was invalid".into()))?;
    let command = shlex::try_join([command.as_str()])
        .map_err(|_| Error::System("executor command was invalid".into()))?;

    let mut script = String::new();
    script.push_str("set -eu;");

    // Add check for stdin file existence
    if let Some(stdin) = &stdin {
        script.push_str("! [ -f ");
        script.push_str(stdin);
        script.push_str(r#" ] && >&2 echo "executor stdin file "#);
        script.push_str(stdin);
        script.push_str(r#" does not exist" && exit 1;"#);
    }

    // Set up stdout redirection
    // We use tee so that both Kubernetes and the requested stdout file have the
    // output
    if let Some(stdout) = &stdout {
        script.push_str(r#"out="${TMPDIR:-/tmp}/stdout";"#);
        script.push_str(r#"mkfifo "$out";"#);
        script.push_str("tee -a ");
        script.push_str(stdout);
        script.push_str(r#" < "$out" &"#);
    }

    // Set up stderr redirection
    // We use tee so that both Kubernetes and the requested stderr file have the
    // output
    if let Some(stderr) = &stderr {
        script.push_str(r#"err="${TMPDIR:-/tmp}/stderr";"#);
        script.push_str(r#"mkfifo "$err";"#);
        script.push_str("tee -a ");
        script.push_str(stderr);
        script.push_str(r#" < "$err" &"#);
    }

    // Add the command in a nested `sh -c` invocation in case it exits
    write!(&mut script, "sh -c {command}").unwrap();

    // Redirect stdout
    if stdout.is_some() {
        script.push_str(" >\"$out\"");
    }

    // Redirect stderr
    if stderr.is_some() {
        script.push_str(" 2>\"$err\"");
    }

    // Redirect stdin
    if let Some(stdin) = stdin {
        script.push_str(" < ");
        script.push_str(&stdin);
    }

    write!(
        &mut script,
        " || CODE=$?; echo \"{EXIT_PREFIX}${{CODE:-0}}\";"
    )
    .unwrap();

    // If not ignoring error, exit on non-zero
    if !executor.ignore_error.unwrap_or(false) {
        script.push_str("if [[ ${CODE:-0} -ne 0 ]]; then exit $CODE; fi;");
    }

    // We must wait for the background tee jobs to complete, otherwise buffers might
    // not be flushed
    script.push_str("wait $(jobs -p) 2>&1 >/dev/null");
    Ok(script)
}

/// Used to determine what state a task pod is in.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum TaskPodState<'a> {
    /// The pod is in an unknown state.
    Unknown,
    /// The pod is waiting to be scheduled.
    Waiting,
    /// The pod is initializing.
    Initializing,
    /// The pod is running.
    Running,
    /// The pod succeeded and the task is complete.
    Succeeded,
    /// The pod failed due to executor error (exited with non-zero).
    ExecutorError(usize),
    /// The pod failed due to system error.
    SystemError,
    /// The pod failed to pull its image and has backed off.
    ImagePullBackOff(&'a str),
}

impl fmt::Display for TaskPodState<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Waiting => write!(f, "waiting"),
            Self::Initializing => write!(f, "initializing"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::ExecutorError(_) => write!(f, "executor error"),
            Self::SystemError => write!(f, "system error"),
            Self::ImagePullBackOff(_) => write!(f, "image pull backoff"),
        }
    }
}

/// An extension trait for Kubernetes pods.
trait PodExt {
    /// Gets the TES task id of the pod.
    ///
    /// Returns an error if the annotation is missing and not in the expected
    /// format.
    fn tes_id(&self) -> Result<&str>;

    /// Determines the state of the pod.
    ///
    /// Returns an error if the pod is missing its phase or the phase is
    /// unknown.
    fn state(&self) -> Result<TaskPodState<'_>>;
}

impl PodExt for Pod {
    fn tes_id(&self) -> Result<&str> {
        use kube::ResourceExt;
        self.labels()
            .get(TASK_LABEL)
            .map(String::as_str)
            .context("pod has no task label")
    }

    fn state(&self) -> Result<TaskPodState<'_>> {
        let status = self.status.as_ref().context("pod has no status")?;
        let phase = status.phase.as_ref().context("pod has no phase")?;

        Ok(match phase.as_str() {
            "Pending" => {
                let statuses = status.container_statuses.as_deref().unwrap_or_default();

                let init_statuses = status
                    .init_container_statuses
                    .as_deref()
                    .unwrap_or_default();

                // Check for any container that has backed off pulling its image
                if let Some(message) = statuses.iter().chain(init_statuses.iter()).find_map(|s| {
                    let s = s.state.as_ref()?.waiting.as_ref()?;
                    if s.reason.as_deref() == Some(IMAGE_PULL_BACKOFF_REASON) {
                        Some(s.reason.as_deref().unwrap_or("task failed to pull image"))
                    } else {
                        None
                    }
                }) {
                    return Ok(TaskPodState::ImagePullBackOff(message));
                }

                // Check for all terminated init containers
                // If the pod is pending, it means it is waiting on the main (outputs) container
                // to start
                if !init_statuses.is_empty()
                    && init_statuses.iter().all(|s| {
                        s.state
                            .as_ref()
                            .map(|s| s.terminated.is_some())
                            .unwrap_or(false)
                    })
                {
                    return Ok(TaskPodState::Running);
                }

                // Check for any running init containers
                if let Some(index) = init_statuses.iter().position(|s| {
                    s.state
                        .as_ref()
                        .map(|s| s.running.is_some())
                        .unwrap_or(false)
                }) {
                    // If the first init container (inputs) is running, the task is initializing
                    if index == 0 {
                        TaskPodState::Initializing
                    } else {
                        // Otherwise an executor is running
                        TaskPodState::Running
                    }
                } else {
                    // No init container is running yet
                    TaskPodState::Waiting
                }
            }
            "Running" => TaskPodState::Running,
            "Succeeded" => TaskPodState::Succeeded,
            "Failed" => {
                let init_statuses = status
                    .init_container_statuses
                    .as_deref()
                    .unwrap_or_default();

                // Check for any failed executor
                if let Some(index) = init_statuses.iter().position(|s| {
                    s.state
                        .as_ref()
                        .and_then(|s| s.terminated.as_ref().map(|s| s.exit_code != 0))
                        .unwrap_or(false)
                }) && index > 0
                {
                    return Ok(TaskPodState::ExecutorError(index - 1));
                }

                TaskPodState::SystemError
            }
            "Unknown" => TaskPodState::Unknown,
            _ => bail!("unknown pod phase `{phase}`"),
        })
    }
}

/// Represents an orchestration error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// A system error with a message occurred.
    ///
    /// This error may be recorded in a task's system log.
    ///
    /// The message should not contain sensitive information.
    #[error("{0}")]
    System(String),
    /// A generic error occurred.
    #[error(transparent)]
    Generic(#[from] anyhow::Error),
    /// A Kubernetes error occurred.
    #[error(transparent)]
    Kubernetes(#[from] kube::Error),
    /// A database error occurred.
    #[error(transparent)]
    Database(#[from] planetary_db::Error),
}

impl Error {
    /// Converts the error to a system log message.
    fn as_system_log_message(&self) -> &str {
        match self {
            Self::System(msg) => msg,
            _ => {
                "an internal error occurred while running the task: contact the system \
                 administrator for details"
            }
        }
    }
}

impl From<Error> for planetary_server::Error {
    fn from(e: Error) -> Self {
        error!("orchestration error: {e:#}");
        planetary_server::Error::internal()
    }
}

/// The result type of the orchestrator methods.
pub type OrchestrationResult<T> = Result<T, Error>;

/// Implements the task orchestrator.
pub struct TaskOrchestrator {
    /// The id (pod name) of the orchestrator.
    id: String,
    /// The planetary database used by the orchestrator.
    database: Arc<dyn Database>,
    /// The Kubernetes client.
    client: Client,
    /// Used to discover available resource types on the cluster.
    discovery: Discovery,
    /// The templates for creating Kubernetes resources.
    templates: Tera,
    /// The task Kubernetes resource namespace.
    tasks_namespace: String,
}

impl TaskOrchestrator {
    /// Constructs a new task orchestrator.
    pub async fn new(
        database: Arc<dyn Database>,
        id: impl Into<String>,
        templates_dir: impl Into<PathBuf>,
        tasks_namespace: impl Into<String>,
    ) -> Result<Self> {
        let client = Client::try_default()
            .await
            .context("failed to get default Kubernetes client")?;

        let discovery = Discovery::new(client.clone())
            .run_aggregated()
            .await
            .context("failed to perform cluster resource discovery")?;

        let templates_dir = templates_dir.into().join("**/*");

        let templates = Tera::new(templates_dir.to_str().with_context(|| {
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
            id: id.into(),
            database,
            client,
            discovery,
            templates,
            tasks_namespace: tasks_namespace.into(),
        })
    }

    /// Starts the given task.
    pub async fn start_task(&self, tes_id: &str) {
        debug!("task `{tes_id}` is starting");

        let start = async {
            // Get the full task for the inputs and outputs
            let task = self
                .database
                .get_task(tes_id, GetTaskParams { view: View::Full })
                .await?
                .into_task()
                .expect("should have full task");

            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!("task `{tes_id}` has been created")],
                )
                .await?;

            // Serialize the task's inputs
            serialize_items(
                inputs_file_path(tes_id),
                task.inputs.as_deref().unwrap_or_default(),
            )?;

            // Serialize the task's outputs
            serialize_items(
                outputs_file_path(tes_id),
                task.outputs.as_deref().unwrap_or_default(),
            )?;

            let resources = self
                .templates
                .render(TEMPLATE_NAME, &create_context(&task)?)
                .context("failed to render task resource template")?;

            // Deserialize and create an API for each object before applying them
            let mut has_pod = false;
            let objects: Vec<_> = serde_yaml_ng::Deserializer::from_str(&resources)
                .map(|de| self.deserialize_object(tes_id, &mut has_pod, de))
                .collect::<Result<_>>()?;

            if !has_pod {
                bail!("task resource template did not define a pod for the task");
            }

            // Create the task's resources from the template
            for (resource, capabilities, object) in objects {
                let api = if capabilities.scope == Scope::Cluster {
                    Api::all_with(self.client.clone(), &resource)
                } else {
                    Api::namespaced_with(self.client.clone(), &self.tasks_namespace, &resource)
                };

                api.create(&PostParams::default(), &object)
                    .await
                    .with_context(|| {
                        format!(
                            "failed to create `{kind}` ({api}) task resource named `{name}`",
                            name = object.name().expect("object should have a name"),
                            kind = resource.kind,
                            api = resource.api_version
                        )
                    })?;
            }

            Ok(())
        };

        if let Err(e) = start.await {
            self.log_error(
                Some(tes_id),
                &format!("failed to start task `{tes_id}`: {e:#}"),
            )
            .await;

            let _ = self
                .database
                .update_task_state(
                    tes_id,
                    State::SystemError,
                    &[&format_log_message!("task `{tes_id}` failed to start")],
                    None,
                    None,
                )
                .await;
        }
    }

    /// Deserializes a Kubernetes object and returns its resolved API resources
    /// and capabilities.
    fn deserialize_object(
        &self,
        tes_id: &str,
        has_pod: &mut bool,
        de: serde_yaml_ng::Deserializer<'_>,
    ) -> Result<(ApiResource, ApiCapabilities, DynamicObject)> {
        let mut object = DynamicObject::deserialize(de)
            .context("failed to deserialize task resource template")?;

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

        if gvk.version == "v1" && gvk.kind == "Pod" {
            if *has_pod {
                bail!("task resource template defines more then one pod for the task");
            }

            // Set the orchestrator label for the task pod
            let labels = object.metadata.labels.get_or_insert_default();
            labels.insert(ORCHESTRATOR_LABEL.to_string(), self.id.clone());
            *has_pod = true;
        }

        // Set the task label for the object
        let labels = object.metadata.labels.get_or_insert_default();
        labels.insert(TASK_LABEL.to_string(), tes_id.to_string());

        let (resource, capabilities) = self.discovery.resolve_gvk(&gvk).with_context(|| {
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
            object.metadata.namespace = Some(self.tasks_namespace.clone());
        }

        Ok((resource, capabilities, object))
    }

    /// Cancels the given task.
    pub async fn cancel_task(&self, tes_id: &str) {
        debug!("task `{tes_id}` is canceling");

        let cancel = async {
            let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.tasks_namespace);

            // Get the pod information so we can record the terminated containers at the
            // time it was canceled.
            let pod = Retry::spawn_notify(
                retry_durations(),
                || async {
                    pods.list(&ListParams::default().labels(&format!("{TASK_LABEL}={tes_id}")))
                        .await
                        .map_err(into_retry_error)
                },
                notify_retry,
            )
            .await?;

            let containers = pod
                .items
                .first()
                .map(|p| self.get_terminated_containers(p).boxed());

            if self
                .database
                .update_task_state(
                    tes_id,
                    State::Canceled,
                    &[&format_log_message!("task `{tes_id}` has been canceled")],
                    containers,
                    None,
                )
                .await?
            {
                // Immediately attempt to stop the pod upon cancellation (don't wait for the
                // monitor)
                if let Some(pod) = pod.items.first() {
                    let _ = pods
                        .delete(
                            &pod.name().expect("pod should have a name"),
                            &DeleteParams::default().grace_period(0),
                        )
                        .await;
                }

                debug!("task `{tes_id}` has been canceled");
            }

            OrchestrationResult::Ok(())
        };

        if let Err(e) = cancel.await {
            self.log_error(
                Some(tes_id),
                &format!("failed to cancel task `{tes_id}`: {e:#}"),
            )
            .await;

            let _ = self
                .database
                .update_task_state(
                    tes_id,
                    State::SystemError,
                    &[&format_log_message!(
                        "task `{tes_id}` failed to be canceled"
                    )],
                    None,
                    None,
                )
                .await;
        }
    }

    /// Adopts an orphaned task pod.
    pub async fn adopt_pod(&self, name: &str) -> OrchestrationResult<()> {
        debug!("adopting task pod `{name}`");

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &self.tasks_namespace);

        // Patch the pod's orchestrator label to be this orchestrator
        Retry::spawn_notify(
            retry_durations(),
            || async {
                pods.patch(
                    name,
                    &PatchParams::default(),
                    &Patch::Merge(Pod {
                        metadata: ObjectMeta {
                            labels: Some(BTreeMap::from_iter([(
                                ORCHESTRATOR_LABEL.to_string(),
                                self.id.clone(),
                            )])),
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                )
                .await
                .map_err(into_retry_error)
            },
            notify_retry,
        )
        .await?;

        Ok(())
    }

    /// Updates a TES task based on the given pod status.
    async fn update_task(&self, tes_id: &str, pod: &Pod) -> OrchestrationResult<()> {
        let state = pod.state()?;

        debug!("task pod `{tes_id}` is in state `{state}`");

        match state {
            TaskPodState::Unknown => self.handle_unknown_pod(tes_id, pod).await?,
            TaskPodState::Waiting => self.handle_waiting_task(tes_id).await?,
            TaskPodState::Initializing => self.handle_initializing_task(tes_id).await?,
            TaskPodState::Running => self.handle_running_task(tes_id).await?,
            TaskPodState::Succeeded => self.handle_succeeded_task(tes_id, pod).await?,
            TaskPodState::ExecutorError(index) => {
                self.handle_executor_error(tes_id, index, pod).await?
            }
            TaskPodState::SystemError => self.handle_system_error(tes_id, pod).await?,
            TaskPodState::ImagePullBackOff(message) => {
                self.handle_image_pull_backoff(tes_id, message, pod).await?
            }
        }

        Ok(())
    }

    /// Handles a waiting task.
    async fn handle_waiting_task(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Queued,
                &[&format_log_message!("task `{tes_id}` is now queued")],
                None,
                None,
            )
            .await?
        {
            debug!("task `{tes_id}` is now queued");
        }

        Ok(())
    }

    /// Handles an initializing task.
    async fn handle_initializing_task(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Initializing,
                &[&format_log_message!("task `{tes_id}` is now initializing")],
                None,
                None,
            )
            .await?
        {
            debug!("task `{tes_id}` is now initializing");
        }

        Ok(())
    }

    /// Handles a running task.
    async fn handle_running_task(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Running,
                &[&format_log_message!("task `{tes_id}` is now running")],
                None,
                None,
            )
            .await?
        {
            debug!("task `{tes_id}` is now running");
        }

        Ok(())
    }

    /// Handles a succeeded task.
    async fn handle_succeeded_task(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let outputs = deserialize_items(uploaded_outputs_file_path(tes_id))?;

        if self
            .database
            .update_task_state(
                tes_id,
                State::Complete,
                &[&format_log_message!("task `{tes_id}` has completed")],
                Some(self.get_terminated_containers(pod).boxed()),
                outputs.as_deref(),
            )
            .await?
        {
            debug!("task `{tes_id}` has completed");
        }

        Ok(())
    }

    /// Handles an executor error.
    async fn handle_executor_error(
        &self,
        tes_id: &str,
        index: usize,
        pod: &Pod,
    ) -> Result<(), Error> {
        let outputs = deserialize_items(uploaded_outputs_file_path(tes_id))?;

        if self
            .database
            .update_task_state(
                tes_id,
                State::ExecutorError,
                &[&format_log_message!(
                    "executor {index} of task `{tes_id}` has failed"
                )],
                Some(self.get_terminated_containers(pod).boxed()),
                outputs.as_deref(),
            )
            .await?
        {
            debug!("executor {index} of task `{tes_id}` has failed");
        }

        Ok(())
    }

    /// Handles a system error.
    async fn handle_system_error(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let outputs = deserialize_items(uploaded_outputs_file_path(tes_id))?;

        if self
            .database
            .update_task_state(
                tes_id,
                State::SystemError,
                &[&format_log_message!(
                    "task `{tes_id}` has failed due to a system error"
                )],
                Some(self.get_terminated_containers(pod).boxed()),
                outputs.as_deref(),
            )
            .await?
        {
            debug!("task `{tes_id}` has failed due to a system error");
        }

        Ok(())
    }

    /// Handles a pod in an unknown state.
    async fn handle_unknown_pod(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let outputs = deserialize_items(uploaded_outputs_file_path(tes_id))?;

        if self
            .database
            .update_task_state(
                tes_id,
                State::SystemError,
                &[&format_log_message!(
                    "communication was lost with a node running task `{tes_id}`: contact the \
                     system administrator for details"
                )],
                Some(self.get_terminated_containers(pod).boxed()),
                outputs.as_deref(),
            )
            .await?
        {
            self.log_error(
                Some(tes_id),
                &format!("task `{tes_id}` has failed: communication was lost with its pod",),
            )
            .await;
        }

        Ok(())
    }

    /// Handles an image pull backoff by failing the task with a system error.
    async fn handle_image_pull_backoff(
        &self,
        tes_id: &str,
        message: &str,
        pod: &Pod,
    ) -> OrchestrationResult<()> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::SystemError,
                &[&format_log_message!("failed to pull image: {message}")],
                Some(self.get_terminated_containers(pod).boxed()),
                None,
            )
            .await?
        {
            debug!("task `{tes_id}` failed to pull image: {message}");
        }

        Ok(())
    }

    /// Handles a pod deleted event.
    ///
    /// This method is not called when a pod is deleted by the orchestrator.
    ///
    /// It is called when a pod is deleted by Kubernetes or manually by a
    /// cluster administrator.
    ///
    /// Currently it treats an external pod deletion as a preempted task; in the
    /// future we may need to distinguish between a pod that's been moved as a
    /// result of a node scale up/down and one that was terminated on a
    /// specifically preemptible node (e.g. a spot instance).
    async fn handle_pod_deleted(&self, tes_id: &str, pod: &Pod) -> OrchestrationResult<()> {
        let _ = self
            .database
            .update_task_state(
                tes_id,
                State::Preempted,
                &[&format_log_message!("task `{tes_id}` has been preempted")],
                Some(self.get_terminated_containers(pod).boxed()),
                None,
            )
            .await?;

        Ok(())
    }

    /// Gets the terminated containers of the given pod.
    async fn get_terminated_containers<'a>(
        &self,
        pod: &'a Pod,
    ) -> anyhow::Result<Vec<TerminatedContainer<'a>>> {
        let name = pod.name().context("pod is missing a name")?;
        let ns = pod.namespace().context("pod is missing a namespace")?;
        let status = pod.status.as_ref().context("pod has no status")?;

        let pods: Api<Pod> = Api::namespaced(self.client.clone(), &ns);

        let init_statuses = status
            .init_container_statuses
            .as_deref()
            .unwrap_or_default();

        let statuses = status.container_statuses.as_deref().unwrap_or_default();

        let now = Timestamp::now();
        let mut containers = Vec::new();
        for (kind, executor_index, state) in init_statuses
            .iter()
            .enumerate()
            .map(|(i, s)| {
                if i == 0 {
                    (ContainerKind::Inputs, None, s)
                } else {
                    (ContainerKind::Executor, Some(i - 1), s)
                }
            })
            .chain(statuses.iter().map(|s| (ContainerKind::Outputs, None, s)))
            .filter_map(|(k, i, s)| Some((k, i, s.state.as_ref()?.terminated.as_ref()?)))
        {
            // Get the container's output
            let mut output = Retry::spawn_notify(
                retry_durations(),
                || async {
                    match pods
                        .logs(
                            &name,
                            &LogParams {
                                container: Some(format_container_name(kind, executor_index)),
                                tail_lines: match kind {
                                    ContainerKind::Inputs | ContainerKind::Outputs => {
                                        // For an inputs and outputs pod, read all the log
                                        None
                                    }
                                    ContainerKind::Executor => Some(MAX_EXECUTOR_LOG_LINES),
                                },
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(output) => Ok(output),
                        Err(kube::Error::Api(s)) if s.is_not_found() || s.code == 400 => {
                            // The pod or container no longer exists; treat as empty output
                            Ok(String::new())
                        }
                        Err(e) => Err(into_retry_error(e)),
                    }
                },
                notify_retry,
            )
            .await?;

            // For executors, extract the real error code which is printed at the end of the
            // output
            let exit_code = if kind == ContainerKind::Executor
                && let Some(pos) = output.rfind(EXIT_PREFIX)
            {
                let exit = output.split_off(pos);
                exit[EXIT_PREFIX.len()..]
                    .trim()
                    .parse()
                    .unwrap_or(state.exit_code)
            } else {
                state.exit_code
            };

            // TODO: once k8s supports split logs, read both stdout and stderr streams
            // Until then, use `stdout` if the pod succeeded and `stderr` if it failed
            let (stdout, stderr) = if exit_code == 0 {
                (Some(output.into()), None)
            } else {
                (None, Some(output.into()))
            };

            containers.push(TerminatedContainer {
                kind,
                executor_index: executor_index.map(|i| i as i32),
                start_time: DateTime::from_timestamp_micros(
                    state
                        .started_at
                        .as_ref()
                        .map(|t| t.0)
                        .unwrap_or(now)
                        .as_microsecond(),
                )
                .context("timestamp out of range")?,
                end_time: DateTime::from_timestamp_micros(
                    state
                        .finished_at
                        .as_ref()
                        .map(|t| t.0)
                        .unwrap_or(now)
                        .as_microsecond(),
                )
                .context("timestamp out of range")?,
                stdout,
                stderr,
                exit_code,
            });
        }

        Ok(containers)
    }

    /// Logs an error with the database.
    ///
    /// The error is also emitted to stderr.
    async fn log_error(&self, tes_id: Option<&str>, message: &str) {
        error!("{message}");
        let _ = self.database.insert_error(&self.id, tes_id, message).await;
    }
}

/// Implements a monitor for Kubernetes pod events.
pub struct Monitor {
    /// The cancellation token for shutting down the monitor.
    shutdown: CancellationToken,
    /// The handle to the events monitoring task.
    handle: JoinHandle<()>,
}

impl Monitor {
    /// Spawns the monitor with the given orchestrator.
    pub fn spawn(state: Arc<crate::State>) -> Self {
        let shutdown = CancellationToken::new();
        let handle = tokio::spawn(Self::monitor_events(state.clone(), shutdown.clone()));
        Self { shutdown, handle }
    }

    /// Shuts down the monitor.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.handle.await.expect("failed to join task");
    }

    /// Monitors Kubernetes task pod events.
    async fn monitor_events(state: Arc<crate::State>, shutdown: CancellationToken) {
        info!("cluster event processing has started");

        let stream = watcher(
            Api::<Pod>::namespaced(
                state.orchestrator.client.clone(),
                &state.orchestrator.tasks_namespace,
            ),
            watcher::Config {
                label_selector: Some(format!(
                    "{ORCHESTRATOR_LABEL}={id}",
                    id = state.orchestrator.id
                )),
                ..Default::default()
            },
        )
        .default_backoff();

        pin!(stream);

        loop {
            select! {
                biased;

                _ = shutdown.cancelled() => break,
                event = stream.next() => {
                    match event {
                        Some(Ok(Event::Apply(pod))) => {
                            let state = state.clone();
                            tokio::spawn(async move {
                                let Ok(tes_id) = pod.tes_id() else { return };

                                if let Err(e) = state.orchestrator.update_task(tes_id, &pod).await {
                                    state.orchestrator
                                        .log_error(
                                            Some(tes_id),
                                            &format!("error while updating task `{tes_id}`: {e:#}"),
                                        )
                                        .await;

                                    let _ = state.orchestrator
                                        .database
                                        .update_task_state(
                                            tes_id,
                                            State::SystemError,
                                            &[&format_log_message!(
                                                "{msg}",
                                                msg = e.as_system_log_message()
                                            )],
                                            None,
                                            None,
                                        )
                                        .await;
                                }
                            });
                        }
                        Some(Ok(Event::Delete(pod))) => {
                            // Treat a deleted pod as preempted
                            // If the associated task is already in a terminal state, this is a no-op
                            let state = state.clone();
                            tokio::spawn(async move {
                                let Ok(tes_id) = pod.tes_id() else { return };

                                if let Err(e) = state.orchestrator.handle_pod_deleted(tes_id, &pod).await {
                                    state.orchestrator
                                        .log_error(
                                            Some(tes_id),
                                            &format!("error while handling pod deletion for task `{tes_id}`: {e:#}"),
                                        )
                                        .await;

                                    let _ = state.orchestrator
                                        .database
                                        .update_task_state(
                                            tes_id,
                                            State::SystemError,
                                            &[&format_log_message!(
                                                "{msg}",
                                                msg = e.as_system_log_message()
                                            )],
                                            None,
                                            None,
                                        )
                                        .await;
                                }
                            });
                        }
                        Some(Ok(Event::Init | Event::InitDone | Event::InitApply(_))) => continue,
                        Some(Err(watcher::Error::WatchError(e))) if e.code == StatusCode::GONE => {
                            // This response happens when the initial resource version
                            // is too old. When this happens, the watcher will get a new
                            // resource version, so don't bother logging the error
                        }
                        Some(Err(e)) => {
                            state.orchestrator.log_error(None, &format!("error while streaming Kubernetes pod events: {e:#}")).await;
                        }
                        None => break,
                    }
                }
            }
        }

        info!("cluster event processing has shut down");
    }
}
