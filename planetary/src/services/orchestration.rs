//! Implementation of the task orchestration service.

use std::collections::BTreeMap;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use axum::http::StatusCode;
use futures::StreamExt;
use k8s_openapi::api::core::v1::Container;
use k8s_openapi::api::core::v1::ContainerState;
use k8s_openapi::api::core::v1::ContainerStateWaiting;
use k8s_openapi::api::core::v1::EnvVar;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::core::v1::PersistentVolumeClaimSpec;
use k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::api::core::v1::Volume;
use k8s_openapi::api::core::v1::VolumeMount;
use k8s_openapi::api::core::v1::VolumeResourceRequirements;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::Api;
use kube::Client;
use kube::ResourceExt;
use kube::api::DeleteParams;
use kube::api::LogParams;
use kube::api::ObjectMeta;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::api::PostParams;
use kube::core::ErrorResponse;
use kube::runtime::WatchStreamExt;
use kube::runtime::reflector::Lookup;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use planetary_db::Alphanumeric;
use planetary_db::Database;
use planetary_db::FinishedPod;
use planetary_db::PodKind;
use planetary_db::PodState;
use planetary_db::TaskIo;
use secrecy::ExposeSecret;
use tes::v1::types::task::Executor;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Resources;
use tes::v1::types::task::State;
use tokio::pin;
use tokio::select;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::UnboundedReceiverStream;
use tokio_util::sync::CancellationToken;
use tracing::debug;
use tracing::error;
use tracing::info;

use crate::format_log_message;

/// The default storage size, in gigabytes.
const DEFAULT_STORAGE_SIZE: f64 = 1.0;

/// The name of the volume that pods will attach to for their storage.
const STORAGE_VOLUME_NAME: &str = "storage";

/// The Kubernetes resources CPU key.
const K8S_KEY_CPU: &str = "cpu";

/// The Kubernetes resources memory key.
const K8S_KEY_MEMORY: &str = "memory";

/// The Kubernetes resources storage key.
const K8S_KEY_STORAGE: &str = "storage";

/// The default transporter image to use for inputs and outputs pods.
const DEFAULT_TRANSPORTER_IMAGE: &str = "stjude-rust-labs/planetary-transporter:latest";

/// The K8S namespace for planetary.
const PLANETARY_K8S_NAMESPACE: &str = "planetary";

/// The name of the orchestrator label.
const PLANETARY_ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// The name of the pod kind label.
const PLANETARY_POD_KIND_LABEL: &str = "planetary/pod-kind";

/// The name of the TES task id annotation.
const PLANETARY_TES_ID_ANNOTATION: &str = "planetary/task-id";

/// The name of the executor index annotation.
///
/// This annotation is only present for executor pods.
const PLANETARY_EXECUTOR_INDEX_ANNOTATION: &str = "planetary/executor-index";

/// The name of the executor ignore error annotation.
///
/// This annotation is only present for executor pods.
const PLANETARY_EXECUTOR_IGNORE_ERROR_ANNOTATION: &str = "planetary/executor-ignore-error";

/// The name of the "after inputs" annotation.
///
/// This annotation is only present for the first executor pod when it is
/// created immediately after the completion of the inputs pod.
const PLANETARY_AFTER_INPUTS_ANNOTATION: &str = "planetary/after-inputs";

/// This annotation is only present for pods that have been deleted by the
/// orchestrator.
const PLANETARY_DELETED_ANNOTATION: &str = "planetary/deleted";

/// The maximum number of lines to tail for an executor pod's logs.
const MAX_EXECUTOR_LOG_LINES: i64 = 15;

/// Formats a pod name given the TES task id, pod kind, and executor index.
///
/// The executor index must be specified when the pod is an executor.
fn format_pod_name(tes_id: &str, kind: PodKind, executor_index: Option<usize>) -> String {
    if kind == PodKind::Executor {
        return format!(
            "task-{tes_id}-{kind}-{index}",
            index = executor_index.expect("missing executor index for executor pod")
        );
    }

    assert!(
        executor_index.is_none(),
        "non-executor pod was given an executor index"
    );
    format!("task-{tes_id}-{kind}")
}

/// Formats the name of the storage PVC.
fn format_pvc_name(tes_id: &str) -> String {
    format!("task-{tes_id}")
}

/// Converts TES resources into K8S resource requirements.
fn convert_resources(resources: Resources) -> ResourceRequirements {
    let mut requests = BTreeMap::new();
    if let Some(cores) = resources.cpu_cores {
        requests.insert(K8S_KEY_CPU.to_owned(), Quantity(cores.to_string()));
    }

    if let Some(memory) = resources.ram_gb {
        requests.insert(
            K8S_KEY_MEMORY.to_owned(),
            Quantity(format!("{memory}G", memory = memory.ceil() as u64)),
        );
    }

    ResourceRequirements {
        requests: Some(requests),
        ..Default::default()
    }
}

/// An extension trait for Kubernetes pods.
trait PodExt {
    /// Gets the TES task id of the pod.
    ///
    /// Returns an error if the annotation is missing and not in the expected
    /// format.
    fn tes_id(&self) -> Result<&str>;

    /// Gets the kind of the pod.
    ///
    /// Returns an error if the label is missing and not in the expected format.
    fn kind(&self) -> Result<PodKind>;

    /// Determines the state of the pod.
    ///
    /// Returns an error if the pod is missing its phase or the phase is
    /// unknown.
    fn state(&self) -> Result<PodState>;

    /// Gets the executor index of the pod.
    ///
    /// Returns `Ok(Some(_))` when the annotation is present and in the expected
    /// format.
    ///
    /// Returns `Ok(None)` when the annotation is not present.
    ///
    /// Returns an error if the annotation is present and not in the expected
    /// format.
    fn executor_index(&self) -> Result<Option<usize>>;

    /// Determines if the pod is an executor marked for ignoring errors.
    ///
    /// Returns `Ok(Some(_))` when the annotation is present and in the expected
    /// format.
    ///
    /// Returns `Ok(None)` when the annotation is not present.
    ///
    /// Returns an error if the annotation is present and not in the expected
    /// format.
    fn executor_ignore_error(&self) -> Result<Option<bool>>;

    /// Gets the first container state of the pod.
    ///
    /// Returns `None` if there is no container state.
    fn first_container_state(&self) -> Option<&ContainerState>;

    /// Determine if the pod was scheduled immediately after the inputs pod.
    ///
    /// This is present only for the first executor pod if the pod was scheduled
    /// after the inputs pod.
    fn is_after_inputs(&self) -> Result<Option<bool>>;
}

impl PodExt for Pod {
    fn tes_id(&self) -> Result<&str> {
        self.annotations()
            .get(PLANETARY_TES_ID_ANNOTATION)
            .map(String::as_str)
            .with_context(|| format!("missing pod annotation `{PLANETARY_TES_ID_ANNOTATION}`"))
    }

    fn kind(&self) -> Result<PodKind> {
        self.labels()
            .get(PLANETARY_POD_KIND_LABEL)
            .with_context(|| format!("missing pod label `{PLANETARY_POD_KIND_LABEL}`"))?
            .parse()
            .with_context(|| format!("invalid value for label `{PLANETARY_POD_KIND_LABEL}`"))
    }

    fn state(&self) -> Result<PodState> {
        let status = self.status.as_ref().context("pod has no status")?;
        let phase = status.phase.as_ref().context("pod has no phase")?;

        Ok(match phase.as_str() {
            "Pending"
                if status
                    .container_statuses
                    .as_deref()
                    .unwrap_or_default()
                    .is_empty() =>
            {
                PodState::Waiting
            }
            "Pending" => PodState::Initializing,
            "Running" => PodState::Running,
            "Succeeded" => PodState::Succeeded,
            "Failed" => PodState::Failed,
            "Unknown" => PodState::Unknown,
            _ => bail!("unknown pod phase `{phase}`"),
        })
    }

    fn executor_index(&self) -> Result<Option<usize>> {
        self.annotations()
            .get(PLANETARY_EXECUTOR_INDEX_ANNOTATION)
            .map(|v| v.parse())
            .transpose()
            .with_context(|| {
                format!("invalid value for annotation `{PLANETARY_EXECUTOR_INDEX_ANNOTATION}`")
            })
    }

    fn executor_ignore_error(&self) -> Result<Option<bool>> {
        self.annotations()
            .get(PLANETARY_EXECUTOR_IGNORE_ERROR_ANNOTATION)
            .map(|v| v.parse())
            .transpose()
            .with_context(|| {
                format!(
                    "invalid value for annotation `{PLANETARY_EXECUTOR_IGNORE_ERROR_ANNOTATION}`"
                )
            })
    }

    fn first_container_state(&self) -> Option<&ContainerState> {
        self.status
            .as_ref()
            .and_then(|s| s.container_statuses.as_ref())
            .and_then(|v| v.first())
            .and_then(|s| s.state.as_ref())
    }

    fn is_after_inputs(&self) -> Result<Option<bool>> {
        self.annotations()
            .get(PLANETARY_AFTER_INPUTS_ANNOTATION)
            .map(|v| v.parse())
            .transpose()
            .with_context(|| {
                format!("invalid value for annotation `{PLANETARY_AFTER_INPUTS_ANNOTATION}`")
            })
    }
}

/// Represents an orchestration error.
#[derive(Debug, thiserror::Error)]
enum Error {
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

/// The result type of the orchestrator methods.
type OrchestrationResult<T> = Result<T, Error>;

/// Implements TES task orchestration.
#[derive(Clone)]
struct TaskOrchestrator {
    /// The unique id of this orchestrator.
    ///
    /// This is used to label the pods it monitors.
    id: String,
    /// The planetary database used by the orchestrator.
    database: Arc<dyn Database>,
    /// The K8S pods API.
    pods: Arc<Api<Pod>>,
    /// The K8S persistent volume claim API.
    pvc: Arc<Api<PersistentVolumeClaim>>,
    /// The storage class name to use for persistent volume claims.
    storage_class: Option<String>,
    /// The transporter image name to use.
    transporter_image: Option<String>,
}

impl TaskOrchestrator {
    /// Constructs a new task orchestrator.
    fn new(
        database: Arc<dyn Database>,
        client: Client,
        storage_class: Option<String>,
        transporter_image: Option<String>,
    ) -> Self {
        Self {
            id: format!("orchestrator-{random}", random = Alphanumeric::new(20)),
            database,
            pods: Arc::new(Api::namespaced(client.clone(), PLANETARY_K8S_NAMESPACE)),
            pvc: Arc::new(Api::namespaced(client.clone(), PLANETARY_K8S_NAMESPACE)),
            storage_class,
            transporter_image,
        }
    }

    /// Starts a task.
    ///
    /// To start a task, the orchestrator will:
    ///
    /// 1) Create a persistent volume claim for the task's storage; storage is
    ///    mounted in every pod created for the task.
    /// 2) Schedule an inputs pod if necessary (if the task has inputs or file
    ///    outputs).
    /// 3) Otherwise, schedule the first executor.
    async fn start_task(&self, tes_id: &str) -> OrchestrationResult<()> {
        debug!("task `{tes_id}` is starting");

        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!("task `{tes_id}` is starting")],
            )
            .await?;

        // Create the storage PVC
        let task_io = self.create_storage_pvc(tes_id).await?;

        // If there are inputs, start the inputs pod
        // The inputs pod is also required if there are any outputs that are files; the
        // inputs pod will create the files so that Kubernetes will mount them as files.
        if !task_io.inputs.is_empty() || task_io.outputs.iter().any(|o| o.ty == IoType::File) {
            return self.start_inputs_pod(tes_id).await;
        }

        // Otherwise, start the first executor of the task
        if !self.start_executor(tes_id, 0, &task_io, false).await? {
            return Err(Error::System(format!("task `{tes_id}` has no executors")));
        }

        Ok(())
    }

    /// Cancels the given task.
    async fn cancel_task(&self, tes_id: &str) -> OrchestrationResult<()> {
        debug!("task `{tes_id}` is canceling");

        // Find the executing pod for the task and delete it; wait for it to be deleted
        if let Some(name) = self.database.find_executing_pod(tes_id).await? {
            match self.pods.delete(&name, &DeleteParams::default()).await {
                Ok(_) | Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
                Err(e) => {
                    error!("failed to delete pod `{name}`: {e}");
                }
            }
        }

        self.cleanup_pvc(tes_id);

        if self
            .database
            .update_task_state(
                tes_id,
                State::Canceled,
                &[&format_log_message!("task `{tes_id}` has been canceled")],
            )
            .await?
        {
            debug!("task `{tes_id}` has been canceled");
        }

        Ok(())
    }

    /// Creates a persistent volume claim for a task's storage.
    async fn create_storage_pvc(&self, tes_id: &str) -> OrchestrationResult<TaskIo> {
        debug!("initializing storage for task `{tes_id}`");

        let task_io = self.database.get_task_io(tes_id).await?;
        self.pvc
            .create(
                &PostParams::default(),
                &PersistentVolumeClaim {
                    metadata: ObjectMeta {
                        name: Some(format_pvc_name(tes_id)),
                        namespace: Some(PLANETARY_K8S_NAMESPACE.into()),
                        ..Default::default()
                    },
                    spec: Some(PersistentVolumeClaimSpec {
                        access_modes: Some(vec!["ReadWriteOnce".into()]),
                        resources: Some(VolumeResourceRequirements {
                            limits: None,
                            requests: Some(BTreeMap::from_iter([(
                                K8S_KEY_STORAGE.to_string(),
                                Quantity(format!(
                                    "{disk}G",
                                    disk = task_io.size_gb.unwrap_or(DEFAULT_STORAGE_SIZE)
                                )),
                            )])),
                        }),
                        storage_class_name: self.storage_class.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            )
            .await?;

        Ok(task_io)
    }

    /// Starts the inputs pod of the task.
    async fn start_inputs_pod(&self, tes_id: &str) -> OrchestrationResult<()> {
        let name = format_pod_name(tes_id, PodKind::Inputs, None);

        // Insert the pod into the database
        if !self
            .database
            .insert_pod(tes_id, &name, PodKind::Inputs, None)
            .await?
        {
            return Ok(());
        }

        // Create the inputs pod
        self.create_inputs_pod(tes_id, &name).await
    }

    /// Starts the executor for the given task given the executor index.
    ///
    /// Returns `true` if the executor was started or `false` if the task
    /// doesn't have an executor with the given index.
    async fn start_executor(
        &self,
        tes_id: &str,
        executor_index: usize,
        task_io: &TaskIo,
        is_after_inputs: bool,
    ) -> OrchestrationResult<bool> {
        // Get the executor to start
        match self
            .database
            .get_task_executor(tes_id, executor_index)
            .await?
        {
            Some((executor, resources)) => {
                let name = format_pod_name(tes_id, PodKind::Executor, Some(executor_index));

                // Insert the pod into the database
                if !self
                    .database
                    .insert_pod(tes_id, &name, PodKind::Executor, Some(executor_index))
                    .await?
                {
                    // Task was canceled
                    return Ok(true);
                }

                // Create the executor pod
                self.create_executor_pod(
                    tes_id,
                    &name,
                    executor_index,
                    executor,
                    resources,
                    task_io,
                    is_after_inputs,
                )
                .await?;

                Ok(true)
            }
            None => Ok(false),
        }
    }

    /// Starts the outputs pod of the task.
    async fn start_outputs_pod(&self, tes_id: &str) -> OrchestrationResult<()> {
        let name = format_pod_name(tes_id, PodKind::Outputs, None);

        // Insert the pod into the database
        if !self
            .database
            .insert_pod(tes_id, &name, PodKind::Outputs, None)
            .await?
        {
            return Ok(());
        }

        // Create the outputs pod
        self.create_outputs_pod(tes_id, &name).await
    }

    /// Creates a pod for a downloading inputs.
    async fn create_inputs_pod(&self, tes_id: &str, name: &str) -> OrchestrationResult<()> {
        debug!("creating inputs pod `{name}` for task `{tes_id}`");

        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some(PLANETARY_K8S_NAMESPACE.into()),
                name: Some(name.to_string()),
                labels: Some(
                    [
                        (PLANETARY_ORCHESTRATOR_LABEL.to_string(), self.id.clone()),
                        (
                            PLANETARY_POD_KIND_LABEL.to_string(),
                            PodKind::Inputs.to_string(),
                        ),
                    ]
                    .into(),
                ),
                annotations: (Some(
                    [(PLANETARY_TES_ID_ANNOTATION.to_string(), tes_id.to_string())].into(),
                )),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "inputs".to_string(),
                    image: Some(
                        self.transporter_image
                            .as_deref()
                            .unwrap_or(DEFAULT_TRANSPORTER_IMAGE)
                            .into(),
                    ),
                    image_pull_policy: Some("Always".into()),
                    args: Some(vec![
                        "-v".into(),
                        "--database-url".into(),
                        self.database.url().expose_secret().to_string(),
                        "--mode".into(),
                        "inputs".into(),
                        "--inputs-dir".into(),
                        "/mnt/inputs".into(),
                        "--outputs-dir".into(),
                        "/mnt/outputs".into(),
                        tes_id.into(),
                    ]),
                    volume_mounts: Some(vec![
                        VolumeMount {
                            name: STORAGE_VOLUME_NAME.into(),
                            mount_path: "/mnt/inputs".into(),
                            sub_path: Some("inputs".into()),
                            ..Default::default()
                        },
                        VolumeMount {
                            name: STORAGE_VOLUME_NAME.into(),
                            mount_path: "/mnt/outputs".into(),
                            sub_path: Some("outputs".into()),
                            ..Default::default()
                        },
                    ]),
                    ..Default::default()
                }],
                restart_policy: Some("Never".to_string()),
                volumes: Some(vec![Volume {
                    name: STORAGE_VOLUME_NAME.into(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: format_pvc_name(tes_id),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        self.pods.create(&PostParams::default(), &pod).await?;
        Ok(())
    }

    /// Creates a pod for a task executor.
    #[allow(clippy::too_many_arguments)]
    async fn create_executor_pod(
        &self,
        tes_id: &str,
        name: &str,
        executor_index: usize,
        executor: Executor,
        resources: Resources,
        task_io: &TaskIo,
        is_after_inputs: bool,
    ) -> OrchestrationResult<()> {
        debug!("creating executor pod `{name}` for task `{tes_id}`");

        let stdin = executor
            .stdin
            .as_ref()
            .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stdin path `{p}`")))
            .transpose()?;

        let stdout = executor
            .stdout
            .as_ref()
            .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stdout path `{p}`")))
            .transpose()?;

        let stderr = executor
            .stderr
            .as_ref()
            .map(|p| shlex::try_quote(p).with_context(|| format!("invalid stderr path `{p}`")))
            .transpose()?;

        let command = shlex::try_join(executor.command.iter().map(AsRef::as_ref))
            .map_err(|_| Error::System("task command was invalid".into()))?;

        let script = Self::format_executor_script(
            stdin.as_deref(),
            stdout.as_deref(),
            stderr.as_deref(),
            &command,
        );

        let volume_mounts = task_io
            .inputs
            .iter()
            .enumerate()
            .map(|(i, input)| {
                Ok(VolumeMount {
                    mount_path: input.path.clone(),
                    name: STORAGE_VOLUME_NAME.into(),
                    read_only: Some(true),
                    recursive_read_only: Some("IfPossible".into()),
                    sub_path: Some(format!("inputs/{i}")),
                    ..Default::default()
                })
            })
            .chain(task_io.outputs.iter().enumerate().map(|(i, output)| {
                Ok(VolumeMount {
                    mount_path: output
                        .path_prefix
                        .as_deref()
                        .unwrap_or(&output.path)
                        .to_string(),
                    name: STORAGE_VOLUME_NAME.into(),
                    read_only: Some(false),
                    sub_path: Some(format!("outputs/{i}")),
                    ..Default::default()
                })
            }))
            .chain(task_io.volumes.iter().enumerate().map(|(i, volume)| {
                Ok(VolumeMount {
                    mount_path: volume.clone(),
                    name: STORAGE_VOLUME_NAME.into(),
                    read_only: Some(false),
                    sub_path: Some(format!("volumes/{i}")),
                    ..Default::default()
                })
            }))
            .collect::<Result<_>>()?;

        let mut annotations = BTreeMap::from([
            (PLANETARY_TES_ID_ANNOTATION.to_string(), tes_id.to_string()),
            (
                PLANETARY_EXECUTOR_INDEX_ANNOTATION.to_string(),
                executor_index.to_string(),
            ),
            (
                PLANETARY_EXECUTOR_IGNORE_ERROR_ANNOTATION.to_string(),
                executor.ignore_error.unwrap_or_default().to_string(),
            ),
        ]);

        if is_after_inputs {
            annotations.insert(PLANETARY_AFTER_INPUTS_ANNOTATION.into(), "true".into());
        }

        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some(PLANETARY_K8S_NAMESPACE.into()),
                name: Some(name.to_string()),
                labels: Some(
                    [
                        (PLANETARY_ORCHESTRATOR_LABEL.to_string(), self.id.clone()),
                        (
                            PLANETARY_POD_KIND_LABEL.to_string(),
                            PodKind::Executor.to_string(),
                        ),
                    ]
                    .into(),
                ),
                annotations: Some(annotations),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "executor-0".to_string(),
                    image: Some(executor.image),
                    args: Some(vec!["-c".to_string(), script]),
                    command: Some(vec!["/bin/sh".to_string()]),
                    env: executor.env.map(|e| {
                        e.into_iter()
                            .map(|(k, v)| EnvVar {
                                name: k,
                                value: Some(v),
                                ..Default::default()
                            })
                            .collect()
                    }),
                    resources: Some(convert_resources(resources)),
                    volume_mounts: Some(volume_mounts),
                    working_dir: executor.workdir,
                    ..Default::default()
                }],
                restart_policy: Some("Never".to_string()),
                volumes: Some(vec![Volume {
                    name: STORAGE_VOLUME_NAME.into(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: format_pvc_name(tes_id),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        self.pods.create(&PostParams::default(), &pod).await?;
        Ok(())
    }

    /// Formats an executor script into a single line that can be used with `sh
    /// -c`.
    ///
    /// It is expected that the arguments are already shell quoted.
    fn format_executor_script(
        stdin: Option<&str>,
        stdout: Option<&str>,
        stderr: Option<&str>,
        command: &str,
    ) -> String {
        let mut script = String::new();
        script.push_str("set -eu;");

        // Add check for stdin file existence
        if let Some(stdin) = stdin {
            script.push_str("! [ -f ");
            script.push_str(stdin);
            script.push_str(r#" ] && >&2 echo "executor stdin file "#);
            script.push_str(stdin);
            script.push_str(r#" does not exist" && exit 1;"#);
        }

        // Set up stdout redirection
        // We use tee so that both Kubernetes and the requested stdout file have the
        // output
        if let Some(stdout) = stdout {
            script.push_str(r#"out="${TMPDIR:-/tmp}/stdout";"#);
            script.push_str(r#"mkfifo "$out";"#);
            script.push_str("tee -a ");
            script.push_str(stdout);
            script.push_str(r#" < "$out" &"#);
        }

        // Set up stderr redirection
        // We use tee so that both Kubernetes and the requested stderr file have the
        // output
        if let Some(stderr) = stderr {
            script.push_str(r#"err="${TMPDIR:-/tmp}/stderr";"#);
            script.push_str(r#"mkfifo "$err";"#);
            script.push_str("tee -a ");
            script.push_str(stderr);
            script.push_str(r#" < "$err" &"#);
        }

        // Add the command
        script.push_str(command);

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
            script.push_str(stdin);
        }

        // We must wait for the background tee jobs to complete, otherwise buffers might
        // not be flushed
        script.push_str("; wait $(jobs -p)");
        script
    }

    /// Creates a pod for a uploading outputs.
    async fn create_outputs_pod(&self, tes_id: &str, name: &str) -> OrchestrationResult<()> {
        debug!("creating outputs pod `{name}` for task `{tes_id}`");

        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some(PLANETARY_K8S_NAMESPACE.into()),
                name: Some(name.to_string()),
                labels: Some(
                    [
                        (PLANETARY_ORCHESTRATOR_LABEL.to_string(), self.id.clone()),
                        (
                            PLANETARY_POD_KIND_LABEL.to_string(),
                            PodKind::Outputs.to_string(),
                        ),
                    ]
                    .into(),
                ),
                annotations: (Some(
                    [(PLANETARY_TES_ID_ANNOTATION.to_string(), tes_id.to_string())].into(),
                )),
                ..Default::default()
            },
            spec: Some(PodSpec {
                containers: vec![Container {
                    name: "outputs".to_string(),
                    image: Some(
                        self.transporter_image
                            .as_deref()
                            .unwrap_or(DEFAULT_TRANSPORTER_IMAGE)
                            .into(),
                    ),
                    image_pull_policy: Some("Always".into()),
                    args: Some(vec![
                        "-v".into(),
                        "--database-url".into(),
                        self.database.url().expose_secret().to_string(),
                        "--mode".into(),
                        "outputs".into(),
                        "--outputs-dir".into(),
                        "/mnt/outputs".into(),
                        tes_id.into(),
                    ]),
                    volume_mounts: Some(vec![VolumeMount {
                        name: STORAGE_VOLUME_NAME.into(),
                        mount_path: "/mnt/outputs".into(),
                        sub_path: Some("outputs".into()),
                        ..Default::default()
                    }]),
                    ..Default::default()
                }],
                restart_policy: Some("Never".to_string()),
                volumes: Some(vec![Volume {
                    name: STORAGE_VOLUME_NAME.into(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: format_pvc_name(tes_id),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        self.pods.create(&PostParams::default(), &pod).await?;
        Ok(())
    }

    /// Updates a TES task based on the given pod status.
    async fn update_task(&self, tes_id: &str, pod: &Pod) -> OrchestrationResult<()> {
        let name = pod
            .metadata
            .name
            .as_deref()
            .context("Kubernetes watch contained a pod with no name")?;

        let kind = pod.kind()?;
        let pod_state = pod.state()?;

        debug!("pod `{name}` is in state `{pod_state}`");

        // Check for an initializing pod that is failing to pull its image.
        if pod_state == PodState::Initializing {
            if let Some(state) = pod.first_container_state().and_then(|s| s.waiting.as_ref()) {
                if state.reason.as_deref() == Some("ErrImagePull") {
                    return self.handle_image_pull_error(tes_id, name, state).await;
                }
            }
        }

        // Update the pod in the database
        let (updated, output) = self.update_pod(pod).await?;
        if !updated {
            return Ok(());
        }

        match (pod_state, kind) {
            // The inputs pod is waiting, the task is now queued
            (PodState::Waiting, PodKind::Inputs) => {
                self.handle_inputs_queued(tes_id).await?;
            }
            // The inputs pod is initializing, the task is now initializing
            (PodState::Initializing, PodKind::Inputs) => {
                self.handle_inputs_initializing(tes_id).await?;
            }
            // The inputs pod is running
            (PodState::Running, PodKind::Inputs) => {
                self.handle_inputs_running(tes_id).await?;
            }
            // The inputs pod has completed
            (PodState::Succeeded, PodKind::Inputs) => {
                self.handle_inputs_completed(tes_id).await?;
            }
            // An executor pod is waiting, the task is now queued
            (PodState::Waiting, PodKind::Executor) => {
                self.handle_executor_queued(tes_id, pod).await?;
            }
            // An executor pod is initializing, the task is now initializing
            (PodState::Initializing, PodKind::Executor) => {
                self.handle_executor_initializing(tes_id, pod).await?;
            }
            // An executor pod is now running, the task is now running
            (PodState::Running, PodKind::Executor) => {
                self.handle_executor_running(tes_id, pod).await?;
            }
            // An executor pod has succeeded
            (PodState::Succeeded, PodKind::Executor) => {
                self.handle_executor_completed(tes_id, pod).await?;
            }
            // An executor has failed
            (PodState::Failed, PodKind::Executor) => {
                self.handle_executor_failed(
                    tes_id,
                    pod,
                    output.as_deref().expect("should have output"),
                )
                .await?;
            }
            // The outputs pod is waiting
            (PodState::Waiting, PodKind::Outputs) => {
                self.handle_outputs_queued(tes_id).await?;
            }
            // The outputs pod is initializing
            (PodState::Initializing, PodKind::Outputs) => {
                self.handle_outputs_initializing(tes_id).await?;
            }
            // The outputs pod is running
            (PodState::Running, PodKind::Outputs) => {
                self.handle_outputs_running(tes_id).await?;
            }
            // The outputs pod has completed, the task is now completed
            (PodState::Succeeded, PodKind::Outputs) => {
                self.handle_outputs_completed(tes_id).await?;
            }
            // The inputs or outputs pod has failed
            (PodState::Failed, PodKind::Inputs | PodKind::Outputs) => {
                self.handle_io_failed(tes_id, kind, output.as_deref().expect("should have output"))
                    .await?;
            }
            // A pod is in an unknown state, the task encountered a system error
            (PodState::Unknown, _) => {
                self.handle_unknown_pod(tes_id, name).await?;
            }
            (PodState::ImagePullError, _) => unreachable!("a pod should not be in this state"),
        }

        Ok(())
    }

    /// Handles a queued inputs pod.
    async fn handle_inputs_queued(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Queued,
                &[&format_log_message!(
                    "downloading of inputs for task `{tes_id}` is now queued"
                )],
            )
            .await?
        {
            debug!("task `{tes_id}` is now queued");
        }

        Ok(())
    }

    /// Handles an initializing inputs pod.
    async fn handle_inputs_initializing(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Initializing,
                &[&format_log_message!(
                    "downloading of inputs for task `{tes_id}` is now initializing"
                )],
            )
            .await?
        {
            debug!("task `{tes_id}` is now initializing");
        }

        Ok(())
    }

    /// Handles a running inputs pod.
    async fn handle_inputs_running(&self, tes_id: &str) -> Result<(), Error> {
        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "downloading of inputs for task `{tes_id}` is now running"
                )],
            )
            .await?;

        Ok(())
    }

    /// Handles a completed inputs pod.
    async fn handle_inputs_completed(&self, tes_id: &str) -> Result<(), Error> {
        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "downloading of inputs for task `{tes_id}` has completed"
                )],
            )
            .await?;

        let task_io = self.database.get_task_io(tes_id).await?;
        if !self.start_executor(tes_id, 0, &task_io, true).await? {
            return Err(Error::System(format!("task `{tes_id}` has no executors")));
        }

        Ok(())
    }

    /// Handles a queued executor pod.
    async fn handle_executor_queued(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let executor_index = pod
            .executor_index()?
            .context("missing executor index annotation")?;

        if executor_index == 0 && !pod.is_after_inputs()?.unwrap_or(false) {
            if self
                .database
                .update_task_state(
                    tes_id,
                    State::Queued,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now queued"
                    )],
                )
                .await?
            {
                debug!("task `{tes_id}` is now queued");
            }
        } else {
            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now queued"
                    )],
                )
                .await?;
        }

        Ok(())
    }

    /// Handles an initializing executor pod.
    async fn handle_executor_initializing(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let executor_index = pod
            .executor_index()?
            .context("missing executor index annotation")?;

        if executor_index == 0 && !pod.is_after_inputs()?.unwrap_or(false) {
            if self
                .database
                .update_task_state(
                    tes_id,
                    State::Initializing,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now initializing"
                    )],
                )
                .await?
            {
                debug!("task `{tes_id}` is now initializing");
            }
        } else {
            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now initializing"
                    )],
                )
                .await?;
        }

        Ok(())
    }

    /// Handles a running executor pod.
    async fn handle_executor_running(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let executor_index = pod
            .executor_index()?
            .context("missing executor index annotation")?;

        if executor_index == 0 {
            if self
                .database
                .update_task_state(
                    tes_id,
                    State::Running,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now running"
                    )],
                )
                .await?
            {
                debug!("task `{tes_id}` is now running");
            }
        } else {
            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now running"
                    )],
                )
                .await?;
        }

        Ok(())
    }

    /// Handles a completed executor pod.
    async fn handle_executor_completed(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        let executor_index = pod
            .executor_index()?
            .context("missing executor index annotation")?;

        // If the task ran too quickly, we might not have seen a running event
        // Therefore, transition the state to running before completing it
        if executor_index == 0
            && self
                .database
                .update_task_state(
                    tes_id,
                    State::Running,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` is now running"
                    )],
                )
                .await?
        {
            debug!("task `{tes_id}` is now running");
        }

        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "executor {executor_index} of task `{tes_id}` has completed"
                )],
            )
            .await?;

        let task_io = self.database.get_task_io(tes_id).await?;
        if !self
            .start_executor(tes_id, executor_index + 1, &task_io, false)
            .await?
        {
            if !task_io.outputs.is_empty() {
                // Start the outputs pod
                self.start_outputs_pod(tes_id).await?;
            } else if self
                .database
                .update_task_state(
                    tes_id,
                    State::Complete,
                    &[&format_log_message!("task `{tes_id}` has completed")],
                )
                .await?
            {
                debug!("task `{tes_id}` has completed");
                self.cleanup_pvc(tes_id);
            }
        }

        Ok(())
    }

    /// Handles a failed executor pod.
    async fn handle_executor_failed(
        &self,
        tes_id: &str,
        pod: &Pod,
        output: &str,
    ) -> Result<(), Error> {
        let executor_index = pod
            .executor_index()?
            .context("missing executor index annotation")?;

        if pod.executor_ignore_error()? == Some(true) {
            // If the task ran too quickly, we might not have seen a running event
            // Therefore, transition the state to running before completing it
            if executor_index == 0
                && self
                    .database
                    .update_task_state(
                        tes_id,
                        State::Running,
                        &[&format_log_message!(
                            "executor {executor_index} of task `{tes_id}` is now running"
                        )],
                    )
                    .await?
            {
                debug!("task `{tes_id}` is now running");
            }

            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!(
                        "executor {executor_index} of task `{tes_id}` has failed (ignored error)"
                    )],
                )
                .await?;

            // Start the next executor, if there is one; if not, schedule an outputs pod or
            // mark the task as completed
            let task_io = self.database.get_task_io(tes_id).await?;
            if !self
                .start_executor(tes_id, executor_index + 1, &task_io, false)
                .await?
            {
                if !task_io.outputs.is_empty() {
                    // Schedule the outputs pod
                    self.start_outputs_pod(tes_id).await?;
                } else if self
                    .database
                    .update_task_state(
                        tes_id,
                        State::Complete,
                        &[&format_log_message!("task `{tes_id}` has completed")],
                    )
                    .await?
                {
                    debug!("task `{tes_id}` has completed");
                    self.cleanup_pvc(tes_id);
                }
            }
        } else if self
            .database
            .update_task_state(
                tes_id,
                State::ExecutorError,
                &[&format_log_message!(
                    "executor {executor_index} of task `{tes_id}` has failed:\n{output}"
                )],
            )
            .await?
        {
            debug!("executor {executor_index} of task `{tes_id}` has failed");
            self.cleanup_pvc(tes_id);
        }

        Ok(())
    }

    /// Handles a queued outputs pod.
    async fn handle_outputs_queued(&self, tes_id: &str) -> Result<(), Error> {
        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "uploading of outputs for task `{tes_id}` is now queued"
                )],
            )
            .await?;

        Ok(())
    }

    /// Handles an initializing outputs pod.
    async fn handle_outputs_initializing(&self, tes_id: &str) -> Result<(), Error> {
        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "uploading of outputs for task `{tes_id}` is now initializing"
                )],
            )
            .await?;

        Ok(())
    }

    /// Handles a running outputs pod.
    async fn handle_outputs_running(&self, tes_id: &str) -> Result<(), Error> {
        self.database
            .append_system_log(
                tes_id,
                &[&format_log_message!(
                    "uploading of outputs for task `{tes_id}` is now running"
                )],
            )
            .await?;

        Ok(())
    }

    /// Handles a completed outputs pod.
    async fn handle_outputs_completed(&self, tes_id: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Complete,
                &[
                    &format_log_message!("uploading of outputs for task `{tes_id}` has completed"),
                    &format_log_message!("task `{tes_id}` has completed"),
                ],
            )
            .await?
        {
            debug!("task `{tes_id}` has completed");
            self.cleanup_pvc(tes_id);
        }

        Ok(())
    }

    /// Handles a failed inputs or outputs pod.
    async fn handle_io_failed(
        &self,
        tes_id: &str,
        kind: PodKind,
        output: &str,
    ) -> Result<(), Error> {
        let action = if kind == PodKind::Inputs {
            "download inputs"
        } else {
            "upload outputs"
        };

        if self
            .database
            .update_task_state(
                tes_id,
                State::SystemError,
                &[&format_log_message!(
                    "failed to {action} of task `{tes_id}`:\n{output}"
                )],
            )
            .await?
        {
            debug!("task `{tes_id}` failed to {action}");
            self.cleanup_pvc(tes_id);
        }

        Ok(())
    }

    /// Handles a pod in an unknown state.
    async fn handle_unknown_pod(&self, tes_id: &str, pod_name: &str) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::SystemError,
                &[&format_log_message!(
                    "communication was lost with a node running task `{tes_id}`: contact the \
                     system administrator for details"
                )],
            )
            .await?
        {
            error!("task `{tes_id}` has failed: communication was lost with pod `{pod_name}`");
            self.cleanup_pod(pod_name.to_string());
            self.cleanup_pvc(tes_id);
        }

        Ok(())
    }

    /// Handles an image pull error by failing the task with a system error.
    async fn handle_image_pull_error(
        &self,
        tes_id: &str,
        name: &str,
        state: &ContainerStateWaiting,
    ) -> Result<(), Error> {
        let message = state
            .message
            .as_deref()
            .context("pod is missing container waiting status message")?;

        if self
            .database
            .update_pod_state(name, PodState::ImagePullError, None)
            .await?
            && self
                .database
                .update_task_state(
                    tes_id,
                    State::SystemError,
                    &[&format_log_message!("failed to pull image: {message}")],
                )
                .await?
        {
            debug!("task `{tes_id}` failed to pull image: {message}");
            self.cleanup_pod(name.to_string());
            self.cleanup_pvc(tes_id);
        }

        Ok(())
    }

    /// Updates the pod in the database.
    ///
    /// Returns a whether or not the pod was updated and the output of the pod
    /// if it terminated.
    async fn update_pod(&self, pod: &Pod) -> OrchestrationResult<(bool, Option<String>)> {
        let name = pod.name().context("pod is missing a name")?;
        let state = pod.state()?;

        let mut output = None;
        let finished = match state {
            PodState::Succeeded | PodState::Failed => {
                let container_state = pod
                    .first_container_state()
                    .context("pod is missing container state")?
                    .terminated
                    .as_ref()
                    .context("succeeded pod is missing terminated state")?;

                let start_time = container_state.started_at.as_ref().map(|t| t.0);
                let end_time = container_state.finished_at.as_ref().map(|t| t.0);

                // Read the pod output; it may have been deleted
                output = Some(
                    match self
                        .pods
                        .logs(
                            &name,
                            &LogParams {
                                tail_lines: match pod.kind()? {
                                    PodKind::Inputs | PodKind::Outputs => {
                                        // For an inputs and outputs pod, read all the log
                                        None
                                    }
                                    PodKind::Executor => Some(MAX_EXECUTOR_LOG_LINES),
                                },
                                ..Default::default()
                            },
                        )
                        .await
                    {
                        Ok(output) => output,
                        Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {
                            // The pod no longer exists
                            return Ok((false, None));
                        }
                        Err(kube::Error::Api(ErrorResponse { code: 400, .. })) => {
                            // The pod exists but logs can't be retrieved, likely because the
                            // container was terminated. Treat it as empty output
                            String::new()
                        }
                        Err(e) => return Err(e.into()),
                    },
                );

                // TODO: once k8s supports split logs, read both stdout and stderr streams
                // Until then, use `stdout` if the pod succeeded and `stderr` if it failed
                let (stdout, stderr) = if state == PodState::Succeeded {
                    (Some(output.as_deref().unwrap()), None)
                } else {
                    (None, Some(output.as_deref().unwrap()))
                };

                Some(FinishedPod {
                    exit_code: container_state.exit_code,
                    start_time,
                    end_time,
                    stdout,
                    stderr,
                })
            }
            _ => None,
        };

        // Update the pod state
        let updated = self
            .database
            .update_pod_state(&name, state, finished)
            .await?;

        // If the pod is finished, delete the pod
        if updated && finished.is_some() {
            self.cleanup_pod(name.into_owned());
        }

        Ok((updated, output))
    }

    /// Cleans up the given pod.
    ///
    /// This method does not wait for the pod to be deleted.
    fn cleanup_pod(&self, name: String) {
        let pods = self.pods.clone();

        tokio::spawn(async move {
            // Add an annotation to track the pod was deleted by the orchestrator
            if let Err(e) = pods
                .patch(
                    &name,
                    &PatchParams::default(),
                    &Patch::Merge(Pod {
                        metadata: ObjectMeta {
                            annotations: Some(BTreeMap::from_iter([(
                                PLANETARY_DELETED_ANNOTATION.to_string(),
                                "true".to_string(),
                            )])),
                            ..Default::default()
                        },
                        ..Default::default()
                    }),
                )
                .await
            {
                error!("failed to patch annotation for pod `{name}`: {e}");
            }

            match pods.delete(&name, &DeleteParams::default()).await {
                Ok(_) | Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
                Err(e) => error!("failed to delete pod `{name}`: {e}"),
            }
        });
    }

    /// Cleans up the given PVC.
    ///
    /// This method does not wait for the PVC to be deleted.
    fn cleanup_pvc(&self, tes_id: &str) {
        let name = format_pvc_name(tes_id);
        let pvc = self.pvc.clone();

        tokio::spawn(async move {
            match pvc.delete(&name, &DeleteParams::default()).await {
                Ok(_) | Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => {}
                Err(e) => error!("failed to delete PVC `{name}`: {e}"),
            }
        });
    }
}

/// Represents a request to the task orchestration service.
enum Request {
    /// The request is to start a task of the TES task id.
    StartTask(String),
    /// The request is to cancel a task of the given TES task id.
    CancelTask(String),
}

/// Represents a sender for sending requests to the task orchestration service.
pub struct TaskOrchestrationSender(mpsc::UnboundedSender<Request>);

impl TaskOrchestrationSender {
    /// Starts a new task.
    ///
    /// The orchestration service will spawn a new Tokio task for running the
    /// TES task.
    pub fn start_task(&self, tes_id: impl Into<String>) {
        self.0.send(Request::StartTask(tes_id.into())).ok();
    }

    /// Cancels a running task.
    pub fn cancel_task(&self, tes_id: impl Into<String>) {
        self.0.send(Request::CancelTask(tes_id.into())).ok();
    }
}

/// Represents the task orchestration service.
///
/// This service is used to run tasks as Kubernetes pods.
pub struct TaskOrchestrationService {
    /// The cancellation token for shutting down the service.
    shutdown: CancellationToken,
    /// The handle to the request queue task.
    queue_handle: JoinHandle<()>,
    /// The handle to the events task.
    events_handle: JoinHandle<()>,
}

impl TaskOrchestrationService {
    /// Constructs a new task orchestration service.
    ///
    /// This method will spawn Tokio tasks for receiving requests and handling
    /// events from Kubernetes.
    ///
    /// Returns the service along with a sender for sending requests to the
    /// service.
    pub fn spawn(
        database: Arc<dyn Database>,
        client: Client,
        storage_class: Option<String>,
        transporter_image: Option<String>,
    ) -> (Self, TaskOrchestrationSender) {
        let (tx, rx) = mpsc::unbounded_channel();

        // TODO: upon startup, the service should reconcile any tasks that haven't
        // completed yet with the current K8S cluster state; if it cannot do so for a
        // task, it should abort the task with a system error.

        let orchestrator = Arc::new(TaskOrchestrator::new(
            database,
            client.clone(),
            storage_class,
            transporter_image,
        ));

        let shutdown = CancellationToken::new();
        let queue_handle = tokio::spawn(Self::request_queue(
            rx,
            orchestrator.clone(),
            shutdown.clone(),
        ));

        let events_handle =
            tokio::spawn(Self::process_events(client, orchestrator, shutdown.clone()));

        (
            Self {
                shutdown,
                queue_handle,
                events_handle,
            },
            TaskOrchestrationSender(tx),
        )
    }

    /// Shuts down the service.
    pub async fn shutdown(self) {
        self.shutdown.cancel();
        self.queue_handle.await.expect("failed to join task");
        self.events_handle.await.expect("failed to join task");
    }

    /// Runs the request queue for the service.
    async fn request_queue(
        rx: mpsc::UnboundedReceiver<Request>,
        orchestrator: Arc<TaskOrchestrator>,
        shutdown: CancellationToken,
    ) {
        info!("task orchestration service has started");

        let mut stream = UnboundedReceiverStream::new(rx);

        loop {
            select! {
                biased;

                _ = shutdown.cancelled() => break,
                req = stream.next() => {
                    match req {
                        Some(Request::StartTask(tes_id)) => {
                            let orchestrator = orchestrator.clone();
                            tokio::spawn(async move {
                                if let Err(e) = orchestrator.start_task(&tes_id).await {
                                    error!("failed to start task `{tes_id}`: {e:#}");

                                    orchestrator
                                        .database
                                        .update_task_state(&tes_id, State::SystemError, &[&format_log_message!("{msg}", msg = e.as_system_log_message())])
                                        .await
                                        .ok();

                                    orchestrator.cleanup_pvc(&tes_id);
                                }
                            });
                        }
                        Some(Request::CancelTask(tes_id)) => {
                            let orchestrator = orchestrator.clone();

                            tokio::spawn(async move {
                                if let Err(e) = orchestrator.cancel_task(&tes_id).await {
                                    error!("failed to cancel task `{tes_id}`: {e:#}");

                                    orchestrator
                                        .database
                                        .update_task_state(&tes_id, State::SystemError, &[&format_log_message!("{msg}", msg = e.as_system_log_message())])
                                        .await
                                        .ok();

                                    orchestrator.cleanup_pvc(&tes_id);
                                }
                            });
                        },
                        None => break,
                    }
                }
            }
        }

        info!("task orchestration service has shut down");
    }

    /// Processes the Kubernetes task pod events.
    async fn process_events(
        client: Client,
        orchestrator: Arc<TaskOrchestrator>,
        shutdown: CancellationToken,
    ) {
        info!("event processing has started");

        let stream = watcher(
            Api::<Pod>::namespaced(client, PLANETARY_K8S_NAMESPACE),
            watcher::Config {
                label_selector: Some(format!(
                    "{PLANETARY_ORCHESTRATOR_LABEL}={id},{PLANETARY_POD_KIND_LABEL} in ({all})",
                    id = orchestrator.id,
                    all = PodKind::all()
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
                            let orchestrator = orchestrator.clone();
                            tokio::spawn(async move {
                                let tes_id = match pod.tes_id() {
                                    Ok(id) => id,
                                    Err(_) => {
                                        let name = pod.name();
                                        error!(
                                            "pod `{name}` is missing an associated TES task identifier",
                                            name = name.as_deref().unwrap_or("unknown")
                                        );
                                        return;
                                    }
                                };

                                if let Err(e) = orchestrator.update_task(tes_id, &pod).await {
                                    error!("error while updating task: {e}");

                                    orchestrator
                                        .database
                                        .update_task_state(tes_id, State::SystemError, &[&format_log_message!("{msg}", msg = e.as_system_log_message())])
                                        .await
                                        .ok();

                                    orchestrator.cleanup_pvc(tes_id);
                                }
                            });
                        }
                        Some(Ok(Event::Delete(pod))) => {
                            // If the orchestrator deleted the pod, ignore it
                            if pod
                                .annotations()
                                .get(PLANETARY_DELETED_ANNOTATION)
                                .is_some()
                            {
                                continue;
                            }

                            // Otherwise, treat this as a preemption
                            let orchestrator = orchestrator.clone();
                            tokio::spawn(async move {
                                let name = match pod.name() {
                                    Some(name) => name,
                                    None => return,
                                };

                                let tes_id = match pod.tes_id() {
                                    Ok(id) => id,
                                    Err(_) => {
                                        error!("pod `{name}` is missing an associated TES task identifier");
                                        return;
                                    }
                                };

                                if let Err(e) = orchestrator
                                    .database
                                    .update_pod_state(
                                        &name,
                                        PodState::Failed,
                                        Some(FinishedPod {
                                            exit_code: 130,
                                            stderr: Some("task was preempted"),
                                            stdout: None,
                                            start_time: None,
                                            end_time: None,
                                        }),
                                    )
                                    .await
                                {
                                    error!("failed to update task `{tes_id}`: {e}");
                                }

                                match orchestrator
                                    .database
                                    .update_task_state(
                                        tes_id,
                                        State::Preempted,
                                        &[&format_log_message!("task `{tes_id}` has been preempted")],
                                    )
                                    .await
                                {
                                    Ok(false) => {}
                                    Ok(true) => {
                                        debug!("task `{tes_id}` has been preempted");
                                    }
                                    Err(e) => error!("failed to update task `{tes_id}`: {e}"),
                                }

                                orchestrator.cleanup_pvc(tes_id);
                            });
                        }
                        Some(Ok(Event::Init | Event::InitDone | Event::InitApply(_))) => continue,
                        Some(Err(watcher::Error::WatchError(e))) if e.code == StatusCode::GONE => {
                            // This response happens when the initial resource version is too old
                            // When this happens, the watcher will get a new resource version, so don't bother logging the error
                        }
                        Some(Err(e)) => {
                            error!("error while streaming Kubernetes pod events: {e}");
                        }
                        None => break,
                    }
                }
            }
        }

        info!("event processing has shut down");
    }
}
