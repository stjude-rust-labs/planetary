//! Implementation of the task orchestrator.
//!
//! The task orchestrator is responsible for creating Kubernetes resources for
//! task execution.
//!
//! It watches for Kubernetes cluster events relating to the pods it is
//! orchestrating and updates the database accordingly.

use std::collections::BTreeMap;
use std::fmt;
use std::fmt::Write;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use axum::http::StatusCode;
use chrono::Utc;
use futures::FutureExt;
use k8s_openapi::api::core::v1::Container;
use k8s_openapi::api::core::v1::EnvFromSource;
use k8s_openapi::api::core::v1::EnvVar;
use k8s_openapi::api::core::v1::EnvVarSource;
use k8s_openapi::api::core::v1::PersistentVolumeClaim;
use k8s_openapi::api::core::v1::PersistentVolumeClaimSpec;
use k8s_openapi::api::core::v1::PersistentVolumeClaimVolumeSource;
use k8s_openapi::api::core::v1::Pod;
use k8s_openapi::api::core::v1::PodSpec;
use k8s_openapi::api::core::v1::ResourceRequirements;
use k8s_openapi::api::core::v1::SecretEnvSource;
use k8s_openapi::api::core::v1::SecretKeySelector;
use k8s_openapi::api::core::v1::Volume;
use k8s_openapi::api::core::v1::VolumeMount;
use k8s_openapi::api::core::v1::VolumeResourceRequirements;
use k8s_openapi::apimachinery::pkg::api::resource::Quantity;
use kube::Api;
use kube::Client;
use kube::ResourceExt;
use kube::api::LogParams;
use kube::api::ObjectMeta;
use kube::api::Patch;
use kube::api::PatchParams;
use kube::api::PostParams;
use kube::core::ErrorResponse;
use kube::runtime::WatchStreamExt;
use kube::runtime::watcher;
use kube::runtime::watcher::Event;
use planetary_db::ContainerKind;
use planetary_db::Database;
use planetary_db::TerminatedContainer;
use planetary_db::format_log_message;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::View;
use tes::v1::types::responses::Task;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Resources;
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
use url::Url;

use crate::into_retry_error;
use crate::notify_retry;
use crate::retry_durations;

/// The default namespace for planetary tasks.
const PLANETARY_TASKS_NAMESPACE: &str = "planetary-tasks";

/// The default storage size, in gigabytes.
///
/// Uses a 1 GiB default.
const DEFAULT_STORAGE_SIZE: f64 = 1.07374182;

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

/// The orchestrator id label.
const PLANETARY_ORCHESTRATOR_LABEL: &str = "planetary/orchestrator";

/// A label applied to resources that are ready for garbage collection.
///
/// The monitor service will periodically delete these resources.
const PLANETARY_GC_LABEL: &str = "planetary/gc";

/// The maximum number of lines to tail for an executor pod's logs.
///
/// This is 16 because there is always an extra line of output in the executor's
/// log to maybe contain the executors real exit code.
const MAX_EXECUTOR_LOG_LINES: i64 = 16;

/// The default CPU request (in cores) for transporter pods.
const DEFAULT_TRANSPORTER_CPU: i32 = 1;

/// The default memory request (in GB) for transporter pods.
///
/// Uses a 256 MiB default.
const DEFAULT_TRANSPORTER_MEMORY: f64 = 0.268435455;

/// The default CPU request (in cores) for pods.
const DEFAULT_POD_CPU: i32 = 1;

/// The default memory request (in GB) for pods.
///
/// Uses a 256 MiB default.
const DEFAULT_POD_MEMORY: f64 = 0.268435455;

/// The name of the S3 credentials secret.
const AWS_S3_CREDENTIALS_SECRET: &str = "aws-s3-credentials";

/// The name of the Google Cloud Storage credentials secret.
const GOOGLE_STORAGE_CREDENTIALS_SECRET: &str = "google-storage-credentials";

/// The reason for an image pull backoff wait.
const IMAGE_PULL_BACKOFF_REASON: &str = "ImagePullBackOff";

/// The name of the orchestrator API key environment variable for the
/// transporter.
const TRANSPORTER_ORCHESTRATOR_API_KEY: &str = "ORCHESTRATOR_API_KEY";

/// The name of the orchestrator auth secret.
const ORCHESTRATOR_AUTH_SECRET_NAME: &str = "orchestrator-auth";

/// The key of the orchestrator auth secret.
const ORCHESTRATOR_AUTH_SECRET_KEY: &str = "api-key";

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

/// Converts TES resources into K8S resource requirements.
fn convert_resources(resources: &Resources) -> ResourceRequirements {
    ResourceRequirements {
        requests: Some(
            [
                (
                    K8S_KEY_CPU.to_string(),
                    Quantity(resources.cpu_cores.unwrap_or(DEFAULT_POD_CPU).to_string()),
                ),
                (
                    K8S_KEY_MEMORY.to_string(),
                    Quantity(format!(
                        "{memory}G",
                        memory = resources.ram_gb.unwrap_or(DEFAULT_POD_MEMORY).ceil() as u64
                    )),
                ),
            ]
            .into(),
        ),
        ..Default::default()
    }
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
        self.metadata.name.as_deref().context("pod has no name")
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

/// Represents information about the transporter pod used by the orchestrator.
#[derive(Clone)]
pub struct TransporterInfo {
    /// The image of the transporter to use.
    pub image: Option<String>,
    /// The number of cpu cores to request for the transporter pod.
    pub cpu: Option<i32>,
    /// The amount of memory (in GB) to request for the transporter pod.
    pub memory: Option<f64>,
}

/// Implements the task orchestrator.
#[derive(Clone)]
pub struct TaskOrchestrator {
    /// The id (pod name) of the orchestrator.
    id: String,
    /// The URL of the orchestrator service.
    service_url: Url,
    /// The planetary database used by the orchestrator.
    database: Arc<dyn Database>,
    /// The namespace to use for K8S resources relating to tasks.
    tasks_namespace: Option<String>,
    /// The K8S pods API.
    pods: Arc<Api<Pod>>,
    /// The K8S persistent volume claim API.
    pvc: Arc<Api<PersistentVolumeClaim>>,
    /// The storage class name to use for persistent volume claims.
    storage_class: Option<String>,
    /// Information about the transporter to use.
    transporter: TransporterInfo,
}

impl TaskOrchestrator {
    /// Constructs a new task orchestrator.
    pub async fn new(
        database: Arc<dyn Database>,
        id: String,
        service_url: Url,
        tasks_namespace: Option<String>,
        storage_class: Option<String>,
        transporter: TransporterInfo,
    ) -> Result<Self> {
        let ns = tasks_namespace
            .as_deref()
            .unwrap_or(PLANETARY_TASKS_NAMESPACE);

        let client = Client::try_default()
            .await
            .context("failed to get default Kubernetes client")?;

        let pods = Arc::new(Api::namespaced(client.clone(), ns));
        let pvc = Arc::new(Api::namespaced(client, ns));

        Ok(Self {
            id,
            service_url,
            database,
            tasks_namespace,
            pods,
            pvc,
            storage_class,
            transporter,
        })
    }

    /// Gets the database associated with the orchestrator.
    pub fn database(&self) -> &Arc<dyn Database> {
        &self.database
    }

    /// Starts the given task.
    pub async fn start_task(&self, tes_id: &str) {
        debug!("task `{tes_id}` is starting");

        let start = async {
            let task = self
                .database
                .get_task(tes_id, GetTaskParams { view: View::Basic })
                .await?
                .into_task()
                .expect("should have basic task");

            self.database
                .append_system_log(
                    tes_id,
                    &[&format_log_message!("task `{tes_id}` has been created")],
                )
                .await?;

            // Create the storage PVC
            self.create_storage_pvc(&task).await?;

            // Create the task pod
            self.create_task_pod(&task).await
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
                )
                .await;

            self.mark_gc_ready(tes_id).await;
        }
    }

    /// Cancels the given task.
    pub async fn cancel_task(&self, tes_id: &str) {
        debug!("task `{tes_id}` is canceling");

        let cancel = async {
            // Get the pod information so we can record the terminated containers as it was
            // canceled.
            let pod = Retry::spawn_notify(
                retry_durations(),
                || async { self.pods.get_opt(tes_id).await.map_err(into_retry_error) },
                notify_retry,
            )
            .await?;

            let containers = pod
                .as_ref()
                .map(|p| self.get_terminated_containers(p).boxed());

            self.mark_gc_ready(tes_id).await;

            if self
                .database
                .update_task_state(
                    tes_id,
                    State::Canceled,
                    &[&format_log_message!("task `{tes_id}` has been canceled")],
                    containers,
                )
                .await?
            {
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
                )
                .await;
        }
    }

    /// Adopts an orphaned task pod.
    pub async fn adopt_pod(&self, name: &str) -> OrchestrationResult<()> {
        debug!("adopting task pod `{name}`");

        // Patch the pod's orchestrator label to be this orchestrator
        Retry::spawn_notify(
            retry_durations(),
            || async {
                self.pods
                    .patch(
                        name,
                        &PatchParams::default(),
                        &Patch::Merge(Pod {
                            metadata: ObjectMeta {
                                labels: Some(BTreeMap::from_iter([(
                                    PLANETARY_ORCHESTRATOR_LABEL.to_string(),
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

    /// Creates a persistent volume claim for a task's storage.
    async fn create_storage_pvc(&self, task: &Task) -> OrchestrationResult<()> {
        let tes_id = task.id.as_ref().expect("task should have id");

        debug!("initializing storage for task `{tes_id}`");

        Retry::spawn_notify(
            retry_durations(),
            || async {
                self.pvc
                    .create(
                        &PostParams::default(),
                        &PersistentVolumeClaim {
                            metadata: ObjectMeta {
                                name: Some(tes_id.clone()),
                                namespace: Some(
                                    self.tasks_namespace
                                        .as_deref()
                                        .unwrap_or(PLANETARY_TASKS_NAMESPACE)
                                        .into(),
                                ),
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
                                            disk = task
                                                .resources
                                                .as_ref()
                                                .and_then(|r| r.disk_gb)
                                                .unwrap_or(0.0)
                                                .max(DEFAULT_STORAGE_SIZE)
                                        )),
                                    )])),
                                }),
                                storage_class_name: self.storage_class.clone(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(into_retry_error)
            },
            notify_retry,
        )
        .await?;

        Ok(())
    }

    /// Creates the pod for the task.
    async fn create_task_pod(&self, task: &Task) -> OrchestrationResult<()> {
        let tes_id = task.id.as_ref().expect("task should have id");

        // The task will use a pod that has init containers for inputs and every
        // executor
        let mut init_containers = Vec::with_capacity(task.executors.len() + 1);
        init_containers.push(Container {
            name: format_container_name(ContainerKind::Inputs, None),
            image: Some(
                self.transporter
                    .image
                    .as_deref()
                    .unwrap_or(DEFAULT_TRANSPORTER_IMAGE)
                    .into(),
            ),
            args: Some(vec![
                "-v".into(),
                "--orchestrator-url".into(),
                self.service_url.to_string(),
                "--mode".into(),
                "inputs".into(),
                "--inputs-dir".into(),
                "/mnt/inputs".into(),
                "--outputs-dir".into(),
                "/mnt/outputs".into(),
                tes_id.into(),
            ]),
            env_from: Some(vec![
                EnvFromSource {
                    secret_ref: Some(SecretEnvSource {
                        name: AWS_S3_CREDENTIALS_SECRET.into(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                },
                EnvFromSource {
                    secret_ref: Some(SecretEnvSource {
                        name: GOOGLE_STORAGE_CREDENTIALS_SECRET.into(),
                        optional: Some(true),
                    }),
                    ..Default::default()
                },
            ]),
            env: Some(vec![EnvVar {
                name: TRANSPORTER_ORCHESTRATOR_API_KEY.into(),
                value_from: Some(EnvVarSource {
                    secret_key_ref: Some(SecretKeySelector {
                        name: ORCHESTRATOR_AUTH_SECRET_NAME.into(),
                        key: ORCHESTRATOR_AUTH_SECRET_KEY.into(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
                ..Default::default()
            }]),
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
            resources: Some(convert_resources(&Resources {
                cpu_cores: Some(self.transporter.cpu.unwrap_or(DEFAULT_TRANSPORTER_CPU)),
                ram_gb: Some(
                    self.transporter
                        .memory
                        .unwrap_or(DEFAULT_TRANSPORTER_MEMORY),
                ),
                ..Default::default()
            })),
            ..Default::default()
        });

        for i in 0..task.executors.len() {
            init_containers.push(Self::create_executor_container_spec(task, i)?);
        }

        let pod = Pod {
            metadata: ObjectMeta {
                namespace: Some(
                    self.tasks_namespace
                        .as_deref()
                        .unwrap_or(PLANETARY_TASKS_NAMESPACE)
                        .into(),
                ),
                name: Some(tes_id.clone()),
                labels: Some([(PLANETARY_ORCHESTRATOR_LABEL.to_string(), self.id.clone())].into()),
                ..Default::default()
            },
            spec: Some(PodSpec {
                init_containers: Some(init_containers),
                containers: vec![Container {
                    name: format_container_name(ContainerKind::Outputs, None),
                    image: Some(
                        self.transporter
                            .image
                            .as_deref()
                            .unwrap_or(DEFAULT_TRANSPORTER_IMAGE)
                            .into(),
                    ),
                    args: Some(vec![
                        "-v".into(),
                        "--orchestrator-url".into(),
                        self.service_url.to_string(),
                        "--mode".into(),
                        "outputs".into(),
                        "--outputs-dir".into(),
                        "/mnt/outputs".into(),
                        tes_id.into(),
                    ]),
                    env_from: Some(vec![
                        EnvFromSource {
                            secret_ref: Some(SecretEnvSource {
                                name: AWS_S3_CREDENTIALS_SECRET.into(),
                                optional: Some(true),
                            }),
                            ..Default::default()
                        },
                        EnvFromSource {
                            secret_ref: Some(SecretEnvSource {
                                name: GOOGLE_STORAGE_CREDENTIALS_SECRET.into(),
                                optional: Some(true),
                            }),
                            ..Default::default()
                        },
                    ]),
                    env: Some(vec![EnvVar {
                        name: TRANSPORTER_ORCHESTRATOR_API_KEY.into(),
                        value_from: Some(EnvVarSource {
                            secret_key_ref: Some(SecretKeySelector {
                                name: ORCHESTRATOR_AUTH_SECRET_NAME.into(),
                                key: ORCHESTRATOR_AUTH_SECRET_KEY.into(),
                                ..Default::default()
                            }),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]),
                    volume_mounts: Some(vec![VolumeMount {
                        name: STORAGE_VOLUME_NAME.into(),
                        mount_path: "/mnt/outputs".into(),
                        sub_path: Some("outputs".into()),
                        ..Default::default()
                    }]),
                    resources: Some(convert_resources(&Resources {
                        cpu_cores: Some(self.transporter.cpu.unwrap_or(DEFAULT_TRANSPORTER_CPU)),
                        ram_gb: Some(
                            self.transporter
                                .memory
                                .unwrap_or(DEFAULT_TRANSPORTER_MEMORY),
                        ),
                        ..Default::default()
                    })),
                    ..Default::default()
                }],
                restart_policy: Some("Never".to_string()),
                volumes: Some(vec![Volume {
                    name: STORAGE_VOLUME_NAME.into(),
                    persistent_volume_claim: Some(PersistentVolumeClaimVolumeSource {
                        claim_name: tes_id.clone(),
                        ..Default::default()
                    }),
                    ..Default::default()
                }]),
                ..Default::default()
            }),
            ..Default::default()
        };

        Retry::spawn_notify(
            retry_durations(),
            || async {
                self.pods
                    .create(&PostParams::default(), &pod)
                    .await
                    .map_err(into_retry_error)
            },
            notify_retry,
        )
        .await?;

        Ok(())
    }

    /// Creates a container specification for a task executor of the given
    /// index.
    fn create_executor_container_spec(
        task: &Task,
        executor_index: usize,
    ) -> OrchestrationResult<Container> {
        let executor = &task.executors[executor_index];

        // Format the executor script
        let script =
            Self::format_executor_script(executor, executor.ignore_error.unwrap_or(false))?;

        // Create volume mounts for the inputs, outputs, and requested volumes
        let volume_mounts = task
            .inputs
            .as_deref()
            .unwrap_or_default()
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
            .chain(
                task.outputs
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(i, output)| {
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
                    }),
            )
            .chain(
                task.volumes
                    .as_deref()
                    .unwrap_or_default()
                    .iter()
                    .enumerate()
                    .map(|(i, volume)| {
                        Ok(VolumeMount {
                            mount_path: volume.clone(),
                            name: STORAGE_VOLUME_NAME.into(),
                            read_only: Some(false),
                            sub_path: Some(format!("volumes/{i}")),
                            ..Default::default()
                        })
                    }),
            )
            .collect::<Result<_>>()?;

        Ok(Container {
            name: format_container_name(ContainerKind::Executor, Some(executor_index)),
            image: Some(executor.image.clone()),
            args: Some(vec!["-c".to_string(), script]),
            command: Some(vec!["/bin/sh".to_string()]),
            env: executor.env.as_ref().map(|env| {
                env.iter()
                    .map(|(k, v)| EnvVar {
                        name: k.clone(),
                        value: Some(v.clone()),
                        ..Default::default()
                    })
                    .collect()
            }),
            resources: task.resources.as_ref().map(convert_resources),
            volume_mounts: Some(volume_mounts),
            working_dir: executor.workdir.clone(),
            ..Default::default()
        })
    }

    /// Formats an executor script into a single line that can be used with `sh
    /// -c`.
    ///
    /// It is expected that the arguments are already shell quoted.
    fn format_executor_script(executor: &Executor, ignore_error: bool) -> Result<String> {
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
        if !ignore_error {
            script.push_str("if [[ ${CODE:-0} -ne 0 ]]; then exit $CODE; fi;");
        }

        // We must wait for the background tee jobs to complete, otherwise buffers might
        // not be flushed
        script.push_str("wait $(jobs -p) 2>&1 >/dev/null");
        Ok(script)
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
            )
            .await?
        {
            debug!("task `{tes_id}` is now running");
        }

        Ok(())
    }

    /// Handles a succeeded task.
    async fn handle_succeeded_task(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::Complete,
                &[&format_log_message!("task `{tes_id}` has completed")],
                Some(self.get_terminated_containers(pod).boxed()),
            )
            .await?
        {
            debug!("task `{tes_id}` has completed");
        }

        self.mark_gc_ready(tes_id).await;
        Ok(())
    }

    /// Handles an executor error.
    async fn handle_executor_error(
        &self,
        tes_id: &str,
        index: usize,
        pod: &Pod,
    ) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::ExecutorError,
                &[&format_log_message!(
                    "executor {index} of task `{tes_id}` has failed"
                )],
                Some(self.get_terminated_containers(pod).boxed()),
            )
            .await?
        {
            debug!("executor {index} of task `{tes_id}` has failed");
        }

        self.mark_gc_ready(tes_id).await;
        Ok(())
    }

    /// Handles a system error.
    async fn handle_system_error(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
        if self
            .database
            .update_task_state(
                tes_id,
                State::ExecutorError,
                &[&format_log_message!(
                    "task `{tes_id}` has failed due to a system error"
                )],
                Some(self.get_terminated_containers(pod).boxed()),
            )
            .await?
        {
            debug!("task `{tes_id}` has failed due to a system error");
        }

        self.mark_gc_ready(tes_id).await;
        Ok(())
    }

    /// Handles a pod in an unknown state.
    async fn handle_unknown_pod(&self, tes_id: &str, pod: &Pod) -> Result<(), Error> {
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
            )
            .await?
        {
            self.log_error(
                Some(tes_id),
                &format!("task `{tes_id}` has failed: communication was lost with its pod",),
            )
            .await;
        }

        self.mark_gc_ready(tes_id).await;
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
            )
            .await?
        {
            debug!("task `{tes_id}` failed to pull image: {message}");
        }

        self.mark_gc_ready(tes_id).await;
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
        self.database
            .update_task_state(
                tes_id,
                State::Preempted,
                &[&format_log_message!("task `{tes_id}` has been preempted")],
                Some(self.get_terminated_containers(pod).boxed()),
            )
            .await?;

        Ok(())
    }

    /// Gets the terminated containers of the given pod.
    async fn get_terminated_containers<'a>(
        &self,
        pod: &'a Pod,
    ) -> anyhow::Result<Vec<TerminatedContainer<'a>>> {
        let tes_id = pod.tes_id()?;
        let status = pod.status.as_ref().context("pod has no status")?;

        let init_statuses = status
            .init_container_statuses
            .as_deref()
            .unwrap_or_default();

        let statuses = status.container_statuses.as_deref().unwrap_or_default();

        let now = Utc::now();
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
                    match self
                        .pods
                        .logs(
                            tes_id,
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
                        Err(kube::Error::Api(ErrorResponse { code: 404, .. }))
                        | Err(kube::Error::Api(ErrorResponse { code: 400, .. })) => {
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
                start_time: state.started_at.as_ref().map(|t| t.0).unwrap_or(now),
                end_time: state.finished_at.as_ref().map(|t| t.0).unwrap_or(now),
                stdout,
                stderr,
                exit_code,
            });
        }

        Ok(containers)
    }

    /// Marks Kubernetes resources related to a task as ready for GC.
    async fn mark_gc_ready(&self, tes_id: &str) {
        if let Err(e) = Retry::spawn_notify(
            retry_durations(),
            || async {
                match self
                    .pods
                    .patch(
                        tes_id,
                        &PatchParams::default(),
                        &Patch::Merge(Pod {
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
                    Ok(_) => Ok(()),
                    Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => Ok(()),
                    Err(e) => Err(into_retry_error(e)),
                }
            },
            notify_retry,
        )
        .await
        {
            self.log_error(
                Some(tes_id),
                &format!("failed to mark pod `{tes_id}` for GC: {e:#}"),
            )
            .await;
        }

        // Add a label to the PVC to mark it ready for GC
        if let Err(e) = Retry::spawn_notify(
            retry_durations(),
            || async {
                match self
                    .pvc
                    .patch(
                        tes_id,
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
                    Ok(_) => Ok(()),
                    Err(kube::Error::Api(ErrorResponse { code: 404, .. })) => Ok(()),
                    Err(e) => Err(into_retry_error(e)),
                }
            },
            notify_retry,
        )
        .await
        {
            self.log_error(
                Some(tes_id),
                &format!("failed to mark PVC `{tes_id}` for GC: {e:#}"),
            )
            .await;
        }
    }

    /// Logs an error with the database.
    ///
    /// The error is also emitted to stderr.
    async fn log_error(&self, tes_id: Option<&str>, message: &str) {
        error!("{message}");
        let _ = self.database.insert_error(&self.id, tes_id, message).await;
    }
}

/// Implements a monitor for Kubernetes pod events and orphaned pods.
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
            state.orchestrator.pods.as_ref().clone(),
            watcher::Config {
                label_selector: Some(format!(
                    "{PLANETARY_ORCHESTRATOR_LABEL}={id}",
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
                            // If the pod is marked for GC, ignore it
                            if pod
                                .labels()
                                .get(PLANETARY_GC_LABEL)
                                .is_some()
                            {
                                continue;
                            }

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
                                        )
                                        .await;

                                    state.orchestrator.mark_gc_ready(tes_id).await;
                                }
                            });
                        }
                        Some(Ok(Event::Delete(pod))) => {
                            // If the pod is marked for GC, ignore it
                            if pod
                                .labels()
                                .get(PLANETARY_GC_LABEL)
                                .is_some()
                            {
                                continue;
                            }

                            // Otherwise, treat this as a preemption
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
                                        )
                                        .await;
                                }

                                state.orchestrator.mark_gc_ready(tes_id).await;
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
