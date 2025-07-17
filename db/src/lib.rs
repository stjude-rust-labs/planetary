//! Implementation of database support for Planetary.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::str::FromStr;

use anyhow::Result;
use anyhow::bail;
use chrono::DateTime;
use chrono::Utc;
use secrecy::SecretString;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::ListTasksParams;
use tes::v1::types::requests::Task as RequestTask;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::responses::TaskResponse;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::types::task::Resources;
use tes::v1::types::task::State;

mod generator;
#[cfg(feature = "postgres")]
pub mod postgres;

pub use generator::*;

/// Represents a database error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provided page token wasn't valid.
    #[error("page token `{0}` is not valid")]
    InvalidPageToken(String),
    /// A PostgreSQL error occurred.
    #[cfg(feature = "postgres")]
    #[error(transparent)]
    Postgres(#[from] postgres::Error),
    /// Another type of error occurred during the database operation.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// The result type for database operations.
pub type DatabaseResult<T> = Result<T, Error>;

/// Represents information about a task's inputs and outputs.
#[derive(Debug, Clone)]
pub struct TaskIo {
    /// The list of inputs for the task.
    pub inputs: Vec<Input>,
    /// The list of outputs for the task.
    pub outputs: Vec<Output>,
    /// The volumes to mounts for the task.
    pub volumes: Vec<String>,
    /// The requested storage size for the task, in gigabytes.
    pub size_gb: Option<f64>,
}

/// Represents a kind of pod.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PodKind {
    /// The pod is for downloading inputs to a volume.
    Inputs,
    /// The pod is a task executor.
    Executor,
    /// The pod is for uploading outputs to storage.
    Outputs,
}

impl fmt::Display for PodKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inputs => write!(f, "inputs"),
            Self::Executor => write!(f, "executor"),
            Self::Outputs => write!(f, "outputs"),
        }
    }
}

impl FromStr for PodKind {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "inputs" => Ok(Self::Inputs),
            "executor" => Ok(Self::Executor),
            "outputs" => Ok(Self::Outputs),
            _ => bail!("invalid pod kind value `{s}`"),
        }
    }
}

/// The state of a pod.
///
/// This differs from a TES task state in that it may not reflect the overall
/// task state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PodState {
    /// The pod is in an unknown state.
    Unknown,
    /// The pod is waiting to be scheduled.
    Waiting,
    /// The pod is initializing.
    Initializing,
    /// The pod is running.
    Running,
    /// The pod succeeded (exited with zero).
    Succeeded,
    /// The pod failed (exited with non-zero).
    Failed,
    /// The pod failed to pull its image.
    ImagePullError,
}

impl fmt::Display for PodState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown => write!(f, "unknown"),
            Self::Waiting => write!(f, "waiting"),
            Self::Initializing => write!(f, "initializing"),
            Self::Running => write!(f, "running"),
            Self::Succeeded => write!(f, "succeeded"),
            Self::Failed => write!(f, "failed"),
            Self::ImagePullError => write!(f, "image pull error"),
        }
    }
}

/// Represents information about a finished pod.
#[derive(Debug, Clone, Copy)]
pub struct FinishedPod<'a> {
    /// The exit code of the pod.
    pub exit_code: i32,
    /// The start time of the pod.
    pub start_time: Option<DateTime<Utc>>,
    /// The end time of the pod.
    pub end_time: Option<DateTime<Utc>>,
    /// The stdout of the pod.
    pub stdout: Option<&'a str>,
    /// The stderr of the pod.
    pub stderr: Option<&'a str>,
}

/// Represents an executing task pod.
#[derive(Debug, Clone)]
pub struct ExecutingPod {
    /// The name of the executing pod.
    name: String,

    /// The TES task identifier associated with the pod.
    tes_id: String,
}

impl ExecutingPod {
    /// Gets the name of the executing pod.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Gets the TES identifier of the task that owns the pod.
    pub fn tes_id(&self) -> &str {
        &self.tes_id
    }
}

/// An abstraction for the planetary database.
#[async_trait::async_trait]
pub trait Database: Send + Sync + 'static {
    /// Gets the URL of the database.
    fn url(&self) -> &SecretString;

    /// Inserts a task into the database.
    ///
    /// Note: it is expected that the newly inserted task has the `UNKNOWN`
    /// state.
    ///
    /// Returns the generated TES task identifier.
    async fn insert_task(&self, task: &RequestTask) -> DatabaseResult<String>;

    /// Gets a task from the database.
    async fn get_task(&self, tes_id: &str, params: GetTaskParams) -> DatabaseResult<TaskResponse>;

    /// Gets tasks from the database.
    ///
    /// Returns a list of tasks and the page token to use for the next request.
    async fn get_tasks(
        &self,
        params: ListTasksParams,
    ) -> DatabaseResult<(Vec<TaskResponse>, Option<String>)>;

    /// Gets the inputs and outputs of a task.
    async fn get_task_io(&self, tes_id: &str) -> DatabaseResult<TaskIo>;

    /// Gets an executor of a task by index.
    ///
    /// Returns `Ok(None)` if the given executor index is out of range.
    async fn get_task_executor(
        &self,
        tes_id: &str,
        executor_index: usize,
    ) -> DatabaseResult<Option<(Executor, Resources)>>;

    /// Updates the state of a task.
    ///
    /// The provided message is added to the task's system log if the task is
    /// transitioned to the given state.
    ///
    /// Returns `Ok(true)` if the status was updated or `Ok(false)` if the
    /// task's current state cannot be transitioned to the given state.
    async fn update_task_state(
        &self,
        tes_id: &str,
        state: State,
        messages: &[&str],
    ) -> DatabaseResult<bool>;

    /// Inserts a pod into the database.
    ///
    /// Returns `Ok(true)` if the pod was inserted or `Ok(false)` if the pod
    /// could not be inserted because the associated task is canceling or
    /// canceled.
    async fn insert_pod(
        &self,
        tes_id: &str,
        name: &str,
        kind: PodKind,
        executor_index: Option<usize>,
    ) -> DatabaseResult<bool>;

    /// Updates the state of a pod.
    ///
    /// Returns `Ok(true)` if the status was updated or `Ok(false)` if the
    /// task's current state cannot be transitioned to the given state.
    async fn update_pod_state(
        &self,
        name: &str,
        state: PodState,
        finished: Option<FinishedPod<'_>>,
    ) -> DatabaseResult<bool>;

    /// Finds an executing pod for the given task.
    ///
    /// Returns the pod name if the task has an executing pod.
    ///
    /// Returns `Ok(None)` if the task has no pods or if all the pods have
    /// completed.
    async fn find_executing_pod(&self, tes_id: &str) -> DatabaseResult<Option<String>>;

    /// Drains the currently executing pods.
    ///
    /// The provided callback is invoked with the currently executing pods and
    /// the returned future is then driven to completion.
    ///
    /// The future returns the set of executing pods to drain (i.e. abort).
    async fn drain_executing_pods(
        &self,
        cb: Box<
            dyn FnOnce(
                    Vec<ExecutingPod>,
                )
                    -> Pin<Box<dyn Future<Output = Result<Vec<ExecutingPod>>> + Send + 'static>>
                + Send,
        >,
    ) -> DatabaseResult<()>;

    /// Appends the given messages to the task's system log.
    async fn append_system_log(&self, tes_id: &str, messages: &[&str]) -> DatabaseResult<()>;

    /// Updates the output files of the given task.
    async fn update_task_output_files(
        &self,
        tes_id: &str,
        files: &[OutputFile],
    ) -> DatabaseResult<()>;
}

/// Formats a log message by including a time stamp.
#[macro_export]
macro_rules! format_log_message {
    ($($arg:tt)*) => { format!("[{ts}] {args}", ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"), args = format_args!($($arg)*)) }
}
