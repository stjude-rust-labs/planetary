//! Implementation of database support for Planetary.

use std::borrow::Cow;
use std::fmt;

use anyhow::Result;
use chrono::DateTime;
use chrono::Utc;
use futures::future::BoxFuture;
use serde::Deserialize;
use serde::Serialize;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::ListTasksParams;
use tes::v1::types::requests::Task as RequestTask;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::responses::TaskResponse;
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::types::task::State;

#[cfg(feature = "postgres")]
pub mod postgres;

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
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskIo {
    /// The list of inputs for the task.
    pub inputs: Vec<Input>,
    /// The list of outputs for the task.
    pub outputs: Vec<Output>,
}

/// Represents a kind of container.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ContainerKind {
    /// The container is for downloading a task's inputs.
    Inputs,
    /// The container is a task executor.
    Executor,
    /// The container is for uploading a task's outputs.
    Outputs,
}

impl fmt::Display for ContainerKind {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Inputs => write!(f, "inputs"),
            Self::Executor => write!(f, "executor"),
            Self::Outputs => write!(f, "outputs"),
        }
    }
}

/// Represents information about a terminated container.
#[derive(Debug, Clone)]
pub struct TerminatedContainer<'a> {
    /// The kind of the container.
    pub kind: ContainerKind,
    /// The index of the executor.
    ///
    /// This is `None` when the container was not an executor.
    pub executor_index: Option<i32>,
    /// The start time of the container.
    pub start_time: DateTime<Utc>,
    /// The end time of the container.
    pub end_time: DateTime<Utc>,
    /// The stdout of the container.
    pub stdout: Option<Cow<'a, str>>,
    /// The stderr of the container.
    pub stderr: Option<Cow<'a, str>>,
    /// The exit code of the container.
    pub exit_code: i32,
}

/// An abstraction for the planetary database.
#[async_trait::async_trait]
pub trait Database: Send + Sync + 'static {
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

    /// Gets the TES identifiers of in-progress tasks.
    ///
    /// Only tasks created before the given datetime are returned.
    ///
    /// An in-progress task is in one of the following states:
    ///
    /// * Unknown
    /// * Queued
    /// * Initializing
    /// * Running
    async fn get_in_progress_tasks(&self, before: DateTime<Utc>) -> DatabaseResult<Vec<String>>;

    /// Updates the state of a task.
    ///
    /// The provided message is added to the task's system log if the task is
    /// transitioned to the given state.
    ///
    /// The given future for retrieving the terminated containers will be called
    /// if the task is transitioned to the given state; the returned containers
    /// are then recorded in the database.
    ///
    /// Returns `Ok(true)` if the status was updated or `Ok(false)` if the
    /// task's current state cannot be transitioned to the given state.
    async fn update_task_state<'a>(
        &self,
        tes_id: &str,
        state: State,
        messages: &[&str],
        containers: Option<BoxFuture<'a, Result<Vec<TerminatedContainer<'a>>>>>,
    ) -> DatabaseResult<bool>;

    /// Appends the given messages to the task's system log.
    async fn append_system_log(&self, tes_id: &str, messages: &[&str]) -> DatabaseResult<()>;

    /// Updates the output files of the given task.
    async fn update_task_output_files(
        &self,
        tes_id: &str,
        files: &[OutputFile],
    ) -> DatabaseResult<()>;

    /// Inserts an internal system error with the database.
    async fn insert_error(
        &self,
        source: &str,
        tes_id: Option<&str>,
        message: &str,
    ) -> DatabaseResult<()>;
}

/// Formats a log message by including a time stamp.
#[macro_export]
macro_rules! format_log_message {
    ($($arg:tt)*) => { format!("[{ts}] {args}", ts = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.6fZ"), args = format_args!($($arg)*)) }
}
