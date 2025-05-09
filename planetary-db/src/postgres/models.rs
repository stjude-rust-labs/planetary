//! The module types for the PostgreSQL database.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Write;

use chrono::DateTime;
use chrono::Utc;
use diesel::FromSqlRow;
use diesel::deserialize;
use diesel::expression::AsExpression;
use diesel::pg::Pg;
use diesel::pg::PgValue;
use diesel::prelude::*;
use diesel::serialize;
use diesel::sql_types;
use serde::Deserialize;
use serde::Serialize;
use serde::Serializer;
use tes::v1::types::requests::Task as RequestTask;
use tes::v1::types::responses::ExecutorLog;
use tes::v1::types::responses::MinimalTask as TesMinimalTask;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::responses::Task as ResponseTask;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::types::task::Resources;

use crate::generator::Alphanumeric;

/// Represents the state of a task.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, diesel_derive_enum::DbEnum)]
#[db_enum(
    existing_type_path = "crate::postgres::schema::sql_types::TaskState",
    value_style = "SCREAMING_SNAKE_CASE"
)]
pub enum TaskState {
    /// The task state is unknown.
    Unknown,
    /// The task has been queued.
    Queued,
    /// The task is initializing.
    Initializing,
    /// The task is running.
    Running,
    /// The task is paused.
    Paused,
    /// The task is complete.
    Complete,
    /// An executor error was encountered.
    ExecutorError,
    /// A system error was encountered.
    SystemError,
    /// The task is canceling.
    Canceling,
    /// The task has been canceled.
    Canceled,
    /// The task was preempted.
    Preempted,
}

impl From<TaskState> for tes::v1::types::task::State {
    fn from(s: TaskState) -> Self {
        use TaskState::*;

        match s {
            Unknown => Self::Unknown,
            Queued => Self::Queued,
            Initializing => Self::Initializing,
            Running => Self::Running,
            Paused => Self::Paused,
            Complete => Self::Complete,
            ExecutorError => Self::ExecutorError,
            SystemError => Self::SystemError,
            Canceling => Self::Canceling,
            Canceled => Self::Canceled,
            Preempted => Self::Preempted,
        }
    }
}

impl From<tes::v1::types::task::State> for TaskState {
    fn from(s: tes::v1::types::task::State) -> Self {
        use tes::v1::types::task::State::*;

        match s {
            Unknown => Self::Unknown,
            Queued => Self::Queued,
            Initializing => Self::Initializing,
            Running => Self::Running,
            Paused => Self::Paused,
            Complete => Self::Complete,
            ExecutorError => Self::ExecutorError,
            SystemError => Self::SystemError,
            Canceling => Self::Canceling,
            Canceled => Self::Canceled,
            Preempted => Self::Preempted,
        }
    }
}

/// Represents the kind of a pod.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, diesel_derive_enum::DbEnum)]
#[db_enum(
    existing_type_path = "crate::postgres::schema::sql_types::PodKind",
    value_style = "SCREAMING_SNAKE_CASE"
)]
pub enum PodKind {
    /// The pod is for downloading inputs for a task.
    Inputs,
    /// The pod is for an executor of a task.
    Executor,
    /// The pod is for uploading outputs for a task.
    Outputs,
}

impl From<PodKind> for crate::PodKind {
    fn from(kind: PodKind) -> Self {
        match kind {
            PodKind::Inputs => Self::Inputs,
            PodKind::Executor => Self::Executor,
            PodKind::Outputs => Self::Outputs,
        }
    }
}

impl From<crate::PodKind> for PodKind {
    fn from(kind: crate::PodKind) -> Self {
        use crate::PodKind::*;

        match kind {
            Inputs => Self::Inputs,
            Executor => Self::Executor,
            Outputs => Self::Outputs,
        }
    }
}

/// Represents the state of a pod.
#[derive(Debug, Copy, Clone, PartialEq, Eq, Hash, diesel_derive_enum::DbEnum)]
#[db_enum(
    existing_type_path = "crate::postgres::schema::sql_types::PodState",
    value_style = "SCREAMING_SNAKE_CASE"
)]
pub enum PodState {
    /// The pod is in an unknown state.
    Unknown,
    /// The pod is waiting.
    Waiting,
    /// The pod is initializing.
    Initializing,
    /// The pod is running.
    Running,
    /// The pod has succeeded.
    Succeeded,
    /// The pod has failed.
    Failed,
    /// The pod failed to pull its image.
    ImagePullError,
}

impl PodState {
    /// Gets the pod states that are considering an "executing" pod.
    pub fn executing() -> &'static [Self] {
        &[Self::Waiting, Self::Initializing, Self::Running]
    }
}

impl From<PodState> for crate::PodState {
    fn from(state: PodState) -> Self {
        use PodState::*;

        match state {
            Unknown => Self::Unknown,
            Waiting => Self::Waiting,
            Running => Self::Running,
            Initializing => Self::Initializing,
            Succeeded => Self::Succeeded,
            Failed => Self::Failed,
            ImagePullError => Self::ImagePullError,
        }
    }
}

impl From<crate::PodState> for PodState {
    fn from(state: crate::PodState) -> Self {
        use crate::PodState::*;

        match state {
            Unknown => Self::Unknown,
            Waiting => Self::Waiting,
            Running => Self::Running,
            Initializing => Self::Initializing,
            Succeeded => Self::Succeeded,
            Failed => Self::Failed,
            ImagePullError => Self::ImagePullError,
        }
    }
}

/// Represents a JSON serializable value.
#[derive(Debug, FromSqlRow, AsExpression)]
#[diesel(sql_type = diesel::sql_types::Jsonb)]
pub struct Json<T>(pub T);

impl<T> Json<T> {
    /// Converts into the inner value.
    pub fn into_inner(self) -> T {
        self.0
    }
}

impl<T: Serialize + std::fmt::Debug> serialize::ToSql<sql_types::Jsonb, Pg> for Json<T> {
    fn to_sql<'b>(&self, out: &mut serialize::Output<'_, '_, Pg>) -> serialize::Result {
        out.write_all(&[1])?;
        serde_json::to_writer(out, &self.0)
            .map(|_| serialize::IsNull::No)
            .map_err(Into::into)
    }
}

impl<T: for<'a> Deserialize<'a>> deserialize::FromSql<sql_types::Jsonb, Pg> for Json<T> {
    fn from_sql(value: PgValue<'_>) -> deserialize::Result<Self> {
        let bytes = value.as_bytes();
        if bytes[0] != 1 {
            return Err("unsupported JSONB encoding version".into());
        }

        Ok(Self(
            serde_json::from_slice(&bytes[1..]).map_err(|e| format!("invalid JSON: {e}"))?,
        ))
    }
}

/// Helper for converting a tag filter into a JSON object.
#[derive(Debug)]
pub struct TagFilter {
    /// The tag key to filter on.
    key: String,
    /// The tag value to filter on.
    value: String,
}

impl TagFilter {
    /// Constructs a new tag filter for the given key and value.
    pub fn new(key: String, value: String) -> Self {
        Self { key, value }
    }
}

impl Serialize for TagFilter {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        use serde::ser::SerializeMap;
        let mut map = serializer.serialize_map(Some(1))?;
        map.serialize_entry(&self.key, &self.value)?;
        map.end()
    }
}

/// Used to insert a new task into the tasks table.
#[derive(Insertable)]
#[diesel(table_name = super::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewTask<'a> {
    /// The state of the task.
    pub state: TaskState,
    /// The generated TES id for the task.
    pub tes_id: String,
    /// The optional name of the new task.
    pub name: Option<&'a str>,
    /// The optional description of the new task.
    pub description: Option<&'a str>,
    /// The inputs of the task.
    pub inputs: Option<Json<Cow<'a, [Input]>>>,
    /// The outputs of the task.
    pub outputs: Option<Json<Cow<'a, [Output]>>>,
    /// The number of requested CPU cores.
    pub cpu_cores: Option<i32>,
    /// Whether or not the task prefers to be preemptible.
    pub preemptible: Option<bool>,
    /// The amount of RAM (in gigabytes).
    pub ram_gb: Option<f64>,
    /// The amount of disk space (in gigabytes).
    pub disk_gb: Option<f64>,
    /// The requested compute zones for the task.
    pub zones: Option<&'a [String]>,
    /// The optional backend parameters for the task.
    pub backend_parameters: Option<Json<Cow<'a, BTreeMap<String, serde_json::Value>>>>,
    /// If set to true, backends should fail the task if any backend parameter
    /// key or value is unsupported, otherwise backends should attempt to run
    /// the task.
    pub backend_parameters_strict: Option<bool>,
    /// The executors of the task.
    pub executors: Json<Cow<'a, [Executor]>>,
    /// The volumes of the task.
    pub volumes: Option<&'a [String]>,
    /// The tags of the task.
    pub tags: Option<Json<Cow<'a, BTreeMap<String, String>>>>,
}

impl<'a> NewTask<'a> {
    /// Constructs a new task model from the given create task request.
    pub fn new(task: &'a RequestTask) -> Self {
        let resources = task.resources.as_ref();

        Self {
            state: TaskState::Unknown,
            tes_id: format!("{}", Alphanumeric::new(20)),
            name: task.name.as_deref(),
            description: task.description.as_deref(),
            inputs: task.inputs.as_deref().map(|i| Json(i.into())),
            outputs: task.outputs.as_deref().map(|o| Json(o.into())),
            cpu_cores: resources.and_then(|r| r.cpu_cores),
            preemptible: resources.and_then(|r| r.preemptible),
            disk_gb: resources.and_then(|r| r.disk_gb),
            ram_gb: resources.and_then(|r| r.ram_gb),
            zones: resources.and_then(|r| r.zones.as_deref()),
            backend_parameters: resources.and_then(|r| {
                r.backend_parameters
                    .as_ref()
                    .map(|p| Json(Cow::Borrowed(p)))
            }),
            backend_parameters_strict: resources.and_then(|r| r.backend_parameters_strict),
            executors: Json(task.executors.as_slice().into()),
            volumes: task.volumes.as_deref(),
            tags: task.tags.as_ref().map(|t| Json(Cow::Borrowed(t))),
        }
    }
}

/// Represents a minimal view of a task.
#[derive(Debug, Queryable, Selectable, Identifiable)]
#[diesel(table_name = super::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct MinimalTask {
    /// The task database identifier.
    pub id: i32,
    /// The TES identifier of the task.
    pub tes_id: String,
    /// The task state.
    pub state: TaskState,
}

impl From<MinimalTask> for TesMinimalTask {
    fn from(task: MinimalTask) -> Self {
        Self {
            id: task.tes_id,
            state: Some(task.state.into()),
        }
    }
}

/// Represents a basic view of a task.
#[derive(Debug, Queryable, Selectable, Identifiable)]
#[diesel(table_name = super::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BasicTask {
    /// The task database identifier.
    pub id: i32,
    /// The TES identifier of the task.
    pub tes_id: String,
    /// The task state.
    pub state: TaskState,
    /// The task name.
    pub name: Option<String>,
    /// The task description.
    pub description: Option<String>,
    /// The task inputs.
    pub inputs: Option<Json<Vec<Input>>>,
    /// The task outputs.
    pub outputs: Option<Json<Vec<Output>>>,
    /// The requested task CPU cores.
    pub cpu_cores: Option<i32>,
    /// Whether or not the task may be preemptible.
    pub preemptible: Option<bool>,
    /// The requested task memory (in GB).
    pub ram_gb: Option<f64>,
    /// The requested task disk (in GB).
    pub disk_gb: Option<f64>,
    /// The requested compute zones for the task.
    pub zones: Option<Vec<Option<String>>>,
    /// The requested backend parameters for the task.
    pub backend_parameters: Option<Json<BTreeMap<String, serde_json::Value>>>,
    /// Whether or not the backend parameters were strictly enforced.
    pub backend_parameters_strict: Option<bool>,
    /// The task executors.
    pub executors: Json<Vec<Executor>>,
    /// The requested volumes for the task.
    pub volumes: Option<Vec<Option<String>>>,
    /// The tags for the task.
    pub tags: Option<Json<BTreeMap<String, String>>>,
    /// The output files.
    pub output_files: Option<Json<Vec<OutputFile>>>,
    /// The creation time for the task.
    pub creation_time: DateTime<Utc>,
}

impl BasicTask {
    /// Whether or not the task has associated resources.
    fn has_resources(&self) -> bool {
        self.cpu_cores.is_some()
            || self.preemptible.is_some()
            || self.ram_gb.is_some()
            || self.disk_gb.is_some()
            || self.zones.is_some()
            || self.backend_parameters.is_some()
            || self.backend_parameters_strict.is_some()
    }
}

// Helper for converting a basic task into a response task, a list of output
// files, and system log entries
impl From<BasicTask> for (ResponseTask, Vec<OutputFile>, Vec<String>) {
    fn from(task: BasicTask) -> Self {
        let resources = if task.has_resources() {
            Some(Resources {
                cpu_cores: task.cpu_cores,
                preemptible: task.preemptible,
                ram_gb: task.ram_gb,
                disk_gb: task.disk_gb,
                zones: task
                    .zones
                    .map(|z| z.into_iter().map(Option::unwrap).collect()),
                backend_parameters: task.backend_parameters.map(Json::into_inner),
                backend_parameters_strict: task.backend_parameters_strict,
            })
        } else {
            None
        };

        let mut inputs = task.inputs.map(Json::into_inner);
        if let Some(inputs) = inputs.as_mut() {
            for input in inputs {
                input.content = None;
            }
        }

        (
            ResponseTask {
                id: Some(task.tes_id),
                state: Some(task.state.into()),
                name: task.name,
                description: task.description,
                inputs,
                outputs: task.outputs.map(Json::into_inner),
                resources,
                executors: task.executors.into_inner(),
                volumes: task
                    .volumes
                    .map(|z| z.into_iter().map(Option::unwrap).collect()),
                tags: task.tags.map(Json::into_inner),
                logs: None,
                creation_time: Some(task.creation_time),
            },
            task.output_files.map(Json::into_inner).unwrap_or_default(),
            Default::default(),
        )
    }
}

/// Represents a full view of a task.
#[derive(Debug, Queryable, Selectable, Identifiable)]
#[diesel(table_name = super::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct FullTask {
    /// The task database identifier.
    pub id: i32,
    /// The TES identifier of the task.
    pub tes_id: String,
    /// The task state.
    pub state: TaskState,
    /// The task name.
    pub name: Option<String>,
    /// The task description.
    pub description: Option<String>,
    /// The task inputs.
    pub inputs: Option<Json<Vec<Input>>>,
    /// The task outputs.
    pub outputs: Option<Json<Vec<Output>>>,
    /// The requested task CPU cores.
    pub cpu_cores: Option<i32>,
    /// Whether or not the task may be preemptible.
    pub preemptible: Option<bool>,
    /// The requested task memory (in GB).
    pub ram_gb: Option<f64>,
    /// The requested task disk (in GB).
    pub disk_gb: Option<f64>,
    /// The requested compute zones for the task.
    pub zones: Option<Vec<Option<String>>>,
    /// The requested backend parameters for the task.
    pub backend_parameters: Option<Json<BTreeMap<String, serde_json::Value>>>,
    /// Whether or not the backend parameters were strictly enforced.
    pub backend_parameters_strict: Option<bool>,
    /// The task executors.
    pub executors: Json<Vec<Executor>>,
    /// The requested volumes for the task.
    pub volumes: Option<Vec<Option<String>>>,
    /// The tags for the task.
    pub tags: Option<Json<BTreeMap<String, String>>>,
    /// The output files.
    pub output_files: Option<Json<Vec<OutputFile>>>,
    /// The task's system logs.
    pub system_logs: Option<Vec<Option<String>>>,
    /// The creation time for the task.
    pub creation_time: DateTime<Utc>,
}

impl FullTask {
    /// Whether or not the task has associated resources.
    fn has_resources(&self) -> bool {
        self.cpu_cores.is_some()
            || self.preemptible.is_some()
            || self.ram_gb.is_some()
            || self.disk_gb.is_some()
            || self.zones.is_some()
            || self.backend_parameters.is_some()
            || self.backend_parameters_strict.is_some()
    }
}

// Helper for converting a full task into a response task, a list of output
// files, and system log entries
impl From<FullTask> for (ResponseTask, Vec<OutputFile>, Vec<String>) {
    fn from(task: FullTask) -> Self {
        let resources = if task.has_resources() {
            Some(Resources {
                cpu_cores: task.cpu_cores,
                preemptible: task.preemptible,
                ram_gb: task.ram_gb,
                disk_gb: task.disk_gb,
                zones: task
                    .zones
                    .map(|z| z.into_iter().map(Option::unwrap).collect()),
                backend_parameters: task.backend_parameters.map(Json::into_inner),
                backend_parameters_strict: task.backend_parameters_strict,
            })
        } else {
            None
        };

        (
            ResponseTask {
                id: Some(task.tes_id),
                state: Some(task.state.into()),
                name: task.name,
                description: task.description,
                inputs: task.inputs.map(Json::into_inner),
                outputs: task.outputs.map(Json::into_inner),
                resources,
                executors: task.executors.into_inner(),
                volumes: task
                    .volumes
                    .map(|z| z.into_iter().map(Option::unwrap).collect()),
                tags: task.tags.map(Json::into_inner),
                logs: None,
                creation_time: Some(task.creation_time),
            },
            task.output_files.map(Json::into_inner).unwrap_or_default(),
            task.system_logs
                .map(|l| l.into_iter().map(Option::unwrap_or_default).collect())
                .unwrap_or_default(),
        )
    }
}

/// Used to insert a new pod into the pods table.
#[derive(Insertable)]
#[diesel(table_name = super::schema::pods)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewPod<'a> {
    /// The task id of the pod.
    pub task_id: i32,
    /// The name of the pod.
    pub name: &'a str,
    /// The kind of pod.
    pub kind: PodKind,
    /// The state of the pod.
    pub state: PodState,
    /// The executor index for the pod.
    pub executor_index: Option<i32>,
}

/// Represents a pod relating to a task.
#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(BasicTask, foreign_key = task_id))]
#[diesel(table_name = super::schema::pods)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BasicPod {
    /// The primary id of the pod.
    pub id: i32,
    /// The task id of the pod.
    pub task_id: i32,
    /// The kind of pod.
    pub kind: PodKind,
    /// The executor index for the pod.
    pub executor_index: Option<i32>,
    /// The start time for the pod.
    pub start_time: Option<DateTime<Utc>>,
    /// The end time for the pod.
    pub end_time: Option<DateTime<Utc>>,
    /// The exit code of the pod.
    pub exit_code: Option<i32>,
}

impl From<BasicPod> for ExecutorLog {
    fn from(pod: BasicPod) -> Self {
        Self {
            start_time: pod.start_time,
            end_time: pod.end_time,
            stdout: None,
            stderr: None,
            exit_code: pod.exit_code.expect("should have exit code"),
        }
    }
}

/// Represents a pod relating to a task.
#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(FullTask, foreign_key = task_id))]
#[diesel(table_name = super::schema::pods)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct FullPod {
    /// The primary id of the pod.
    pub id: i32,
    /// The task id of the pod.
    pub task_id: i32,
    /// The kind of pod.
    pub kind: PodKind,
    /// The executor index for the pod.
    pub executor_index: Option<i32>,
    /// The start time for the pod.
    pub start_time: Option<DateTime<Utc>>,
    /// The end time for the pod.
    pub end_time: Option<DateTime<Utc>>,
    /// The stdout of the pod.
    pub stdout: Option<String>,
    /// The stderr of the pod.
    pub stderr: Option<String>,
    /// The exit code of the pod.
    pub exit_code: Option<i32>,
}

impl From<FullPod> for ExecutorLog {
    fn from(pod: FullPod) -> Self {
        Self {
            start_time: pod.start_time,
            end_time: pod.end_time,
            stdout: pod.stdout,
            stderr: pod.stderr,
            exit_code: pod.exit_code.expect("should have exit code"),
        }
    }
}
