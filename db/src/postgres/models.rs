//! The module types for the PostgreSQL database.
use std::borrow::Cow;
use std::collections::BTreeMap;
use std::io::Write;

use chrono::DateTime;
use chrono::Utc;
use cloud_copy::Alphanumeric;
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
    /// The username associated with the task.
    pub username: &'a str,
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
    pub fn new(username: &'a str, task: &'a RequestTask) -> Self {
        let resources = task.resources.as_ref();

        Self {
            state: TaskState::Unknown,
            username,
            tes_id: format!("{:#}", Alphanumeric::new(20)),
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

/// The name prefix of executor containers within a task pod.
///
/// This must match the container naming used by the orchestrator's task pod
/// template (`executor-N` for the task's Nth executor).
const EXECUTOR_CONTAINER_PREFIX: &str = "executor-";

/// Represents the aggregated resource usage of a single task container.
#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(BasicTask, foreign_key = task_id))]
#[diesel(belongs_to(FullTask, foreign_key = task_id))]
#[diesel(table_name = super::schema::task_container_usage)]
#[diesel(primary_key(task_id, container_name))]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct ContainerUsage {
    /// The task database identifier.
    pub task_id: i32,
    /// The name of the container within the task's pod.
    pub container_name: String,
    /// The peak observed working set memory of the container, in bytes.
    pub peak_memory_bytes: Option<i64>,
    /// The running total of sampled working set memory, in bytes.
    pub memory_total_bytes: Option<i64>,
    /// The number of memory samples taken.
    pub memory_sample_count: Option<i64>,
    /// The accumulated CPU time of the container, in milliseconds.
    pub cpu_time_ms: Option<i64>,
}

/// Builds a usage metadata entry from aggregate values.
///
/// Values are encoded as strings, as the TES specification types
/// `TaskLog.metadata` as a map of strings; returns `None` if the aggregate
/// carries no measurements.
fn usage_entry(
    peak_memory_bytes: Option<i64>,
    memory_total_bytes: Option<i64>,
    memory_sample_count: Option<i64>,
    cpu_time_ms: Option<i64>,
) -> Option<serde_json::Map<String, serde_json::Value>> {
    let mut entry = serde_json::Map::new();

    if let Some(peak) = peak_memory_bytes {
        entry.insert("peak_memory_bytes".to_string(), peak.to_string().into());
    }

    if let (Some(total), Some(count)) = (memory_total_bytes, memory_sample_count)
        && count > 0
    {
        entry.insert(
            "avg_memory_bytes".to_string(),
            (total / count).to_string().into(),
        );
    }

    if let Some(cpu) = cpu_time_ms {
        entry.insert("cpu_time_ms".to_string(), cpu.to_string().into());
    }

    if entry.is_empty() { None } else { Some(entry) }
}

/// Builds the task log metadata object from per-container aggregated
/// resource usage.
///
/// The task-level `peak_memory_bytes`, `avg_memory_bytes`, and `cpu_time_ms`
/// keys cover the task's executor containers only (excluding the input and
/// output transporter containers). Because a task pod's executors run
/// sequentially, the task-level peak is the greatest of the executor peaks
/// and the CPU time is the sum of the executor CPU times.
///
/// The `resource_usage` key carries the per-container breakdown, keyed by
/// container name (`inputs`, `executor-N`, `outputs`). A container absent
/// from the breakdown was never sampled (e.g. it completed within a single
/// sampling interval), which is not the same as zero usage.
///
/// Returns `None` if no usage was recorded.
pub(super) fn resource_usage_metadata(usage: &[ContainerUsage]) -> Option<serde_json::Value> {
    let mut metadata = serde_json::Map::new();

    // Fold the executor containers' aggregates into the task-level keys
    let mut peak: Option<i64> = None;
    let mut total: Option<i64> = None;
    let mut count: Option<i64> = None;
    let mut cpu: Option<i64> = None;
    for container in usage {
        if !container
            .container_name
            .starts_with(EXECUTOR_CONTAINER_PREFIX)
        {
            continue;
        }

        if let Some(p) = container.peak_memory_bytes {
            peak = Some(peak.unwrap_or(0).max(p));
        }

        if let Some(t) = container.memory_total_bytes {
            total = Some(total.unwrap_or(0) + t);
        }

        if let Some(c) = container.memory_sample_count {
            count = Some(count.unwrap_or(0) + c);
        }

        if let Some(c) = container.cpu_time_ms {
            cpu = Some(cpu.unwrap_or(0) + c);
        }
    }

    if let Some(entry) = usage_entry(peak, total, count, cpu) {
        metadata.extend(entry);
    }

    // Add the per-container breakdown
    let mut breakdown = serde_json::Map::new();
    for container in usage {
        if let Some(entry) = usage_entry(
            container.peak_memory_bytes,
            container.memory_total_bytes,
            container.memory_sample_count,
            container.cpu_time_ms,
        ) {
            breakdown.insert(container.container_name.clone(), entry.into());
        }
    }

    if !breakdown.is_empty() {
        metadata.insert("resource_usage".to_string(), breakdown.into());
    }

    if metadata.is_empty() {
        None
    } else {
        Some(serde_json::Value::Object(metadata))
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
                    .map(|z| z.into_iter().map(Option::unwrap_or_default).collect()),
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
                    .map(|z| z.into_iter().map(Option::unwrap_or_default).collect()),
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
                    .map(|z| z.into_iter().map(Option::unwrap_or_default).collect()),
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
                    .map(|z| z.into_iter().map(Option::unwrap_or_default).collect()),
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

/// Represents template data of a task.
#[derive(Debug, Queryable, Selectable, Identifiable)]
#[diesel(table_name = super::schema::tasks)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct TaskTemplateData {
    /// The task database identifier.
    pub id: i32,
    /// The username associated with the task.
    pub username: String,
    /// The TES identifier of the task.
    pub tes_id: String,
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
    /// The task executors.
    pub executors: Json<Vec<Executor>>,
    /// The requested volumes for the task.
    pub volumes: Option<Vec<Option<String>>>,
}

impl From<TaskTemplateData> for crate::TaskTemplateData {
    fn from(data: TaskTemplateData) -> Self {
        Self {
            id: data.tes_id,
            username: data.username,
            preemptible: data.preemptible.unwrap_or(false),
            cpu: data.cpu_cores,
            memory: data.ram_gb,
            disk: data.disk_gb,
            inputs: data.inputs.map(Json::into_inner).unwrap_or_default(),
            outputs: data.outputs.map(Json::into_inner).unwrap_or_default(),
            volumes: data
                .volumes
                .map(|c| c.into_iter().map(Option::unwrap_or_default).collect())
                .unwrap_or_default(),
            executors: data.executors.into_inner(),
        }
    }
}

/// Used to insert a new container into the containers table.
#[derive(Insertable)]
#[diesel(table_name = super::schema::containers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewContainer<'a> {
    /// The task id of the container.
    pub task_id: i32,
    /// The name of the container.
    pub name: &'a str,
    /// The executor index of the container.
    ///
    /// This is `NULL` for input and output containers.
    pub executor_index: Option<i32>,
    /// The start time for the container.
    pub start_time: DateTime<Utc>,
    /// The end time for the container.
    pub end_time: DateTime<Utc>,
    /// The stdout contents of the container.
    pub stdout: Option<Cow<'a, str>>,
    /// The stderr contents of the container.
    pub stderr: Option<Cow<'a, str>>,
    /// The exit code of the container.
    pub exit_code: i32,
}

impl<'a> NewContainer<'a> {
    /// Constructs a new container from information relating to a terminated
    /// container.
    pub fn new(task_id: i32, container: crate::TerminatedContainer<'a>) -> Self {
        Self {
            task_id,
            name: container.name,
            executor_index: container.executor_index,
            start_time: container.start_time,
            end_time: container.end_time,
            stdout: container.stdout,
            stderr: container.stderr,
            exit_code: container.exit_code,
        }
    }
}

/// Represents a container relating to a task.
///
/// This does not retrieve the container's logs.
#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(BasicTask, foreign_key = task_id))]
#[diesel(table_name = super::schema::containers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct BasicContainer {
    /// The primary id of the container.
    pub id: i32,
    /// The task id of the container.
    pub task_id: i32,
    /// The start time for the container.
    pub start_time: DateTime<Utc>,
    /// The end time for the container.
    pub end_time: DateTime<Utc>,
    /// The exit code of the container.
    pub exit_code: i32,
}

impl From<BasicContainer> for ExecutorLog {
    fn from(container: BasicContainer) -> Self {
        Self {
            start_time: Some(container.start_time),
            end_time: Some(container.end_time),
            stdout: None,
            stderr: None,
            exit_code: container.exit_code,
        }
    }
}

/// Represents a container relating to a task.
///
/// This retrieves the container's logs.
#[derive(Queryable, Selectable, Identifiable, Associations, Debug, PartialEq)]
#[diesel(belongs_to(FullTask, foreign_key = task_id))]
#[diesel(table_name = super::schema::containers)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct FullContainer {
    /// The primary id of the container.
    pub id: i32,
    /// The task id of the container.
    pub task_id: i32,
    /// The start time for the container.
    pub start_time: DateTime<Utc>,
    /// The end time for the container.
    pub end_time: DateTime<Utc>,
    /// The stdout of the container.
    pub stdout: Option<String>,
    /// The stderr of the container.
    pub stderr: Option<String>,
    /// The exit code of the container.
    pub exit_code: i32,
}

impl From<FullContainer> for ExecutorLog {
    fn from(container: FullContainer) -> Self {
        Self {
            start_time: Some(container.start_time),
            end_time: Some(container.end_time),
            stdout: container.stdout,
            stderr: container.stderr,
            exit_code: container.exit_code,
        }
    }
}

/// Used to insert a new error into the errors table.
#[derive(Insertable)]
#[diesel(table_name = super::schema::errors)]
#[diesel(check_for_backend(diesel::pg::Pg))]
pub struct NewError<'a> {
    /// The source of the error.
    pub source: &'a str,
    /// The task id related to the error.
    pub task_id: Option<i32>,
    /// The error message.
    pub message: &'a str,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Builds a container usage row for tests.
    fn usage(
        name: &str,
        peak: Option<i64>,
        total: Option<i64>,
        count: Option<i64>,
        cpu: Option<i64>,
    ) -> ContainerUsage {
        ContainerUsage {
            task_id: 1,
            container_name: name.to_string(),
            peak_memory_bytes: peak,
            memory_total_bytes: total,
            memory_sample_count: count,
            cpu_time_ms: cpu,
        }
    }

    #[test]
    fn metadata_is_none_without_usage() {
        assert_eq!(resource_usage_metadata(&[]), None);
        assert_eq!(
            resource_usage_metadata(&[usage("executor-0", None, None, None, None)]),
            None
        );
    }

    #[test]
    fn task_level_keys_cover_executors_only() {
        let metadata = resource_usage_metadata(&[
            usage("inputs", Some(500), Some(1000), Some(2), Some(50)),
            usage("executor-0", Some(100), Some(150), Some(2), Some(1000)),
            usage("executor-1", Some(300), Some(300), Some(1), Some(2000)),
            usage("outputs", Some(400), Some(400), Some(1), Some(25)),
        ])
        .expect("should have metadata");

        // Peak is the greatest executor peak; the average and CPU time fold
        // across the executors; the transporters are excluded
        assert_eq!(metadata["peak_memory_bytes"], "300");
        assert_eq!(metadata["avg_memory_bytes"], "150");
        assert_eq!(metadata["cpu_time_ms"], "3000");

        // The breakdown carries every sampled container
        let breakdown = &metadata["resource_usage"];
        assert_eq!(breakdown["inputs"]["peak_memory_bytes"], "500");
        assert_eq!(breakdown["inputs"]["avg_memory_bytes"], "500");
        assert_eq!(breakdown["inputs"]["cpu_time_ms"], "50");
        assert_eq!(breakdown["executor-0"]["cpu_time_ms"], "1000");
        assert_eq!(breakdown["executor-1"]["peak_memory_bytes"], "300");
        assert_eq!(breakdown["outputs"]["cpu_time_ms"], "25");
    }

    #[test]
    fn transporter_only_usage_omits_task_level_keys() {
        let metadata =
            resource_usage_metadata(&[usage("inputs", Some(500), Some(500), Some(1), Some(50))])
                .expect("should have metadata");

        assert!(metadata.get("peak_memory_bytes").is_none());
        assert!(metadata.get("avg_memory_bytes").is_none());
        assert!(metadata.get("cpu_time_ms").is_none());
        assert_eq!(metadata["resource_usage"]["inputs"]["cpu_time_ms"], "50");
    }

    #[test]
    fn partial_dimensions_are_omitted() {
        let metadata =
            resource_usage_metadata(&[usage("executor-0", None, None, Some(0), Some(123))])
                .expect("should have metadata");

        assert!(metadata.get("peak_memory_bytes").is_none());
        assert!(metadata.get("avg_memory_bytes").is_none());
        assert_eq!(metadata["cpu_time_ms"], "123");
    }
}
