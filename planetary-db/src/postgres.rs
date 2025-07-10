//! Implementation of a TES database using PostgreSQL.

use std::future::Future;
use std::pin::Pin;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use diesel::Connection;
use diesel::sql_types::BigInt;
use diesel::sql_types::Bool;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_migrations::EmbeddedMigrations;
use diesel_migrations::HarnessWithOutput;
use diesel_migrations::MigrationHarness;
use diesel_migrations::embed_migrations;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tes::v1::types::requests::DEFAULT_PAGE_SIZE;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::ListTasksParams;
use tes::v1::types::requests::Task as TesTask;
use tes::v1::types::requests::View;
use tes::v1::types::responses::ExecutorLog;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::responses::Task;
use tes::v1::types::responses::TaskLog;
use tes::v1::types::responses::TaskResponse;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::types::task::Resources;
use tes::v1::types::task::State;
use tracing::info;

use super::Database;
use super::DatabaseResult;
use super::FinishedPod;
use super::PodKind;
use super::PodState;
use super::TaskIo;

pub(crate) mod models;
#[allow(clippy::missing_docs_in_private_items)]
pub(crate) mod schema;

/// Used to embed the migrations into the binary so they can be applied at
/// runtime.
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/postgres/migrations");

/// Helper for zipping two uneven iterators.
///
/// The shorter iterator will yield default values after it terminates.
fn zip_longest<A, B>(a: A, b: B) -> impl Iterator<Item = (A::Item, B::Item)>
where
    A: IntoIterator,
    A::Item: Default,
    B: IntoIterator,
    B::Item: Default,
{
    let mut a = a.into_iter();
    let mut b = b.into_iter();
    std::iter::from_fn(move || match (a.next(), b.next()) {
        (None, None) => None,
        (a, b) => Some((a.unwrap_or_default(), b.unwrap_or_default())),
    })
}

/// Represents a PostgreSQL database error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provided TES task identifier was not found.
    #[error("task `{0}` was not found")]
    TaskNotFound(String),
    /// A diesel connection pool error occurred.
    #[error("a connection could not be established to the database server: {0}")]
    Pool(#[from] diesel_async::pooled_connection::deadpool::PoolError),
    /// A diesel error occurred.
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
}

/// Converts a task model into a TES task.
fn into_task<T, P>(task: T, task_pods: Vec<P>) -> Task
where
    T: Into<(Task, Vec<OutputFile>, Vec<String>)>,
    P: Into<ExecutorLog>,
{
    let (mut task, outputs, system_logs) = task.into();
    let executor_logs: Vec<_> = task_pods.into_iter().map(Into::into).collect();

    if !outputs.is_empty() || !executor_logs.is_empty() || !system_logs.is_empty() {
        let start_time = executor_logs.first().and_then(|e| e.start_time);
        let end_time = executor_logs.last().and_then(|e| e.end_time);

        task.logs = Some(vec![TaskLog {
            logs: executor_logs,
            metadata: None,
            start_time,
            end_time,
            outputs,
            system_logs: if system_logs.is_empty() {
                None
            } else {
                Some(system_logs)
            },
        }]);
    }

    task
}

/// Implements a planetary database using a PostgreSQL server.
pub struct PostgresDatabase {
    /// The database URL.
    url: SecretString,
    /// The database connection pool.
    pool: Pool<AsyncPgConnection>,
}

impl PostgresDatabase {
    /// Constructs a new PostgreSQL database with the given database URL.
    pub fn new(url: SecretString) -> Result<Self> {
        let config = AsyncDieselConnectionManager::new(url.expose_secret());
        Ok(Self {
            url,
            pool: Pool::builder(config)
                .build()
                .context("failed to initialize PostgreSQL connection pool")?,
        })
    }

    /// Runs any pending migrations for the database.
    pub async fn run_pending_migrations(&self) -> Result<()> {
        struct Writer;
        impl std::io::Write for Writer {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                let buf = String::from_utf8_lossy(buf);
                info!("{buf}", buf = buf.trim_end());
                Ok(buf.len())
            }

            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }

        // Required to use a direct connection here as `diesel-migration` doesn't
        // support async
        let mut conn = diesel::pg::PgConnection::establish(self.url.expose_secret())?;
        HarnessWithOutput::new(&mut conn, std::io::LineWriter::new(Writer))
            .run_pending_migrations(MIGRATIONS)
            .map_err(|e| anyhow!("failed to run pending database migrations: {e}"))?;

        Ok(())
    }
}

#[async_trait::async_trait]
impl Database for PostgresDatabase {
    fn url(&self) -> &SecretString {
        &self.url
    }

    async fn try_with_lock(
        &self,
        lock_id: i64,
        fut: Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>,
    ) -> DatabaseResult<Result<bool>> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        // Helper for getting the result of the `pg_try_advisory_xact_lock` function
        struct Acquired(bool);

        impl<DB> QueryableByName<DB> for Acquired
        where
            DB: backend::Backend,
            bool: deserialize::FromSql<Bool, DB>,
        {
            fn build<'a>(row: &impl row::NamedRow<'a, DB>) -> deserialize::Result<Self> {
                let acquired = row::NamedRow::get::<Bool, _>(row, "acquired")?;
                Ok(Self(acquired))
            }
        }

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;
        let transaction = conn.transaction(|conn| {
            async move {
                // Acquire a transaction advisory lock
                let acquired: Acquired =
                    sql_query("SELECT pg_try_advisory_xact_lock($1) AS acquired")
                        .bind::<BigInt, _>(lock_id)
                        .get_result(conn)
                        .await
                        .map_err(Error::Diesel)?;

                if !acquired.0 {
                    return Ok(false);
                }

                // Complete the future
                fut.await?;
                Ok(true)
            }
            .scope_boxed()
        });

        let result = transaction.await;
        Ok(result)
    }

    async fn insert_task(&self, task: &TesTask) -> DatabaseResult<String> {
        use diesel_async::RunQueryDsl;

        let task = models::NewTask::new(task);

        // Insert the task
        let mut conn = self.pool.get().await.map_err(Error::Pool)?;
        diesel::insert_into(schema::tasks::table)
            .values(&task)
            .execute(&mut conn)
            .await
            .map_err(Error::Diesel)?;

        Ok(task.tes_id)
    }

    async fn get_task(&self, tes_id: &str, params: GetTaskParams) -> DatabaseResult<TaskResponse> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        match params.view {
            View::Minimal => Ok(TaskResponse::Minimal(
                schema::tasks::table
                    .select(models::MinimalTask::as_select())
                    .filter(schema::tasks::tes_id.eq(tes_id))
                    .first(&mut conn)
                    .await
                    .optional()
                    .map_err(Error::Diesel)?
                    .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?
                    .into(),
            )),
            View::Basic => {
                let task = schema::tasks::table
                    .select(models::BasicTask::as_select())
                    .filter(schema::tasks::tes_id.eq(tes_id))
                    .first(&mut conn)
                    .await
                    .optional()
                    .map_err(Error::Diesel)?
                    .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

                let task_pods = models::BasicPod::belonging_to(&task)
                    .select(models::BasicPod::as_select())
                    .filter(
                        schema::pods::kind
                            .eq(models::PodKind::Executor)
                            .and(schema::pods::exit_code.is_not_null()),
                    )
                    .order_by(schema::pods::executor_index)
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?;

                Ok(TaskResponse::Basic(into_task(task, task_pods)))
            }
            View::Full => {
                let task = schema::tasks::table
                    .select(models::FullTask::as_select())
                    .filter(schema::tasks::tes_id.eq(tes_id))
                    .first(&mut conn)
                    .await
                    .optional()
                    .map_err(Error::Diesel)?
                    .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

                let task_pods = models::FullPod::belonging_to(&task)
                    .select(models::FullPod::as_select())
                    .filter(
                        schema::pods::kind
                            .eq(models::PodKind::Executor)
                            .and(schema::pods::exit_code.is_not_null()),
                    )
                    .order_by(schema::pods::executor_index)
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?;

                Ok(TaskResponse::Full(into_task(task, task_pods)))
            }
        }
    }

    async fn get_tasks(
        &self,
        params: ListTasksParams,
    ) -> DatabaseResult<(Vec<TaskResponse>, Option<String>)> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut query = schema::tasks::table.into_boxed();

        // Add the name prefix to the query
        if let Some(prefix) = &params.name_prefix {
            query = query.filter(schema::tasks::name.like(format!("{prefix}%")));
        }

        // Add the state to the query
        if let Some(state) = params.state {
            query = query.filter(schema::tasks::state.eq(models::TaskState::from(state)));
        }

        // Add the page token to the query
        let offset = if let Some(page_token) = params.page_token {
            let offset: i64 = page_token
                .parse()
                .map_err(|_| super::Error::InvalidPageToken(page_token.clone()))?;

            if offset < 0 {
                return Err(super::Error::InvalidPageToken(page_token));
            }

            query = query.offset(offset);
            offset
        } else {
            0
        };

        // Add the tags to the query
        for (k, v) in zip_longest(params.tag_keys, params.tag_values) {
            if !v.is_empty() {
                query = query.filter(
                    schema::tasks::tags.contains(models::Json(models::TagFilter::new(k, v))),
                );
            } else {
                query = query.filter(schema::tasks::tags.has_key(k));
            }
        }

        // Add the page size to the query and order by the id
        let page_size = params.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
        query = query.limit(page_size as i64).order_by(schema::tasks::id);

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        match params.view {
            View::Minimal => {
                let tasks = query
                    .select(models::MinimalTask::as_select())
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?;

                let token = if tasks.len() < page_size as usize {
                    None
                } else {
                    Some((offset as usize + tasks.len()).to_string())
                };

                Ok((
                    tasks
                        .into_iter()
                        .map(|t| TaskResponse::Minimal(t.into()))
                        .collect(),
                    token,
                ))
            }
            View::Basic => {
                let tasks: Vec<_> = query
                    .select(models::BasicTask::as_select())
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?
                    .into_iter()
                    .collect();

                let token = if tasks.len() < page_size as usize {
                    None
                } else {
                    Some((offset as usize + tasks.len()).to_string())
                };

                Ok((
                    models::BasicPod::belonging_to(&tasks)
                        .select(models::BasicPod::as_select())
                        .filter(
                            schema::pods::kind
                                .eq(models::PodKind::Executor)
                                .and(schema::pods::exit_code.is_not_null()),
                        )
                        .order_by(schema::pods::executor_index)
                        .load(&mut conn)
                        .await
                        .map_err(Error::Diesel)?
                        .grouped_by(&tasks)
                        .into_iter()
                        .zip(tasks)
                        .map(|(task_pods, task)| TaskResponse::Basic(into_task(task, task_pods)))
                        .collect(),
                    token,
                ))
            }
            View::Full => {
                let tasks: Vec<_> = query
                    .select(models::FullTask::as_select())
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?
                    .into_iter()
                    .collect();

                let token = if tasks.len() < page_size as usize {
                    None
                } else {
                    Some((offset as usize + tasks.len()).to_string())
                };

                Ok((
                    models::FullPod::belonging_to(&tasks)
                        .select(models::FullPod::as_select())
                        .filter(
                            schema::pods::kind
                                .eq(models::PodKind::Executor)
                                .and(schema::pods::exit_code.is_not_null()),
                        )
                        .order_by(schema::pods::executor_index)
                        .load(&mut conn)
                        .await
                        .map_err(Error::Diesel)?
                        .grouped_by(&tasks)
                        .into_iter()
                        .zip(tasks)
                        .map(|(task_pods, task)| TaskResponse::Full(into_task(task, task_pods)))
                        .collect(),
                    token,
                ))
            }
        }
    }

    async fn get_task_io(&self, tes_id: &str) -> DatabaseResult<TaskIo> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        let (inputs, outputs, volumes, size_gb): (
            Option<models::Json<Vec<Input>>>,
            Option<models::Json<Vec<Output>>>,
            Option<Vec<Option<String>>>,
            Option<f64>,
        ) = schema::tasks::table
            .select((
                schema::tasks::inputs,
                schema::tasks::outputs,
                schema::tasks::volumes,
                schema::tasks::disk_gb,
            ))
            .filter(schema::tasks::tes_id.eq(tes_id))
            .first(&mut conn)
            .await
            .optional()
            .map_err(Error::Diesel)?
            .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

        Ok(TaskIo {
            inputs: inputs.map(models::Json::into_inner).unwrap_or_default(),
            outputs: outputs.map(models::Json::into_inner).unwrap_or_default(),
            volumes: volumes
                .unwrap_or_default()
                .into_iter()
                .map(Option::unwrap)
                .collect(),
            size_gb,
        })
    }

    async fn get_task_executor(
        &self,
        tes_id: &str,
        executor_index: usize,
    ) -> DatabaseResult<Option<(Executor, Resources)>> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        let (executor, cpu_cores, preemptible, ram_gb, disk_gb): (
            Option<models::Json<Executor>>,
            Option<i32>,
            Option<bool>,
            Option<f64>,
            Option<f64>,
        ) = schema::tasks::table
            .select((
                schema::tasks::executors
                    .retrieve_as_object(
                        i32::try_from(executor_index).expect("executor index is out of range"),
                    )
                    .nullable(),
                schema::tasks::cpu_cores,
                schema::tasks::preemptible,
                schema::tasks::ram_gb,
                schema::tasks::disk_gb,
            ))
            .filter(schema::tasks::tes_id.eq(tes_id))
            .first(&mut conn)
            .await
            .optional()
            .map_err(Error::Diesel)?
            .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

        Ok(executor.map(|e| {
            (
                e.into_inner(),
                Resources {
                    cpu_cores,
                    preemptible,
                    ram_gb,
                    disk_gb,
                    ..Default::default()
                },
            )
        }))
    }

    async fn update_task_state(
        &self,
        tes_id: &str,
        state: State,
        messages: &[&str],
    ) -> DatabaseResult<bool> {
        use diesel::pg::sql_types::Array;
        use diesel::sql_types::Text;
        use diesel::*;
        use diesel_async::RunQueryDsl;
        use models::TaskState;

        // Determine the allowed previous state for the task.
        let previous = match state {
            // Unknown has no previous state and paused isn't supported
            State::Unknown | State::Paused => {
                return Ok(false);
            }
            // Unknown -> Queued
            State::Queued => &[TaskState::Unknown] as &[TaskState],
            // [Unknown | Queued] -> Initializing
            State::Initializing => &[TaskState::Unknown, TaskState::Queued],
            // [Unknown | Queued | Initializing] -> Running
            State::Running => &[
                TaskState::Unknown,
                TaskState::Queued,
                TaskState::Initializing,
            ],
            // [Unknown | Queued | Initializing | Running] -> [Complete | ExecutorError]
            State::Complete | State::ExecutorError => &[
                TaskState::Unknown,
                TaskState::Queued,
                TaskState::Initializing,
                TaskState::Running,
            ],
            // [Unknown | Queued | Initializing | Running] -> [SystemError | Canceling]
            State::SystemError | State::Canceling => &[
                TaskState::Unknown,
                TaskState::Queued,
                TaskState::Initializing,
                TaskState::Running,
            ],
            // Canceling -> Canceled
            State::Canceled => &[TaskState::Canceling],
            // [Unknown | Queued | Initializing | Running] -> Preempted
            State::Preempted => &[
                TaskState::Unknown,
                TaskState::Queued,
                TaskState::Initializing,
                TaskState::Running,
            ],
        };

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        // TODO: currently diesel hasn't released support for the PostgreSQL
        // `array_cat` function; remove the raw query when diesel supports it
        let updated = sql_query(
            "UPDATE tasks SET state = $1, system_logs = array_cat(system_logs, $2) WHERE tes_id = \
             $3 AND state = ANY ($4)",
        )
        .bind::<schema::sql_types::TaskState, _>(TaskState::from(state))
        .bind::<Array<Text>, _>(messages)
        .bind::<Text, _>(tes_id)
        .bind::<Array<schema::sql_types::TaskState>, _>(previous)
        .execute(&mut conn)
        .await
        .map_err(Error::Diesel)?;

        Ok(updated == 1)
    }

    async fn insert_pod(
        &self,
        tes_id: &str,
        name: &str,
        kind: PodKind,
        executor_index: Option<usize>,
    ) -> DatabaseResult<bool> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        let transaction = conn.transaction(|conn| {
            async move {
                // Find the task and its state
                let task = schema::tasks::table
                    .select(models::MinimalTask::as_select())
                    .filter(schema::tasks::tes_id.eq(tes_id))
                    .for_update()
                    .first(conn)
                    .await
                    .optional()
                    .map_err(Error::Diesel)?
                    .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

                match task.state {
                    models::TaskState::Canceling | models::TaskState::Canceled => {
                        return Ok::<_, Error>(false);
                    }
                    _ => {}
                }

                // Insert the pod
                diesel::insert_into(schema::pods::table)
                    .values(models::NewPod {
                        task_id: task.id,
                        name,
                        kind: kind.into(),
                        state: PodState::Unknown.into(),
                        executor_index: executor_index
                            .map(|i| i.try_into().expect("executor index is out of range")),
                    })
                    .execute(conn)
                    .await
                    .map_err(Error::Diesel)?;

                Ok(true)
            }
            .scope_boxed()
        });

        let inserted = transaction.await?;
        Ok(inserted)
    }

    /// Updates the state of a pod.
    ///
    /// Returns `Ok(true)` if the status was updated or `Ok(false)` if the
    /// task's current state cannot be transitioned to the given state.
    async fn update_pod_state(
        &self,
        name: &str,
        state: PodState,
        finished: Option<FinishedPod<'_>>,
    ) -> DatabaseResult<bool> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        // Determine the allowed previous state for the pod.
        let previous = match state {
            // Unknown has no previous state
            PodState::Unknown => {
                return Ok(false);
            }
            // [Unknown] -> Waiting
            PodState::Waiting => &[models::PodState::Unknown] as &[models::PodState],
            // [Unknown | Waiting] -> Initializing
            PodState::Initializing => &[models::PodState::Unknown, models::PodState::Waiting],
            // [Unknown | Waiting | Initializing] -> Running
            PodState::Running => &[
                models::PodState::Unknown,
                models::PodState::Waiting,
                models::PodState::Initializing,
            ],
            // [Unknown | Queued | Initializing | Running] -> [Succeeded | Failed]
            PodState::Succeeded | PodState::Failed => &[
                models::PodState::Unknown,
                models::PodState::Waiting,
                models::PodState::Initializing,
                models::PodState::Running,
            ],
            // [Unknown | Waiting | Initializing] => ImagePullError
            PodState::ImagePullError => &[
                models::PodState::Unknown,
                models::PodState::Waiting,
                models::PodState::Initializing,
            ],
        };

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        // Only update if the exit code is still null
        let updated = diesel::update(schema::pods::table)
            .filter(
                schema::pods::name
                    .eq(name)
                    .and(schema::pods::state.eq_any(previous))
                    .and(schema::pods::exit_code.is_null()),
            )
            .set((
                schema::pods::state.eq(models::PodState::from(state)),
                schema::pods::start_time.eq(finished.and_then(|f| f.start_time)),
                schema::pods::end_time.eq(finished.and_then(|f| f.end_time)),
                schema::pods::exit_code.eq(finished.map(|f| f.exit_code)),
                schema::pods::stdout.eq(finished.and_then(|f| f.stdout)),
                schema::pods::stderr.eq(finished.and_then(|f| f.stderr)),
            ))
            .execute(&mut conn)
            .await
            .map_err(Error::Diesel)?;

        Ok(updated == 1)
    }

    async fn find_executing_pod(&self, tes_id: &str) -> DatabaseResult<Option<String>> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        // Find the task
        let task_id: i32 = schema::tasks::table
            .select(schema::tasks::id)
            .filter(schema::tasks::tes_id.eq(tes_id))
            .first(&mut conn)
            .await
            .optional()
            .map_err(Error::Diesel)?
            .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

        let name = schema::pods::table
            .select(schema::pods::name)
            .filter(
                schema::pods::task_id
                    .eq(task_id)
                    .and(schema::pods::state.eq_any(models::PodState::executing())),
            )
            .order_by(schema::pods::id.desc())
            .first(&mut conn)
            .await
            .optional()
            .map_err(Error::Diesel)?;

        Ok(name)
    }

    async fn append_system_log(&self, tes_id: &str, messages: &[&str]) -> DatabaseResult<()> {
        use diesel::pg::sql_types::Array;
        use diesel::sql_types::Text;
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        // Append the log entry
        // TODO: currently diesel hasn't released support for the PostgreSQL
        // `array_cat` function; remove the raw query when diesel supports it
        sql_query("UPDATE tasks SET system_logs = array_cat(system_logs, $1) WHERE tes_id = $2")
            .bind::<Array<Text>, _>(messages)
            .bind::<Text, _>(tes_id)
            .execute(&mut conn)
            .await
            .map_err(Error::Diesel)?;

        Ok(())
    }

    /// Updates the output files of the given task.
    async fn update_task_output_files(
        &self,
        tes_id: &str,
        files: &[OutputFile],
    ) -> DatabaseResult<()> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        diesel::update(schema::tasks::table)
            .filter(
                schema::tasks::tes_id
                    .eq(tes_id)
                    .and(schema::tasks::output_files.is_null()),
            )
            .set(schema::tasks::output_files.eq(models::Json(files)))
            .execute(&mut conn)
            .await
            .map_err(Error::Diesel)?;

        Ok(())
    }
}
