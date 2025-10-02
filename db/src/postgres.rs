//! Implementation of a TES database using PostgreSQL.

use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use chrono::DateTime;
use chrono::Utc;
use diesel::Connection;
use diesel_async::AsyncConnection;
use diesel_async::AsyncPgConnection;
use diesel_async::pooled_connection::AsyncDieselConnectionManager;
use diesel_async::pooled_connection::deadpool::Pool;
use diesel_async::scoped_futures::ScopedFutureExt;
use diesel_migrations::EmbeddedMigrations;
use diesel_migrations::HarnessWithOutput;
use diesel_migrations::MigrationHarness;
use diesel_migrations::embed_migrations;
use futures::future::BoxFuture;
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
use tes::v1::types::task::Input;
use tes::v1::types::task::Output;
use tes::v1::types::task::State;
use tracing::debug;
use tracing::info;

use super::Database;
use super::DatabaseResult;
use super::TaskIo;
use crate::TerminatedContainer;

pub(crate) mod models;
#[allow(clippy::missing_docs_in_private_items)]
pub(crate) mod schema;

/// Used to embed the migrations into the binary so they can be applied at
/// runtime.
const MIGRATIONS: EmbeddedMigrations = embed_migrations!("src/postgres/migrations");

/// The interval between attempts to retain unexpired connections from the
/// connection pool.
const POOL_RETAIN_INTERVAL: Duration = Duration::from_secs(30);

/// The maximum age a database connection will remain in the pool since it was
/// last used.
const MAX_CONNECTION_AGE: Duration = Duration::from_secs(60);

/// The maximum number of connections in the connection pool.
///
/// This is currently a fixed-size limit as we keep a connection pool per-pod.
const MAX_POOL_SIZE: usize = 10;

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

/// Formats the Postgres database URL.
pub fn format_database_url(
    user: &str,
    password: &SecretString,
    host: &str,
    port: i32,
    database_name: &str,
    app_name: &str,
) -> String {
    format!(
        "postgres://{user}:{password}@{host}:{port}/{database_name}?application_name={app_name}",
        password = password.expose_secret(),
    )
}

/// Represents a PostgreSQL database error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// The provided TES task identifier was not found.
    #[error("task `{0}` was not found")]
    TaskNotFound(String),
    /// A diesel connection pool error occurred.
    #[error(transparent)]
    Pool(#[from] diesel_async::pooled_connection::deadpool::PoolError),
    /// A diesel error occurred.
    #[error(transparent)]
    Diesel(#[from] diesel::result::Error),
}

/// Converts a task model into a TES task.
fn into_task<T, C>(task: T, containers: Vec<C>) -> Task
where
    T: Into<(Task, Vec<OutputFile>, Vec<String>)>,
    C: Into<ExecutorLog>,
{
    let (mut task, outputs, system_logs) = task.into();
    let executor_logs: Vec<_> = containers.into_iter().map(Into::into).collect();

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
        debug!("creating database connection pool with {MAX_POOL_SIZE} slots");

        let pool = Pool::builder(config)
            .max_size(MAX_POOL_SIZE)
            .build()
            .context("failed to initialize PostgreSQL connection pool")?;

        let p = pool.clone();

        // Span a task that is responsible for removing connections from the pool that
        // exceed
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(POOL_RETAIN_INTERVAL).await;

                let res = p.retain(|_, metrics| metrics.last_used() < MAX_CONNECTION_AGE);

                debug!(
                    "removed {removed} and retained {retained} connections(s) from the database \
                     connection pool",
                    removed = res.removed.len(),
                    retained = res.retained
                );
            }
        });

        Ok(Self { url, pool })
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

                let containers = models::BasicContainer::belonging_to(&task)
                    .select(models::BasicContainer::as_select())
                    .filter(schema::containers::executor_index.is_not_null())
                    .order_by(schema::containers::executor_index)
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?;

                Ok(TaskResponse::Basic(into_task(task, containers)))
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

                let containers = models::FullContainer::belonging_to(&task)
                    .select(models::FullContainer::as_select())
                    .filter(schema::containers::executor_index.is_not_null())
                    .order_by(schema::containers::executor_index)
                    .load(&mut conn)
                    .await
                    .map_err(Error::Diesel)?;

                Ok(TaskResponse::Full(into_task(task, containers)))
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
        for (k, v) in zip_longest(
            params.tag_keys.unwrap_or_default(),
            params.tag_values.unwrap_or_default(),
        ) {
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

        match params.view.unwrap_or_default() {
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
                    models::BasicContainer::belonging_to(&tasks)
                        .select(models::BasicContainer::as_select())
                        .filter(schema::containers::executor_index.is_not_null())
                        .order_by(schema::containers::executor_index)
                        .load(&mut conn)
                        .await
                        .map_err(Error::Diesel)?
                        .grouped_by(&tasks)
                        .into_iter()
                        .zip(tasks)
                        .map(|(containers, task)| TaskResponse::Basic(into_task(task, containers)))
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
                    models::FullContainer::belonging_to(&tasks)
                        .select(models::FullContainer::as_select())
                        .filter(schema::containers::executor_index.is_not_null())
                        .order_by(schema::containers::executor_index)
                        .load(&mut conn)
                        .await
                        .map_err(Error::Diesel)?
                        .grouped_by(&tasks)
                        .into_iter()
                        .zip(tasks)
                        .map(|(containers, task)| TaskResponse::Full(into_task(task, containers)))
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

        let (inputs, outputs): (
            Option<models::Json<Vec<Input>>>,
            Option<models::Json<Vec<Output>>>,
        ) = schema::tasks::table
            .select((schema::tasks::inputs, schema::tasks::outputs))
            .filter(schema::tasks::tes_id.eq(tes_id))
            .first(&mut conn)
            .await
            .optional()
            .map_err(Error::Diesel)?
            .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?;

        Ok(TaskIo {
            inputs: inputs.map(models::Json::into_inner).unwrap_or_default(),
            outputs: outputs.map(models::Json::into_inner).unwrap_or_default(),
        })
    }

    async fn get_in_progress_tasks(&self, before: DateTime<Utc>) -> DatabaseResult<Vec<String>> {
        use diesel::pg::sql_types::Timestamptz;
        use diesel::*;
        use diesel_async::RunQueryDsl;
        use models::TaskState;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        Ok(schema::tasks::table
            .select(schema::tasks::tes_id)
            .filter(
                schema::tasks::state
                    .eq_any(&[
                        TaskState::Unknown,
                        TaskState::Queued,
                        TaskState::Initializing,
                        TaskState::Running,
                    ])
                    .and(schema::tasks::creation_time.le(before.into_sql::<Timestamptz>())),
            )
            .get_results(&mut conn)
            .await
            .map_err(Error::Diesel)?)
    }

    async fn update_task_state<'a>(
        &self,
        tes_id: &str,
        state: State,
        messages: &[&str],
        containers: Option<BoxFuture<'a, Result<Vec<TerminatedContainer<'a>>>>>,
    ) -> DatabaseResult<bool> {
        use diesel::pg::sql_types::Array;
        use diesel::sql_types::Text;
        use diesel::*;
        use diesel_async::RunQueryDsl;
        use models::TaskState;

        /// Helper for getting the id for an updated task.
        /// This is required because `sql_query` returns data by name, not
        /// index.
        #[derive(QueryableByName)]
        #[diesel(table_name = schema::tasks)]
        #[diesel(check_for_backend(diesel::pg::Pg))]
        struct UpdatedTask {
            /// The id of the updated task.
            id: i32,
        }

        // Determine the allowed previous state for the task.
        let previous: &[TaskState] = match state {
            // Unknown has no previous state and paused isn't supported
            State::Unknown | State::Paused => {
                return Ok(false);
            }
            // Unknown -> Queued
            State::Queued => &[TaskState::Unknown],
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

        let updated = conn
            .transaction(|conn| {
                async move {
                    // TODO: currently diesel hasn't released support for the PostgreSQL
                    // `array_cat` function; remove the raw query when diesel supports it
                    let updated: Option<UpdatedTask> = sql_query(
                        "UPDATE tasks SET state = $1, system_logs = array_cat(system_logs, $2) \
                         WHERE tes_id = $3 AND state = ANY ($4) RETURNING id",
                    )
                    .bind::<schema::sql_types::TaskState, _>(TaskState::from(state))
                    .bind::<Array<Text>, _>(messages)
                    .bind::<Text, _>(tes_id)
                    .bind::<Array<schema::sql_types::TaskState>, _>(previous)
                    .get_result(conn)
                    .await
                    .optional()
                    .map_err(Error::Diesel)?;

                    match updated {
                        Some(UpdatedTask { id }) => {
                            if let Some(containers) = containers {
                                // Insert the containers
                                let containers = containers.await?;
                                diesel::insert_into(schema::containers::table)
                                    .values(
                                        containers
                                            .into_iter()
                                            .map(|c| models::NewContainer::new(id, c))
                                            .collect::<Vec<_>>(),
                                    )
                                    .on_conflict_do_nothing()
                                    .execute(conn)
                                    .await
                                    .map_err(Error::Diesel)?;
                            }

                            anyhow::Ok(true)
                        }
                        None => Ok(false),
                    }
                }
                .scope_boxed()
            })
            .await?;

        Ok(updated)
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

    async fn insert_error(
        &self,
        source: &str,
        tes_id: Option<&str>,
        message: &str,
    ) -> DatabaseResult<()> {
        use diesel::*;
        use diesel_async::RunQueryDsl;

        let mut conn = self.pool.get().await.map_err(Error::Pool)?;

        let transaction = conn.transaction(|conn| {
            async move {
                // Lookup the associated task id, if there is one
                let task_id = if let Some(tes_id) = tes_id {
                    Some(
                        schema::tasks::table
                            .select(schema::tasks::id)
                            .filter(schema::tasks::tes_id.eq(tes_id))
                            .for_update()
                            .first(conn)
                            .await
                            .optional()
                            .map_err(Error::Diesel)?
                            .ok_or_else(|| Error::TaskNotFound(tes_id.to_string()))?,
                    )
                } else {
                    None
                };

                // Insert the new error
                diesel::insert_into(schema::errors::table)
                    .values(models::NewError {
                        source,
                        task_id,
                        message,
                    })
                    .execute(conn)
                    .await
                    .map_err(Error::Diesel)
            }
            .scope_boxed()
        });

        transaction.await?;
        Ok(())
    }
}
