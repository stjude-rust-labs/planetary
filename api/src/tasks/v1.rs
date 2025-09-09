//! The implementation of the TES task endpoints (v1).

use std::path::Component;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use axum::extract::State;
use axum::http::StatusCode;
use glob::Pattern;
use planetary_db::format_log_message;
use planetary_server::Error;
use planetary_server::Json;
use planetary_server::Path;
use planetary_server::Query;
use planetary_server::ServerResponse;
use tes::v1::types::requests::DEFAULT_PAGE_SIZE;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::ListTasksParams;
use tes::v1::types::requests::MAX_PAGE_SIZE;
use tes::v1::types::requests::Task as RequestTask;
use tes::v1::types::responses::CreatedTask;
use tes::v1::types::responses::ListTasks;
use tes::v1::types::responses::TaskResponse;
use tes::v1::types::task::Executor;
use tes::v1::types::task::Input;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Output;
use tes::v1::types::task::Resources;
use tes::v1::types::task::State as TesState;
use tokio_retry2::Retry;
use tokio_retry2::RetryError;
use url::Url;

use crate::notify_retry;
use crate::retry_durations;

/// Performs validation on the request.
fn validate_task(task: &RequestTask) -> Result<()> {
    use std::path::Path;

    fn validate_input(input: &Input) -> Result<()> {
        if !Path::new(&input.path).is_absolute() {
            bail!("input path `{path}` is not absolute", path = input.path);
        }

        match (&input.url, &input.content) {
            (None, None) => bail!("input URL is required"),
            (Some(url), None) => {
                let url: Url = url
                    .parse()
                    .map_err(|_| anyhow!("invalid input URL `{url}`"))?;

                match url.scheme() {
                    "http" | "https" | "az" | "s3" | "gs" => {
                        // Supported
                    }
                    scheme => bail!("input URL `{url}` uses unsupported scheme `{scheme}`"),
                }
            }
            (_, Some(_)) => {
                // If content is specified, URL is ignored; it must be a file type
                if input.ty != IoType::File {
                    bail!("input specifies content but is not a file")
                }
            }
        }

        Ok(())
    }

    fn validate_output(output: &Output) -> Result<()> {
        if output.url.is_empty() {
            bail!("output URL is required");
        }

        // The URL must parse
        let url = output
            .url
            .parse::<Url>()
            .map_err(|_| anyhow!("invalid output URL `{url}`", url = output.url))?;

        match url.scheme() {
            "https" | "az" | "s3" | "gs" => {
                // Supported
            }
            scheme => bail!("output URL `{url}` uses unsupported scheme `{scheme}`"),
        }

        // The output path must be absolute
        let path = Path::new(&output.path);
        if !path.is_absolute() {
            bail!("output path `{path}` is not absolute", path = output.path);
        }

        // The output path cannot be root
        if output.path == "/" {
            bail!("output path cannot be `/`");
        }

        // The output path cannot contain `..` segments
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!(
                "output path `{path}` cannot contain `..` path segments",
                path = output.path
            );
        }

        // If a path prefix was specified, then the output must be a directory
        if let Some(prefix) = &output.path_prefix {
            if output.ty != IoType::Directory {
                bail!("output has a path prefix specified but the output type is not `DIRECTORY`");
            }

            // Ensure the output path pattern is valid
            Pattern::new(&output.path).with_context(|| {
                format!("invalid output path pattern `{path}`", path = output.path)
            })?;

            // The path prefix must itself be absolute
            if !Path::new(prefix).is_absolute() {
                bail!("output path prefix `{prefix}` is not absolute");
            }

            // The output path prefix cannot be root
            if prefix == "/" {
                bail!("output path prefix cannot be `/`");
            }

            // The output path must start with the prefix too
            if !output.path.starts_with(prefix) {
                bail!(
                    "output path `{path}` does not start with the prefix `{prefix}`",
                    path = output.path
                );
            }
        }

        Ok(())
    }

    fn validate_executor(executor: &Executor) -> Result<()> {
        if executor.image.is_empty() {
            bail!("executor image is required");
        }

        match executor.command.first() {
            Some(first) if !first.is_empty() => {}
            _ => bail!("executor command must have at least one entry"),
        }

        if let Some(stdin) = &executor.stdin
            && !Path::new(stdin).is_absolute()
        {
            bail!("executor stdin path must be a absolute");
        }

        if let Some(stdout) = &executor.stdout
            && !Path::new(stdout).is_absolute()
        {
            bail!("executor stdout path must be a absolute");
        }

        if let Some(stderr) = &executor.stderr
            && !Path::new(stderr).is_absolute()
        {
            bail!("executor stderr path must be a absolute");
        }

        Ok(())
    }

    fn validate_resources(resources: &Resources) -> Result<()> {
        if let Some(cpus) = resources.cpu_cores
            && cpus <= 0
        {
            bail!("task resource cpu cores must be greater than zero");
        }

        if let Some(ram) = resources.ram_gb {
            let ram = ram.ceil();

            if ram <= 0. {
                bail!("task resource RAM must be greater than zero");
            }

            if ram > u64::MAX as f64 {
                bail!(
                    "task resource RAM cannot exceed `{max}` gigabytes",
                    max = u64::MAX
                );
            }
        }

        if let Some(disk) = resources.disk_gb {
            let disk = disk.ceil();

            if disk <= 0. {
                bail!("task resource disk must be greater than zero");
            }

            if disk > u64::MAX as f64 {
                bail!(
                    "task resource disk cannot exceed `{max}` gigabytes",
                    max = u64::MAX
                );
            }
        }

        if resources.backend_parameters_strict.unwrap_or(false)
            && resources
                .backend_parameters
                .as_ref()
                .map(|p| !p.is_empty())
                .unwrap_or(false)
        {
            bail!("backend parameters are not supported by this implementation");
        }

        Ok(())
    }

    fn validate_volume(volume: &str) -> Result<()> {
        if !Path::new(volume).is_absolute() {
            bail!("volume path `{volume}` is not absolute");
        }

        Ok(())
    }

    for input in task.inputs.as_deref().unwrap_or_default() {
        validate_input(input)?;
    }

    for output in task.outputs.as_deref().unwrap_or_default() {
        validate_output(output)?;
    }

    if task.executors.is_empty() {
        bail!("task must contain at least one executor");
    }

    for executor in &task.executors {
        validate_executor(executor)?;
    }

    if let Some(resources) = &task.resources {
        validate_resources(resources)?;
    }

    for volume in task.volumes.as_deref().unwrap_or_default() {
        validate_volume(volume)?;
    }

    Ok(())
}

/// The `POST /tasks` route.
pub async fn create_task(
    State(state): State<crate::State>,
    Json(task): Json<RequestTask>,
) -> ServerResponse<Json<CreatedTask>> {
    if let Err(e) = validate_task(&task) {
        return Err(Error::bad_request(e.to_string()));
    }

    let id = state.database.insert_task(&task).await?;

    // Notify an orchestrator service to start the task
    Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to the server
            // Don't retry if the service returned an error response
            state
                .client
                .post(
                    state
                        .orchestrator_service
                        .join(&format!("/v1/tasks/{id}"))
                        .expect("URL should join"),
                )
                .send()
                .await
                .map_err(RetryError::transient)?
                .error_for_status()
                .map_err(RetryError::permanent)
        },
        notify_retry,
    )
    .await?;

    Ok(Json(CreatedTask { id }))
}

/// The `GET /tasks` route.
pub async fn list_tasks(
    Query(params): Query<ListTasksParams>,
    State(state): State<crate::State>,
) -> ServerResponse<Json<ListTasks<TaskResponse>>> {
    let page_size = params.page_size.unwrap_or(DEFAULT_PAGE_SIZE);
    if page_size == 0 {
        return Err(Error::bad_request("page size must be greater than zero"));
    }

    if page_size >= MAX_PAGE_SIZE {
        return Err(Error::bad_request(format!(
            "page size must be less than {MAX_PAGE_SIZE}"
        )));
    }

    if params.tag_values.as_ref().map(|v| v.len()).unwrap_or(0)
        > params.tag_keys.as_ref().map(|k| k.len()).unwrap_or(0)
    {
        return Err(Error::bad_request(
            "the number of tag values cannot exceed the number of tag keys",
        ));
    }

    let (tasks, next_page_token) = state.database.get_tasks(params).await?;

    Ok(Json(ListTasks {
        tasks,
        next_page_token,
    }))
}

/// The `GET /tasks/:id` route.
pub async fn get_task(
    Path(id): Path<String>,
    Query(params): Query<GetTaskParams>,
    State(state): State<crate::State>,
) -> ServerResponse<Json<TaskResponse>> {
    let task = state.database.get_task(&id, params).await?;
    Ok(Json(task))
}

/// The `POST /tasks/:id:cancel` route.
pub async fn cancel_task(
    Path(id): Path<String>,
    State(state): State<crate::State>,
) -> ServerResponse<Json<serde_json::Value>> {
    // TODO: remove this once `matchit` 0.8.6 can be used
    let id = id.strip_suffix(":cancel").ok_or_else(Error::not_found)?;

    // Mark the task as canceling
    if !state
        .database
        .update_task_state(
            id,
            TesState::Canceling,
            &[&format_log_message!("canceling task `{id}`")],
        )
        .await?
    {
        return Err(Error {
            status: StatusCode::BAD_REQUEST,
            message: "task is not in a cancelable state".into(),
        });
    }

    // Notify the orchestrator service to cancel the task
    Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to the server
            // Don't retry if the service returned an error response
            state
                .client
                .delete(
                    state
                        .orchestrator_service
                        .join(&format!("/v1/tasks/{id}"))
                        .expect("URL should join"),
                )
                .send()
                .await
                .map_err(RetryError::transient)?
                .error_for_status()
                .map_err(RetryError::permanent)
        },
        notify_retry,
    )
    .await?;

    // The spec uses a 200 response with an empty JSON object
    Ok(Json(serde_json::Value::Object(Default::default())))
}
