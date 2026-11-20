//! The implementation of the TES task endpoints (v1).

use std::path::Component;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use axum::Extension;
use axum::extract::State;
use glob::Pattern;
use planetary_db::format_log_message;
use planetary_server::Error;
use planetary_server::Json;
use planetary_server::Path;
use planetary_server::Query;
use planetary_server::ServerResponse;
use reqwest::header;
use secrecy::ExposeSecret;
use tes::v1::types::requests::DEFAULT_PAGE_SIZE;
use tes::v1::types::requests::GetTaskParams;
use tes::v1::types::requests::ListTasksParams;
use tes::v1::types::requests::MAX_PAGE_SIZE;
use tes::v1::types::requests::Task as RequestTask;
use tes::v1::types::requests::View;
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

use crate::Username;
use crate::notify_retry;
use crate::retry_durations;

/// Ensures that the given URL is supported by Planetary.
fn ensure_supported_url(url: &str, kind: &str, allow_file_urls: bool) -> Result<()> {
    let url: Url = url
        .parse()
        .with_context(|| format!("invalid {kind} URL `{url}`"))?;

    match url.scheme() {
        "http" | "https" | "az" | "s3" | "gs" => {}
        "file" if allow_file_urls => {}
        scheme => bail!("{kind} URL `{url}` uses unsupported scheme `{scheme}`"),
    }

    // File URLs are required to be convertible to a UTF-8 file path with no
    // parent-directory segments
    if url.scheme() == "file" {
        let path = url.to_file_path().map_err(|_| {
            anyhow!("invalid {kind} file URL `{url}`: host must be empty or `localhost`")
        })?;

        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("{kind} file URL `{url}` cannot contain `..` path segments");
        }

        if path.into_os_string().into_string().is_err() {
            bail!("{kind} URL `{url}` cannot be represented in UTF-8");
        }
    }

    Ok(())
}

/// Performs validation on the request.
fn validate_task(task: &RequestTask, allow_file_urls: bool) -> Result<()> {
    use std::path::Path;

    fn validate_input(input: &Input, allow_file_urls: bool) -> Result<()> {
        let path = Path::new(&input.path);
        if !path.is_absolute() {
            bail!("input path `{path}` is not absolute", path = input.path);
        }

        // The input path cannot contain `..` segments
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!(
                "input path `{path}` cannot contain `..` path segments",
                path = input.path
            );
        }

        match (&input.url, &input.content) {
            (None, None) => bail!("input URL is required"),
            (Some(url), None) => {
                // Check for supported URL schemes
                ensure_supported_url(url, "input", allow_file_urls)?;
            }
            (_, Some(_)) => {
                // If content is specified, URL is ignored; it must be a file
                // type
                if input.ty != IoType::File {
                    bail!("input specifies content but is not a file")
                }
            }
        }

        Ok(())
    }

    fn validate_output(output: &Output, allow_file_urls: bool) -> Result<()> {
        // The output path must be absolute
        let path = Path::new(&output.path);
        if !path.is_absolute() {
            bail!("output path `{path}` is not absolute", path = output.path);
        }

        // The output path cannot contain `..` segments
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!(
                "output path `{path}` cannot contain `..` path segments",
                path = output.path
            );
        }

        // The output path cannot be root
        if output.path == "/" {
            bail!("output path cannot be `/`");
        }

        // Validate the path prefix, if present
        if let Some(prefix) = &output.path_prefix {
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

        // Check for supported URL schemes
        ensure_supported_url(&output.url, "output", allow_file_urls)?;

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

            if disk < 0.0 {
                bail!("task resource disk cannot be less than zero");
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
        let path = Path::new(volume);
        if !path.is_absolute() {
            bail!("volume path `{volume}` is not absolute");
        }

        // The volume path cannot contain `..` segments
        if path.components().any(|c| matches!(c, Component::ParentDir)) {
            bail!("volume path `{volume}` cannot contain `..` path segments");
        }

        Ok(())
    }

    for input in task.inputs.as_deref().unwrap_or_default() {
        validate_input(input, allow_file_urls)?;
    }

    for output in task.outputs.as_deref().unwrap_or_default() {
        validate_output(output, allow_file_urls)?;
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

    validate_mount_paths(task)?;

    Ok(())
}

/// Ensures that no two inputs or outputs share a mount path and that no volume
/// shares an input's mount path.
///
/// Each input and output is mounted separately into the task's containers and
/// Kubernetes rejects pods with duplicate mount paths. A volume that shares an
/// output's mount path (or another volume's) is allowed as the output's mount
/// already provides a writable directory at that path.
fn validate_mount_paths(task: &RequestTask) -> Result<()> {
    use std::path::Path;

    let inputs = task.inputs.as_deref().unwrap_or_default();
    let paths =
        inputs
            .iter()
            .map(|i| (i.path.as_str(), "input path"))
            .chain(task.outputs.as_deref().unwrap_or_default().iter().map(
                |o| match &o.path_prefix {
                    Some(prefix) => (prefix.as_str(), "output path prefix"),
                    None => (o.path.as_str(), "output path"),
                },
            ));

    let mut mounts: Vec<&Path> = Vec::new();
    for (path, kind) in paths {
        if let Some(existing) = mounts.iter().find(|m| **m == Path::new(path)) {
            bail!(
                "{kind} `{path}` duplicates the mount path `{existing}`",
                existing = existing.display()
            );
        }

        mounts.push(Path::new(path));
    }
    for volume in task.volumes.as_deref().unwrap_or_default() {
        if inputs
            .iter()
            .any(|i| Path::new(&i.path) == Path::new(volume))
        {
            bail!("volume path `{volume}` duplicates an input path");
        }
    }

    Ok(())
}

/// The `POST /tasks` route.
pub async fn create_task(
    Extension(username): Extension<Username>,
    State(state): State<crate::State>,
    Json(task): Json<RequestTask>,
) -> ServerResponse<Json<CreatedTask>> {
    if let Err(e) = validate_task(&task, state.allow_file_urls) {
        return Err(Error::bad_request(format!("{e:#}")));
    }

    let id = state.database.insert_task(&username.0, &task).await?;

    // Notify an orchestrator service to start the task
    Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to
            // the server Don't retry if the service returned an
            // error response
            state
                .client
                .post(
                    state
                        .orchestrator
                        .url
                        .join(&format!("/v1/tasks/{id}"))
                        .expect("URL should join"),
                )
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Bearer {token}",
                        token = state.orchestrator.api_key.expose_secret()
                    ),
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
    Extension(username): Extension<Username>,
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

    let (tasks, next_page_token) = state.database.get_tasks(&username.0, params).await?;

    Ok(Json(ListTasks {
        tasks,
        next_page_token,
    }))
}

/// The `GET /tasks/:id` route.
pub async fn get_task(
    Path(id): Path<String>,
    Extension(username): Extension<Username>,
    Query(params): Query<GetTaskParams>,
    State(state): State<crate::State>,
) -> ServerResponse<Json<TaskResponse>> {
    let task = state.database.get_task(&username.0, &id, params).await?;
    Ok(Json(task))
}

/// The `POST /tasks/:id:cancel` route.
pub async fn cancel_task(
    Path(id): Path<String>,
    Extension(username): Extension<Username>,
    State(state): State<crate::State>,
) -> ServerResponse<Json<serde_json::Value>> {
    // TODO: remove this once `matchit` 0.8.6 can be used
    let id = id.strip_suffix(":cancel").ok_or_else(Error::not_found)?;

    // Check that the task is currently in a running state
    // This also ensures that the task is associated with the request's user
    let task = state
        .database
        .get_task(
            &username.0,
            id,
            GetTaskParams {
                view: View::Minimal,
            },
        )
        .await?;
    if !task
        .as_minimal()
        .expect("should be a minimal task")
        .state
        .unwrap_or_default()
        .is_executing()
    {
        return Err(Error::not_cancelable());
    }

    // Mark the task as canceling
    if !state
        .database
        .update_task_state(
            id,
            TesState::Canceling,
            &[&format_log_message!("canceling task `{id}`")],
            None,
            None,
        )
        .await?
    {
        return Err(Error::not_cancelable());
    }

    // Notify the orchestrator service to cancel the task
    Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to
            // the server Don't retry if the service returned an
            // error response
            state
                .client
                .delete(
                    state
                        .orchestrator
                        .url
                        .join(&format!("/v1/tasks/{id}"))
                        .expect("URL should join"),
                )
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Bearer {token}",
                        token = state.orchestrator.api_key.expose_secret()
                    ),
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

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    /// Validates a task with the given inputs, outputs, and volumes.
    fn validate(
        inputs: serde_json::Value,
        outputs: serde_json::Value,
        volumes: &[&str],
    ) -> Result<()> {
        let task: RequestTask = serde_json::from_value(json!({
            "inputs": inputs,
            "outputs": outputs,
            "volumes": volumes,
            "executors": [{ "image": "alpine", "command": ["true"] }],
        }))
        .unwrap();
        validate_task(&task, false)
    }

    #[test]
    fn duplicate_input_and_output_paths_are_rejected() {
        let input = |path: &str| json!({ "path": path, "content": "x", "type": "FILE" });
        let output = |path: &str| json!({ "path": path, "url": "az://a/b", "type": "FILE" });

        let e = validate(json!([input("/mnt/a"), input("/mnt/a/")]), json!([]), &[]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "input path `/mnt/a/` duplicates the mount path `/mnt/a`"
        );

        let e = validate(json!([input("/mnt/a")]), json!([output("/mnt/a")]), &[]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "output path `/mnt/a` duplicates the mount path `/mnt/a`"
        );

        let glob = json!({
            "path": "/mnt/b/*.txt",
            "path_prefix": "/mnt/b",
            "url": "az://a/b",
            "type": "FILE",
        });
        let e = validate(json!([]), json!([output("/mnt/b"), glob]), &[]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "output path prefix `/mnt/b` duplicates the mount path `/mnt/b`"
        );
    }

    #[test]
    fn volumes_cannot_contain_parent_segments() {
        let e = validate(json!([]), json!([]), &["/mnt/x/../usage"]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "volume path `/mnt/x/../usage` cannot contain `..` path segments"
        );
    }

    #[test]
    fn volumes_cannot_duplicate_input_paths() {
        let input = json!({ "path": "/mnt/a", "content": "x", "type": "FILE" });
        let e = validate(json!([input]), json!([]), &["/mnt/a"]).unwrap_err();
        assert_eq!(
            e.to_string(),
            "volume path `/mnt/a` duplicates an input path"
        );
    }

    #[test]
    fn volumes_may_duplicate_output_paths_and_other_volumes() {
        let output = json!({ "path": "/mnt/a", "url": "az://a/b", "type": "DIRECTORY" });
        validate(json!([]), json!([output]), &["/mnt/a", "/mnt/b", "/mnt/b/"]).unwrap();
    }
}
