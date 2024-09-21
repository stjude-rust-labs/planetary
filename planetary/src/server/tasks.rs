//! Routes related to tasks.

use axum::Json;
use axum::Router;
use axum::extract::Path;
use axum::extract::Query;
use axum::extract::State;
use axum::routing::get;
use axum::routing::post;
use serde::Deserialize;
use serde::Serialize;
use tes::v1::types::Task;
use tes::v1::types::responses::CreateTask;
use tes::v1::types::responses::ListTasks;
use tes::v1::types::responses::task::Response as TaskResponse;
use tes::v1::types::responses::task::View;
use tracing::debug;

use crate::name::Generator;

/// Gets the router for the service information.
pub fn router() -> Router<crate::server::State> {
    Router::new()
        .route("/v1/tasks", get(get_tasks))
        .route("/v1/tasks", post(post_tasks))
        .route("/v1/tasks/:id", get(get_tasks_by))
}

/// The `POST /tasks` route.
async fn post_tasks(
    State(state): State<crate::server::State>,
    Json(task): Json<Task>,
) -> Json<CreateTask> {
    // TODO(clay): use Braden's name generator, remove `name.rs`.
    let name = state.names.generate();
    debug!("task: {task:?}");
    state.engine.queue(name.clone(), task).await;
    Json(CreateTask { id: name })
}

/// A method to get the default view of the task-related routes.
fn default_view() -> View {
    View::Minimal
}

/// The query parameters for task endpoints.
#[derive(Debug, Deserialize, Serialize)]
pub struct TaskQueryParams {
    /// Gets the view of the task(s).
    #[serde(default = "default_view")]
    view: View,
}

/// The `GET /tasks` route.
async fn get_tasks(
    Query(params): Query<TaskQueryParams>,
    State(state): State<crate::server::State>,
) -> Json<ListTasks<TaskResponse>> {
    let states = state.engine.all_states(params.view).await;
    Json(ListTasks {
        tasks: states,
        next_page_token: None,
    })
}

/// The `GET /tasks/:id` route.
async fn get_tasks_by(
    Path(id): Path<String>,
    Query(params): Query<TaskQueryParams>,
    State(state): State<crate::server::State>,
) -> Json<TaskResponse> {
    Json(
        state
            .engine
            .state(id, params.view)
            .await
            .expect("task not found"),
    )
}
