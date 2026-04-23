//! Routes related to tasks.

use axum::Router;
use axum::middleware;
use axum::routing::get;
use axum::routing::post;

use crate::State;

mod v1;

/// Gets the router for the service information.
pub fn router(allow_authorization_fallback: bool) -> Router<State> {
    Router::new()
        .without_v07_checks()
        .route("/v1/tasks", get(v1::list_tasks))
        .route("/v1/tasks", post(v1::create_task))
        .route("/v1/tasks/{id}", get(v1::get_task))
        // TODO: the path should be `/v1/tasks/{id}:cancel`, but that's not supported until
        // `matchit`` 0.8.6; we're currently on 0.8.4 via `axum`
        .route("/v1/tasks/{id}", post(v1::cancel_task))
        .layer(middleware::from_fn(move |a, r, n| {
            crate::auth(allow_authorization_fallback, a, r, n)
        }))
}
