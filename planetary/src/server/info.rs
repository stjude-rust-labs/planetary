//! Routes related to the service information.

use axum::Router;
use axum::routing::get;

use crate::server;

mod v1;

/// Gets the router for the service information.
pub fn router() -> Router<server::State> {
    Router::new().route("/v1/service-info", get(v1::service_info))
}
