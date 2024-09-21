//! Routes related to the service information.

use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::routing::get;
use tes::v1::types::responses::ServiceInfo;

/// Gets the router for the service information.
pub fn router() -> Router<crate::server::State> {
    Router::new().route("/v1/service-info", get(service_info))
}

/// The `GET /service-info` route.
async fn service_info(State(state): State<crate::server::State>) -> Json<ServiceInfo> {
    Json((*state.info).clone())
}
