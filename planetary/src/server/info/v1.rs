//! The implementation of the TES service info endpoints (v1).

use std::sync::Arc;

use axum::extract::State;
use tes::v1::types::responses::ServiceInfo;

use crate::server;
use crate::server::Json;

/// The `GET /service-info` route.
pub async fn service_info(State(state): State<server::State>) -> Json<Arc<ServiceInfo>> {
    Json(state.info.clone())
}
