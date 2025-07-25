//! The implementation of the TES service info endpoints (v1).

use std::sync::Arc;

use axum::extract::State;
use planetary_server::Json;
use tes::v1::types::responses::ServiceInfo;

/// The `GET /service-info` route.
pub async fn service_info(State(state): State<crate::State>) -> Json<Arc<ServiceInfo>> {
    Json(state.info.clone())
}
