//! The server.

use std::sync::Arc;

use axum::Router;
use axum::http::HeaderName;
use axum::http::header;
use eyre::Context as _;
use tes::v1::types::responses::ServiceInfo;
use tokio::net::TcpListener;
use tower::ServiceBuilder;
use tower_http::LatencyUnit;
use tower_http::compression::CompressionLayer;
use tower_http::sensitive_headers::SetSensitiveRequestHeadersLayer;
use tower_http::sensitive_headers::SetSensitiveResponseHeadersLayer;
use tower_http::trace::DefaultMakeSpan;
use tower_http::trace::DefaultOnResponse;
use tower_http::trace::TraceLayer;
use tracing::info;

mod builder;
mod service_info;
mod tasks;

pub use builder::Builder;

use crate::Engine;
use crate::name;

/// Header values to be blocked from logging.
pub const SENSITIVE_HEADERS: [HeaderName; 2] = [header::AUTHORIZATION, header::COOKIE];

/// The state for the server.
#[derive(Clone)]
pub struct State {
    /// The service information.
    pub info: Arc<ServiceInfo>,

    /// The engine.
    pub engine: Arc<Engine>,

    /// A name generator.
    pub names: Arc<name::Alphanumeric>,
}

impl std::fmt::Debug for State {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("State")
            .field("info", &self.info)
            .field("client", &"<OPAQUE>")
            .finish()
    }
}

/// The state for a task execution service (TES) server.
#[derive(Clone, Debug)]
pub struct Server {
    /// The host to bind to.
    host: String,

    /// The port to bind to.
    port: usize,

    /// The state.
    state: State,
}

impl Server {
    /// Gets a builder for a [`Server`].
    pub fn builder() -> Builder {
        Builder::default()
    }

    /// Runs the server.
    pub async fn run(self) -> eyre::Result<()> {
        let middleware = ServiceBuilder::new()
            .layer(SetSensitiveRequestHeadersLayer::new(SENSITIVE_HEADERS))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().include_headers(true))
                    .on_response(
                        DefaultOnResponse::new()
                            .level(tracing::Level::DEBUG)
                            .latency_unit(LatencyUnit::Micros),
                    ),
            )
            .layer(CompressionLayer::new())
            .layer(SetSensitiveResponseHeadersLayer::new(SENSITIVE_HEADERS));

        let engine = self.state.engine.clone();

        tokio::spawn(async move {
            engine.start().await;
        });

        let app = Router::new()
            .merge(service_info::router())
            .merge(tasks::router())
            .layer(middleware)
            .with_state(self.state);

        let addr = format!("{}:{}", self.host, self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .context("binding to the provided address")?;

        info!("listening at {addr}");

        axum::serve(listener, app)
            .await
            .context("queued the axum server")?;

        Ok(())
    }
}
