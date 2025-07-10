//! The server.

use std::any::Any;
use std::future::Future;
use std::sync::Arc;

use anyhow::Context as _;
use axum::Router;
use axum::body::Body;
use axum::extract::FromRequest;
use axum::extract::FromRequestParts;
use axum::extract::rejection::JsonRejection;
use axum::extract::rejection::PathRejection;
use axum::http;
use axum::http::HeaderName;
use axum::http::StatusCode;
use axum::http::header;
use axum::response::IntoResponse;
use axum::response::Response;
use bon::Builder;
use kube::Client;
use planetary_db::Database;
use serde::Serialize;
use serde::Serializer;
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
use tracing::Span;
use tracing::debug;
use tracing::error;
use tracing::info;

mod info;
mod tasks;

use crate::services::TaskOrchestrationSender;
use crate::services::TaskOrchestrationService;

/// The default address to bind the server to.
pub const DEFAULT_ADDRESS: &str = "0.0.0.0";

/// The default port to bind the server to.
pub const DEFAULT_PORT: u16 = 6492;

/// Header values to be blocked from logging.
pub const SENSITIVE_HEADERS: [HeaderName; 2] = [header::AUTHORIZATION, header::COOKIE];

/// A panic handler for returning 500.
fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    if let Some(s) = err.downcast_ref::<String>() {
        error!("server panicked: {s}");
    } else if let Some(s) = err.downcast_ref::<&str>() {
        error!("server panicked: {s}");
    } else {
        error!("server panicked: unknown panic message");
    };

    Error {
        status: StatusCode::INTERNAL_SERVER_ERROR,
        message: "internal server error".to_string(),
    }
    .into_response()
}

/// An extractor that wraps the JSON extractor of Axum.
///
/// This extractor returns an error object on rejection.
#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(Error))]
struct Json<T>(pub T);

impl<T> IntoResponse for Json<T>
where
    T: Serialize,
{
    fn into_response(self) -> Response {
        axum::Json(self.0).into_response()
    }
}

/// Helper for serializing a HTTP status code.
fn serialize_status<S>(status: &StatusCode, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_u16(status.as_u16())
}

/// Represents a generic error from the API.
#[derive(Serialize, Debug)]
pub struct Error {
    /// The status code being returned in the response.
    #[serde(serialize_with = "serialize_status")]
    pub status: StatusCode,
    /// The error message.
    pub message: String,
}

#[cfg(feature = "postgres")]
impl From<planetary_db::postgres::Error> for Error {
    fn from(e: planetary_db::postgres::Error) -> Self {
        use planetary_db::postgres::Error::*;

        let (status, message) = match &e {
            TaskNotFound(_) => (StatusCode::NOT_FOUND, e.to_string()),
            Pool(e) => {
                // Log the error but do not return it to the client
                tracing::error!("database connection error: {e:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    StatusCode::INTERNAL_SERVER_ERROR.to_string(),
                )
            }
            Diesel(e) => {
                // Log the error but do not return it to the client
                tracing::error!("database error: {e:#}");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    StatusCode::INTERNAL_SERVER_ERROR.to_string(),
                )
            }
        };

        Self { status, message }
    }
}

impl From<planetary_db::Error> for Error {
    fn from(e: planetary_db::Error) -> Self {
        let (status, message) = match e {
            planetary_db::Error::InvalidPageToken(_) => (StatusCode::BAD_REQUEST, e.to_string()),
            #[cfg(feature = "postgres")]
            planetary_db::Error::Postgres(e) => return e.into(),
        };

        Self { status, message }
    }
}

impl From<JsonRejection> for Error {
    fn from(rejection: JsonRejection) -> Self {
        Self {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

impl IntoResponse for Error {
    fn into_response(self) -> Response {
        (self.status, axum::Json(self)).into_response()
    }
}

/// Represents the response type for most API endpoints.
type ServerResponse<T> = Result<T, Error>;

/// An extractor that wraps the path extractor of Axum.
///
/// This extractor returns an API error on rejection.
#[derive(FromRequestParts)]
#[from_request(via(axum::extract::Path), rejection(Error))]
struct Path<T>(T);

impl From<PathRejection> for Error {
    fn from(rejection: PathRejection) -> Self {
        Self {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

/// An extractor that wraps the query extractor of Axum (extra).
///
/// This extractor returns an API error on rejection.
#[derive(FromRequestParts)]
#[from_request(via(axum_extra::extract::Query), rejection(Error))]
struct Query<T>(T);

impl From<axum_extra::extract::QueryRejection> for Error {
    fn from(rejection: axum_extra::extract::QueryRejection) -> Self {
        Self {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

/// Returns a "not found" JSON error response.
fn not_found() -> Error {
    Error {
        status: StatusCode::NOT_FOUND,
        message: "the requested resource was not found".to_string(),
    }
}

/// Returns a "bad request" JSON error response.
fn bad_request(message: impl Into<String>) -> Error {
    Error {
        status: StatusCode::BAD_REQUEST,
        message: message.into(),
    }
}

/// The state for the server.
#[derive(Clone)]
pub struct State {
    /// The service information.
    info: Arc<ServiceInfo>,

    /// The TES database.
    database: Arc<dyn Database>,

    /// The request sender for the orchestration service.
    orchestrator: Arc<TaskOrchestrationSender>,
}

impl State {
    /// Constructs a new state given the service info, database, and
    /// orchestrator service sender.
    pub fn new(
        info: ServiceInfo,
        database: Arc<dyn Database>,
        orchestrator: TaskOrchestrationSender,
    ) -> Self {
        Self {
            info: Arc::new(info),
            database: database.clone(),
            orchestrator: Arc::new(orchestrator),
        }
    }
}

/// The state for a task execution service (TES) server.
#[derive(Clone, Builder)]
pub struct Server {
    /// The address to bind the server to.
    #[builder(into, default = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[builder(into, default = DEFAULT_PORT)]
    port: u16,

    /// The Kubernetes client.
    client: Option<Client>,

    /// The service information.
    #[builder(into)]
    info: ServiceInfo,

    /// The TES database to use for the server.
    #[builder(name = "shared_database")]
    database: Arc<dyn Database>,

    /// The Kubernetes storage class to use for tasks.
    #[builder(into)]
    storage_class: Option<String>,

    /// The transporter image to use.
    ///
    /// Defaults to `stjude-rust-labs/planetary-transporter:latest`.
    #[builder(into)]
    transporter_image: Option<String>,

    /// The Kubernetes namespace to use for TES task resources.
    ///
    /// Defaults to `planetary-tasks`.
    #[builder(into)]
    tasks_namespace: Option<String>,
}

impl<S: server_builder::State> ServerBuilder<S> {
    /// The TES database to use for the server.
    ///
    /// This is a convenience method for setting the shared database server
    /// from any type that implements `Database`.
    pub fn database(
        self,
        database: impl Database + 'static,
    ) -> ServerBuilder<server_builder::SetSharedDatabase<S>>
    where
        S::SharedDatabase: server_builder::IsUnset,
    {
        self.shared_database(Arc::new(database))
    }
}

impl Server {
    /// Runs the server.
    pub async fn run<F>(self, shutdown: F) -> anyhow::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
        // Get the kubernetes client if one wasn't provided
        let client = match self.client {
            Some(client) => client,
            None => Client::try_default()
                .await
                .context("failed to initialize Kubernetes client")?,
        };

        // Spawn the orchestration service
        let (orchestration, sender) = TaskOrchestrationService::spawn(
            self.database.clone(),
            client.clone(),
            self.tasks_namespace,
            self.storage_class,
            self.transporter_image,
        );

        // Hook up the axum middleware
        let middleware = ServiceBuilder::new()
            .layer(SetSensitiveRequestHeadersLayer::new(SENSITIVE_HEADERS))
            .layer(
                TraceLayer::new_for_http()
                    .make_span_with(DefaultMakeSpan::new().include_headers(true))
                    .on_request(|request: &http::Request<Body>, _span: &Span| {
                        debug!(
                            "{method} {path}",
                            method = request.method(),
                            path = request.uri().path()
                        )
                    })
                    .on_response(
                        DefaultOnResponse::new()
                            .level(tracing::Level::DEBUG)
                            .latency_unit(LatencyUnit::Micros),
                    ),
            )
            .layer(CompressionLayer::new())
            .layer(SetSensitiveResponseHeadersLayer::new(SENSITIVE_HEADERS));

        // Construct the axum app
        let app = Router::new()
            .merge(info::router())
            .merge(tasks::router())
            .fallback(async || not_found())
            .layer(middleware)
            .layer(tower_http::catch_panic::CatchPanicLayer::custom(
                handle_panic,
            ))
            .with_state(State::new(self.info, self.database.clone(), sender));

        // Run the server
        let addr = format!("{address}:{port}", address = self.address, port = self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .context("binding to the provided address")?;

        info!("listening at {addr}");

        axum::serve(listener, app)
            .with_graceful_shutdown(shutdown)
            .await
            .context("failed to run API server")?;

        // Shutdown the service
        orchestration.shutdown().await;
        Ok(())
    }
}
