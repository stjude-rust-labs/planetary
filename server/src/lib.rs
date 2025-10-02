//! Implements a common server for Planetary microservices.

use std::any::Any;
use std::future::Future;

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
use axum::routing::get;
use bon::Builder;
use serde::Serialize;
use serde::Serializer;
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

/// The default address to bind the server to.
pub const DEFAULT_ADDRESS: &str = "0.0.0.0";

/// The default port to bind the server to.
pub const DEFAULT_PORT: u16 = 8080;

/// Header values to be blocked from logging.
const SENSITIVE_HEADERS: [HeaderName; 2] = [header::AUTHORIZATION, header::COOKIE];

/// A panic handler for returning 500.
fn handle_panic(err: Box<dyn Any + Send + 'static>) -> Response {
    if let Some(s) = err.downcast_ref::<String>() {
        error!("server panicked: {s}");
    } else if let Some(s) = err.downcast_ref::<&str>() {
        error!("server panicked: {s}");
    } else {
        error!("server panicked: unknown panic message");
    };

    Error::internal().into_response()
}

/// An extractor that wraps the JSON extractor of Axum.
///
/// This extractor returns an error object on rejection.
#[derive(FromRequest)]
#[from_request(via(axum::Json), rejection(Error))]
pub struct Json<T>(pub T);

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

/// Represents a generic error from the server.
#[derive(Serialize, Debug)]
pub struct Error {
    /// The status code being returned in the response.
    #[serde(serialize_with = "serialize_status")]
    pub status: StatusCode,
    /// The error message.
    pub message: String,
}

impl Error {
    /// Returns a "not found" JSON error response.
    pub fn not_found() -> Error {
        Error {
            status: StatusCode::NOT_FOUND,
            message: "the requested resource was not found".to_string(),
        }
    }

    /// Returns a "bad request" JSON error response.
    pub fn bad_request(message: impl Into<String>) -> Error {
        Error {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    /// Returns a "forbidden" JSON error response.
    pub fn forbidden() -> Error {
        Self {
            status: StatusCode::FORBIDDEN,
            message: StatusCode::FORBIDDEN.to_string(),
        }
    }

    /// Returns an "internal server error" JSON error response.
    pub fn internal() -> Error {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: StatusCode::INTERNAL_SERVER_ERROR.to_string(),
        }
    }
}

impl From<anyhow::Error> for Error {
    fn from(e: anyhow::Error) -> Self {
        tracing::error!("{e:#}");
        Self::internal()
    }
}

impl From<reqwest::Error> for Error {
    fn from(e: reqwest::Error) -> Self {
        tracing::error!("{e:#}");
        Self::internal()
    }
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
                return Self::internal();
            }
            Diesel(e) => {
                // Log the error but do not return it to the client
                tracing::error!("database error: {e:#}");
                return Self::internal();
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
            planetary_db::Error::Other(e) => return e.into(),
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

/// Represents the response type for most endpoints.
pub type ServerResponse<T> = Result<T, Error>;

/// An extractor that wraps the path extractor of Axum.
///
/// This extractor returns an error on rejection.
#[derive(FromRequestParts)]
#[from_request(via(axum::extract::Path), rejection(Error))]
pub struct Path<T>(pub T);

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
/// This extractor returns an error on rejection.
#[derive(FromRequestParts)]
#[from_request(via(axum_extra::extract::Query), rejection(Error))]
pub struct Query<T>(pub T);

impl From<axum_extra::extract::QueryRejection> for Error {
    fn from(rejection: axum_extra::extract::QueryRejection) -> Self {
        Self {
            status: rejection.status(),
            message: rejection.body_text(),
        }
    }
}

/// The state for a task execution service (TES) server.
#[derive(Clone, Builder)]
pub struct Server<S> {
    /// The address to bind the server to.
    #[builder(into, default = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[builder(into, default = DEFAULT_PORT)]
    port: u16,

    /// The routers for the server.
    #[builder(into, default)]
    routers: Vec<Router<S>>,
}

impl<S> Server<S>
where
    S: Clone + Send + Sync + 'static,
{
    /// Runs the server with the given state and shutdown function.
    pub async fn run<F>(self, state: S, shutdown: F) -> anyhow::Result<()>
    where
        F: Future<Output = ()> + Send + 'static,
    {
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
        let mut router = Router::new().route("/ping", get(|| async {}));

        for merge in self.routers {
            router = router.merge(merge);
        }

        // Run the server
        let addr = format!("{address}:{port}", address = self.address, port = self.port);
        let listener = TcpListener::bind(&addr)
            .await
            .context("binding to the provided address")?;

        info!("listening at {addr}");

        axum::serve(
            listener,
            router
                .fallback(async || Error::not_found())
                .layer(middleware)
                .layer(tower_http::catch_panic::CatchPanicLayer::custom(
                    handle_panic,
                ))
                .with_state(state),
        )
        .with_graceful_shutdown(shutdown)
        .await
        .context("failed to run server")?;

        Ok(())
    }
}
