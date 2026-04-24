//! The implementation of the Planetary TES API server.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::Request;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum_extra::TypedHeader;
use axum_extra::headers::Authorization;
use axum_extra::headers::authorization::Basic;
use bon::Builder;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use planetary_server::Error;
use reqwest::Client;
use secrecy::SecretString;
use tes::v1::types::responses::ServiceInfo;
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_retry2::strategy::MaxInterval;
use tracing::warn;
use url::Url;

mod info;
mod tasks;

/// Gets an iterator over the retry durations for network operations.
///
/// Retries use an exponential power of 2 backoff, starting at 1 second with
/// a maximum duration of 60 seconds.
fn retry_durations() -> impl Iterator<Item = Duration> {
    const INITIAL_DELAY_MILLIS: u64 = 1000;
    const BASE_FACTOR: f64 = 2.0;
    const MAX_DURATION: Duration = Duration::from_secs(60);
    const RETRIES: usize = 5;

    ExponentialFactorBackoff::from_millis(INITIAL_DELAY_MILLIS, BASE_FACTOR)
        .max_duration(MAX_DURATION)
        .take(RETRIES)
}

/// Helper for notifying that a network operation failed and will be retried.
fn notify_retry(e: &reqwest::Error, duration: Duration) {
    warn!(
        "network operation failed: {e} (retrying after {duration} seconds)",
        duration = duration.as_secs()
    );
}

/// An extension for passing the request username through from the
/// authentication middleware.
#[derive(Clone)]
struct Username(String);

/// Middleware function to perform auth lookup against the request.
///
/// This middleware does not actually perform any authentication of the user.
///
/// If the `X-Forwarded-User` header is present, it is respected over the
/// `Authorization` header.
///
/// The intention of this middleware is to simply to enforce that a username was
/// sent with the request.
async fn auth(
    allow_authorization_fallback: bool,
    authorization: Option<TypedHeader<Authorization<Basic>>>,
    mut request: Request,
    next: Next,
) -> Response {
    // First respect the `X-Forwarded-User` header, then the `Authorization` header
    let username = match (
        request
            .headers()
            .get("X-Forwarded-User")
            .map(|v| v.to_str()),
        &authorization,
    ) {
        // Note: if the `X-Forwarded-User` is present but malformed (i.e. `Some(Err(_))`), then we
        // intentionally return forbidden rather than look at the `Authorization` header
        (Some(Ok(username)), _) if !username.is_empty() => username,
        (None, Some(auth)) if allow_authorization_fallback && !auth.username().is_empty() => {
            auth.username()
        }
        _ => return Error::forbidden().into_response(),
    }
    .to_string();

    request.extensions_mut().insert(Username(username));
    next.run(request).await
}

/// Represents information about the orchestrator service.
struct OrchestratorServiceInfo {
    /// The URL of the orchestrator service.
    url: Url,
    /// The orchestrator service API key.
    api_key: SecretString,
}

/// The state for the server.
#[derive(Clone)]
struct State {
    /// The HTTP client for communicating with the orchestration service.
    client: Arc<Client>,
    /// The service information.
    info: Arc<ServiceInfo>,
    /// The TES database.
    database: Arc<dyn Database>,
    /// The orchestrator service information.
    orchestrator: Arc<OrchestratorServiceInfo>,
}

impl State {
    /// Constructs a new state given the service info, database, and
    /// orchestrator service sender.
    pub fn new(
        info: ServiceInfo,
        database: Arc<dyn Database>,
        orchestrator_url: Url,
        orchestrator_api_key: SecretString,
    ) -> Self {
        Self {
            client: Arc::new(Client::new()),
            info: Arc::new(info),
            database: database.clone(),
            orchestrator: Arc::new(OrchestratorServiceInfo {
                url: orchestrator_url,
                api_key: orchestrator_api_key,
            }),
        }
    }
}

/// The TES API server.
#[derive(Clone, Builder)]
pub struct Server {
    /// The address to bind the server to.
    #[builder(into, default = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[builder(into, default = DEFAULT_PORT)]
    port: u16,

    /// The service information.
    #[builder(into)]
    info: ServiceInfo,

    /// The TES database to use for the server.
    #[builder(name = "shared_database")]
    database: Arc<dyn Database>,

    /// The Planetary orchestrator service URL.
    #[builder(into)]
    orchestrator_url: Url,

    /// The Planetary orchestrator service API key.
    #[builder(into)]
    orchestrator_api_key: SecretString,

    /// Whether or not to allow fallback to the `Authorization` header.
    allow_authorization_fallback: bool,
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
        let server = planetary_server::Server::builder()
            .address(self.address)
            .port(self.port)
            .routers(bon::vec![
                info::router(),
                tasks::router(self.allow_authorization_fallback)
            ])
            .build();

        let state = State::new(
            self.info,
            self.database,
            self.orchestrator_url,
            self.orchestrator_api_key,
        );

        server.run(state, shutdown).await?;
        Ok(())
    }
}
