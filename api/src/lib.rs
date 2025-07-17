//! The implementation of the Planetary TES API server.

use std::future::Future;
use std::sync::Arc;

use bon::Builder;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use reqwest::Client;
use tes::v1::types::responses::ServiceInfo;
use url::Url;

mod info;
mod tasks;

/// The state for the server.
#[derive(Clone)]
struct State {
    /// The HTTP client for communicating with the orchestration service.
    client: Arc<Client>,

    /// The service information.
    info: Arc<ServiceInfo>,

    /// The TES database.
    database: Arc<dyn Database>,

    /// The orchestrator service URL.
    orchestrator_service: Arc<Url>,
}

impl State {
    /// Constructs a new state given the service info, database, and
    /// orchestrator service sender.
    pub fn new(info: ServiceInfo, database: Arc<dyn Database>, orchestrator_service: Url) -> Self {
        Self {
            client: Arc::new(Client::new()),
            info: Arc::new(info),
            database: database.clone(),
            orchestrator_service: Arc::new(orchestrator_service),
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
            .routers(bon::vec![info::router(), tasks::router()])
            .build();

        let state = State::new(self.info, self.database, self.orchestrator_url);
        server.run(state, shutdown).await?;
        Ok(())
    }
}
