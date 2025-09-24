//! Implements the Planetary task monitor.

use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use bon::Builder;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use secrecy::SecretString;
use url::Url;

use crate::monitor::Monitor;
use crate::monitor::OrchestratorServiceInfo;

mod monitor;

/// The task monitor server.
#[derive(Clone, Builder)]
pub struct Server {
    /// The address to bind the server to.
    #[builder(into, default = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[builder(into, default = DEFAULT_PORT)]
    port: u16,

    /// The TES database to use for the server.
    #[builder(name = "shared_database")]
    database: Arc<dyn Database>,

    /// The Kubernetes namespace for the Planetary services.
    ///
    /// Defaults to `planetary`.
    #[builder(into)]
    planetary_namespace: Option<String>,

    /// The Kubernetes namespace to use for TES task resources.
    ///
    /// Defaults to `planetary-tasks`.
    #[builder(into)]
    tasks_namespace: Option<String>,

    /// The interval for which the monitor should check the cluster state.
    ///
    /// Defaults to 60 seconds.
    #[builder(into)]
    interval: Duration,

    /// The Planetary orchestrator service URL.
    #[builder(into)]
    orchestrator_url: Url,

    /// The Planetary orchestrator service API key.
    #[builder(into)]
    orchestrator_api_key: SecretString,
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
            .build();

        // Spawn the monitor
        let monitor = Monitor::spawn(
            self.database,
            OrchestratorServiceInfo {
                url: self.orchestrator_url,
                api_key: self.orchestrator_api_key,
            },
            self.planetary_namespace,
            self.tasks_namespace,
            self.interval,
        )
        .await?;

        // Run the server to completion
        server.run((), shutdown).await?;

        // Finally, shutdown the monitor
        monitor.shutdown().await;
        Ok(())
    }
}
