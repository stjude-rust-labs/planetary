//! Implements the Planetary task orchestrator.

use std::future::Future;
use std::sync::Arc;

use axum::Router;
use axum::extract::State as ExtractState;
use axum::routing::delete;
use axum::routing::patch;
use axum::routing::post;
use bon::Builder;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use planetary_server::Path;
use planetary_server::ServerResponse;

use crate::orchestrator::Monitor;
use crate::orchestrator::TaskOrchestrator;
use crate::orchestrator::TransporterInfo;

mod orchestrator;

/// The state for the server.
#[derive(Clone)]
struct State {
    /// The task orchestrator.
    orchestrator: Arc<TaskOrchestrator>,
}

/// The task orchestrator server.
#[derive(Clone, Builder)]
pub struct Server {
    /// The address to bind the server to.
    #[builder(into, default = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[builder(into, default = DEFAULT_PORT)]
    port: u16,

    /// The pod name of the orchestrator.
    #[builder(into)]
    pod_name: String,

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

    /// The number of CPU cores to request for transporter pods.
    ///
    /// Defaults to `4` CPU cores.
    #[builder(into)]
    transporter_cpu: Option<i32>,

    /// The amount of memory (in GiB) to request for transporter pods.
    ///
    /// Defaults to `2.0` GiB.
    #[builder(into)]
    transporter_memory: Option<f64>,
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
                Router::new()
                    .route("/v1/tasks/{tes_id}", post(Self::start_task))
                    .route("/v1/tasks/{tes_id}", delete(Self::cancel_task))
                    .route("/v1/pods/{name}", patch(Self::adopt_pod))
            ])
            .build();

        let orchestrator = Arc::new(
            TaskOrchestrator::new(
                self.database,
                self.pod_name,
                self.tasks_namespace,
                self.storage_class,
                TransporterInfo {
                    image: self.transporter_image,
                    cpu: self.transporter_cpu,
                    memory: self.transporter_memory,
                },
            )
            .await?,
        );

        // Spawn the monitor
        let monitor = Monitor::spawn(orchestrator.clone());

        // Run the server to completion
        server.run(State { orchestrator }, shutdown).await?;

        // Finally, shutdown the monitor
        monitor.shutdown().await;
        Ok(())
    }

    /// Implements the API endpoint for starting a task.
    async fn start_task(
        Path(tes_id): Path<String>,
        ExtractState(state): ExtractState<State>,
    ) -> ServerResponse<()> {
        state.orchestrator.start_task(&tes_id).await?;
        Ok(())
    }

    /// Implements the API endpoint for canceling a task.
    async fn cancel_task(
        Path(tes_id): Path<String>,
        ExtractState(state): ExtractState<State>,
    ) -> ServerResponse<()> {
        state.orchestrator.cancel_task(&tes_id).await?;
        Ok(())
    }

    /// Implements the API endpoint for adopting a pod to this orchestrator.
    async fn adopt_pod(
        Path(name): Path<String>,
        ExtractState(state): ExtractState<State>,
    ) -> ServerResponse<()> {
        state.orchestrator.adopt_pod(&name).await?;
        Ok(())
    }
}
