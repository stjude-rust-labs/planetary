//! Implements the Planetary task orchestrator.

use std::future::Future;
use std::sync::Arc;

use axum::Router;
use axum::extract::Request;
use axum::extract::State as ExtractState;
use axum::middleware;
use axum::middleware::Next;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::delete;
use axum::routing::get;
use axum::routing::patch;
use axum::routing::post;
use axum::routing::put;
use axum_extra::TypedHeader;
use axum_extra::headers::Authorization;
use axum_extra::headers::authorization::Bearer;
use bon::Builder;
use planetary_db::Database;
use planetary_db::TaskIo;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use planetary_server::Error;
use planetary_server::Json;
use planetary_server::Path;
use planetary_server::ServerResponse;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tes::v1::types::responses::OutputFile;
use url::Url;

use crate::orchestrator::Monitor;
use crate::orchestrator::TaskOrchestrator;
use crate::orchestrator::TransporterInfo;

mod orchestrator;

/// Middleware function to perform bearer token auth against the service's API
/// key.
async fn auth(
    axum::extract::State(state): axum::extract::State<Arc<State>>,
    TypedHeader(authorization): TypedHeader<Authorization<Bearer>>,
    request: Request,
    next: Next,
) -> Response {
    if authorization.token() != state.service_api_key.expose_secret() {
        return Error::forbidden().into_response();
    }

    next.run(request).await
}

/// The state for the server.
struct State {
    /// The API key of the service.
    service_api_key: SecretString,
    /// The task orchestrator.
    orchestrator: TaskOrchestrator,
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

    /// The URL of the orchestrator service.
    #[builder(into)]
    service_url: Url,

    /// The API key of the orchestrator service.
    #[builder(into)]
    service_api_key: SecretString,

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

    /// The amount of memory (in GB) to request for transporter pods.
    ///
    /// Defaults to `1.07374182` GB (i.e 1 GiB).
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
        let state = Arc::new(State {
            service_api_key: self.service_api_key,
            orchestrator: TaskOrchestrator::new(
                self.database,
                self.pod_name,
                self.service_url,
                self.tasks_namespace,
                self.storage_class,
                TransporterInfo {
                    image: self.transporter_image,
                    cpu: self.transporter_cpu,
                    memory: self.transporter_memory,
                },
            )
            .await?,
        });

        let server = planetary_server::Server::builder()
            .address(self.address)
            .port(self.port)
            .routers(bon::vec![
                Router::new()
                    .route("/v1/tasks/{tes_id}", post(Self::start_task))
                    .route("/v1/tasks/{tes_id}", delete(Self::cancel_task))
                    .route("/v1/tasks/{tes_id}/io", get(Self::get_task_io))
                    .route("/v1/tasks/{tes_id}/outputs", put(Self::put_task_outputs))
                    .route("/v1/pods/{name}", patch(Self::adopt_pod))
                    .layer(middleware::from_fn_with_state(state.clone(), auth))
            ])
            .build();

        // Spawn the monitor
        let monitor = Monitor::spawn(state.clone());

        // Run the server to completion
        server.run(state, shutdown).await?;

        // Finally, shutdown the monitor
        monitor.shutdown().await;
        Ok(())
    }

    /// Implements the API endpoint for starting a task.
    ///
    /// This endpoint is used by the TES API service.
    async fn start_task(
        ExtractState(state): ExtractState<Arc<State>>,
        Path(tes_id): Path<String>,
    ) -> ServerResponse<()> {
        tokio::spawn(async move {
            state.orchestrator.start_task(&tes_id).await;
        });

        Ok(())
    }

    /// Implements the API endpoint for canceling a task.
    ///
    /// This endpoint is used by the TES API service.
    async fn cancel_task(
        ExtractState(state): ExtractState<Arc<State>>,
        Path(tes_id): Path<String>,
    ) -> ServerResponse<()> {
        tokio::spawn(async move {
            state.orchestrator.cancel_task(&tes_id).await;
        });

        Ok(())
    }

    /// Implements the API endpoint for getting a task's inputs and outputs.
    ///
    /// This endpoint is used by the transporter.
    async fn get_task_io(
        ExtractState(state): ExtractState<Arc<State>>,
        Path(tes_id): Path<String>,
    ) -> ServerResponse<Json<TaskIo>> {
        Ok(Json(
            state.orchestrator.database().get_task_io(&tes_id).await?,
        ))
    }

    /// Implements the API endpoint for updating a task's output files.
    ///
    /// This endpoint is used by the transporter.
    async fn put_task_outputs(
        ExtractState(state): ExtractState<Arc<State>>,
        Path(tes_id): Path<String>,
        Json(files): Json<Vec<OutputFile>>,
    ) -> ServerResponse<()> {
        state
            .orchestrator
            .database()
            .update_task_output_files(&tes_id, &files)
            .await?;
        Ok(())
    }

    /// Implements the API endpoint for adopting a pod to this orchestrator.
    ///
    /// This endpoint is used by the monitor.
    async fn adopt_pod(
        ExtractState(state): ExtractState<Arc<State>>,
        Path(name): Path<String>,
    ) -> ServerResponse<()> {
        state.orchestrator.adopt_pod(&name).await?;
        Ok(())
    }
}
