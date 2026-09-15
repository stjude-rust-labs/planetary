//! Implements the Planetary task monitor.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use bon::Builder;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use secrecy::SecretString;
use url::Url;

use crate::monitor::Intervals;
use crate::monitor::KubeletConfig;
use crate::monitor::Monitor;
use crate::monitor::Namespaces;
use crate::monitor::OrchestratorServiceInfo;

mod monitor;
mod usage;

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
    #[builder(into)]
    planetary_namespace: String,

    /// The Kubernetes namespace to use for TES task resources.
    #[builder(into)]
    tasks_namespace: String,

    /// The directory containing the Kubernetes resource templates.
    #[builder(into)]
    templates_dir: PathBuf,

    /// The interval for which the monitor should check the cluster state.
    ///
    /// Defaults to 60 seconds.
    #[builder(into)]
    check_interval: Duration,

    /// The interval for which Kubernetes resources are kept after a task
    /// enters a terminal state.
    #[builder(into)]
    keep_interval: Duration,

    /// The port kubelets listen on for resource usage sampling.
    ///
    /// Defaults to 10250.
    #[builder(default = crate::usage::DEFAULT_KUBELET_PORT)]
    kubelet_port: u16,

    /// Whether to skip verification of kubelet serving certificates when
    /// sampling resource usage.
    ///
    /// An escape hatch for clusters whose kubelets serve self-signed
    /// certificates (for example, `kind`). Defaults to `false`.
    #[builder(default)]
    kubelet_insecure_tls: bool,

    /// An override for the certificate authority bundle used to verify
    /// kubelet serving certificates.
    ///
    /// `None` uses the in-cluster service account certificate authority
    /// bundle. Ignored when `kubelet_insecure_tls` is enabled.
    kubelet_ca_path: Option<PathBuf>,

    /// The interval for sampling task pod resource usage directly from the
    /// kubelets hosting task pods (authorized via `nodes/metrics`).
    ///
    /// `None` disables resource usage sampling.
    usage_sample_interval: Option<Duration>,

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
            Namespaces {
                planetary: self.planetary_namespace,
                tasks: self.tasks_namespace,
            },
            self.templates_dir,
            Intervals {
                check: self.check_interval,
                keep: self.keep_interval,
                usage: self.usage_sample_interval,
            },
            KubeletConfig {
                port: self.kubelet_port,
                insecure_tls: self.kubelet_insecure_tls,
                ca_path: self.kubelet_ca_path,
            },
        )
        .await?;

        // Run the server to completion
        server.run((), shutdown).await?;

        // Finally, shutdown the monitor
        monitor.shutdown().await;
        Ok(())
    }
}
