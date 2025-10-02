//! The `planetary` command line tool.

use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use planetary_db::Database;
use planetary_orchestrator::Server;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use secrecy::SecretString;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

/// The Planetary task orchestrator service.
#[derive(Parser)]
pub struct Args {
    /// The address to bind the service to.
    #[clap(short, long, default_value = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the service to.
    #[clap(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// The pod name of the orchestrator.
    #[clap(long, env)]
    pod_name: String,

    /// The service URL for the orchestrator service.
    #[clap(long, env)]
    service_url: Url,

    /// The service API key.
    #[clap(long, env, hide_env_values(true))]
    service_api_key: SecretString,

    /// The verbosity level.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,

    /// The Planetary database user name to use.
    #[clap(long, env)]
    database_user: String,

    /// The Planetary database password to use.
    #[clap(long, env, hide_env_values(true))]
    database_password: SecretString,

    /// The Planetary database host to use.
    #[clap(long, env)]
    database_host: String,

    /// The Planetary database port to use.
    #[clap(long, env, default_value_t = 5432)]
    database_port: i32,

    /// The Planetary database name to use.
    #[clap(long, env, default_value = "planetary")]
    database_name: String,

    /// The Kubernetes storage class to use for tasks.
    #[clap(long, env)]
    storage_class: Option<String>,

    /// The transporter image to use.
    ///
    /// Defaults to `stjude-rust-labs/planetary-transporter:latest`
    #[clap(long, env)]
    transporter_image: Option<String>,

    /// The Kubernetes namespace to use for TES tasks created by the Planetary
    /// service.
    ///
    /// Defaults to `planetary-tasks`.
    #[clap(long, env)]
    tasks_namespace: Option<String>,

    /// The number of CPU cores to request for transporter pods.
    ///
    /// Defaults to `4` CPU cores.
    #[clap(long, env)]
    transporter_cpu: Option<i32>,

    /// The amount of memory (in GiB) to request for transporter pods.
    ///
    /// Defaults to `2.0` GiB.
    #[clap(long, env)]
    transporter_memory: Option<f64>,
}

impl Args {
    /// Gets the TES database from the CLI options.
    async fn database(&self) -> Result<Arc<dyn Database>> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "postgres")] {
                let url = planetary_db::postgres::format_database_url(
                    &self.database_user,
                    &self.database_password,
                    &self.database_host,
                    self.database_port,
                    &self.database_name,
                    &self.pod_name,
                );
                Ok(Arc::new(planetary_db::postgres::PostgresDatabase::new(
                    url.into(),
                )?))
            } else {
                compile_error!("no database feature was enabled");
            }
        }
    }
}

#[cfg(unix)]
/// An async function that waits for a termination signal.
async fn terminate() {
    use tokio::select;
    use tokio::signal::unix::SignalKind;
    use tokio::signal::unix::signal;
    use tracing::info;

    let mut sigterm = signal(SignalKind::terminate()).expect("failed to create SIGTERM handler");
    let mut sigint = signal(SignalKind::interrupt()).expect("failed to create SIGINT handler");

    let signal = select! {
        _ = sigterm.recv() => "SIGTERM",
        _ = sigint.recv() => "SIGINT",
    };

    info!("received {signal} signal: initiating shutdown");
}

#[cfg(windows)]
/// An async function that waits for a termination signal.
async fn terminate() {
    use tokio::signal::windows::ctrl_c;
    use tracing::info;

    let mut signal = ctrl_c().expect("failed to create ctrl-c handler");
    signal.await;

    info!("received Ctrl-C signal: initiating shutdown");
}

/// The main method.
#[tokio::main]
pub async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    match std::env::var("RUST_LOG") {
        Ok(_) => tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .with_ansi(std::io::stdout().is_terminal())
            .init(),
        Err(_) => tracing_subscriber::fmt()
            .with_max_level(args.verbose.log_level_filter().as_trace())
            .with_ansi(std::io::stdout().is_terminal())
            .init(),
    }

    let database = args.database().await?;
    Server::builder()
        .address(&args.address)
        .port(args.port)
        .shared_database(database)
        .maybe_storage_class(args.storage_class)
        .maybe_transporter_image(args.transporter_image)
        .pod_name(args.pod_name)
        .service_url(args.service_url)
        .service_api_key(args.service_api_key)
        .maybe_tasks_namespace(args.tasks_namespace)
        .maybe_transporter_cpu(args.transporter_cpu)
        .maybe_transporter_memory(args.transporter_memory)
        .build()
        .run(terminate())
        .await
        .context("failed to run server")
}
