//! The `planetary` command line tool.

use std::io::IsTerminal;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use planetary_db::Database;
use planetary_monitor::Server;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use secrecy::SecretString;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

/// The default interval for the monitoring service, in seconds.
const MONITORING_INTERVAL_SECONDS: u64 = 60;

/// A tool for executing tasks in Kubernetes via the GA4GH TES specification.
#[derive(Parser)]
pub struct Args {
    /// The address to bind the service to.
    #[clap(short, long, default_value = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the service to.
    #[clap(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// The verbosity level.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,

    /// The interval for which the monitor should check the cluster state.
    ///
    /// Defaults to 60 seconds.
    #[clap(long, default_value_t = MONITORING_INTERVAL_SECONDS)]
    interval: u64,

    /// The name of the pod running the service.
    #[clap(long, env)]
    pod_name: String,

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

    /// The Kubernetes namespace running the Planetary services.
    ///
    /// Defaults to `planetary`
    #[clap(long, env)]
    planetary_namespace: Option<String>,

    /// The Kubernetes namespace to use for TES tasks created by the Planetary
    /// service.
    ///
    /// Defaults to `planetary-tasks`.
    #[clap(long, env)]
    tasks_namespace: Option<String>,

    /// The Planetary orchestrator service URL to use.
    #[clap(long, env)]
    orchestrator_url: Url,

    /// The Planetary orchestrator service API key.
    #[clap(long, env, hide_env_values(true))]
    orchestrator_api_key: SecretString,
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
        .maybe_planetary_namespace(args.planetary_namespace)
        .maybe_tasks_namespace(args.tasks_namespace)
        .interval(Duration::from_secs(args.interval))
        .orchestrator_url(args.orchestrator_url)
        .orchestrator_api_key(args.orchestrator_api_key)
        .build()
        .run(terminate())
        .await
        .context("failed to run server")
}
