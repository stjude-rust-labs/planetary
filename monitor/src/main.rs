//! The `planetary` command line tool.

use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use planetary_db::Database;
use planetary_monitor::Server;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

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

    /// The Planetary database URL to use.
    #[clap(long, env, hide_env_values(true))]
    database_url: secrecy::SecretString,

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
}

impl Args {
    /// Gets the TES database from the CLI options.
    async fn database(&self) -> Result<Arc<dyn Database>> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "postgres")] {
                Ok(Arc::new(planetary_db::postgres::PostgresDatabase::new(self.database_url.clone())?))
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
        .orchestrator_url(args.orchestrator_url)
        .build()
        .run(terminate())
        .await
        .context("failed to run server")
}
