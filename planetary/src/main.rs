//! The `planetary` command line tool.

use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use planetary::Server;
use planetary::server::DEFAULT_ADDRESS;
use planetary::server::DEFAULT_PORT;
use planetary_db::Database;
use tes::v1::types::responses::ServiceInfo;
use tes::v1::types::responses::service_info;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

/// A tool for executing tasks in Kubernetes via the GA4GH TES specification.
#[derive(Parser)]
pub struct Args {
    /// The address to bind the server to.
    #[clap(short, long, default_value = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the server to.
    #[clap(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// The identifier for the server.
    #[clap(long, default_value = "org.example.planetary")]
    server_identifier: String,

    /// The name for the server.
    #[clap(long, default_value = "Planetary Server")]
    server_name: String,

    /// The description for the server.
    #[clap(long)]
    description: Option<String>,

    /// The organization name for the server.
    #[clap(long, default_value = "Example Organization")]
    organization_name: String,

    /// The organization URL for the server.
    #[clap(long, default_value = "https://example.com")]
    organization_url: Url,

    /// The contact URL for the server.
    #[clap(long)]
    contact_url: Option<String>,

    /// The documentation URL for the server.
    #[clap(long)]
    documentation_url: Option<Url>,

    /// The environment for the server.
    #[clap(long)]
    environment: Option<String>,

    /// The version of the server.
    ///
    /// Defaults to the crate version of `planetary`.
    #[clap(long)]
    server_version: Option<String>,

    /// A set of supported storage locations.
    #[clap(long)]
    storage: Option<Vec<String>>,

    /// The verbosity level.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,

    /// The Planetary database URL to use.
    #[clap(long, env)]
    database_url: secrecy::SecretString,

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

    /// Disables running database migrations on server startup.
    #[cfg(feature = "postgres")]
    #[clap(long)]
    no_database_migrations: bool,
}

impl Args {
    /// Gets the TES database from the CLI options.
    async fn database(&self) -> Result<Arc<dyn Database>> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "postgres")] {
                use planetary_db::postgres::PostgresDatabase;

                let database = PostgresDatabase::new(self.database_url.clone())?;

                if !self.no_database_migrations {
                    database.run_pending_migrations().await?;
                }

                Ok(Arc::new(database))
            } else {
                compile_error!("no database feature was enabled");
            }
        }
    }
}

/// Builds TES service information from the provided arguments.
fn build_info(args: Args) -> anyhow::Result<ServiceInfo> {
    let mut builder = service_info::Builder::default()
        .id(args.server_identifier)
        .name(args.server_name)
        .org_name(args.organization_name)
        .org_url(args.organization_url)
        .version(
            args.server_version
                .unwrap_or_else(|| env!("CARGO_PKG_VERSION").into()),
        );

    if let Some(description) = args.description {
        builder = builder.description(description);
    }

    if let Some(contact_url) = args.contact_url {
        builder = builder.contact_url(contact_url);
    }

    if let Some(documentation_url) = args.documentation_url {
        builder = builder.documentation_url(documentation_url);
    }

    if let Some(environment) = args.environment {
        builder = builder.environment(environment);
    }

    if let Some(storage) = args.storage {
        builder = builder.storage(storage);
    }

    builder
        .try_build()
        .context("building service info response")
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
    let mut args = Args::parse();

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
    let storage_class = args.storage_class.take();
    let transporter_image = args.transporter_image.take();
    let tasks_namespace = args.tasks_namespace.take();
    Server::builder()
        .address(&args.address)
        .port(args.port)
        .shared_database(database)
        .maybe_storage_class(storage_class)
        .maybe_transporter_image(transporter_image)
        .maybe_tasks_namespace(tasks_namespace)
        .info(build_info(args)?)
        .build()
        .run(terminate())
        .await
        .context("failed to run server")
}
