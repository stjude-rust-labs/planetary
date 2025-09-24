//! The `planetary` command line tool.

use std::io::IsTerminal;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use planetary_api::Server;
use planetary_db::Database;
use planetary_server::DEFAULT_ADDRESS;
use planetary_server::DEFAULT_PORT;
use secrecy::SecretString;
use tes::v1::types::responses::ServiceInfo;
use tes::v1::types::responses::service_info;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

/// Arguments relating to service information.
#[derive(Parser)]
pub struct ServiceInfoArgs {
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
}

impl TryFrom<ServiceInfoArgs> for ServiceInfo {
    type Error = anyhow::Error;

    fn try_from(args: ServiceInfoArgs) -> Result<Self> {
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
}

/// The Planetary TES API service.
#[derive(Parser)]
pub struct Args {
    /// The address to bind the service to.
    #[clap(short, long, default_value = DEFAULT_ADDRESS)]
    address: String,

    /// The port to bind the service to.
    #[clap(short, long, default_value_t = DEFAULT_PORT)]
    port: u16,

    /// Arguments relating to service information.
    #[clap(flatten)]
    service_info: ServiceInfoArgs,

    /// The verbosity level.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,

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
        .orchestrator_url(args.orchestrator_url)
        .orchestrator_api_key(args.orchestrator_api_key)
        .info(ServiceInfo::try_from(args.service_info)?)
        .build()
        .run(terminate())
        .await
        .context("failed to run server")
}
