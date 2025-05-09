//! The `planetary` command line tool.

use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use eyre::Context;
use kube::Client;
use planetary::Server;
use tes::v1::types::responses::ServiceInfo;
use tes::v1::types::responses::service_info;
use tracing_log::AsTrace as _;
use tracing_subscriber::EnvFilter;
use url::Url;

/// A tool for executing tasks in Kubernetes via the TES specification.
#[derive(Parser)]
pub struct Args {
    /// An identifier for the server.
    #[clap(short = 'i', long, default_value = "com.example.tes")]
    server_identifier: String,

    /// A name for the server.
    #[clap(short = 'n', long, default_value = "My Planetary Server")]
    server_name: String,

    /// A description for the server.
    #[clap(short = 'd', long, default_value = None)]
    description: Option<String>,

    /// A name for the server.
    #[clap(short = 'o', long, default_value = "My Organization Name")]
    organization_name: String,

    /// A URL describing the organization.
    #[clap(short = 'p', long, default_value = "https://example.com")]
    organization_url: Url,

    /// A contact URL for the server.
    #[clap(short = 'c', long, default_value = None)]
    contact_url: Option<String>,

    /// A documentation URL for the server.
    #[clap(short = 'd', long, default_value = None)]
    documentation_url: Option<Url>,

    /// An environment for the server.
    #[clap(short = 'e', long, default_value = None)]
    environment: Option<String>,

    /// A version for the server.
    #[clap(short = 'w', long, required = true)]
    server_version: String,

    /// A set of supported storage locations.
    #[clap(long)]
    storage: Option<Vec<String>>,

    /// The verbosity flags.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,
}

/// Builds TES service information from the provided arguments.
fn build_info(args: Args) -> eyre::Result<ServiceInfo> {
    let mut builder = service_info::Builder::default()
        .id(args.server_identifier)
        .name(args.server_name)
        .org_name(args.organization_name)
        .org_url(args.organization_url)
        .version(args.server_version);

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

/// Runs the planetary server.
async fn run(args: Args) -> eyre::Result<()> {
    let info = build_info(args)?;
    let client = Client::try_default()
        .await
        .context("creating kubernetes client")?;

    Server::builder()
        .client(client)
        .service_info(info)
        .try_build()
        .context("building the planetary server")?
        .run()
        .await
        .context("queued the planetary server")
}

/// The main method.
pub fn main() -> eyre::Result<()> {
    let args = Args::parse();

    match std::env::var("RUST_LOG") {
        Ok(_) => tracing_subscriber::fmt()
            .with_env_filter(EnvFilter::from_default_env())
            .init(),
        Err(_) => tracing_subscriber::fmt()
            .with_max_level(args.verbose.log_level_filter().as_trace())
            .init(),
    };

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .context("starting tokio runtime")?
        .block_on(run(args))
}
