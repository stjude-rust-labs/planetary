//! Cloud storage copy utility.

use std::io::IsTerminal;
use std::io::stderr;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud::Config;
use cloud::GoogleAuthConfig;
use cloud::GoogleConfig;
use cloud::Location;
use cloud::S3AuthConfig;
use cloud::S3Config;
use colored::Colorize;
use secrecy::SecretString;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;

/// Represents the copy command.
#[derive(Parser, Debug)]
pub struct CopyCommand {
    /// The source location to copy from.
    #[clap(value_name = "SOURCE")]
    pub source: String,

    /// The destination location to copy to.
    #[clap(value_name = "DESTINATION")]
    pub destination: String,

    /// The block size to use for file transfers; the default block size depends
    /// on the cloud service.
    #[clap(long, value_name = "SIZE")]
    pub block_size: Option<u64>,

    /// The parallelism level for network operations; defaults to the host's
    /// available parallelism.
    #[clap(long, value_name = "NUM")]
    pub parallelism: Option<usize>,

    /// The number of retries to attempt for network operations.
    #[clap(long, value_name = "RETRIES")]
    pub retries: Option<usize>,

    /// The AWS Access Key ID to use.
    #[clap(long, env, value_name = "ID")]
    pub aws_access_key_id: Option<String>,

    /// The AWS Secret Access Key to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "KEY",
        requires = "aws_access_key_id"
    )]
    pub aws_secret_access_key: Option<SecretString>,

    /// The default AWS region.
    #[clap(long, env, value_name = "REGION")]
    pub aws_default_region: Option<String>,

    /// The Google Cloud Storage HMAC access key to use.
    #[clap(long, env, value_name = "KEY")]
    pub google_hmac_access_key: Option<String>,

    /// The Google Cloud Storage HMAC secret to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "SECRET",
        requires = "google_hmac_access_key"
    )]
    pub google_hmac_secret: Option<SecretString>,
}

impl CopyCommand {
    /// Runs the command.
    async fn run(self) -> Result<()> {
        // Only handle transfer events if for a terminal to display the progress
        let (handler, events_tx) = if std::io::stderr().is_terminal() {
            let (events_tx, events_rx) = broadcast::channel(1000);
            let (shutdown_tx, shutdown_rx) = oneshot::channel();
            let handler =
                tokio::spawn(async move { cloud::handle_events(events_rx, shutdown_rx).await });
            (Some((shutdown_tx, handler)), Some(events_tx))
        } else {
            (None, None)
        };

        let (config, source, destination) = self.into_parts();

        let result = cloud::copy(config, &source, &destination, events_tx)
            .await
            .with_context(|| {
                format!(
                    "failed to copy `{source}` to `{destination}`",
                    source = Location::new(&source),
                    destination = Location::new(&destination),
                )
            });

        if let Some((shutdown_tx, handler)) = handler {
            shutdown_tx.send(()).ok();
            handler.await.expect("failed to join events handler");
        }

        result
    }

    /// Converts the command into a `Config`, source, and destination.
    fn into_parts(self) -> (Config, String, String) {
        let s3_auth =
            if let (Some(id), Some(key)) = (self.aws_access_key_id, self.aws_secret_access_key) {
                Some(S3AuthConfig {
                    access_key_id: id,
                    secret_access_key: key,
                })
            } else {
                None
            };

        let google_auth = if let (Some(access_key), Some(secret)) =
            (self.google_hmac_access_key, self.google_hmac_secret)
        {
            Some(GoogleAuthConfig { access_key, secret })
        } else {
            None
        };

        let config = Config {
            block_size: self.block_size,
            parallelism: self.parallelism,
            retries: self.retries,
            s3: S3Config {
                region: self.aws_default_region,
                auth: s3_auth,
            },
            google: GoogleConfig { auth: google_auth },
        };

        (config, self.source, self.destination)
    }
}

/// Represents the available commands for the CLI.
#[derive(Subcommand, Debug)]
enum Command {
    /// Copies files between local and remote locations.
    Copy(CopyCommand),
}

#[derive(Parser, Debug)]
struct Cli {
    /// The command to run.
    #[command(subcommand)]
    command: Command,

    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<WarnLevel>,
}

/// Runs the parsed command.
async fn run() -> Result<()> {
    let cli = Cli::parse();
    match std::env::var("RUST_LOG") {
        Ok(_) => {
            let indicatif_layer = IndicatifLayer::new();

            let subscriber = tracing_subscriber::fmt::Subscriber::builder()
                .with_env_filter(EnvFilter::from_default_env())
                .with_ansi(stderr().is_terminal())
                .with_writer(indicatif_layer.get_stderr_writer())
                .finish()
                .with(indicatif_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
        Err(_) => {
            let indicatif_layer = IndicatifLayer::new();

            let subscriber = tracing_subscriber::fmt::Subscriber::builder()
                .with_max_level(cli.verbosity)
                .with_ansi(stderr().is_terminal())
                .with_writer(indicatif_layer.get_stderr_writer())
                .finish()
                .with(indicatif_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
    }

    match cli.command {
        Command::Copy(cmd) => cmd.run().await,
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

#[tokio::main]
async fn main() {
    tokio::select! {
        _ = terminate() => return,
        r = run() => {
            if let Err(e) = r {
                eprintln!(
                    "{error}: {e:?}",
                    error = if std::io::stderr().is_terminal() {
                        "error".red().bold()
                    } else {
                        "error".normal()
                    }
                );

                std::process::exit(1);
            }
        }
    }
}
