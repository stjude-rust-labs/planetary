//! Cloud storage copy utility.

use std::io::IsTerminal;
use std::io::stderr;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud::CopyConfig;
use cloud::Location;
use colored::Colorize;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;

/// Represents the copy command.
#[derive(Parser, Debug)]
pub struct CopyCommand {
    /// The source location to copy from.
    #[clap(name = "SOURCE")]
    pub source: String,

    /// The destination location to copy to.
    #[clap(name = "DESTINATION")]
    pub destination: String,
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

        let result = cloud::copy(
            CopyConfig::default(),
            &self.source,
            &self.destination,
            events_tx,
        )
        .await
        .with_context(|| {
            format!(
                "failed to copy `{source}` to `{destination}`",
                source = Location::new(&self.source),
                destination = Location::new(&self.destination),
            )
        });

        if let Some((shutdown_tx, handler)) = handler {
            shutdown_tx.send(()).ok();
            handler.await.expect("failed to join events handler");
        }

        result
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
