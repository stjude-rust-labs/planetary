//! Cloud storage copy utility.

use std::collections::HashMap;
use std::io::IsTerminal;
use std::io::stderr;
use std::num::NonZero;
use std::thread::available_parallelism;

use anyhow::Context;
use anyhow::Result;
use clap::Parser;
use clap::Subcommand;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud::CopyConfig;
use cloud::CopyEvent;
use cloud::Location;
use colored::Colorize;
use tokio::select;
use tokio::sync::broadcast;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::oneshot;
use tracing::warn;
use tracing::warn_span;
use tracing_indicatif::IndicatifLayer;
use tracing_indicatif::span_ext::IndicatifSpanExt;
use tracing_indicatif::style::ProgressStyle;
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
        let (events_tx, events_rx) =
            broadcast::channel(16 * available_parallelism().map(NonZero::get).unwrap_or(1));

        let (shutdown_tx, shutdown_rx) = oneshot::channel();
        let handler =
            tokio::spawn(async move { Self::handle_events(events_rx, shutdown_rx).await });

        cloud::copy(
            CopyConfig::default(),
            &self.source,
            &self.destination,
            Some(events_tx),
        )
        .await
        .with_context(|| {
            format!(
                "failed to copy `{source}` to `{destination}`",
                source = Location::new(&self.source),
                destination = Location::new(&self.destination),
            )
        })?;

        shutdown_tx.send(()).ok();
        handler.await.expect("failed to join events handler");
        Ok(())
    }

    /// Handles events that may occur during the copy operation.
    ///
    /// This is responsible for showing and updating progress bars for files
    /// being transferred.
    async fn handle_events(mut events: Receiver<CopyEvent>, mut shutdown: oneshot::Receiver<()>) {
        let mut bars = HashMap::new();
        let mut warned = false;

        loop {
            select! {
                _ = &mut shutdown => break,
                event = events.recv() => match event {
                    Ok(CopyEvent::TransferStarted { id, path, size }) => {
                        let bar = warn_span!("progress");
                        let style = ProgressStyle::with_template(
                            "[{elapsed_precise:.cyan/blue}] {bar:40.cyan/blue} {bytes:.cyan/blue} / \
                                {total_bytes:.cyan/blue} ({bytes_per_sec:.cyan/blue}) [ETA \
                                {eta_precise:.cyan/blue}]: {msg}",
                        )
                        .unwrap();
                        bar.pb_set_style(&style);
                        bar.pb_set_length(size);
                        bar.pb_set_message(path.to_str().unwrap_or("<unknown>"));
                        bar.pb_start();
                        bars.insert(id, (bar, 0u64));
                    }
                    Ok(CopyEvent::TransferProgress { id, transferred }) => {
                        if let Some((bar, position)) = bars.get_mut(&id) {
                            *position += transferred;
                            bar.pb_set_position(*position);
                        }
                    }
                    Ok(CopyEvent::TransferComplete { id }) => {
                        bars.remove(&id);
                    }
                    Err(RecvError::Closed) => break,
                    Err(RecvError::Lagged(_)) => {
                        if !warned {
                            warn!("event stream is lagging: progress may be incorrect");
                            warned = true;
                        }
                    }
                }
            }
        }
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

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
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
