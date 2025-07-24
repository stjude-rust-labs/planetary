//! Responsible for "transporting" a Planetary task's inputs and outputs.
//!
//! This tool is built into a container image that is used by Planetary.
//!
//! The executable requires either the `--inputs` or `--outputs` option.
//!
//! If the `--inputs` option is specified, the argument is expected to be a path
//! to a JSON file containing an array of TES inputs.
//!
//! If the `--outputs` option is specified, the argument is expected to be a
//! path to a JSON file containing an array of TES outputs.
//!
//! The `--target` argument is the directory where either inputs are created or
//! the outputs are sourced from.
//!
//! The entries of the target directory will be created or accessed based on
//! their index within the array of inputs or outputs.
//!
//! For example, if the `--inputs` option is used with `--targets /mnt/inputs`,
//! this program will create entries such as `/mnt/inputs/0`, `/mnt/inputs/1`,
//! etc.
//!
//! Likewise, if the `--outputs` option is used with `--targets /mnt/outputs`,
//! it will access `/mnt/outputs/0`, `/mnt/outputs/1`, etc.

use std::io::IsTerminal;
use std::io::stderr;
use std::num::NonZero;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::thread::available_parallelism;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud::TransferEvent;
use cloud::UrlExt;
use colored::Colorize;
use glob::Pattern;
use planetary_db::Database;
use reqwest::Url;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Output;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::sync::broadcast;
use tokio::sync::oneshot;
use tracing::error;
use tracing::info;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use walkdir::WalkDir;

/// The mode of operation.
#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
enum Mode {
    /// Transport the task inputs into the target directory.
    Inputs,
    /// Transport the task outputs from the target directory.
    Outputs,
}

impl FromStr for Mode {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s {
            "inputs" => Ok(Self::Inputs),
            "outputs" => Ok(Self::Outputs),
            _ => bail!("invalid mode `{s}`"),
        }
    }
}

/// A tool for transporting Planetary task's inputs and outputs.
#[derive(Parser)]
struct Args {
    /// The Planetary database URL to use.
    #[clap(long, env)]
    database_url: secrecy::SecretString,

    /// The mode of operation.
    #[arg(long)]
    mode: Mode,

    /// The path to the inputs directory.
    #[arg(long, required_if_eq("mode", "inputs"))]
    inputs_dir: Option<PathBuf>,

    /// The path to the outputs directory.
    #[arg(long)]
    outputs_dir: PathBuf,

    /// The TES identifier of the task.
    tes_id: String,

    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<WarnLevel>,
}

impl Args {
    /// Gets the TES database from the CLI options.
    fn database(&self) -> Result<Arc<dyn Database>> {
        cfg_if::cfg_if! {
            if #[cfg(feature = "postgres")] {
                Ok(Arc::new(planetary_db::postgres::PostgresDatabase::new(self.database_url.clone())?))
            } else {
                compile_error!("no database feature was enabled");
            }
        }
    }
}

/// Downloads inputs into the inputs directory.
///
/// This will also create empty files and directories for outputs in the outputs
/// directory.
async fn download_inputs(
    database: Arc<dyn Database>,
    tes_id: &str,
    inputs_dir: &Path,
    outputs_dir: &Path,
) -> Result<()> {
    let task_io = database.get_task_io(tes_id).await.map_err(|e| {
        error!("failed to retrieve inputs and outputs of task `{tes_id}`: {e:#}");
        anyhow!("failed to retrieve information for task `{tes_id}`")
    })?;

    // Create the inputs directory
    create_dir_all(inputs_dir).await.with_context(|| {
        format!(
            "failed to create inputs directory `{path}`",
            path = inputs_dir.display()
        )
    })?;

    // Create the outputs directory
    create_dir_all(outputs_dir).await.with_context(|| {
        format!(
            "failed to create outputs directory `{path}`",
            path = inputs_dir.display()
        )
    })?;

    let (events_tx, events_rx) =
        broadcast::channel(16 * available_parallelism().map(NonZero::get).unwrap_or(1));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handler = tokio::spawn(async move { cloud::handle_events(events_rx, shutdown_rx).await });

    let transfer = async move || {
        for (index, input) in task_io.inputs.into_iter().enumerate() {
            let path = inputs_dir.join(index.to_string());

            // Write the contents if directly given
            if let Some(contents) = &input.content {
                assert_eq!(
                    input.ty,
                    IoType::File,
                    "cannot create content for a directory"
                );

                info!(
                    "creating input file `{path}` with specified contents",
                    path = input.path,
                );

                tokio::fs::write(&path, contents).await.with_context(|| {
                    format!("failed to create input file `{path}`", path = input.path)
                })?;
                continue;
            }

            let url = input
                .url
                .context("input is missing a URL")?
                .parse::<Url>()
                .context("input URL is invalid")?;

            // Perform the copy
            cloud::copy(
                Default::default(),
                url.clone(),
                &path,
                Some(events_tx.clone()),
            )
            .await
            .with_context(|| {
                format!(
                    "failed to download input `{url}` to `{path}`",
                    url = url.display(),
                    path = input.path
                )
            })?;

            // Check that the result matches the input type
            match (path.is_file(), input.ty) {
                (true, IoType::Directory) => bail!(
                    "input `{url}` was a file but the input type was `DIRECTORY`",
                    url = url.display()
                ),
                (false, IoType::File) => bail!(
                    "input `{url}` was a directory but the input type was `FILE`",
                    url = url.display()
                ),
                _ => {}
            }
        }

        Ok(())
    };

    let result = transfer().await;

    shutdown_tx.send(()).ok();
    handler.await.expect("failed to join events handler");

    result?;

    // We also need to create any file outputs so that Kubernetes will mount them as
    // files and not directories
    for (index, output) in task_io.outputs.into_iter().enumerate() {
        if output.ty == IoType::File {
            let path = outputs_dir.join(index.to_string());
            File::create(&path).await.with_context(|| {
                format!(
                    "failed to create output file `{path}`",
                    path = path.display()
                )
            })?;
        }
    }

    Ok(())
}

/// Uploads outputs from the specified outputs directory.
async fn upload_outputs(
    database: Arc<dyn Database>,
    tes_id: &str,
    outputs_dir: &Path,
) -> Result<()> {
    let task_io = database.get_task_io(tes_id).await.map_err(|e| {
        error!("failed to retrieve inputs and outputs of task `{tes_id}`: {e:#}");
        anyhow!("failed to retrieve information for task `{tes_id}`")
    })?;

    let (events_tx, events_rx) =
        broadcast::channel(16 * available_parallelism().map(NonZero::get).unwrap_or(1));
    let (shutdown_tx, shutdown_rx) = oneshot::channel();
    let handler = tokio::spawn(async move { cloud::handle_events(events_rx, shutdown_rx).await });

    let transfer = async move || {
        let mut files = Vec::new();
        for (index, output) in task_io.outputs.iter().enumerate() {
            let url = output.url.parse::<Url>().context("output URL is invalid")?;
            let path = outputs_dir.join(index.to_string());
            let metadata = path.metadata().with_context(|| {
                format!(
                    "failed to read metadata of output `{path}`",
                    path = output.path
                )
            })?;

            let path = path
                .to_str()
                .with_context(|| format!("path `{path}` is not UTF-8", path = path.display()))?;

            if metadata.is_file() {
                upload_file(
                    output,
                    url,
                    path,
                    metadata.len(),
                    events_tx.clone(),
                    &mut files,
                )
                .await?;
            } else {
                upload_directory(output, &url, path, events_tx.clone(), &mut files).await?;
            }
        }

        anyhow::Ok(files)
    };

    let result = transfer().await;

    shutdown_tx.send(()).ok();
    handler.await.expect("failed to join events handler");

    database.update_task_output_files(tes_id, &result?).await?;
    Ok(())
}

/// Uploads a file output.
async fn upload_file(
    output: &Output,
    mut url: Url,
    path: &str,
    size: u64,
    events: broadcast::Sender<TransferEvent>,
    files: &mut Vec<OutputFile>,
) -> Result<()> {
    if output.ty != IoType::File {
        bail!(
            "output `{path}` exists but the output is not a file",
            path = output.path
        );
    }

    // Perform the copy
    cloud::copy(Default::default(), path, url.clone(), Some(events))
        .await
        .with_context(|| {
            format!(
                "failed to upload output `{path}` to `{url}`",
                path = output.path,
                url = url.display(),
            )
        })?;

    // Clear the query and fragment before saving the output
    url.set_query(None);
    url.set_fragment(None);

    files.push(OutputFile {
        url: url.as_str().to_string(),
        path: output.path.clone(),
        size_bytes: size.to_string(),
    });
    Ok(())
}

/// Uploads a directory output.
async fn upload_directory(
    output: &Output,
    url: &Url,
    path: &str,
    events: broadcast::Sender<TransferEvent>,
    files: &mut Vec<OutputFile>,
) -> Result<()> {
    if output.ty != IoType::Directory {
        bail!(
            "output `{path}` exists but the output is not a directory",
            path = output.path
        );
    }
    let container_base_path = Path::new(output.path_prefix.as_deref().unwrap_or(&output.path));
    let pattern =
        if output.path_prefix.is_some() {
            Some(Pattern::new(&output.path).with_context(|| {
                format!("invalid output path pattern `{path}`", path = output.path)
            })?)
        } else {
            None
        };

    for entry in WalkDir::new(path) {
        let entry = entry
            .with_context(|| format!("failed to read directory `{path}`", path = output.path))?;

        let relative_path = entry.path().strip_prefix(path).expect("should be relative");
        let container_path = container_base_path.join(relative_path);
        let container_path = container_path.to_str().with_context(|| {
            format!(
                "output `{path}` is not UTF-8",
                path = container_path.display()
            )
        })?;
        let metadata = entry
            .metadata()
            .with_context(|| format!("failed to read metadata for output `{container_path}`"))?;

        // Only upload files
        if metadata.is_dir() {
            continue;
        }

        // If there's a pattern, ensure the container path matches it
        if let Some(pattern) = &pattern {
            if !pattern.matches(container_path) {
                info!("skipping output file `{container_path}` as it does not match the pattern");
                continue;
            }
        }

        info!(
            "uploading output file `{container_path}` to `{url}`",
            url = url.display()
        );

        let mut url = url.clone();

        {
            // Append the relative path to the URL
            let mut segments = url.path_segments_mut().unwrap();
            for component in relative_path.components() {
                match component {
                    Component::Normal(segment) => {
                        segments.push(segment.to_str().unwrap());
                    }
                    _ => bail!(
                        "invalid relative path `{path}`",
                        path = relative_path.display()
                    ),
                }
            }
        }

        // Perform the copy
        cloud::copy(
            Default::default(),
            entry.path(),
            url.clone(),
            Some(events.clone()),
        )
        .await
        .with_context(|| {
            format!(
                "failed to upload output `{container_path}` to `{url}`",
                url = url.display(),
            )
        })?;

        // Clear the query and fragment before saving the output
        url.set_query(None);
        url.set_fragment(None);

        files.push(OutputFile {
            url: url.as_str().to_string(),
            path: container_path.to_string(),
            size_bytes: metadata.len().to_string(),
        });
    }

    Ok(())
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

/// Runs the program.
async fn run() -> Result<()> {
    let args = Args::parse();

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
                .with_max_level(args.verbosity)
                .with_ansi(stderr().is_terminal())
                .with_writer(indicatif_layer.get_stderr_writer())
                .finish()
                .with(indicatif_layer);

            tracing::subscriber::set_global_default(subscriber)?;
        }
    }

    match args.mode {
        Mode::Inputs => {
            download_inputs(
                args.database()?,
                &args.tes_id,
                &args.inputs_dir.expect("option should be present"),
                &args.outputs_dir,
            )
            .await
        }
        Mode::Outputs => upload_outputs(args.database()?, &args.tes_id, &args.outputs_dir).await,
    }
}

/// The main method.
#[tokio::main]
pub async fn main() {
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
