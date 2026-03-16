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

use std::fs;
use std::fs::Permissions;
use std::io::BufReader;
use std::io::BufWriter;
use std::io::IsTerminal;
use std::io::stderr;
use std::os::unix::fs::PermissionsExt;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use byte_unit::Byte;
use byte_unit::UnitType;
use chrono::TimeDelta;
use chrono::Utc;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use cloud_copy::AzureConfig;
use cloud_copy::Config;
use cloud_copy::GoogleConfig;
use cloud_copy::HttpClient;
use cloud_copy::S3Config;
use cloud_copy::TransferEvent;
use cloud_copy::UrlExt;
use cloud_copy::cli::TimeDeltaExt;
use cloud_copy::cli::handle_events;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use glob::Pattern;
use reqwest::Url;
use secrecy::SecretString;
use serde::Serialize;
use serde::de::DeserializeOwned;
use serde_json::from_reader;
use serde_json::to_writer;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::task::Input;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Output;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::fs::set_permissions;
use tokio::pin;
use tokio::sync::broadcast;
use tokio_util::sync::CancellationToken;
use tracing::info;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use walkdir::WalkDir;

/// Prints the statistics after transferring inputs or outputs.
fn print_stats(delta: TimeDelta, files: usize, bytes: u64) {
    let seconds = delta.num_seconds();

    println!(
        "{files} file{s} copied with a total of {bytes:#} transferred in {time} ({speed:#.3}/s)",
        s = if files == 1 { "" } else { "s" },
        bytes = format!(
            "{:#.3}",
            Byte::from_u64(bytes).get_appropriate_unit(UnitType::Binary)
        ),
        time = delta.english(),
        speed = if seconds == 0 {
            Byte::from_u64(bytes)
        } else {
            Byte::from_u64(bytes / seconds as u64)
        }
        .get_appropriate_unit(UnitType::Binary)
    );
}

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
    /// The mode of operation.
    #[arg(long)]
    mode: Mode,

    /// The path to the orchestrator shared directory.
    #[arg(long)]
    orchestrator_dir: PathBuf,

    /// The path to the task's inputs directory.
    #[arg(long, required_if_eq("mode", "inputs"))]
    inputs_dir: Option<PathBuf>,

    /// The path to the task's outputs directory.
    #[arg(long)]
    outputs_dir: PathBuf,

    /// The verbosity level.
    #[command(flatten)]
    verbosity: Verbosity<WarnLevel>,

    /// The block size to use for file transfers; the default block size depends
    /// on the cloud service.
    #[clap(long, value_name = "SIZE")]
    block_size: Option<u64>,

    /// The parallelism level for network operations; defaults to the host's
    /// available parallelism.
    #[clap(long, value_name = "NUM")]
    parallelism: Option<usize>,

    /// The number of retries to attempt for network operations.
    #[clap(long, value_name = "RETRIES")]
    retries: Option<usize>,

    /// The Azure Storage account name to use.
    #[clap(long, env, value_name = "NAME", requires = "azure_access_key")]
    azure_account_name: Option<String>,

    /// The Azure Storage access key to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "KEY",
        requires = "azure_account_name"
    )]
    azure_access_key: Option<SecretString>,

    /// The AWS Access Key ID to use.
    #[clap(long, env, value_name = "ID")]
    aws_access_key_id: Option<String>,

    /// The AWS Secret Access Key to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "KEY",
        requires = "aws_access_key_id"
    )]
    aws_secret_access_key: Option<SecretString>,

    /// The default AWS region.
    #[clap(long, env, value_name = "REGION")]
    aws_default_region: Option<String>,

    /// The Google Cloud Storage HMAC access key to use.
    #[clap(long, env, value_name = "KEY")]
    google_hmac_access_key: Option<String>,

    /// The Google Cloud Storage HMAC secret to use.
    #[clap(
        long,
        env,
        hide_env_values(true),
        value_name = "SECRET",
        requires = "google_hmac_access_key"
    )]
    google_hmac_secret: Option<SecretString>,

    /// The TES identifier of the task.
    tes_id: String,
}

/// Gets the inputs file path for a TES task.
fn inputs_file_path(orchestrator_dir: &Path, tes_id: &str) -> PathBuf {
    orchestrator_dir.join(tes_id).join("inputs.json")
}

/// Gets the outputs file path for a TES task.
fn outputs_file_path(orchestrator_dir: &Path, tes_id: &str) -> PathBuf {
    orchestrator_dir.join(tes_id).join("outputs.json")
}

/// Gets the uploaded outputs file path for a TES task.
fn uploaded_outputs_file_path(orchestrator_dir: &Path, tes_id: &str) -> PathBuf {
    orchestrator_dir.join(tes_id).join("uploaded.json")
}

/// Helper function for serializing an array of serializable items to a file.
fn serialize_items<T: Serialize>(path: impl AsRef<Path>, items: &[T]) -> anyhow::Result<()> {
    let path = path.as_ref();

    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create directory `{parent}`",
                parent = parent.display()
            )
        })?;
    }

    let file = fs::File::create_new(path)
        .with_context(|| format!("failed to create file `{path}`", path = path.display()))?;

    to_writer(BufWriter::new(file), items)
        .with_context(|| format!("failed to write file `{path}`", path = path.display()))?;

    Ok(())
}

/// Helper function for deserializing an array of deserializable items from a
/// file.
fn deserialize_items<T: DeserializeOwned>(path: impl AsRef<Path>) -> anyhow::Result<Vec<T>> {
    let path = path.as_ref();

    let file = fs::File::open(path)
        .with_context(|| format!("failed to open file `{path}`", path = path.display()))?;

    from_reader(BufReader::new(file))
        .with_context(|| format!("failed to read file `{path}`", path = path.display()))
}

/// Downloads inputs into the inputs directory.
///
/// This will also create empty files and directories for outputs in the outputs
/// directory.
async fn download_inputs(
    config: Config,
    tes_id: &str,
    orchestrator_dir: &Path,
    inputs_dir: &Path,
    outputs_dir: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let inputs: Vec<Input> = deserialize_items(inputs_file_path(orchestrator_dir, tes_id))?;
    let outputs: Vec<Output> = deserialize_items(outputs_file_path(orchestrator_dir, tes_id))?;

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

    // Create an event handling task
    let (events_tx, events_rx) = broadcast::channel(1000);
    let c = cancel.clone();
    let handler = tokio::spawn(async move { handle_events(events_rx, false, c).await });

    let files_created = Arc::new(AtomicUsize::new(0));

    let client = HttpClient::new();
    let created = files_created.clone();
    let transfer = async move || {
        let mut downloads = stream::iter(inputs.into_iter().enumerate())
            .map(|(index, input)| {
                let path = inputs_dir.join(index.to_string());
                let cancel = cancel.clone();
                let config = config.clone();
                let client = client.clone();
                let events_tx = events_tx.clone();
                let created = created.clone();
                tokio::spawn(async move {
                    let permissions = if let Some(contents) = &input.content {
                        // Write the contents if directly given, but only if the input is a file
                        match input.ty {
                            IoType::File => {}
                            IoType::Directory => bail!(
                                "cannot create content for directory input `{path}`",
                                path = input.path
                            ),
                        }

                        info!(
                            "creating input file `{path}` with specified contents",
                            path = input.path,
                        );

                        tokio::fs::write(&path, contents).await.with_context(|| {
                            format!("failed to create input file `{path}`", path = input.path)
                        })?;

                        created.fetch_add(1, Ordering::SeqCst);
                        0o444
                    } else {
                        // Perform the cloud copy for a URL
                        let url = input
                            .url
                            .context("input is missing a URL")?
                            .parse::<Url>()
                            .context("input URL is invalid")?;

                        cloud_copy::copy(
                            config,
                            client.clone(),
                            url.clone(),
                            &path,
                            cancel,
                            Some(events_tx),
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
                        match input.ty {
                            IoType::Directory => {
                                if path.is_file() {
                                    bail!(
                                        "input `{url}` was a file but the input type was \
                                         `DIRECTORY`",
                                        url = url.display()
                                    );
                                }

                                0o555
                            }
                            IoType::File => {
                                if !path.is_file() {
                                    bail!(
                                        "input `{url}` was a directory but the input type was \
                                         `FILE`",
                                        url = url.display()
                                    );
                                }

                                0o444
                            }
                        }
                    };

                    // Set the permissions (world-readable) so any user an executor container runs
                    // as will be able to read the input
                    set_permissions(&path, Permissions::from_mode(permissions))
                        .await
                        .with_context(|| {
                            format!(
                                "failed to set permissions for input `{path}`",
                                path = input.path
                            )
                        })?;

                    Ok(())
                })
                .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(config.parallelism());

        loop {
            let result = downloads.next().await;
            match result {
                Some(r) => r?,
                None => break,
            }
        }

        anyhow::Ok(())
    };

    // Perform the transfer
    let start = Utc::now();
    let result = transfer().await;
    let end = Utc::now();

    let stats = handler.await.expect("failed to join events handler");

    // Print the statistics upon success
    if result.is_ok()
        && let Some(stats) = stats
    {
        print_stats(
            end - start,
            files_created.load(Ordering::SeqCst) + stats.files,
            stats.bytes,
        );
    }

    result?;

    // We also need to create any file outputs so that Kubernetes will mount them as
    // files and not directories
    for (index, output) in outputs.into_iter().enumerate() {
        let path = outputs_dir.join(index.to_string());
        let permissions = if output.ty == IoType::File {
            // Create the file
            File::create(&path)
                .await
                .with_context(|| format!("failed to create output `{path}`", path = output.path))?;

            0o666
        } else {
            // Create the directory
            create_dir_all(&path)
                .await
                .with_context(|| format!("failed to create output `{path}`", path = output.path))?;

            0o777
        };

        // Set the permissions (world-writable) so any user an executor container runs
        // as will be able to write to the output.
        set_permissions(&path, Permissions::from_mode(permissions))
            .await
            .with_context(|| {
                format!(
                    "failed to set permissions for output `{path}`",
                    path = output.path
                )
            })?;
    }

    Ok(())
}

/// Uploads outputs from the specified outputs directory.
async fn upload_outputs(
    config: Config,
    tes_id: &str,
    orchestrator_dir: &Path,
    outputs_dir: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let outputs: Vec<Output> = deserialize_items(outputs_file_path(orchestrator_dir, tes_id))?;

    // Create an event handling task
    let (events_tx, events_rx) = broadcast::channel(1000);
    let c = cancel.clone();
    let handler = tokio::spawn(async move { handle_events(events_rx, false, c).await });

    // Transfer the outputs
    let client = HttpClient::new();
    let transfer = async || {
        let mut files = Vec::new();
        for (index, output) in outputs.iter().enumerate() {
            let mut url = output.url.parse::<Url>().context("output URL is invalid")?;
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

            if metadata.is_dir() {
                files.extend(
                    upload_directory(
                        config.clone(),
                        client.clone(),
                        output,
                        &url,
                        path,
                        Some(events_tx.clone()),
                        cancel.clone(),
                    )
                    .await?,
                );
                continue;
            }

            if output.ty != IoType::File {
                bail!(
                    "output `{path}` exists but the output is not a file",
                    path = output.path
                );
            }

            // Perform the copy
            cloud_copy::copy(
                config.clone(),
                client.clone(),
                path,
                url.clone(),
                cancel.clone(),
                Some(events_tx.clone()),
            )
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
                size_bytes: metadata.len().to_string(),
            });
        }

        Ok(files)
    };

    // Perform the transfer
    let start = Utc::now();
    let result = transfer().await;
    let end = Utc::now();

    drop(events_tx);
    let stats = handler.await.expect("failed to join events handler");

    let outputs = result?;

    // Write the uploads file
    serialize_items(
        uploaded_outputs_file_path(orchestrator_dir, tes_id),
        &outputs,
    )?;

    // Print the statistics
    if let Some(stats) = stats {
        print_stats(end - start, stats.files, stats.bytes);
    }

    Ok(())
}

/// Uploads a directory output.
async fn upload_directory(
    config: Config,
    client: HttpClient,
    output: &Output,
    url: &Url,
    path: &str,
    events: Option<broadcast::Sender<TransferEvent>>,
    cancel: CancellationToken,
) -> Result<Vec<OutputFile>> {
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

    let mut files = Vec::new();
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
        if let Some(pattern) = &pattern
            && !pattern.matches(container_path)
        {
            info!("skipping output file `{container_path}` as it does not match the pattern");
            continue;
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
        cloud_copy::copy(
            config.clone(),
            client.clone(),
            entry.path(),
            url.clone(),
            cancel.clone(),
            events.clone(),
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

    Ok(files)
}

#[cfg(unix)]
/// An async function that waits for a termination signal.
async fn terminate(cancel: CancellationToken) {
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
    cancel.cancel();
}

#[cfg(windows)]
/// An async function that waits for a termination signal.
async fn terminate(cancel: CancellationToken) {
    use tokio::signal::windows::ctrl_c;
    use tracing::info;

    let mut signal = ctrl_c().expect("failed to create ctrl-c handler");
    signal.await;

    info!("received Ctrl-C signal: initiating shutdown");
    cancel.cancel();
}

/// Runs the program.
async fn run(cancel: CancellationToken) -> Result<()> {
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

    let azure = args
        .azure_account_name
        .and_then(|name| Some((name, args.azure_access_key?)))
        .map(|(name, key)| AzureConfig::default().with_auth(name, key))
        .unwrap_or_default();

    let s3 = args
        .aws_access_key_id
        .and_then(|id| Some((id, args.aws_secret_access_key?)))
        .map(|(id, key)| S3Config::default().with_auth(id, key))
        .unwrap_or_default()
        .with_maybe_region(args.aws_default_region);

    let google = args
        .google_hmac_access_key
        .and_then(|key| Some((key, args.google_hmac_secret?)))
        .map(|(key, secret)| GoogleConfig::default().with_auth(key, secret))
        .unwrap_or_default();

    let config = Config::builder()
        .with_link_to_cache(false)
        .with_overwrite(false)
        .with_maybe_block_size(args.block_size)
        .with_maybe_parallelism(args.parallelism)
        .with_azure(azure)
        .with_s3(s3)
        .with_google(google)
        .build();

    match args.mode {
        Mode::Inputs => {
            download_inputs(
                config,
                &args.tes_id,
                &args.orchestrator_dir,
                &args.inputs_dir.expect("option should be present"),
                &args.outputs_dir,
                cancel,
            )
            .await
        }
        Mode::Outputs => {
            upload_outputs(
                config,
                &args.tes_id,
                &args.orchestrator_dir,
                &args.outputs_dir,
                cancel,
            )
            .await
        }
    }
}

#[tokio::main]
async fn main() {
    let cancel = CancellationToken::new();

    let run = run(cancel.clone());
    pin!(run);

    loop {
        tokio::select! {
            biased;
            _ = terminate(cancel.clone()) => continue,
            r = &mut run => {
                if let Err(e) = r {
                    eprintln!("error: {e:?}");
                    std::process::exit(1);
                }

                break;
            }
        }
    }
}
