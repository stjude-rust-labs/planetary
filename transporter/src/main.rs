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

use std::fs::Permissions;
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
use std::time::Duration;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
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
use colored::Colorize;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use glob::Pattern;
use planetary_db::TaskIo;
use reqwest::Url;
use reqwest::header;
use secrecy::ExposeSecret;
use secrecy::SecretString;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Output;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::fs::set_permissions;
use tokio::pin;
use tokio::sync::broadcast;
use tokio_retry2::Retry;
use tokio_retry2::RetryError;
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_retry2::strategy::MaxInterval;
use tokio_util::sync::CancellationToken;
use tracing::error;
use tracing::info;
use tracing::warn;
use tracing_indicatif::IndicatifLayer;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use walkdir::WalkDir;

/// Gets an iterator over the retry durations for network operations.
///
/// Retries use an exponential power of 2 backoff, starting at 1 second with
/// a maximum duration of 60 seconds.
fn retry_durations() -> impl Iterator<Item = Duration> {
    const INITIAL_DELAY_MILLIS: u64 = 1000;
    const BASE_FACTOR: f64 = 2.0;
    const MAX_DURATION: Duration = Duration::from_secs(60);
    const RETRIES: usize = 5;

    ExponentialFactorBackoff::from_millis(INITIAL_DELAY_MILLIS, BASE_FACTOR)
        .max_duration(MAX_DURATION)
        .take(RETRIES)
}

/// Helper for notifying that a network operation failed and will be retried.
fn notify_retry(e: &reqwest_middleware::Error, duration: Duration) {
    warn!(
        "network operation failed: {e} (retrying after {duration} seconds)",
        duration = duration.as_secs()
    );
}

/// Prints the statistics after transferring inputs or outputs.
fn print_stats(delta: TimeDelta, files: usize, bytes: u64) {
    let seconds = delta.num_seconds();

    println!(
        "{files} file{s} copied with a total of {bytes:#} transferred in {time} ({speed})",
        files = files.to_string().cyan(),
        s = if files == 1 { "" } else { "s" },
        bytes = format!(
            "{:#.3}",
            Byte::from_u64(bytes).get_appropriate_unit(UnitType::Binary)
        )
        .cyan(),
        time = delta.english().to_string().cyan(),
        speed = format!(
            "{bytes:#.3}/s",
            bytes = if seconds == 0 || bytes < 60 {
                Byte::from_u64(bytes)
            } else {
                Byte::from_u64(bytes / seconds as u64)
            }
            .get_appropriate_unit(UnitType::Binary)
        )
        .cyan()
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

/// Represents information about the orchestrator service.
struct OrchestratorServiceInfo {
    /// The URL of the orchestrator service.
    url: Url,
    /// The orchestrator service API key.
    api_key: SecretString,
}

/// A tool for transporting Planetary task's inputs and outputs.
#[derive(Parser)]
struct Args {
    /// The URL for the orchestrator service.
    #[clap(long, env)]
    orchestrator_url: Url,

    /// The Planetary orchestrator service API key.
    #[clap(long, env, hide_env_values(true))]
    orchestrator_api_key: SecretString,

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
}

/// Gets the inputs and outputs for the given task.
async fn get_task_io(
    client: &HttpClient,
    orchestrator: &OrchestratorServiceInfo,
    tes_id: &str,
) -> Result<TaskIo> {
    Ok(Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to the server
            // Don't retry if the service returned an error response
            client
                .get(format!("{url}v1/tasks/{tes_id}/io", url = orchestrator.url))
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Bearer {token}",
                        token = orchestrator.api_key.expose_secret()
                    ),
                )
                .send()
                .await
                .map_err(RetryError::transient)?
                .error_for_status()
                .map_err(|e| RetryError::permanent(e.into()))?
                .json()
                .await
                .map_err(|e| RetryError::permanent(e.into()))
        },
        notify_retry,
    )
    .await?)
}

/// Updates a task's output files.
async fn update_output_files(
    client: &HttpClient,
    orchestrator: &OrchestratorServiceInfo,
    tes_id: &str,
    outputs: &[OutputFile],
) -> Result<()> {
    Retry::spawn_notify(
        retry_durations(),
        || async {
            // Retry the operation is there is a problem sending the request to the server
            // Don't retry if the service returned an error response
            client
                .put(format!(
                    "{url}v1/tasks/{tes_id}/outputs",
                    url = orchestrator.url
                ))
                .header(
                    header::AUTHORIZATION,
                    format!(
                        "Bearer {token}",
                        token = orchestrator.api_key.expose_secret()
                    ),
                )
                .json(outputs)
                .send()
                .await
                .map_err(RetryError::transient)?
                .error_for_status()
                .map_err(|e| RetryError::permanent(e.into()))
        },
        notify_retry,
    )
    .await?;

    Ok(())
}

/// Downloads inputs into the inputs directory.
///
/// This will also create empty files and directories for outputs in the outputs
/// directory.
async fn download_inputs(
    config: Config,
    orchestrator: &OrchestratorServiceInfo,
    tes_id: &str,
    inputs_dir: &Path,
    outputs_dir: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let client = HttpClient::new();
    let task_io = get_task_io(&client, orchestrator, tes_id)
        .await
        .map_err(|e| {
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

    // Create an event handling task
    let (events_tx, events_rx) = broadcast::channel(1000);
    let c = cancel.clone();
    let handler = tokio::spawn(async move { handle_events(events_rx, c).await });

    let files_created = Arc::new(AtomicUsize::new(0));

    let created = files_created.clone();
    let transfer = async move || {
        let mut downloads = stream::iter(task_io.inputs.into_iter().enumerate())
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
    for (index, output) in task_io.outputs.into_iter().enumerate() {
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
    orchestrator: &OrchestratorServiceInfo,
    tes_id: &str,
    outputs_dir: &Path,
    cancel: CancellationToken,
) -> Result<()> {
    let client = HttpClient::new();
    let task_io = get_task_io(&client, orchestrator, tes_id)
        .await
        .map_err(|e| {
            error!("failed to retrieve inputs and outputs of task `{tes_id}`: {e:#}");
            anyhow!("failed to retrieve information for task `{tes_id}`")
        })?;

    // Create an event handling task
    let (events_tx, events_rx) = broadcast::channel(1000);
    let c = cancel.clone();
    let handler = tokio::spawn(async move { handle_events(events_rx, c).await });

    // Transfer the outputs
    let transfer = async || {
        let mut files = Vec::new();
        for (index, output) in task_io.outputs.iter().enumerate() {
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

    // Print the statistics upon success
    if result.is_ok()
        && let Some(stats) = stats
    {
        print_stats(end - start, stats.files, stats.bytes);
    }

    if let Err(e) = update_output_files(&client, orchestrator, tes_id, &result?).await {
        error!("failed to update output files for task `{tes_id}`: {e:#}");
        bail!("failed to update output files for task `{tes_id}`")
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

    let orchestrator = OrchestratorServiceInfo {
        url: args.orchestrator_url,
        api_key: args.orchestrator_api_key,
    };

    match args.mode {
        Mode::Inputs => {
            download_inputs(
                config,
                &orchestrator,
                &args.tes_id,
                &args.inputs_dir.expect("option should be present"),
                &args.outputs_dir,
                cancel,
            )
            .await
        }
        Mode::Outputs => {
            upload_outputs(
                config,
                &orchestrator,
                &args.tes_id,
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

                break;
            }
        }
    }
}
