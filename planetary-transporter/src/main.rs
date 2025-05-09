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

use std::fmt;
use std::io::IsTerminal;
use std::path::Component;
use std::path::Path;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use anyhow::Result;
use anyhow::anyhow;
use anyhow::bail;
use clap::Parser;
use clap_verbosity_flag::Verbosity;
use clap_verbosity_flag::WarnLevel;
use futures::stream::StreamExt;
use glob::Pattern;
use planetary_db::Database;
use reqwest::Url;
use tes::v1::types::responses::OutputFile;
use tes::v1::types::task::Input;
use tes::v1::types::task::IoType;
use tes::v1::types::task::Output;
use tokio::fs::File;
use tokio::fs::create_dir_all;
use tokio::io::AsyncWriteExt;
use tokio::io::BufWriter;
use tokio::sync::Semaphore;
use tokio::task::JoinSet;
use tracing::error;
use tracing::info;
use tracing_log::AsTrace;
use tracing_subscriber::EnvFilter;
use walkdir::WalkDir;

mod azure;

/// The maximum number of concurrent downloads.
const MAX_CONCURRENT_DOWNLOADS: usize = 10;

/// Rewrites a cloud storage URL to `https`.
pub fn rewrite_url(url: &str) -> Result<Url> {
    let url: Url = url
        .parse()
        .with_context(|| format!("invalid URL `{url}`", url = DisplayUrl(url)))?;
    match url.scheme() {
        "http" | "https" => Ok(url),
        "az" => Ok(azure::rewrite_url(&url)?),
        scheme => bail!("URL scheme `{scheme}` is not supported"),
    }
}

/// Helper for displaying URLs in log messages.
struct DisplayUrl<T>(T);

impl<T: AsRef<str>> fmt::Display for DisplayUrl<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let s = self.0.as_ref();
        let url = if let Some(index) = s.find('?') {
            &s[..index]
        } else {
            s
        };

        write!(f, "{url}")
    }
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

    /// The verbosity flags.
    #[command(flatten)]
    verbose: Verbosity<WarnLevel>,
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

    // Spawn concurrent input downloads
    let semaphore = Arc::new(Semaphore::new(MAX_CONCURRENT_DOWNLOADS));
    let mut set = JoinSet::new();
    for (index, input) in task_io.inputs.into_iter().enumerate() {
        let semaphore = semaphore.clone();
        let path = inputs_dir.join(index.to_string());
        set.spawn(async move {
            let _permit = semaphore.acquire().await.expect("failed to acquire permit");

            let path = path
                .to_str()
                .with_context(|| format!("path `{path}` is not UTF-8", path = path.display()))?;

            if input.ty == IoType::File {
                download_file(&input, path).await?;
            } else {
                download_directory(&input, path).await?;
            }

            anyhow::Ok(())
        });
    }

    // Wait for all downloads to complete
    loop {
        let res = set.join_next().await;
        match res {
            Some(res) => {
                res.expect("task panicked")?;
            }
            None => break,
        }
    }

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

/// Downloads a file input to the given path.
async fn download_file(input: &Input, path: &str) -> Result<()> {
    assert_eq!(input.ty, IoType::File);

    if let Some(contents) = &input.content {
        info!(
            "creating input file `{path}` with specified contents",
            path = input.path,
        );
        return tokio::fs::write(&path, contents)
            .await
            .with_context(|| format!("failed to create input file `{path}`", path = input.path));
    }

    let url = input.url.as_deref().context("input is missing URL")?;
    let url: Url = rewrite_url(url)
        .with_context(|| format!("invalid input URL `{url}`", url = DisplayUrl(url)))?;

    info!(
        "downloading `{url}` to input file `{path}`",
        url = DisplayUrl(&url),
        path = input.path,
    );

    if azure::is_azure_url(&url) {
        // Use copy as it might be more efficient that downloading the file as a stream
        return azure::copy(url.as_str(), path, false)
            .await
            .with_context(|| {
                format!(
                    "failed to download input `{url}` to `{path}`",
                    url = DisplayUrl(&url),
                    path = input.path,
                )
            });
    }

    let response = reqwest::get(url.as_str())
        .await
        .with_context(|| format!("failed to download `{url}`"))?;

    if response.status().is_success() {
        bail!(
            "failed to download `{url}`: server returned status {status}",
            url = DisplayUrl(&url),
            status = response.status()
        );
    }

    let mut stream = response.bytes_stream();
    let mut writer =
        BufWriter::new(File::create(&path).await.with_context(|| {
            format!("failed to create input file `{path}`", path = input.path,)
        })?);
    loop {
        let chunk = stream.next().await;
        match chunk {
            Some(chunk) => {
                let chunk = chunk.with_context(|| format!("failed to download `{url}`"))?;
                writer.write_all(&chunk).await?;
            }
            None => break,
        }
    }
    writer.flush().await?;
    Ok(())
}

/// Downloads a directory input to the given path.
async fn download_directory(input: &Input, path: &str) -> Result<()> {
    assert_eq!(input.ty, IoType::Directory);

    let url = input.url.as_deref().context("input is missing URL")?;
    let url: Url = rewrite_url(url)
        .with_context(|| format!("invalid input URL `{url}`", url = DisplayUrl(url)))?;

    info!(
        "downloading `{url}` to input directory `{path}`",
        url = DisplayUrl(&url),
        path = input.path,
    );

    if azure::is_azure_url(&url) {
        return azure::copy(url.as_str(), path, true)
            .await
            .with_context(|| {
                format!(
                    "failed to download input `{url}` to `{path}`",
                    url = DisplayUrl(&url),
                    path = input.path,
                )
            });
    } else {
        bail!("only Azure storage URLs are currently supported")
    }
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

    let mut files = Vec::new();
    for (index, output) in task_io.outputs.iter().enumerate() {
        let url: Url = rewrite_url(&output.url).with_context(|| {
            format!("invalid output URL `{url}`", url = DisplayUrl(&output.url))
        })?;

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
            upload_file(output, url, path, metadata.len(), &mut files).await?;
        } else {
            upload_directory(output, &url, path, &mut files).await?;
        }
    }

    database.update_task_output_files(tes_id, &files).await?;
    Ok(())
}

/// Uploads a file output.
async fn upload_file(
    output: &Output,
    mut url: Url,
    path: &str,
    size: u64,
    files: &mut Vec<OutputFile>,
) -> Result<()> {
    if output.ty != IoType::File {
        bail!(
            "output `{path}` exists but the output is not a file",
            path = output.path
        );
    }

    info!(
        "uploading output file `{path}` to `{url}`",
        path = output.path,
        url = DisplayUrl(&url),
    );

    if azure::is_azure_url(&url) {
        azure::copy(path, url.as_str(), false)
            .await
            .with_context(|| {
                format!(
                    "failed to upload output `{path}` to `{url}`",
                    path = output.path,
                    url = DisplayUrl(&url),
                )
            })?;
    } else {
        bail!("only Azure storage URLs are currently supported")
    }

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
            url = DisplayUrl(url)
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

        if azure::is_azure_url(&url) {
            azure::copy(
                entry.path().to_str().with_context(|| {
                    format!("path `{path}` is not UTF-8", path = entry.path().display())
                })?,
                url.as_str(),
                false,
            )
            .await
            .with_context(|| {
                format!(
                    "failed to upload output `{path}` to `{url}`",
                    path = output.path,
                    url = DisplayUrl(&url),
                )
            })?;
        } else {
            bail!("only Azure storage URLs are currently supported")
        }

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

/// The main method.
#[tokio::main]
pub async fn main() -> Result<()> {
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
