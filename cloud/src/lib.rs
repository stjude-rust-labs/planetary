//! Cloud storage copy utility.

use std::fmt;
use std::num::NonZero;
use std::path::Path;
use std::path::PathBuf;
use std::thread::available_parallelism;
use std::time::Duration;

use tokio::sync::broadcast;
use tokio_retry2::RetryError;
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_retry2::strategy::MaxInterval;
use tracing::warn;
use url::Host;
use url::Url;

use crate::backend::azure::AzureBlobStorageBackend;
use crate::backend::azure::AzureCopyError;
use crate::backend::generic::GenericStorageBackend;
use crate::transfer::FileTransfer;

mod backend;
mod generator;
mod os;
mod pool;
mod streams;
mod transfer;

/// The utility user agent.
const USER_AGENT: &str = concat!("cloud-copy v", env!("CARGO_PKG_VERSION"));

/// The default number of retries for network operations.
const DEFAULT_RETRIES: usize = 5;

/// Helper for notifying that a network operation failed and will be retried.
fn notify_retry(e: &CopyError, duration: Duration) {
    warn!(
        "network operation failed: {e} (retrying after {duration} seconds)",
        duration = duration.as_secs()
    );
}

/// Represents either a local or remote location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Location<'a> {
    /// The location is a local path.
    Path(&'a Path),
    /// The location is a URL.
    Url(Url),
}

impl<'a> Location<'a> {
    /// Constructs a new location.
    pub fn new(s: &'a str) -> Self {
        match s.parse::<Url>() {
            Ok(url) => Self::Url(url),
            Err(_) => Self::Path(Path::new(s)),
        }
    }
}

impl fmt::Display for Location<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Path(path) => write!(f, "{path}", path = path.display()),
            Self::Url(url) => write!(f, "{url}", url = url.display()),
        }
    }
}

impl<'a> From<&'a str> for Location<'a> {
    fn from(value: &'a str) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a String> for Location<'a> {
    fn from(value: &'a String) -> Self {
        Self::new(value)
    }
}

impl<'a> From<&'a Path> for Location<'a> {
    fn from(value: &'a Path) -> Self {
        Self::Path(value)
    }
}

impl<'a> From<&'a PathBuf> for Location<'a> {
    fn from(value: &'a PathBuf) -> Self {
        Self::Path(value.as_path())
    }
}

impl From<Url> for Location<'_> {
    fn from(value: Url) -> Self {
        Self::Url(value)
    }
}

/// Extension trait for `Url`.
pub trait UrlExt {
    /// Converts the URL to a local path if it uses the `file` scheme.
    ///
    /// Returns `Ok(None)` if the URL is not a `file` scheme.
    ///
    /// Returns an error if the URL uses a `file` scheme but cannot be
    /// represented as a local path.
    fn to_local_path(&self) -> Result<Option<PathBuf>>;

    /// Determines if the URL is for Azure.
    ///
    /// This method only returns true for `https` schemed URLs for Azure
    /// Storage.
    fn is_azure_storage(&self) -> bool;

    /// Displays a URL without its query parameters.
    ///
    /// This is used to prevent authentication information from being displayed
    /// to users.
    fn display(&self) -> impl fmt::Display;
}

impl UrlExt for Url {
    fn to_local_path(&self) -> Result<Option<PathBuf>> {
        if self.scheme() != "file" {
            return Ok(None);
        }

        self.to_file_path()
            .map(Some)
            .map_err(|_| CopyError::InvalidFileUrl(self.clone()))
    }

    fn is_azure_storage(&self) -> bool {
        if self.scheme() != "https" {
            return false;
        }

        self.host()
            .map(|host| match host {
                Host::Domain(domain) => domain
                    .strip_suffix(backend::azure::AZURE_STORAGE_DOMAIN_SUFFIX)
                    .is_some(),
                _ => false,
            })
            .unwrap_or(false)
    }

    fn display(&self) -> impl fmt::Display {
        /// Utility for displaying URLs without query parameters.
        struct Display<'a>(&'a Url);

        impl fmt::Display for Display<'_> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(
                    f,
                    "{scheme}://{host}{path}",
                    scheme = self.0.scheme(),
                    host = self.0.host_str().unwrap_or_default(),
                    path = self.0.path()
                )
            }
        }

        Display(self)
    }
}

/// Represents a copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum CopyError {
    /// Copying between remote locations is not supported.
    #[error("copying between remote locations is not supported")]
    RemoteCopyNotSupported,
    /// A remote URL has an unsupported URL scheme.
    #[error("remote URL has an unsupported URL scheme `{0}`")]
    UnsupportedUrlScheme(String),
    /// Unsupported remote URL.
    #[error("URL `{url}` is not for a supported cloud service", url = .0.display())]
    UnsupportedUrl(Url),
    /// Invalid URl with a `file` scheme.
    #[error("file URL `{url}` cannot be represented as a local path", url = .0.display())]
    InvalidFileUrl(Url),
    /// The specified path is invalid.
    #[error("the specified path cannot be a root directory or empty")]
    InvalidPath,
    /// The remote content was modified during a download.
    #[error("the remote content was modified during the download")]
    RemoteContentModified,
    /// Failed to create a directory.
    #[error("failed to create directory `{path}`: {error}", path = .path.display())]
    DirectoryCreationFailed {
        /// The path to the directory that failed to be created.
        path: PathBuf,
        /// The error that occurred creating the directory.
        error: std::io::Error,
    },
    /// Failed to create a temporary file.
    #[error("failed to create temporary file: {error}")]
    CreateTempFile {
        /// The error that occurred creating the temporary file.
        error: std::io::Error,
    },
    /// Failed to persist a temporary file.
    #[error("failed to persist temporary file: {error}")]
    PersistTempFile {
        /// The error that occurred creating the temporary file.
        error: std::io::Error,
    },
    /// The destination path already exists.
    #[error("the destination path `{path}` already exists", path = .0.display())]
    DestinationExists(PathBuf),
    /// The server returned an error.
    #[error("server returned status {status}: {message}", status = .status.as_u16())]
    ServerError {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The response error message.
        ///
        /// This may be the contents of the entire response body.
        message: String,
    },
    /// An Azure copy error occurred.
    #[error(transparent)]
    Azure(#[from] backend::azure::AzureCopyError),
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A directory walking error occurred.
    #[error(transparent)]
    Walk(#[from] walkdir::Error),
    /// A reqwest error occurred.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// A temp file persist error occurred.
    #[error(transparent)]
    Temp(#[from] tempfile::PersistError),
}

impl CopyError {
    /// Converts the copy error into a retry error.
    fn into_retry_error(self) -> RetryError<Self> {
        match &self {
            CopyError::ServerError { status, .. }
            | CopyError::Azure(AzureCopyError::UnexpectedResponse { status, .. })
                if status.is_server_error() =>
            {
                RetryError::transient(self)
            }
            CopyError::Io(_) | CopyError::Reqwest(_) => RetryError::transient(self),
            _ => RetryError::permanent(self),
        }
    }
}

/// Represents a result for copy operations.
pub type Result<T> = std::result::Result<T, CopyError>;

/// Used to configure a cloud copy operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyConfig {
    /// The block size to use for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    block_size: Option<u64>,
    /// The parallelism level for network operations.
    ///
    /// Defaults to the host's available parallelism.
    parallelism: Option<usize>,
    /// The number of retries to attempt for network operations.
    ///
    /// Defaults to `5`.
    retries: Option<usize>,
}

impl CopyConfig {
    /// Constructs a new copy configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the block size (in bytes) for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    pub fn with_block_size(mut self, size: u64) -> Self {
        self.block_size = Some(size);
        self
    }

    /// Sets the parallelism count for copy operations.
    ///
    /// Defaults to the host's available parallelism.
    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Sets the number of retries to use for network operations.
    ///
    /// The default number of retries is `5`.
    ///
    /// Retries use an exponential power of 2 backoff, starting at 1 second with
    /// a maximum duration of 10 minutes.
    pub fn with_retries(&mut self, retries: usize) -> &mut Self {
        self.retries = Some(retries);
        self
    }

    /// Gets the block size (in bytes) for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    pub fn block_size(&self) -> Option<u64> {
        self.block_size
    }

    /// Gets the parallelism supported for uploads and downloads.
    ///
    /// For uploads, this is the number of files and blocks that may be
    /// concurrently transferred.
    ///
    /// For downloads, this is the number of files that may be concurrently
    /// transferred.
    ///
    /// Defaults to the host's available parallelism.
    pub fn parallelism(&self) -> usize {
        self.parallelism
            .unwrap_or_else(|| available_parallelism().map(NonZero::get).unwrap_or(1))
    }

    /// Gets the number of retries for network operations.
    pub fn retries(&self) -> usize {
        self.retries.unwrap_or(DEFAULT_RETRIES)
    }

    /// Gets an iterator over the retry durations for network operations.
    ///
    /// Retries use an exponential power of 2 backoff, starting at 1 second with
    /// a maximum duration of 10 minutes.
    pub fn retry_durations<'a>(&self) -> impl Iterator<Item = Duration> + use<'a> {
        const INITIAL_DELAY_MILLIS: u64 = 1000;
        const BASE_FACTOR: f64 = 2.0;
        const MAX_DURATION: Duration = Duration::from_secs(600);

        ExponentialFactorBackoff::from_millis(INITIAL_DELAY_MILLIS, BASE_FACTOR)
            .max_duration(MAX_DURATION)
            .take(self.retries.unwrap_or(DEFAULT_RETRIES))
    }
}

/// Rewrites a cloud storage URL.
fn rewrite_url(url: Url) -> Result<Url> {
    match url.scheme() {
        "file" | "http" | "https" => Ok(url),
        "az" => Ok(backend::azure::rewrite_url(&url)?),
        scheme => Err(CopyError::UnsupportedUrlScheme(scheme.to_string())),
    }
}

/// Represents an event that may occur during a file transfer.
#[derive(Debug, Clone)]
pub enum TransferEvent {
    /// A transfer of a file has been started.
    TransferStarted {
        /// The id of the file transfer.
        ///
        /// This is a monotonic counter that is increased every transfer.
        id: u64,
        /// The path of the file being transferred.
        path: PathBuf,
        /// The number of blocks in the file.
        blocks: u64,
        /// The size of the file being transferred.
        ///
        /// This is `None` when downloading a file of unknown size.
        size: Option<u64>,
    },
    /// A transfer of a block has started.
    BlockStarted {
        /// The id of the file transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// The size of the block being transferred.
        ///
        /// This is `None` when downloading a file of unknown size (single
        /// block).
        size: Option<u64>,
    },
    /// A transfer of a remote file has made progress.
    BlockProgress {
        /// The id of the transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// The number of bytes transferred in this update.
        transferred: u64,
    },
    /// A transfer of a remote file has completed.
    BlockCompleted {
        /// The id of the transfer.
        id: u64,
        /// The block number being transferred.
        block: u64,
        /// Whether or not the transfer failed.
        failed: bool,
    },
    /// A file transfer has completed.
    TransferCompleted {
        /// The id of the transfer.
        id: u64,
        /// Whether or not the transfer failed.
        failed: bool,
    },
}

/// Copies a source location to a destination location.
///
/// A location may either be a local path or a remote URL.
///
/// _Note: copying between two remote locations is not supported._
///
/// # Azure Blob Storage
///
/// Supported remote URLs for Azure Blob Storage:
///
/// * `az` schemed URLs in the format `az://<account>/<container>/<blob>`.
/// * `https` schemed URLs in the format `https://<account>.blob.core.windows.net/<container>/<blob>`.
///
/// If authorization is required, the URL is expected to contain a SAS token in
/// its query parameters.
///
/// # Amazon S3
///
/// Support coming soon
///
/// # Google Cloud Storage
///
/// Support coming soon.
pub async fn copy(
    config: CopyConfig,
    source: impl Into<Location<'_>>,
    destination: impl Into<Location<'_>>,
    events: Option<broadcast::Sender<TransferEvent>>,
) -> Result<()> {
    let source = source.into();
    let destination = destination.into();

    match (source, destination) {
        (Location::Path(source), Location::Path(destination)) => {
            // Two local locations, just perform a copy
            tokio::fs::copy(source, destination)
                .await
                .map(|_| ())
                .map_err(Into::into)
        }
        (Location::Path(source), Location::Url(destination)) => {
            // Perform a copy if the the destination is a local path
            if let Some(destination) = destination.to_local_path()? {
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            let destination = rewrite_url(destination)?;
            if destination.is_azure_storage() {
                let transfer = FileTransfer::new(AzureBlobStorageBackend::new(config, events));
                transfer.upload(source, destination).await
            } else {
                Err(CopyError::UnsupportedUrl(destination))
            }
        }
        (Location::Url(source), Location::Path(destination)) => {
            // Perform a copy if the the source is a local path
            if let Some(source) = source.to_local_path()? {
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            if destination.exists() {
                return Err(CopyError::DestinationExists(destination.to_path_buf()));
            }

            let source = rewrite_url(source)?;
            if source.is_azure_storage() {
                let transfer = FileTransfer::new(AzureBlobStorageBackend::new(config, events));
                transfer.download(source, destination).await
            } else {
                let transfer = FileTransfer::new(GenericStorageBackend::new(config, events));
                transfer.download(source, destination).await
            }
        }
        (Location::Url(source), Location::Url(destination)) => {
            if let (Some(source), Some(destination)) =
                (source.to_local_path()?, destination.to_local_path()?)
            {
                // Two local locations, just perform a copy
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            Err(CopyError::RemoteCopyNotSupported)
        }
    }
}

/// Handles events that may occur during the copy operation.
///
/// This is responsible for showing and updating progress bars for files
/// being transferred.
///
/// Used from CLI implementations.
#[cfg(feature = "cli")]
pub async fn handle_events(
    mut events: broadcast::Receiver<TransferEvent>,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
) {
    use std::collections::HashMap;

    use tokio::sync::broadcast::error::RecvError;
    use tracing::Span;
    use tracing::warn_span;
    use tracing_indicatif::span_ext::IndicatifSpanExt;
    use tracing_indicatif::style::ProgressStyle;

    struct BlockTransferState {
        /// The size of the block being transferred.
        size: Option<u64>,
        /// The number of bytes that were transferred for the block.
        transferred: u64,
    }

    struct TransferState {
        /// The progress bar to display for a transfer.
        bar: Span,
        /// The total number of bytes transferred.
        transferred: u64,
        /// Block transfer state.
        block_transfers: HashMap<u64, BlockTransferState>,
    }

    let mut transfers = HashMap::new();
    let mut warned = false;

    loop {
        tokio::select! {
            _ = &mut shutdown => break,
            event = events.recv() => match event {
                Ok(TransferEvent::TransferStarted { id, path, size, .. }) => {
                    let bar = warn_span!("progress");

                    let style = match size {
                        Some(size) => {
                            bar.pb_set_length(size);
                            ProgressStyle::with_template(
                                "[{elapsed_precise:.cyan/blue}] {bar:40.cyan/blue} {bytes:.cyan/blue} / {total_bytes:.cyan/blue} ({bytes_per_sec:.cyan/blue}) [ETA {eta_precise:.cyan/blue}]: {msg}",
                            ).unwrap()
                        }
                        None => {
                            ProgressStyle::with_template(
                                "[{elapsed_precise:.cyan/blue}] {spinner:.cyan/blue} {bytes:.cyan/blue} ({bytes_per_sec:.cyan/blue}): {msg}",
                            ).unwrap()
                        }
                    };

                    bar.pb_set_style(&style);
                    bar.pb_set_message(path.to_str().unwrap_or("<path not UTF-8>"));
                    bar.pb_start();
                    transfers.insert(id, TransferState { bar, transferred: 0, block_transfers: HashMap::new() });
                }
                Ok(TransferEvent::BlockStarted { id, block, size }) => {
                    if let Some(state) = transfers.get_mut(&id) {
                        state.block_transfers.insert(block, BlockTransferState { size, transferred: 0});
                    }
                }
                Ok(TransferEvent::BlockProgress { id, block, transferred }) => {
                    if let Some(state) = transfers.get_mut(&id) {
                        state.transferred += transferred;

                        if let Some(state) = state.block_transfers.get_mut(&block) {
                            state.transferred += transferred;
                        }

                        state.bar.pb_set_position(state.transferred);
                    }
                }
                Ok(TransferEvent::BlockCompleted { id, block, failed }) => {
                    if let Some(state) = transfers.get_mut(&id) {
                        let block = state.block_transfers.remove(&block).unwrap();
                        state.transferred -= block.transferred;

                        if !failed && let Some(size) = block.size {
                            state.transferred += size;
                        }

                        state.bar.pb_set_position(state.transferred);
                    }
                }
                Ok(TransferEvent::TransferCompleted { id, .. }) => {
                    transfers.remove(&id);
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
