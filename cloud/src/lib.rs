//! Cloud storage copy utility.
//!
//! The `cloud` crate offers a simple API for transferring files to and from
//! Azure Blob Storage, Amazon S3, and Google Cloud Storage.
//!
//! It exports only a single function named [`copy`] which is responsible for
//! copying a source to a destination.
//!
//! An optional transfer event stream provided to the [`copy`] function can be
//! used to display transfer progress.
//!
//! Additionally, when this crate is built with the `cli` feature enabled, a
//! [`handle_events`] function is exported that will display progress bars using
//! the `tracing_indicatif` crate.

#![deny(rustdoc::broken_intra_doc_links)]
#![cfg_attr(docsrs, feature(doc_cfg))]

use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::time::Duration;

use reqwest::Client;
use reqwest::StatusCode;
use tokio::sync::broadcast;
use tokio_retry2::RetryError;
use tracing::warn;
use url::Url;

use crate::backend::azure::AzureBlobStorageBackend;
use crate::backend::generic::GenericStorageBackend;
use crate::backend::s3::S3StorageBackend;
use crate::transfer::FileTransfer;

mod backend;
mod config;
mod generator;
mod os;
mod pool;
mod streams;
mod transfer;

pub use backend::azure::AzureError;
pub use backend::s3::S3Error;
pub use config::*;

/// The utility user agent.
const USER_AGENT: &str = concat!("cloud-copy v", env!("CARGO_PKG_VERSION"));

/// Represents one mebibyte in bytes.
const ONE_MEBIBYTE: u64 = 1024 * 1024;

/// The threshold for which block size calculation uses to minimize the block
/// size (256 MiB).
const BLOCK_SIZE_THRESHOLD: u64 = 256 * ONE_MEBIBYTE;

/// Helper for notifying that a network operation failed and will be retried.
fn notify_retry(e: &Error, duration: Duration) {
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
            .map_err(|_| Error::InvalidFileUrl(self.clone()))
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

/// Constructs a new HTTP client with default options.
fn new_http_client() -> Client {
    /// The timeout for the connecting phase of the client.
    const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(60);
    /// The timeout for a read of the client.
    const DEFAULT_READ_TIMEOUT: Duration = Duration::from_secs(60);

    Client::builder()
        .connect_timeout(DEFAULT_CONNECT_TIMEOUT)
        .read_timeout(DEFAULT_READ_TIMEOUT)
        .build()
        .expect("failed to build HTTP client")
}

/// Helper for displaying a message in `Error`.
struct DisplayMessage<'a> {
    /// The status code of the error.
    status: StatusCode,
    /// The message to display.
    ///
    /// If empty, the status code's canonical reason will be used.
    message: &'a str,
}

impl fmt::Display for DisplayMessage<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(
                f,
                " ({reason})",
                reason = self
                    .status
                    .canonical_reason()
                    .unwrap_or("<unknown status code>")
                    .to_lowercase()
            )
        } else {
            write!(f, ": {message}", message = self.message)
        }
    }
}

/// Represents a copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
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
    #[error(
        "server returned status {status}{message}",
        status = .status.as_u16(),
        message = DisplayMessage { status: *.status, message }
    )]
    Server {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The response error message.
        ///
        /// This may be the contents of the entire response body.
        message: String,
    },
    /// An Azure error occurred.
    #[error(transparent)]
    Azure(#[from] AzureError),
    /// An S3 error occurred.
    #[error(transparent)]
    S3(#[from] S3Error),
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

impl Error {
    /// Converts the error into a retry error.
    fn into_retry_error(self) -> RetryError<Self> {
        match &self {
            Error::Server { status, .. }
            | Error::Azure(AzureError::UnexpectedResponse { status, .. })
                if status.is_server_error() =>
            {
                RetryError::transient(self)
            }
            Error::Io(_) | Error::Reqwest(_) => RetryError::transient(self),
            _ => RetryError::permanent(self),
        }
    }
}

/// Represents a result for copy operations.
pub type Result<T> = std::result::Result<T, Error>;

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
/// If authentication is required, the URL is expected to contain a SAS token in
/// its query parameters.
///
/// # Amazon S3
///
/// Supported remote URLs for S3 Storage:
///
/// * `s3` schemed URLs in the format: `s3://<bucket>/<object>` (note: uses the
///   default region).
/// * `https` schemed URLs in the format `https://<bucket>.s3.<region>.amazonaws.com/<object>`.
/// * `https` schemed URLs in the format `https://<region>.s3.amazonaws.com/<bucket>/<object>`.
///
/// If authentication is required, the provided `Config` must have S3
/// authentication information.
///
/// # Google Cloud Storage
///
/// Support coming soon.
pub async fn copy(
    config: Config,
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

            if backend::azure::is_azure_url(&destination) {
                let destination = backend::azure::rewrite_url(destination)?;
                let transfer = FileTransfer::new(AzureBlobStorageBackend::new(config, events));
                transfer.upload(source, destination).await
            } else if backend::s3::is_s3_url(&destination) {
                let destination = backend::s3::rewrite_url(&config, destination)?;
                let transfer = FileTransfer::new(S3StorageBackend::new(config, events));
                transfer.upload(source, destination).await
            } else {
                Err(Error::UnsupportedUrl(destination))
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
                return Err(Error::DestinationExists(destination.to_path_buf()));
            }

            if backend::azure::is_azure_url(&source) {
                let source = backend::azure::rewrite_url(source)?;
                let transfer = FileTransfer::new(AzureBlobStorageBackend::new(config, events));
                transfer.download(source, destination).await
            } else if backend::s3::is_s3_url(&source) {
                let source = backend::s3::rewrite_url(&config, source)?;
                let transfer = FileTransfer::new(S3StorageBackend::new(config, events));
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

            Err(Error::RemoteCopyNotSupported)
        }
    }
}

/// Handles events that may occur during a copy operation.
///
/// This is responsible for showing and updating progress bars for files
/// being transferred.
///
/// Used from CLI implementations.
#[cfg(feature = "cli")]
#[cfg_attr(docsrs, doc(cfg(feature = "cli")))]
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
                    if let Some(transfer) = transfers.get_mut(&id) {
                        transfer.block_transfers.insert(block, BlockTransferState { size, transferred: 0});
                    }
                }
                Ok(TransferEvent::BlockProgress { id, block, transferred }) => {
                    if let Some(transfer) = transfers.get_mut(&id) {
                        if let Some(block) = transfer.block_transfers.get_mut(&block) {
                            transfer.transferred += transferred;
                            block.transferred += transferred;
                            transfer.bar.pb_set_position(transfer.transferred);
                        }
                    }
                }
                Ok(TransferEvent::BlockCompleted { id, block, failed }) => {
                    if let Some(transfer) = transfers.get_mut(&id) {
                        if let Some(block) = transfer.block_transfers.get_mut(&block) {
                            transfer.transferred -= block.transferred;
                            if !failed && let Some(size) = block.size {
                                transfer.transferred += size;
                            }

                            transfer.bar.pb_set_position(transfer.transferred);
                        }
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
