//! Cloud storage copy utility.

use std::fmt;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;

use tokio::sync::broadcast;
use url::Host;
use url::Url;

mod azure;
mod generator;
mod os;

/// The utility user agent.
const USER_AGENT: &str = concat!("cloud-copy v", env!("CARGO_PKG_VERSION"));

/// Represents either a local or remote location.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum Location<'a> {
    /// The location is a local path.
    Local(&'a Path),
    /// The location is a remote URL.
    Remote(Url),
}

impl<'a> Location<'a> {
    /// Constructs a new location.
    pub fn new(s: &'a str) -> Self {
        match s.parse::<Url>() {
            Ok(url) => Self::Remote(url),
            Err(_) => Self::Local(Path::new(s)),
        }
    }
}

impl fmt::Display for Location<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Local(path) => write!(f, "{path}", path = path.display()),
            Self::Remote(url) => write!(f, "{url}", url = url.display()),
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
        Self::Local(value)
    }
}

impl<'a> From<&'a PathBuf> for Location<'a> {
    fn from(value: &'a PathBuf) -> Self {
        Self::Local(value.as_path())
    }
}

impl From<Url> for Location<'_> {
    fn from(value: Url) -> Self {
        Self::Remote(value)
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
            .map_err(|_| CopyError::InvalidFileUrl)
    }

    fn is_azure_storage(&self) -> bool {
        if self.scheme() != "https" {
            return false;
        }

        self.host()
            .map(|host| match host {
                Host::Domain(domain) => domain
                    .strip_suffix(azure::AZURE_STORAGE_DOMAIN_SUFFIX)
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
    #[error("remote URL has an unsupported URL scheme")]
    UnsupportedUrlScheme,
    /// Unsupported remote URL.
    #[error("remote URL is not for a supported cloud service")]
    UnsupportedUrl,
    /// Invalid URl with a `file` scheme.
    #[error("invalid URL with `file` scheme: the URL cannot be represented as a local path")]
    InvalidFileUrl,
    /// The specified path is invalid.
    #[error("the specified path cannot be a root directory or empty")]
    InvalidPath,
    /// The destination path already exists.
    #[error("the destination path already exists")]
    DestinationExists,
    /// An Azure copy error occurred.
    #[error(transparent)]
    Azure(#[from] azure::AzureCopyError),
    /// An I/O error occurred.
    #[error(transparent)]
    Io(#[from] std::io::Error),
    /// A directory walking error occurred.
    #[error(transparent)]
    Walk(#[from] walkdir::Error),
    /// A reqwest error occurred.
    #[error(transparent)]
    Reqwest(#[from] reqwest::Error),
    /// A temp file persistent error occurred.
    #[error(transparent)]
    Temp(#[from] tempfile::PersistError),
}

/// Represents a result for copy operations.
pub type Result<T> = std::result::Result<T, CopyError>;

/// Used to configure a cloud copy operation.
#[derive(Debug, Clone, Copy, Default)]
pub struct CopyConfig {
    /// The Azure copy configuration.
    azure: azure::AzureCopyConfig,
}

impl CopyConfig {
    /// Constructs a new copy configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Gets the Azure copy configuration.
    pub fn azure(&self) -> &azure::AzureCopyConfig {
        &self.azure
    }

    /// Gets a mutable Azure copy configuration.
    pub fn azure_mut(&mut self) -> &mut azure::AzureCopyConfig {
        &mut self.azure
    }
}

/// Rewrites a cloud storage URL.
fn rewrite_url(url: Url) -> Result<Url> {
    match url.scheme() {
        "file" | "https" => Ok(url),
        "az" => Ok(azure::rewrite_url(&url)?),
        _ => Err(CopyError::UnsupportedUrlScheme),
    }
}

/// Represents an event that may occur during a copy operation.
#[derive(Debug, Clone)]
pub enum CopyEvent {
    /// A transfer of a remote file has started.
    TransferStarted {
        /// The id of the remote file that has started.
        id: Arc<String>,
        /// The path of the local file.
        path: PathBuf,
        /// The size of the file, in bytes.
        size: u64,
    },
    /// A transfer of a remote file has made progress.
    TransferProgress {
        /// The id of the file that made progress.
        id: Arc<String>,
        /// The number of bytes transferred in this update.
        transferred: u64,
    },
    /// A transfer of a remote file has completed.
    TransferComplete {
        /// The id of the file that has completed.
        id: Arc<String>,
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
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
    let source = source.into();
    let destination = destination.into();

    match (source, destination) {
        (Location::Local(source), Location::Local(destination)) => {
            // Two local locations, just perform a copy
            tokio::fs::copy(source, destination)
                .await
                .map(|_| ())
                .map_err(Into::into)
        }
        (Location::Local(source), Location::Remote(destination)) => {
            // Perform a copy if the the destination is a local path
            if let Some(destination) = destination.to_local_path()? {
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            let destination = rewrite_url(destination)?;
            if destination.is_azure_storage() {
                azure::copy(
                    config,
                    Location::Local(source),
                    Location::Remote(destination),
                    events.clone(),
                )
                .await
            } else {
                Err(CopyError::UnsupportedUrl)
            }
        }
        (Location::Remote(source), Location::Local(destination)) => {
            // Perform a copy if the the source is a local path
            if let Some(source) = source.to_local_path()? {
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            if destination.exists() {
                return Err(CopyError::DestinationExists);
            }

            let source = rewrite_url(source)?;
            if source.is_azure_storage() {
                azure::copy(
                    config,
                    Location::Remote(source),
                    Location::Local(destination),
                    events.clone(),
                )
                .await
            } else {
                Err(CopyError::UnsupportedUrl)
            }
        }
        (Location::Remote(_), Location::Remote(_)) => Err(CopyError::RemoteCopyNotSupported),
    }
}
