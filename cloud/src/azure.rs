//! Implementation of support for Azure Blob Storage.
//!
//! Currently this directly talks to the Azure Storage REST API as the Azure SDK
//! for Rust does not yet have support for SAS tokens.

use std::num::NonZero;
use std::thread::available_parallelism;

use reqwest::Response;
use serde::Deserialize;
use tokio::sync::broadcast;
use tracing::error;
use url::Url;

use crate::CopyConfig;
use crate::CopyError;
use crate::CopyEvent;
use crate::Location;
use crate::Result;
use crate::UrlExt;

mod download;
mod upload;

/// The Azure Blob Storage domain suffix.
pub(crate) const AZURE_STORAGE_DOMAIN_SUFFIX: &str = ".blob.core.windows.net";

/// Represents one mebibyte in bytes.
const ONE_MEBIBYTE: u64 = 1024 * 1024;

/// The default block size in bytes (4 MiB).
const DEFAULT_BLOCK_SIZE: u64 = 4 * ONE_MEBIBYTE;

/// The maximum block size in bytes (4000 MiB).
const MAX_BLOCK_SIZE: u64 = 4000 * ONE_MEBIBYTE;

/// The maximum number of blocks for any blob.
const MAX_BLOCK_COUNT: u64 = 50000;

/// The maximum supported blob size.
const MAX_BLOB_SIZE: u64 = MAX_BLOCK_SIZE * MAX_BLOCK_COUNT;

/// The threshold for which block size calculation uses to minimize the block
/// size (256 MiB).
const BLOCK_SIZE_THRESHOLD: u64 = 256 * ONE_MEBIBYTE;

/// The header for the Azure storage version supported.
const AZURE_VERSION_HEADER: &str = "x-ms-version";

/// The header for the blob type.
const AZURE_BLOB_TYPE_HEADER: &str = "x-ms-blob-type";

/// The header for the CRC64 checksum.
const AZURE_CONTENT_CRC_HEADER: &str = "x-ms-content-crc64";

/// The current supported Azure storage version.
const AZURE_STORAGE_VERSION: &str = "2025-05-05";

/// The Azure blob type uploaded by this tool.
const AZURE_BLOB_TYPE: &str = "BlockBlob";

/// Represents an Azure-specific copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum AzureCopyError {
    /// The specified Azure blob block size exceeds the maximum.
    #[error("Azure blob block size cannot exceed {MAX_BLOCK_SIZE} bytes")]
    InvalidBlockSize,
    /// The source size exceeds the supported maximum size.
    #[error("the size of the source file exceeds the supported maximum of {MAX_BLOB_SIZE} bytes")]
    MaximumSizeExceeded,
    /// Invalid URL with an `az` scheme.
    #[error("invalid URL with `az` scheme: the URL is not in a supported format")]
    InvalidAzureScheme,
    /// Unexpected response from server.
    #[error("unexpected {status} response from server: failed to deserialize response contents: {error}", status = .status.as_u16())]
    UnexpectedResponse {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The deserialization error.
        error: serde_xml_rs::Error,
    },
    /// The server returned an error.
    #[error("server return status {status}: {message}", status = .status.as_u16())]
    ServerError {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The response message.s
        message: String,
    },
    /// The server returned a response without a content length.
    #[error("the server returned a response without a content length")]
    ContentLengthMissing,
    /// The blob name is missing in the URL.
    #[error("a blob name is missing from the provided URL")]
    BlobNameMissing,
}

/// Represents copy configuration specific to Azure storage.
#[derive(Debug, Clone, Copy, Default)]
pub struct AzureCopyConfig {
    /// The Azure blob block size to use.
    block_size: Option<u64>,
    /// The parallelism level for copy operations.
    ///
    /// Defaults to the host's available parallelism.
    parallelism: Option<usize>,
}

impl AzureCopyConfig {
    /// Constructs a new Azure copy configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the blob block size (in bytes) for uploads.
    ///
    /// Returns an error if the size exceeds the maximum allowed blob block
    /// size.
    pub fn with_block_size(&mut self, size: u64) -> Result<&mut Self> {
        if size > MAX_BLOCK_SIZE {
            return Err(AzureCopyError::InvalidBlockSize.into());
        }

        self.block_size = Some(size);
        Ok(self)
    }

    /// Sets the parallelism count for copy operations.
    pub fn with_parallelism(&mut self, parallelism: usize) -> &mut Self {
        self.parallelism = Some(parallelism);
        self
    }

    /// Determines the blob block size for the given source file size (in
    /// bytes).
    ///
    /// Returns the configured blob block size (in bytes) if one was specified.
    ///
    /// Otherwise, this calculates a block size preferring to minimize the
    /// number of blocks.
    pub fn block_size(&self, source_size: u64) -> Result<u64> {
        // Return the block size if one was specified
        if let Some(size) = self.block_size {
            return Ok(size);
        }

        const BLOCK_COUNT_INCREMENT: u64 = 50;

        // Try to balance the number of blocks with the size of the blocks
        let mut num_blocks: u64 = BLOCK_COUNT_INCREMENT;
        while num_blocks < MAX_BLOCK_COUNT {
            let block_size = source_size.div_ceil(num_blocks).next_power_of_two();
            if block_size <= BLOCK_SIZE_THRESHOLD {
                return Ok(block_size.max(DEFAULT_BLOCK_SIZE));
            }

            num_blocks += BLOCK_COUNT_INCREMENT;
        }

        // Couldn't fit the number of blocks within the size threshold; fallback to
        // whatever will fit
        let block_size: u64 = source_size.div_ceil(MAX_BLOCK_COUNT);
        if block_size > MAX_BLOCK_SIZE {
            return Err(AzureCopyError::MaximumSizeExceeded.into());
        }

        Ok(block_size)
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
}

/// Converts an error response from an Azure REST API call to a `CopyError`.
async fn error_response(response: Response) -> CopyError {
    /// Represents an error response.
    #[derive(Deserialize)]
    #[serde(rename = "Error")]
    struct ErrorResponse {
        /// The error message.
        #[serde(rename = "Message")]
        message: String,
    }

    let status = response.status();
    let text = match response.text().await {
        Ok(text) => text,
        Err(e) => return e.into(),
    };

    let response: ErrorResponse = match serde_xml_rs::from_str(&text) {
        Ok(response) => response,
        Err(e) => {
            return AzureCopyError::UnexpectedResponse { status, error: e }.into();
        }
    };

    AzureCopyError::ServerError {
        status,
        message: response.message,
    }
    .into()
}

/// Rewrites an Azure Blob Storage URL (az://) into a HTTPS URL.
pub(crate) fn rewrite_url(url: &Url) -> Result<Url> {
    assert_eq!(url.scheme(), "az");

    let account = url.host_str().ok_or(AzureCopyError::InvalidAzureScheme)?;

    match (url.query(), url.fragment()) {
        (None, None) => format!(
            "https://{account}{AZURE_STORAGE_DOMAIN_SUFFIX}{path}",
            path = url.path()
        ),
        (None, Some(fragment)) => {
            format!(
                "https://{account}{AZURE_STORAGE_DOMAIN_SUFFIX}{path}#{fragment}",
                path = url.path()
            )
        }
        (Some(query), None) => format!(
            "https://{account}{AZURE_STORAGE_DOMAIN_SUFFIX}{path}?{query}",
            path = url.path()
        ),
        (Some(query), Some(fragment)) => {
            format!(
                "https://{account}{AZURE_STORAGE_DOMAIN_SUFFIX}{path}?{query}#{fragment}",
                path = url.path()
            )
        }
    }
    .parse()
    .map_err(|_| AzureCopyError::InvalidAzureScheme.into())
}

/// Copies a source location to a destination location.
///
/// Remote URLs are expected to be to Azure blob storage.
///
/// Remote URLs are expected to contain SAS tokens for authentication.
///
/// Copying between two remote locations is not supported.
pub(crate) async fn copy<'a, 'b>(
    config: CopyConfig,
    source: Location<'a>,
    destination: Location<'b>,
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
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

            upload::upload(config, source, destination, events).await
        }
        (Location::Remote(source), Location::Local(destination)) => {
            // Perform a copy if the the source is a local path
            if let Some(source) = source.to_local_path()? {
                return tokio::fs::copy(source, destination)
                    .await
                    .map(|_| ())
                    .map_err(Into::into);
            }

            download::download(config, source, destination, events).await
        }
        (Location::Remote(_), Location::Remote(_)) => Err(CopyError::RemoteCopyNotSupported),
    }
}
