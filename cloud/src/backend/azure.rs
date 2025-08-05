//! Implementation of the Azure Blob Storage backend.

use std::ops::Range;
use std::sync::Arc;

use base64::Engine;
use base64::prelude::BASE64_STANDARD;
use chrono::Utc;
use crc64fast_nvme::Digest;
use reqwest::Body;
use reqwest::Client;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::header;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::debug;
use url::Url;

use crate::BLOCK_SIZE_THRESHOLD;
use crate::Config;
use crate::Error;
use crate::ONE_MEBIBYTE;
use crate::Result;
use crate::TransferEvent;
use crate::USER_AGENT;
use crate::UrlExt;
use crate::backend::StorageBackend;
use crate::backend::Upload;
use crate::generator::Alphanumeric;
use crate::new_http_client;
use crate::streams::ByteStream;
use crate::streams::TransferStream;

/// The Azure Blob Storage domain suffix.
const AZURE_BLOB_STORAGE_ROOT_DOMAIN: &str = "blob.core.windows.net";

/// The default block size in bytes (4 MiB).
const DEFAULT_BLOCK_SIZE: u64 = 4 * ONE_MEBIBYTE;

/// The maximum block size in bytes (4000 MiB).
const MAX_BLOCK_SIZE: u64 = 4000 * ONE_MEBIBYTE;

/// The maximum number of blocks for any blob.
const MAX_BLOCK_COUNT: u64 = 50000;

/// The maximum supported blob size.
const MAX_BLOB_SIZE: u64 = MAX_BLOCK_SIZE * MAX_BLOCK_COUNT;

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

/// The name of the root container.
const AZURE_ROOT_CONTAINER: &str = "$root";

/// Represents an Azure-specific copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum AzureError {
    /// The specified Azure blob block size exceeds the maximum.
    #[error("Azure blob block size cannot exceed {MAX_BLOCK_SIZE} bytes")]
    InvalidBlockSize,
    /// The source size exceeds the supported maximum size.
    #[error("the size of the source file exceeds the supported maximum of {MAX_BLOB_SIZE} bytes")]
    MaximumSizeExceeded,
    /// Invalid URL with an `az` scheme.
    #[error("invalid URL with `az` scheme: the URL is not in a supported format")]
    InvalidScheme,
    /// Cannot upload a directory to the root container.
    #[error("uploading a directory to the root container is not supported by Azure")]
    RootDirectoryUploadNotSupported,
    /// Unexpected response from server.
    #[error("unexpected {status} response from server: failed to deserialize response contents: {error}", status = .status.as_u16())]
    UnexpectedResponse {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The deserialization error.
        error: serde_xml_rs::Error,
    },
    /// The blob name is missing in the URL.
    #[error("a blob name is missing from the provided URL")]
    BlobNameMissing,
}

/// Determines if the given URL is an Azure Blob Storage URL.
pub fn is_azure_url(url: &Url) -> bool {
    match url.scheme() {
        "az" => true,
        "https" => {
            let Some(domain) = url.domain() else {
                return false;
            };

            // Virtual host style URL of the form https://<account>.blob.core.windows.net/<container>/<path>
            let Some((_, domain)) = domain.split_once('.') else {
                return false;
            };
            domain.eq_ignore_ascii_case(AZURE_BLOB_STORAGE_ROOT_DOMAIN)
        }
        _ => false,
    }
}

/// Rewrites an Azure Blob Storage URL (az://) into a HTTPS URL.
///
/// If the URL is not `az` schemed, the given URL is returned as-is.
pub fn rewrite_url(url: Url) -> Result<Url> {
    match url.scheme() {
        "az" => {
            let account = url.host_str().ok_or(AzureError::InvalidScheme)?;

            if url.path() == "/" {
                return Err(AzureError::InvalidScheme.into());
            }

            match (url.query(), url.fragment()) {
                (None, None) => format!(
                    "https://{account}.{AZURE_BLOB_STORAGE_ROOT_DOMAIN}{path}",
                    path = url.path()
                ),
                (None, Some(fragment)) => {
                    format!(
                        "https://{account}.{AZURE_BLOB_STORAGE_ROOT_DOMAIN}{path}#{fragment}",
                        path = url.path()
                    )
                }
                (Some(query), None) => format!(
                    "https://{account}.{AZURE_BLOB_STORAGE_ROOT_DOMAIN}{path}?{query}",
                    path = url.path()
                ),
                (Some(query), Some(fragment)) => {
                    format!(
                        "https://{account}.{AZURE_BLOB_STORAGE_ROOT_DOMAIN}{path}?{query}#{fragment}",
                        path = url.path()
                    )
                }
            }
            .parse()
            .map_err(|_| AzureError::InvalidScheme.into())
        }
        _ => Ok(url),
    }
}

/// Represents information about a blob.
#[derive(Debug, Deserialize)]
struct Blob {
    /// The name of the blob.
    #[serde(rename = "Name")]
    name: String,
}

/// Represents a list of blobs.
#[derive(Default, Debug, Deserialize)]
struct Blobs {
    /// The blob names.
    #[serde(default, rename = "Blob")]
    items: Vec<Blob>,
}

/// Represents results of a list operation.
#[derive(Debug, Deserialize)]
#[serde(rename = "EnumerationResults")]
struct Results {
    /// The error message.
    #[serde(default, rename = "Blobs")]
    blobs: Blobs,
    /// The next marker to use to query for more blobs.
    #[serde(rename = "NextMarker", default)]
    next: Option<String>,
}

/// Represents a block list that comprises an Azure blob.
#[derive(Serialize)]
#[serde(rename = "BlockList")]
struct BlockList<'a> {
    /// Use the latest block.
    #[serde(rename = "Latest")]
    latest: &'a [String],
}

/// Extension trait for response.
trait ResponseExt {
    /// Converts an error response from Azure into an `Error`.
    async fn into_error(self) -> Error;
}

impl ResponseExt for Response {
    async fn into_error(self) -> Error {
        /// Represents an error response.
        #[derive(Default, Deserialize)]
        #[serde(rename = "Error")]
        struct ErrorResponse {
            /// The error message.
            #[serde(rename = "Message")]
            message: String,
        }

        let status = self.status();
        let text: String = match self.text().await {
            Ok(text) => text,
            Err(e) => return e.into(),
        };

        if text.is_empty() {
            return Error::Server {
                status,
                message: text,
            };
        }

        let message = match serde_xml_rs::from_str::<ErrorResponse>(&text) {
            Ok(response) => response.message,
            Err(e) => {
                return AzureError::UnexpectedResponse { status, error: e }.into();
            }
        };

        Error::Server { status, message }
    }
}

/// Represents an upload of a blob to Azure Blob Storage.
pub struct AzureBlobUpload {
    /// The HTTP client to use for the upload.
    client: Client,
    /// The blob URL.
    url: Url,
    /// The Azure block id.
    block_id: Arc<String>,
    /// The channel for sending progress updates.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl AzureBlobUpload {
    /// Constructs a new blob upload.
    fn new(
        client: Client,
        url: Url,
        block_id: Arc<String>,
        events: Option<broadcast::Sender<TransferEvent>>,
    ) -> Self {
        Self {
            client,
            url,
            block_id,
            events,
        }
    }
}

impl Upload for AzureBlobUpload {
    type Part = String;

    async fn put(&self, id: u64, block: u64, bytes: bytes::Bytes) -> Result<Self::Part> {
        let block_id =
            BASE64_STANDARD.encode(format!("{block_id}:{block:05}", block_id = self.block_id));

        debug!(
            "uploading block {block} with id `{block_id}` for `{url}`",
            url = self.url.display()
        );

        let mut url = self.url.clone();
        {
            // Append the operation and block id to the URL
            // These parameters are documented here: https://learn.microsoft.com/en-us/rest/api/storageservices/put-block
            let mut pairs = url.query_pairs_mut();
            // The component being created (a block)
            pairs.append_pair("comp", "block");
            // The id of the block being created
            pairs.append_pair("blockid", &block_id);
        }

        // Calculate the CRC64 checksum
        let mut crc64 = Digest::new();
        crc64.write(&bytes);
        let checksum = BASE64_STANDARD.encode(crc64.sum64().to_le_bytes());

        let length = bytes.len();
        let body = Body::wrap_stream(TransferStream::new(
            ByteStream::new(bytes),
            id,
            block,
            self.events.clone(),
        ));

        let response = self
            .client
            .put(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, length)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .header(AZURE_BLOB_TYPE_HEADER, AZURE_BLOB_TYPE)
            .header(AZURE_CONTENT_CRC_HEADER, checksum)
            .body(body)
            .send()
            .await?;

        if response.status() == StatusCode::CREATED {
            Ok(block_id)
        } else {
            Err(response.into_error().await)
        }
    }

    async fn finalize(&self, parts: &[Self::Part]) -> Result<()> {
        debug!("uploading block list for `{url}`", url = self.url.display());

        let mut url = self.url.clone();

        {
            // Append the operation to the URL
            // These parameter are documented here: https://learn.microsoft.com/en-us/rest/api/storageservices/put-block-list
            let mut pairs = url.query_pairs_mut();
            // The component being created (a block list)
            pairs.append_pair("comp", "blocklist");
        }

        let body = serde_xml_rs::to_string(&BlockList { latest: parts }).expect("should serialize");

        let response = self
            .client
            .put(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, body.len())
            .header(header::CONTENT_TYPE, "application/xml")
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .body(body)
            .send()
            .await?;

        if response.status() == StatusCode::CREATED {
            Ok(())
        } else {
            Err(response.into_error().await)
        }
    }
}

/// Represents a storage backend for Azure Blob Storage.
#[derive(Clone)]
pub struct AzureBlobStorageBackend {
    /// The config to use for transferring files.
    config: Config,
    /// The HTTP client to use for transferring files.
    client: Client,
    /// The channel for sending transfer events.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl AzureBlobStorageBackend {
    /// Constructs a new Azure Blob Storage backend with the given configuration
    /// and events channel.
    pub fn new(config: Config, events: Option<broadcast::Sender<TransferEvent>>) -> Self {
        Self {
            config,
            client: new_http_client(),
            events,
        }
    }
}

impl StorageBackend for AzureBlobStorageBackend {
    type Upload = AzureBlobUpload;

    fn config(&self) -> &Config {
        &self.config
    }

    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>> {
        &self.events
    }

    fn block_size(&self, file_size: u64) -> Result<u64> {
        /// The number of blocks to increment by in search of a block size
        const BLOCK_COUNT_INCREMENT: u64 = 50;

        // Return the block size if one was specified
        if let Some(size) = self.config.block_size {
            if size > MAX_BLOCK_SIZE {
                return Err(AzureError::InvalidBlockSize.into());
            }

            return Ok(size);
        }

        // Try to balance the number of blocks with the size of the blocks
        let mut num_blocks: u64 = BLOCK_COUNT_INCREMENT;
        while num_blocks < MAX_BLOCK_COUNT {
            let block_size = file_size.div_ceil(num_blocks).next_power_of_two();
            if block_size <= BLOCK_SIZE_THRESHOLD {
                return Ok(block_size.max(DEFAULT_BLOCK_SIZE));
            }

            num_blocks += BLOCK_COUNT_INCREMENT;
        }

        // Couldn't fit the number of blocks within the size threshold; fallback to
        // whatever will fit
        let block_size: u64 = file_size.div_ceil(MAX_BLOCK_COUNT);
        if block_size > MAX_BLOCK_SIZE {
            return Err(AzureError::MaximumSizeExceeded.into());
        }

        Ok(block_size)
    }

    fn join_url<'a>(&self, mut url: Url, segments: impl Iterator<Item = &'a str>) -> Result<Url> {
        let mut segments = segments.peekable();

        // Check to see if we're joining a path to the root container; that's not
        // supported
        let mut existing = url.path_segments().expect("URL should have path");
        if let (Some(first), None) = (existing.next(), existing.next())
            && !first.is_empty()
            && segments.peek().is_some()
        {
            return Err(AzureError::RootDirectoryUploadNotSupported.into());
        }

        // Append on the segments
        {
            let mut existing = url.path_segments_mut().expect("url should have path");
            existing.pop_if_empty();
            existing.extend(segments);
        }

        Ok(url)
    }

    async fn head(&self, url: Url) -> Result<Response> {
        debug_assert!(
            is_azure_url(&url) && url.scheme() == "https",
            "expected Azure HTTPS URL"
        );

        debug!("sending HEAD request for `{url}`", url = url.display());

        let response = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get(&self, url: Url) -> Result<Response> {
        debug_assert!(
            is_azure_url(&url) && url.scheme() == "https",
            "expected Azure HTTPS URL"
        );

        debug!("sending GET request for `{url}`", url = url.display());

        let response = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get_range(&self, url: Url, etag: &str, range: Range<u64>) -> Result<Response> {
        debug_assert!(
            is_azure_url(&url) && url.scheme() == "https",
            "expected Azure HTTPS URL"
        );

        debug!(
            "sending ranged GET request for `{url}` ({start}-{end})",
            url = url.display(),
            start = range.start,
            end = range.end
        );

        let response = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(
                header::RANGE,
                format!("bytes={start}-{end}", start = range.start, end = range.end),
            )
            .header(header::IF_RANGE, etag)
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(Error::RemoteContentModified);
        }

        Ok(response)
    }

    async fn walk(&self, url: Url) -> Result<Vec<String>> {
        debug_assert!(
            is_azure_url(&url) && url.scheme() == "https",
            "expected Azure HTTPS URL"
        );

        debug!("walking `{url}` as a directory", url = url.display());

        let mut container = url.clone();

        // Clear the path segments for the list request; we only want the container name
        let mut prefix = {
            let mut container_segments = container
                .path_segments_mut()
                .expect("URL should have a path");
            container_segments.clear();

            // Start by treating the first path segment as the container to list the
            // contents of
            let mut source_segments = url.path_segments().expect("URL should have a path");
            let name = source_segments.next().ok_or(AzureError::BlobNameMissing)?;
            container_segments.push(name);

            // The remainder is the prefix we're going to search for
            source_segments.fold(String::new(), |mut p, s| {
                if !p.is_empty() {
                    p.push('/');
                }

                p.push_str(s);
                p
            })
        };

        // If there's no prefix, then we need to use the implicit root container
        if prefix.is_empty() {
            let mut container_segments = container
                .path_segments_mut()
                .expect("URL should have a path");
            container_segments.clear();
            container_segments.push(AZURE_ROOT_CONTAINER);

            prefix = url.path_segments().expect("URL should have a path").fold(
                String::new(),
                |mut p, s| {
                    if !p.is_empty() {
                        p.push('/');
                    }

                    p.push_str(s);
                    p
                },
            );

            assert!(!prefix.is_empty());
        }

        // The prefix should end with `/` to signify a directory.
        prefix.push('/');

        {
            // Append the operation and block id to the URL
            // These parameters are documented here: https://learn.microsoft.com/en-us/rest/api/storageservices/list-blobs
            let mut pairs = container.query_pairs_mut();
            // The resource operation is on the container
            pairs.append_pair("restype", "container");
            // The operation is a list
            pairs.append_pair("comp", "list");
            // The prefix to use for listing blobs in the container.
            pairs.append_pair("prefix", &prefix);
        }

        let mut next = String::new();
        let mut paths = Vec::new();
        loop {
            let mut url = container.clone();
            if !next.is_empty() {
                // The marker to start listing from, returned by the previous query
                url.query_pairs_mut().append_pair("marker", &next);
            }

            // List the blobs with the prefix
            let response = self
                .client
                .get(url)
                .header(header::USER_AGENT, USER_AGENT)
                .header(header::DATE, Utc::now().to_rfc2822())
                .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
                .send()
                .await?;

            let status = response.status();
            if !status.is_success() {
                return Err(response.into_error().await);
            }

            let text = response.text().await?;
            let results: Results = match serde_xml_rs::from_str(&text) {
                Ok(response) => response,
                Err(e) => {
                    return Err(AzureError::UnexpectedResponse { status, error: e }.into());
                }
            };

            // If there is only one result and the result is an empty path, then the given
            // URL was to a file and not a "directory"
            if paths.is_empty()
                && results.blobs.items.len() == 1
                && results.next.is_none()
                && let Some("") = results.blobs.items[0].name.strip_prefix(&prefix)
            {
                return Ok(paths);
            }

            paths.extend(results.blobs.items.into_iter().map(|b| {
                b.name
                    .strip_prefix(&prefix)
                    .map(Into::into)
                    .unwrap_or(b.name)
            }));

            next = results.next.unwrap_or_default();
            if next.is_empty() {
                break;
            }
        }

        Ok(paths)
    }

    async fn new_upload(&self, url: Url) -> Result<Self::Upload> {
        debug_assert!(
            is_azure_url(&url) && url.scheme() == "https",
            "expected Azure HTTPS URL"
        );

        Ok(AzureBlobUpload::new(
            self.client.clone(),
            url,
            Arc::new(Alphanumeric::new(16).to_string()),
            self.events.clone(),
        ))
    }
}
