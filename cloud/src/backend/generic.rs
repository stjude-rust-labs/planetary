//! Implementation of a generic storage backend.
//!
//! The generic storage backend can only be used for downloading files.

use std::ops::Range;

use bytes::Bytes;
use chrono::Utc;
use reqwest::Client;
use reqwest::Response;
use reqwest::StatusCode;
use reqwest::header;
use tokio::sync::broadcast;
use tracing::debug;
use url::Url;

use crate::CopyConfig;
use crate::CopyError;
use crate::Result;
use crate::TransferEvent;
use crate::USER_AGENT;
use crate::UrlExt;
use crate::backend::StorageBackend;
use crate::backend::Upload;

/// Helper trait for converting responses into `CopyError`.
trait IntoCopyError {
    /// Converts a generic error response to a `CopyError`.
    async fn into_copy_error(self) -> CopyError;
}

impl IntoCopyError for Response {
    async fn into_copy_error(self) -> CopyError {
        let status = self.status();
        let text: String = match self.text().await {
            Ok(text) => text,
            Err(e) => return e.into(),
        };

        CopyError::ServerError {
            status,
            message: text,
        }
    }
}

/// Represents a generic upload.
///
/// As the generic backend cannot be used to upload files, this implementation
/// panics on use.
pub struct GenericUpload;

impl Upload for GenericUpload {
    type Part = ();

    async fn put(&self, _: u64, _: u64, _: Bytes) -> Result<Self::Part> {
        unimplemented!()
    }

    async fn finalize(&self, _: &[Self::Part]) -> Result<()> {
        unimplemented!()
    }
}

/// Represents a generic storage backend.
///
/// The generic storage backend can only be used to download files.
pub struct GenericStorageBackend {
    /// The configuration to use for transferring files.
    config: CopyConfig,
    /// The HTTP client to use for transferring files.
    client: Client,
    /// The channel for sending transfer events.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl GenericStorageBackend {
    /// Constructs a new generic storage backend with the given configuration
    /// and events channel.
    pub fn new(config: CopyConfig, events: Option<broadcast::Sender<TransferEvent>>) -> Self {
        Self {
            config,
            client: Client::new(),
            events,
        }
    }
}

impl StorageBackend for GenericStorageBackend {
    type Upload = GenericUpload;

    fn config(&self) -> &CopyConfig {
        &self.config
    }

    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>> {
        &self.events
    }

    fn block_size(&self, _: u64) -> Result<u64> {
        // Return the block size if one was specified
        if let Some(size) = self.config.block_size() {
            return Ok(size);
        }

        // Used a fixed block size of 4 MiB
        Ok(4 * 1024 * 1024)
    }

    async fn head(&self, url: url::Url) -> Result<Response> {
        debug!("sending HEAD request for `{url}`", url = url.display());

        let response = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_copy_error().await);
        }

        Ok(response)
    }

    async fn get(&self, url: url::Url) -> Result<Response> {
        debug!("sending GET request for `{url}`", url = url.display());

        let response = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_copy_error().await);
        }

        Ok(response)
    }

    async fn get_range(&self, url: url::Url, etag: &str, range: Range<u64>) -> Result<Response> {
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
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(response.into_copy_error().await);
        }

        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(CopyError::RemoteContentModified);
        }

        Ok(response)
    }

    async fn walk(&self, _: Url) -> Result<Vec<String>> {
        // The generic backend treats all URLs as files.
        Ok(Vec::default())
    }

    async fn new_upload(&self, _: Url) -> Result<Self::Upload> {
        panic!("generic storage backend cannot be used for uploading");
    }
}
