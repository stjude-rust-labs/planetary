//! Implementation of storage backends.

use std::ops::Range;

use bytes::Bytes;
use reqwest::Response;
use tokio::sync::broadcast;
use url::Url;

use crate::CopyConfig;
use crate::Result;
use crate::TransferEvent;

pub(crate) mod azure;
pub(crate) mod generic;

/// Represents an abstract representation of an upload.
pub trait Upload: Send + Sync + 'static {
    /// Represents information about a part that was uploaded.
    type Part: Default + Clone + Send;

    /// Puts a block as part of the upload.
    ///
    /// Upon success, returns information about the part that was uploaded.
    fn put(
        &self,
        id: u64,
        block: u64,
        bytes: Bytes,
    ) -> impl Future<Output = Result<Self::Part>> + Send;

    /// Finalizes the upload given the parts that were uploaded.
    fn finalize(&self, parts: &[Self::Part]) -> impl Future<Output = Result<()>> + Send;
}

/// Represents an abstraction of a storage backend.
pub trait StorageBackend {
    /// The upload type the backend uses.
    type Upload: Upload;

    /// Gets the copy configuration used by the backend.
    fn config(&self) -> &CopyConfig;

    /// Gets the channel for sending transfer events.
    fn events(&self) -> &Option<broadcast::Sender<TransferEvent>>;

    /// Gets the block size given the size of a file.
    fn block_size(&self, file_size: u64) -> Result<u64>;

    /// Sends a HEAD request for the given URL.
    ///
    /// Returns an error if the request was not successful.
    fn head(&self, url: Url) -> impl Future<Output = Result<Response>> + Send;

    /// Sends a GET request for the given URL.
    ///
    /// Returns an error if the request was not successful.
    fn get(&self, url: Url) -> impl Future<Output = Result<Response>> + Send;

    /// Sends a conditional ranged GET request for the given URL.
    ///
    /// Returns `Ok(_)` if the response returns 206 (partial content).
    ///
    /// Returns an error if the request was not successful or if the condition
    /// was not met.
    fn get_range(
        &self,
        url: Url,
        etag: &str,
        range: Range<u64>,
    ) -> impl Future<Output = Result<Response>> + Send;

    /// Walks a given storage URL as if it were a directory.
    ///
    /// Returns a list of relative paths from the given URL.
    ///
    /// If the given storage URL is not a directory, an empty list is returned.
    fn walk(&self, url: Url) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Creates a new upload.
    fn new_upload(&self, url: Url) -> impl Future<Output = Result<Self::Upload>> + Send;
}
