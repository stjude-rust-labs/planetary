//! Implements blob downloading.

use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;
use chrono::Utc;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::TryStreamExt;
use futures::stream;
use pin_project_lite::pin_project;
use reqwest::Client;
use reqwest::header;
use serde::Deserialize;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::fs::File;
use tokio::io::BufWriter;
use tokio::sync::broadcast;
use tokio_util::io::StreamReader;
use tracing::info;
use url::Url;

use super::AzureCopyError;
use crate::CopyConfig;
use crate::CopyError;
use crate::CopyEvent;
use crate::Result;
use crate::USER_AGENT;
use crate::UrlExt;
use crate::azure::AZURE_STORAGE_VERSION;
use crate::azure::AZURE_VERSION_HEADER;
use crate::azure::error_response;
use crate::generator::Alphanumeric;

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
    /// The blob name
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

pin_project! {
    /// A wrapper around a byte stream that sends progress events.
    struct TransferStream<S> {
        #[pin]
        stream: S,
        id: Arc<String>,
        last: Option<SystemTime>,
        events: Option<broadcast::Sender<CopyEvent>>,
        finished: bool,
    }
}

impl<S> TransferStream<S> {
    /// Constructs a new hash stream.
    fn new(stream: S, id: Arc<String>, events: Option<broadcast::Sender<CopyEvent>>) -> Self
    where
        S: Stream<Item = std::io::Result<Bytes>>,
    {
        Self {
            stream,
            id,
            last: None,
            events,
            finished: false,
        }
    }
}

impl<S> Stream for TransferStream<S>
where
    S: Stream<Item = std::io::Result<Bytes>>,
{
    type Item = std::io::Result<Bytes>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const UPDATE_INTERVAL: Duration = Duration::from_millis(50);

        if self.finished {
            return Poll::Ready(None);
        }

        let this = self.project();
        match this.stream.poll_next(cx) {
            Poll::Ready(Some(Ok(bytes))) => {
                let now = SystemTime::now();
                let update = this
                    .last
                    .and_then(|last| now.duration_since(last).ok().map(|d| d >= UPDATE_INTERVAL))
                    .unwrap_or(true);

                if update {
                    if let Some(events) = &this.events {
                        events
                            .send(CopyEvent::TransferProgress {
                                id: this.id.clone(),
                                transferred: bytes.len().try_into().unwrap(),
                            })
                            .ok();
                    }
                }

                Poll::Ready(Some(Ok(bytes)))
            }
            Poll::Ready(Some(Err(e))) => {
                *this.finished = true;
                Poll::Ready(Some(Err(e)))
            }
            Poll::Ready(None) => {
                *this.finished = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Gets a list of blobs that may be prefixed with a path.
///
/// Upon success, returns the prefix and a list of blobs that start with the
/// prefix.
///
/// If the container does not have any blobs with that prefix, an empty list of
/// blobs is returned; the source can then be treated as a "file" rather than a
/// "directory".
async fn list_prefixed_blobs(client: &Client, source: &Url) -> Result<(String, Vec<String>)> {
    let mut container = source.clone();

    // Clear the path segments for the list request; we only want the container name
    let mut prefix = {
        let mut container_segments = container
            .path_segments_mut()
            .expect("URL should have a path");
        container_segments.clear();

        // Start by treating the first path segment as the container to list the
        // contents of
        let mut source_segments = source.path_segments().expect("URL should have a path");
        let name = source_segments
            .next()
            .ok_or(AzureCopyError::BlobNameMissing)?;
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

    // If there's no prefix, then we need to check the implicit `$root` container
    if prefix.is_empty() {
        let mut container_segments = container
            .path_segments_mut()
            .expect("URL should have a path");
        container_segments.clear();
        container_segments.push("$root");

        prefix = source
            .path_segments()
            .expect("URL should have a path")
            .fold(String::new(), |mut p, s| {
                if !p.is_empty() {
                    p.push('/');
                }

                p.push_str(s);
                p
            });

        assert!(!prefix.is_empty());
    }

    // The prefix should end with `/` to signify a directory.
    prefix.push('/');

    {
        // Append the operation and block id to the URL
        let mut pairs = container.query_pairs_mut();
        pairs.append_pair("restype", "container");
        pairs.append_pair("comp", "list");
        pairs.append_pair("prefix", &prefix);
    }

    let mut next = String::new();
    let mut blobs = Vec::new();
    loop {
        let mut url = container.clone();
        if !next.is_empty() {
            url.query_pairs_mut().append_pair("marker", &next);
        }

        // List the blobs with the prefix
        let response = client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(error_response(response).await);
        }

        let text = response.text().await?;
        let results: Results = match serde_xml_rs::from_str(&text) {
            Ok(response) => response,
            Err(e) => {
                return Err(AzureCopyError::UnexpectedResponse { status, error: e }.into());
            }
        };

        blobs.extend(results.blobs.items.into_iter().map(|b| b.name));
        next = results.next.unwrap_or_default();
        if next.is_empty() {
            break;
        }
    }

    Ok((prefix, blobs))
}

/// Downloads a file from Azure blob storage.
async fn download_file(
    client: &Client,
    source: Url,
    destination: &Path,
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
    info!(
        "downloading `{source}` to `{destination}`",
        source = source.display(),
        destination = destination.display(),
    );

    // Start by creating the destination's parent directory
    let parent = destination.parent().ok_or(CopyError::InvalidPath)?;
    fs::create_dir_all(parent).await?;

    // Send the request for the file
    let response = client
        .get(source)
        .header(header::USER_AGENT, USER_AGENT)
        .header(header::DATE, Utc::now().to_rfc2822())
        .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
        .send()
        .await?;

    let status = response.status();
    if !status.is_success() {
        return Err(error_response(response).await);
    }

    let size = response
        .content_length()
        .ok_or(AzureCopyError::ContentLengthMissing)?;

    let id = Arc::new(format!("{random}", random = Alphanumeric::new(16)));

    // Send the transfer started event
    if let Some(events) = &events {
        events
            .send(CopyEvent::TransferStarted {
                id: id.clone(),
                path: destination.to_path_buf(),
                size,
            })
            .ok();
    }

    // Use a temp file that will be atomically renamed when the download completes
    let temp = NamedTempFile::with_prefix_in(".", parent)?;

    let mut reader = StreamReader::new(TransferStream::new(
        response.bytes_stream().map_err(std::io::Error::other),
        id.clone(),
        events.clone(),
    ));

    let mut writer = BufWriter::new(File::create(temp.path()).await?);

    // Copy the response stream to the temp file
    tokio::io::copy(&mut reader, &mut writer).await?;

    drop(reader);
    drop(writer);

    // Persist the temp file to the destination
    temp.persist(destination)?;

    if let Some(events) = &events {
        events.send(CopyEvent::TransferComplete { id }).ok();
    }

    Ok(())
}

/// Downloads a file or directory from Azure blob storage.
///
/// This works by querying blobs under a path prefix.
///
/// If there are blobs under the path prefix, the download is treated as a
/// directory.
///
/// Otherwise, the download is treated as a file.
pub async fn download(
    config: CopyConfig,
    source: Url,
    destination: &Path,
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
    let client = Client::new();
    let (prefix, blobs) = list_prefixed_blobs(&client, &source).await?;

    if !blobs.is_empty() {
        // Create a stream of tasks for downloading the files in the "directory"
        let mut stream = stream::iter(blobs.into_iter())
            .map(|blob| {
                let blob = blob.strip_prefix(&prefix).expect("blob not prefixed");
                let mut source = source.clone();
                let mut path = destination.to_path_buf();

                // Adjust the URL and paths relative to the "directory"
                {
                    let mut segments = source.path_segments_mut().expect("URL should have a path");
                    for segment in blob.split('/') {
                        segments.push(segment);
                        path.push(segment);
                    }
                }

                // Spawn the task to download the file
                let client = client.clone();
                let events = events.clone();
                tokio::spawn(async move { download_file(&client, source, &path, events).await })
                    .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(config.azure().parallelism());

        // Wait for each download to complete
        loop {
            let result = stream.next().await;
            match result {
                Some(result) => result?,
                None => break,
            }
        }

        return Ok(());
    }

    // The source is a file, do a single download
    download_file(&client, source, destination, events).await?;
    Ok(())
}
