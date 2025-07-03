//! Implements blob downloading.

use std::path::Path;

use chrono::Utc;
use futures::FutureExt;
use futures::StreamExt;
use futures::stream;
use reqwest::Client;
use reqwest::header;
use serde::Deserialize;
use tokio::sync::broadcast;
use url::Url;

use super::AzureCopyError;
use crate::CopyConfig;
use crate::CopyEvent;
use crate::Result;
use crate::USER_AGENT;
use crate::azure::AZURE_STORAGE_VERSION;
use crate::azure::AZURE_VERSION_HEADER;
use crate::azure::error_response;
use crate::download_file;

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
    let mut blobs = Vec::new();
    loop {
        let mut url = container.clone();
        if !next.is_empty() {
            // The marker to start listing from, returned by the previous query
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
                tokio::spawn(async move {
                    download_file(
                        &client,
                        source,
                        [(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)],
                        &path,
                        events,
                        error_response,
                    )
                    .await
                })
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
    download_file(
        &client,
        source,
        [(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)],
        destination,
        events,
        error_response,
    )
    .await?;
    Ok(())
}
