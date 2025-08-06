//! Implementation of the Google Cloud Storage backend.

use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use chrono::DateTime;
use chrono::Utc;
use reqwest::Body;
use reqwest::Client;
use reqwest::Request;
use reqwest::Response;
use reqwest::header;
use reqwest::header::HeaderValue;
use secrecy::ExposeSecret;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::broadcast;
use tracing::debug;
use url::Url;

use crate::BLOCK_SIZE_THRESHOLD;
use crate::Config;
use crate::Error;
use crate::GoogleAuthConfig;
use crate::ONE_MEBIBYTE;
use crate::Result;
use crate::TransferEvent;
use crate::USER_AGENT;
use crate::UrlExt as _;
use crate::backend::StorageBackend;
use crate::backend::Upload;
use crate::backend::auth::RequestSigner;
use crate::backend::auth::SignatureProvider;
use crate::backend::auth::sha256_hex_string;
use crate::backend::s3::InitiateMultipartUploadResult;
use crate::backend::s3::ListBucketResult;
use crate::new_http_client;
use crate::streams::ByteStream;
use crate::streams::TransferStream;

/// The root domain for Google Cloud Storage.
pub const GOOGLE_ROOT_DOMAIN: &str = "storage.googleapis.com";

/// The maximum number of parts in an upload.
const MAX_PARTS: u64 = 10000;

/// The minimum size of a part in bytes (5 MiB); applies to every part except
/// the last.
const MIN_PART_SIZE: u64 = 5 * ONE_MEBIBYTE;

/// The maximum size in bytes (5 GiB) for an upload part.
const MAX_PART_SIZE: u64 = MIN_PART_SIZE * 1024;

/// The maximum size of a file on Google in bytes (5 TiB).
const MAX_FILE_SIZE: u64 = MAX_PART_SIZE * 1024;

/// The Google date header name.
const GOOGLE_DATE_HEADER: &str = "x-goog-date";

/// The Google content SHA256 header name.
const GOOGLE_CONTENT_SHA256_HEADER: &str = "x-goog-content-sha256";

/// Represents a Google-specific copy operation error.
#[derive(Debug, thiserror::Error)]
pub enum GoogleError {
    /// The specified Google Storage block size exceeds the maximum.
    #[error("Google Storage block size cannot exceed {MAX_PART_SIZE} bytes")]
    InvalidBlockSize,
    /// The source size exceeds the supported maximum size.
    #[error("the size of the source file exceeds the supported maximum of {MAX_FILE_SIZE} bytes")]
    MaximumSizeExceeded,
    /// Invalid URL with an `gs` scheme.
    #[error("invalid URL with `gs` scheme: the URL is not in a supported format")]
    InvalidScheme,
    /// The Google Cloud HMAC secret is invalid.
    #[error("invalid Google Cloud Storage HMAC secret")]
    InvalidSecretAccessKey,
    /// The response was missing an ETag header.
    #[error("response from server was missing an ETag header")]
    ResponseMissingETag,
    /// The bucket name in the URL was invalid.
    #[error("the bucket name specified in the URL is invalid")]
    InvalidBucketName,
    /// Unexpected response from server.
    #[error("unexpected {status} response from server: failed to deserialize response contents: {error}", status = .status.as_u16())]
    UnexpectedResponse {
        /// The response status code.
        status: reqwest::StatusCode,
        /// The deserialization error.
        error: serde_xml_rs::Error,
    },
}

/// Represents a Google Cloud Storage signature provider.
pub struct GoogleSignatureProvider<'a> {
    /// The Google Storage authentication configuration.
    auth: &'a GoogleAuthConfig,
}

impl SignatureProvider for GoogleSignatureProvider<'_> {
    fn algorithm(&self) -> &str {
        "GOOG4-HMAC-SHA256"
    }

    fn secret_key_prefix(&self) -> &str {
        "GOOG4"
    }

    fn request_type(&self) -> &str {
        "goog4_request"
    }

    fn region(&self) -> &str {
        "any"
    }

    fn service(&self) -> &str {
        "storage"
    }

    fn date_header_name(&self) -> &str {
        GOOGLE_DATE_HEADER
    }

    fn content_hash_header_name(&self) -> &str {
        GOOGLE_CONTENT_SHA256_HEADER
    }

    fn access_key_id(&self) -> &str {
        &self.auth.access_key
    }

    fn secret_access_key(&self) -> &str {
        self.auth.secret.expose_secret()
    }
}

/// Appends the authentication header to the request.
fn append_authentication_header(
    auth: &GoogleAuthConfig,
    date: DateTime<Utc>,
    request: &mut Request,
) -> Result<()> {
    let signer = RequestSigner::new(GoogleSignatureProvider { auth });
    let auth = signer
        .sign(date, request)
        .ok_or(GoogleError::InvalidSecretAccessKey)?;
    request.headers_mut().append(
        header::AUTHORIZATION,
        HeaderValue::try_from(auth).expect("value should be valid"),
    );
    Ok(())
}

/// Determines if the given URL is a Google Cloud Storage URL.
pub fn is_gcs_url(url: &Url) -> bool {
    match url.scheme() {
        "gs" => true,
        "https" => {
            let Some(domain) = url.domain() else {
                return false;
            };

            if domain.eq_ignore_ascii_case(GOOGLE_ROOT_DOMAIN) {
                // Path-style URL of the form http://storage.googleapis.com/<bucket>/<object>
                // There must be at least two path segments
                return url
                    .path_segments()
                    .map(|mut s| s.nth(1).is_some())
                    .unwrap_or(false);
            }

            // Virtual host style URL of the form https://<bucket>.storage.googleapis.com/<object>
            let Some((bucket, domain)) = domain.split_once('.') else {
                return false;
            };

            // There must be at least one path segment
            !bucket.is_empty()
                && domain.eq_ignore_ascii_case(GOOGLE_ROOT_DOMAIN)
                && url
                    .path_segments()
                    .map(|mut s| s.next().is_some())
                    .unwrap_or(false)
        }
        _ => false,
    }
}

/// Rewrites a Google Cloud Storage URL (gs://) into a HTTPS URL.
///
/// If the URL is not `gs` schemed, the given URL is returned as-is.
pub fn rewrite_url(url: Url) -> Result<Url> {
    match url.scheme() {
        "gs" => {
            let bucket = url.host_str().ok_or(GoogleError::InvalidScheme)?;
            let path = url.path();

            if url.path() == "/" {
                return Err(GoogleError::InvalidScheme.into());
            }

            match (url.query(), url.fragment()) {
                (None, None) => format!("https://{bucket}.{GOOGLE_ROOT_DOMAIN}{path}"),
                (None, Some(fragment)) => {
                    format!("https://{bucket}.{GOOGLE_ROOT_DOMAIN}{path}#{fragment}")
                }
                (Some(query), None) => {
                    format!("https://{bucket}.{GOOGLE_ROOT_DOMAIN}{path}?{query}")
                }
                (Some(query), Some(fragment)) => {
                    format!("https://{bucket}.{GOOGLE_ROOT_DOMAIN}{path}?{query}#{fragment}")
                }
            }
            .parse()
            .map_err(|_| GoogleError::InvalidScheme.into())
        }
        _ => Ok(url),
    }
}

/// URL extensions for Google Cloud Storage.
trait UrlExt {
    /// Extracts the bucket name and object path from the URL.
    ///
    /// # Panics
    ///
    /// Panics if the URL is not a valid S3 URL.
    fn bucket_and_path(&self) -> (&str, &str);
}

impl UrlExt for Url {
    fn bucket_and_path(&self) -> (&str, &str) {
        let domain = self.domain().expect("URL should have domain");

        if domain.eq_ignore_ascii_case(GOOGLE_ROOT_DOMAIN) {
            // Path-style URL of the form https://storage.googleapis.com/<bucket>/<path>
            let bucket = self
                .path_segments()
                .expect("URL should have path")
                .next()
                .expect("URL should have at least one path segment");

            (
                bucket,
                self.path()
                    .strip_prefix('/')
                    .unwrap()
                    .strip_prefix(bucket)
                    .unwrap(),
            )
        } else {
            // Virtual host style URL of the form https://<bucket>.storage.googleapis.com/<path>
            let Some((bucket, _)) = domain.split_once('.') else {
                panic!("URL domain does not contain a bucket");
            };

            (bucket, self.path())
        }
    }
}

/// Extension trait for response.
trait ResponseExt {
    /// Converts an error response from Google Cloud Storage into an `Error`.
    async fn into_error(self) -> Error;
}

impl ResponseExt for Response {
    async fn into_error(self) -> Error {
        /// Represents an error response.
        #[derive(Default, Deserialize)]
        #[serde(rename = "Error")]
        struct ErrorResponse {
            /// The error message.
            #[serde(rename = "Message", default)]
            message: String,
            /// The error details.
            #[serde(rename = "Details", default)]
            details: Option<String>,
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
            Ok(response) => match response.details {
                Some(details) => {
                    format!("{message}\ndetails: {details}", message = response.message)
                }
                None => response.message,
            },
            Err(e) => {
                return GoogleError::UnexpectedResponse { status, error: e }.into();
            }
        };

        Error::Server { status, message }
    }
}

/// Represents a completed part of an upload.
#[derive(Default, Clone, Serialize)]
#[serde(rename = "Part")]
pub struct GoogleUploadPart {
    /// The part number of the upload.
    #[serde(rename = "PartNumber")]
    number: u64,
    /// The ETag of the part.
    #[serde(rename = "ETag")]
    etag: String,
}

/// Represents an Google Cloud Storage file upload.
pub struct GoogleUpload {
    /// The configuration to use for the upload.
    config: Arc<Config>,
    /// The HTTP client to use for uploading.
    client: Client,
    /// The URL of the object being uploaded.
    url: Url,
    /// The identifier of this upload.
    id: String,
    /// The channel for sending progress updates.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl Upload for GoogleUpload {
    type Part = GoogleUploadPart;

    async fn put(&self, id: u64, block: u64, bytes: Bytes) -> Result<Self::Part> {
        // See: https://cloud.google.com/storage/docs/xml-api/put-object-multipart

        debug!(
            "sending PUT request for block {block} of `{url}`",
            url = self.url.display()
        );

        let mut url = self.url.clone();

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("partNumber", &format!("{number}", number = block + 1));
            pairs.append_pair("uploadId", &self.id);
        }

        let digest = sha256_hex_string(&bytes);
        let length = bytes.len();
        let body = Body::wrap_stream(TransferStream::new(
            ByteStream::new(bytes),
            id,
            block,
            self.events.clone(),
        ));

        let date = Utc::now();
        let mut request = self
            .client
            .put(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, length)
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, &digest)
            .body(body)
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok())
            .ok_or(GoogleError::ResponseMissingETag)?;

        Ok(GoogleUploadPart {
            number: block + 1,
            etag: etag.to_string(),
        })
    }

    async fn finalize(&self, parts: &[Self::Part]) -> Result<()> {
        // See: https://cloud.google.com/storage/docs/xml-api/post-object-complete

        /// Represents the request body for completing a multipart upload.
        #[derive(Serialize)]
        #[serde(rename = "CompleteMultipartUpload")]
        struct CompleteUpload<'a> {
            /// The parts of the upload.
            #[serde(rename = "Part")]
            parts: &'a [GoogleUploadPart],
        }

        debug!(
            "sending POST request to finalize upload of `{url}`",
            url = self.url.display()
        );

        let mut url = self.url.clone();

        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("uploadId", &self.id);
        }

        let body = serde_xml_rs::SerdeXml::new()
            .to_string(&CompleteUpload { parts })
            .expect("should serialize");

        let date = Utc::now();
        let mut request = self
            .client
            .post(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, body.len())
            .header(header::CONTENT_TYPE, "application/xml")
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string(&body))
            .body(body)
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(())
    }
}

/// Represents the Google Cloud Storage backend.
pub struct GoogleStorageBackend {
    /// The config to use for transferring files.
    config: Arc<Config>,
    /// The HTTP client to use for transferring files.
    client: Client,
    /// The channel for sending transfer events.
    events: Option<broadcast::Sender<TransferEvent>>,
}

impl GoogleStorageBackend {
    /// Constructs a new Google Cloud Storage backend.
    pub fn new(config: Config, events: Option<broadcast::Sender<TransferEvent>>) -> Self {
        Self {
            config: Arc::new(config),
            client: new_http_client(),
            events,
        }
    }
}

impl StorageBackend for GoogleStorageBackend {
    type Upload = GoogleUpload;

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
            if size > MAX_PART_SIZE {
                return Err(GoogleError::InvalidBlockSize.into());
            }

            return Ok(size);
        }

        // Try to balance the number of blocks with the size of the blocks
        let mut num_blocks: u64 = BLOCK_COUNT_INCREMENT;
        while num_blocks < MAX_PARTS {
            let block_size = file_size.div_ceil(num_blocks).next_power_of_two();
            if block_size <= BLOCK_SIZE_THRESHOLD {
                return Ok(block_size.max(MIN_PART_SIZE));
            }

            num_blocks += BLOCK_COUNT_INCREMENT;
        }

        // Couldn't fit the number of blocks within the size threshold; fallback to
        // whatever will fit
        let block_size: u64 = file_size.div_ceil(MAX_PARTS);
        if block_size > MAX_PART_SIZE {
            return Err(GoogleError::MaximumSizeExceeded.into());
        }

        Ok(block_size)
    }

    fn join_url<'a>(&self, mut url: Url, segments: impl Iterator<Item = &'a str>) -> Result<Url> {
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
            is_gcs_url(&url) && url.scheme() == "https",
            "expected Google Cloud Storage HTTPS URL"
        );

        debug!("sending HEAD request for `{url}`", url = url.display());

        let date = Utc::now();
        let mut request = self
            .client
            .head(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get(&self, url: Url) -> Result<Response> {
        debug_assert!(
            is_gcs_url(&url) && url.scheme() == "https",
            "expected Google Cloud Storage HTTPS URL"
        );

        debug!("sending GET request for `{url}`", url = url.display());

        let date = Utc::now();
        let mut request = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn get_range(&self, url: Url, etag: &str, range: Range<u64>) -> Result<Response> {
        debug_assert!(
            is_gcs_url(&url) && url.scheme() == "https",
            "expected Google Cloud Storage HTTPS URL"
        );

        debug!(
            "sending ranged GET request for `{url}` ({start}-{end})",
            url = url.display(),
            start = range.start,
            end = range.end
        );

        let date = Utc::now();

        let mut request = self
            .client
            .get(url)
            .header(header::USER_AGENT, USER_AGENT)
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .header(
                header::RANGE,
                format!("bytes={start}-{end}", start = range.start, end = range.end),
            )
            .header(header::IF_RANGE, etag)
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;
        if !response.status().is_success() {
            return Err(response.into_error().await);
        }

        Ok(response)
    }

    async fn walk(&self, mut url: Url) -> Result<Vec<String>> {
        // See: https://cloud.google.com/storage/docs/xml-api/get-bucket-list

        debug_assert!(
            is_gcs_url(&url) && url.scheme() == "https",
            "expected Google Cloud Storage HTTPS URL"
        );

        debug!("walking `{url}` as a directory", url = url.display());

        let (bucket, path) = url.bucket_and_path();

        // The prefix should end with `/` to signify a directory.
        let mut prefix = path.strip_prefix('/').unwrap_or(path).to_string();
        prefix.push('/');

        // Format the request to always use the virtual-host style URL
        url.set_host(Some(&format!("{bucket}.{GOOGLE_ROOT_DOMAIN}")))
            .map_err(|_| GoogleError::InvalidBucketName)?;
        url.set_path("/");

        {
            let mut pairs = url.query_pairs_mut();
            // Use version 2.0 of the API
            pairs.append_pair("list-type", "2");
            // Only return objects with this prefix
            pairs.append_pair("prefix", &prefix);
        }

        let date = Utc::now();
        let mut token = String::new();
        let mut paths = Vec::new();
        loop {
            let mut url = url.clone();
            if !token.is_empty() {
                url.query_pairs_mut()
                    .append_pair("continuation-token", &token);
            }

            // List the objects with the prefix
            let mut request = self
                .client
                .get(url)
                .header(header::USER_AGENT, USER_AGENT)
                .header(
                    GOOGLE_DATE_HEADER,
                    date.format("%Y%m%dT%H%M%SZ").to_string(),
                )
                .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string([]))
                .build()?;

            if let Some(auth) = &self.config.google.auth {
                append_authentication_header(auth, date, &mut request)?;
            }

            let response = self.client.execute(request).await?;

            let status = response.status();
            if !status.is_success() {
                return Err(response.into_error().await);
            }

            let text = response.text().await?;
            let results: ListBucketResult = match serde_xml_rs::from_str(&text) {
                Ok(response) => response,
                Err(e) => {
                    return Err(GoogleError::UnexpectedResponse { status, error: e }.into());
                }
            };

            // If there is only one result and the result is an empty path, then the given
            // URL was to a file and not a "directory"
            if paths.is_empty()
                && results.contents.len() == 1
                && results.token.is_none()
                && let Some("") = results.contents[0].key.strip_prefix(&prefix)
            {
                return Ok(paths);
            }

            paths.extend(
                results
                    .contents
                    .into_iter()
                    .map(|c| c.key.strip_prefix(&prefix).map(Into::into).unwrap_or(c.key)),
            );

            token = results.token.unwrap_or_default();
            if token.is_empty() {
                break;
            }
        }

        Ok(paths)
    }

    async fn new_upload(&self, url: Url) -> Result<Self::Upload> {
        // See: https://cloud.google.com/storage/docs/xml-api/post-object-multipart

        debug_assert!(
            is_gcs_url(&url) && url.scheme() == "https",
            "expected Google Cloud Storage HTTPS URL"
        );

        debug!("sending POST request for `{url}`", url = url.display());

        let mut create = url.clone();
        create.query_pairs_mut().append_key_only("uploads");

        let date = Utc::now();

        let mut request = self
            .client
            .post(create)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, "0")
            .header(
                GOOGLE_DATE_HEADER,
                date.format("%Y%m%dT%H%M%SZ").to_string(),
            )
            .header(GOOGLE_CONTENT_SHA256_HEADER, sha256_hex_string([]))
            .build()?;

        if let Some(auth) = &self.config.google.auth {
            append_authentication_header(auth, date, &mut request)?;
        }

        let response = self.client.execute(request).await?;

        let status = response.status();
        if !status.is_success() {
            return Err(response.into_error().await);
        }

        let text: String = match response.text().await {
            Ok(text) => text,
            Err(e) => return Err(e.into()),
        };

        let id = match serde_xml_rs::from_str::<InitiateMultipartUploadResult>(&text) {
            Ok(response) => response.upload_id,
            Err(e) => {
                return Err(GoogleError::UnexpectedResponse { status, error: e }.into());
            }
        };

        Ok(GoogleUpload {
            config: self.config.clone(),
            client: self.client.clone(),
            url,
            id,
            events: self.events.clone(),
        })
    }
}
