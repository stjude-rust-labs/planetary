//! Implementation of cloud configuration.

use std::num::NonZero;
use std::thread::available_parallelism;
use std::time::Duration;

use secrecy::SecretString;
use serde::Deserialize;
use tokio_retry2::strategy::ExponentialFactorBackoff;
use tokio_retry2::strategy::MaxInterval;

/// The default number of retries for network operations.
const DEFAULT_RETRIES: usize = 5;

/// Represents authentication configuration for S3.
#[derive(Debug, Clone, Deserialize)]
pub struct S3AuthConfig {
    /// The AWS Access Key ID to use.
    pub access_key_id: String,
    /// The AWS Secret Access Key to use.
    pub secret_access_key: SecretString,
}

/// Represents authentication configuration for Google Cloud Storage.
#[derive(Debug, Clone, Deserialize)]
pub struct GoogleAuthConfig {
    /// The HMAC Access Key to use.
    pub access_key: String,
    /// The HMAC Secret to use.
    pub secret: SecretString,
}

/// Represents configuration for AWS S3.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct S3Config {
    /// The default region to apply to `s3` schemed URLs.
    ///
    /// Defaults to `us-east-1`.
    #[serde(default)]
    pub region: Option<String>,
    /// The auth to use for S3.
    ///
    /// If `None`, no authentication header will be put on requests.
    #[serde(default)]
    pub auth: Option<S3AuthConfig>,
}

/// Represents configuration for Google Cloud Storage.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct GoogleConfig {
    /// The auth to use for Google Cloud Storage.
    ///
    /// If `None`, no authentication header will be put on requests.
    #[serde(default)]
    pub auth: Option<GoogleAuthConfig>,
}

/// Configuration used in a cloud copy operation.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Config {
    /// The block size to use for file transfers.
    ///
    /// The default block size depends on the cloud storage service.
    #[serde(default)]
    pub block_size: Option<u64>,
    /// The parallelism level for network operations.
    ///
    /// Defaults to the host's available parallelism multiplied by 4.
    #[serde(default)]
    pub parallelism: Option<usize>,
    /// The number of retries to attempt for network operations.
    ///
    /// Defaults to `5`.
    #[serde(default)]
    pub retries: Option<usize>,
    /// The AWS S3 configuration.
    #[serde(default)]
    pub s3: S3Config,
    /// The Google Cloud Storage configuration.
    #[serde(default)]
    pub google: GoogleConfig,
}

impl Config {
    /// Gets the parallelism supported for uploads and downloads.
    ///
    /// For uploads, this is the number of blocks that may be concurrently
    /// transferred.
    ///
    /// For downloads, this is the number of blocks that may be concurrently
    /// downloaded if the download supports ranged requests.
    ///
    /// Defaults to the host's available parallelism (or 1 if it cannot be
    /// determined) multiplied by 4.
    pub fn parallelism(&self) -> usize {
        self.parallelism
            .unwrap_or_else(|| available_parallelism().map(NonZero::get).unwrap_or(1) * 4)
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
