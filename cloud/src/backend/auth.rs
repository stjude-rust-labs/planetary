//! Implements AWS Signature Version 4 authentication.
//!
//! This authentication is used by both the S3 and Google Cloud Storage
//! backends.

use std::ops::Deref;

use chrono::DateTime;
use chrono::Utc;
use hmac::Mac;
use reqwest::Request;
use sha2::Digest;
use sha2::Sha256;

/// The HMAC type used in authentication;
type Hmac = hmac::Hmac<Sha256>;

/// Represents a provider of signature information for request signing.
pub trait SignatureProvider {
    /// Gets the name of the signature algorithm.
    fn algorithm(&self) -> &str;

    /// Gets the prefix for the secret key.
    fn secret_key_prefix(&self) -> &str;

    /// Gets the request type.
    fn request_type(&self) -> &str;

    /// Gets the name of the region for the request.
    fn region(&self) -> &str;

    /// Gets the name of the service for the request.
    fn service(&self) -> &str;

    /// Gets the name of the date header.
    fn date_header_name(&self) -> &str;

    /// Gets the name of the content hash header.
    fn content_hash_header_name(&self) -> &str;

    /// Gets the access key ID to sign with.
    fn access_key_id(&self) -> &str;

    /// Gets the secret access key to sign with.
    fn secret_access_key(&self) -> &str;
}

/// Implements a request signer.
///
/// The signer implements the AWS Signature Version 4 algorithm.
pub struct RequestSigner<P>(P);

impl<P> RequestSigner<P>
where
    P: SignatureProvider,
{
    /// Constructs a new signer with the given provider.
    pub fn new(provider: P) -> Self {
        Self(provider)
    }

    /// Signs the given request.
    ///
    /// Returns the signature that can be used as the value of an authentication
    /// header.
    ///
    /// Returns `None` if the provider's signing key wasn't valid.
    pub fn sign(&self, date: DateTime<Utc>, request: &Request) -> Option<String> {
        let scope = format!(
            "{date}/{region}/{service}/{request_type}",
            date = date.format("%Y%m%d"),
            region = self.0.region(),
            service = self.0.service(),
            request_type = self.0.request_type()
        );

        let canonical_request = self.create_canonical_request(request);
        let string_to_sign = self.create_string_to_sign(date, &scope, &canonical_request);
        let signing_key = self.derive_signing_key(date)?;

        let mut hmac = Hmac::new_from_slice(&signing_key).ok()?;
        hmac.update(string_to_sign.as_bytes());
        let signature = hex::encode(hmac.finalize().into_bytes());

        Some(format!(
            "{algorithm} \
             Credential={access_key_id}/{scope},SignedHeaders=host;{content_hash_header_name};\
             {date_header_name},Signature={signature}",
            algorithm = self.0.algorithm(),
            access_key_id = self.0.access_key_id(),
            content_hash_header_name = self.0.content_hash_header_name(),
            date_header_name = self.0.date_header_name(),
        ))
    }

    /// Creates a canonical request string used in authentication.
    ///
    /// # Panics
    ///
    /// Panics if the given request does not have the expected
    /// authentication-related headers.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn create_canonical_request(&self, request: &Request) -> String {
        let url = request.url();

        // Sort the query pairs
        let mut query = url.query_pairs().collect::<Vec<_>>();
        query.sort_by(|(a, _), (b, _)| a.cmp(b));

        let mut query_string = String::new();
        for (key, value) in query {
            if !query_string.is_empty() {
                query_string.push('&');
            }

            query_string.push_str(&urlencoding::encode(&key));
            query_string.push('=');
            query_string.push_str(&urlencoding::encode(&value));
        }

        // Get the date header
        let date = request
            .headers()
            .get(self.0.date_header_name())
            .expect("request missing date header");

        // Get the content hash header
        let content_hash = request
            .headers()
            .get(self.0.content_hash_header_name())
            .expect("request missing content hash header");

        // Format the canonical request
        format!(
            "\
{method}
{path}
{query_string}
host:{domain}
{content_hash_header}:{content_hash}
{date_header}:{date}

host;{content_hash_header};{date_header}
{content_hash}",
            method = request.method(),
            path = url.path(),
            domain = request.url().domain().expect("should have domain").trim(),
            date_header = self.0.date_header_name(),
            date = date.to_str().expect("date should be a string").trim(),
            content_hash_header = self.0.content_hash_header_name(),
            content_hash = content_hash
                .to_str()
                .expect("content hash should be a string")
                .trim()
        )
    }

    /// Creates a string to sign for authentication.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn create_string_to_sign(
        &self,
        date: DateTime<Utc>,
        scope: &str,
        canonical_request: &str,
    ) -> String {
        let mut hash = Sha256::new();
        hash.update(canonical_request);
        let hash = hash.finalize();

        format!(
            "{algorithm}\n{date}\n{scope}\n{hash}",
            algorithm = self.0.algorithm(),
            date = date.format("%Y%m%dT%H%M%SZ"),
            hash = hex::encode(hash)
        )
    }

    /// Derives a signing key used in authentication.
    ///
    /// Returns `None` if the given secret access key is invalid.
    ///
    /// See: https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html
    fn derive_signing_key<'a>(
        &self,
        date: DateTime<Utc>,
    ) -> Option<impl Deref<Target = [u8]> + use<'a, P>> {
        let mut hmac = Hmac::new_from_slice(
            format!(
                "{prefix}{secret_access_key}",
                prefix = self.0.secret_key_prefix(),
                secret_access_key = self.0.secret_access_key()
            )
            .as_bytes(),
        )
        .ok()?;
        hmac.update(format!("{date}", date = date.format("%Y%m%d")).as_bytes());
        let date_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_key).ok()?;
        hmac.update(self.0.region().as_bytes());
        let date_region_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_region_key).ok()?;
        hmac.update(self.0.service().as_bytes());
        let date_region_service_key = hmac.finalize().into_bytes();

        let mut hmac = Hmac::new_from_slice(&date_region_service_key).ok()?;
        hmac.update(self.0.request_type().as_bytes());
        Some(hmac.finalize().into_bytes())
    }
}

/// Creates a SHA-256 digest of the given bytes encoded as a hex string.
pub fn sha256_hex_string(bytes: impl AsRef<[u8]>) -> String {
    let mut hash = Sha256::new();
    hash.update(bytes);
    hex::encode(hash.finalize())
}
