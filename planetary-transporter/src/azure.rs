//! Implementation of Azure blob storage support.

use std::process::Stdio;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;
use tokio::process::Command;
use url::Host;
use url::Url;

/// The Azure Blob Storage domain suffix.
const AZURE_STORAGE_DOMAIN_SUFFIX: &str = ".blob.core.windows.net";

/// Rewrites an Azure Blob Storage URL (az://) into a HTTPS URL.
pub(crate) fn rewrite_url(url: &Url) -> Result<Url> {
    assert_eq!(url.scheme(), "az");

    let account = url
        .host_str()
        .with_context(|| format!("invalid Azure URL `{url}`: storage account name is missing"))?;

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
    .with_context(|| format!("invalid Azure URL `{url}`"))
}

/// Determines if a `https` schemed URL is for Azure blob storage.
pub(crate) fn is_azure_url(url: &Url) -> bool {
    if url.scheme() != "https" {
        return false;
    }

    url.host()
        .map(|host| match host {
            Host::Domain(domain) => domain.strip_suffix(AZURE_STORAGE_DOMAIN_SUFFIX).is_some(),
            _ => false,
        })
        .unwrap_or(false)
}

/// Invokes `azcopy` with the given source and destination arguments.
pub(crate) async fn copy(source: &str, destination: &str, source_is_dir: bool) -> Result<()> {
    let mut command = Command::new("azcopy");

    // Note: azcopy doesn't log anything to stderr, even errors.
    command
        .args(["cp", source, destination])
        .stdout(Stdio::piped())
        .stderr(Stdio::null());

    if source_is_dir {
        command.args(["--recursive", "--as-subdir=false"]);
    }

    let child = command.spawn().context("failed to spawn `azcopy`")?;

    let output = child
        .wait_with_output()
        .await
        .context("failed to wait for `azcopy`")?;

    if !output.status.success() {
        // TODO: evaluate the output of `azcopy` and check to see if it is safe to
        // include in the system log
        match std::str::from_utf8(&output.stdout) {
            Ok(stdout) => {
                bail!(
                    "`azcopy` failed with {status}:\n{stdout}",
                    status = output.status
                );
            }
            Err(_) => {
                bail!("`azcopy` failed with {status}", status = output.status);
            }
        }
    }

    Ok(())
}
