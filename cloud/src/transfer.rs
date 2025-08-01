//! Implementation of file transfers.

use std::fs::File;
use std::ops::Range;
use std::path::Path;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures::FutureExt;
use futures::StreamExt;
use futures::TryFutureExt;
use futures::TryStreamExt;
use futures::stream;
use reqwest::header;
use tempfile::NamedTempFile;
use tokio::fs;
use tokio::io::AsyncReadExt;
use tokio::io::BufWriter;
use tokio::task::spawn_blocking;
use tokio_retry2::Retry;
use tokio_retry2::RetryError;
use tokio_util::io::StreamReader;
use tracing::debug;
use tracing::info;
use url::Url;
use walkdir::WalkDir;

use crate::Error;
use crate::Result;
use crate::TransferEvent;
use crate::UrlExt;
use crate::backend::StorageBackend;
use crate::backend::Upload;
use crate::notify_retry;
use crate::pool::BufferGuard;
use crate::pool::BufferPool;
use crate::streams::TransferStream;

/// Represents information about a file being downloaded.
#[derive(Clone)]
struct DownloadInfo {
    /// The file transfer id.
    id: u64,
    /// The size of the file being downloaded.
    file_size: u64,
    /// The block size to use for the download.
    block_size: u64,
    /// The number of blocks in the file.
    num_blocks: u64,
    /// The ETAG of the file being downloaded.
    etag: Arc<String>,
}

/// Represents information about a block being downloaded.
struct BlockDownloadInfo {
    /// The file transfer id.
    id: u64,
    /// The ETAG of the resource being downloaded.
    etag: Arc<String>,
    /// The file being written.
    file: Arc<File>,
    /// The block number being downloaded.
    block: u64,
    /// The buffer to use to read and write the block.
    buffer: BufferGuard,
    /// The range in the file being downloaded.
    range: Range<u64>,
}

/// Represents information about a file being uploaded.
#[derive(Clone, Copy)]
struct UploadInfo {
    /// The file transfer id.
    id: u64,
    /// The size of the file being uploaded.
    file_size: u64,
    /// The block size for the upload.
    block_size: u64,
    /// The number of blocks in the file.
    num_blocks: u64,
}

/// Inner state for file transfers.
struct FileTransferInner<B> {
    /// The storage backend to use for transferring files.
    backend: B,
    /// The buffer pool for buffering blocks before writing them to disk.
    pool: BufferPool,
    /// Stores the next transfer id to use for events.
    next_id: AtomicU64,
}

impl<B> FileTransferInner<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    /// Gets the next transfer id.
    fn next_id(&self) -> u64 {
        self.next_id.fetch_add(1, Ordering::SeqCst)
    }

    /// Downloads a file using a single block.
    async fn download(
        &self,
        id: u64,
        source: Url,
        destination: &Path,
        file_size: Option<u64>,
    ) -> Result<()> {
        let transfer = async move || {
            // Start by creating the destination's parent directory
            let parent = destination.parent().ok_or(Error::InvalidPath)?;
            fs::create_dir_all(parent)
                .await
                .map_err(|error| Error::DirectoryCreationFailed {
                    path: parent.to_path_buf(),
                    error,
                })?;

            // Use a temp file that will be atomically renamed when the download completes
            let temp = NamedTempFile::with_prefix_in(".", parent)
                .map_err(|error| Error::CreateTempFile { error })?;

            let response = self.backend.get(source).await?;
            let mut reader = StreamReader::new(TransferStream::new(
                response.bytes_stream().map_err(std::io::Error::other),
                id,
                1,
                self.backend.events().clone(),
            ));

            let mut writer = BufWriter::new(
                fs::File::create(temp.path())
                    .await
                    .map_err(|error| Error::CreateTempFile { error })?,
            );

            // Copy the response stream to the temp file
            tokio::io::copy(&mut reader, &mut writer)
                .await
                .map_err(Error::from)?;

            // Persist the temp file to the destination
            temp.persist(destination)
                .map_err(|e| Error::PersistTempFile { error: e.error })?;

            Ok(())
        };

        // Transfer as a single block
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block: 1,
                    size: file_size,
                })
                .ok();
        }

        let result = transfer().await;

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockCompleted {
                    id,
                    block: 1,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Downloads a block of a file using a ranged GET request.
    async fn download_block(&self, source: Url, mut info: BlockDownloadInfo) -> Result<()> {
        let id = info.id;
        let block = info.block;
        let start = info.range.start;
        let block_size = info.range.end - start;

        let transfer = async move || {
            let response = self
                .backend
                .get_range(source, info.etag.as_str(), info.range)
                .await?;

            let mut reader = StreamReader::new(TransferStream::new(
                response.bytes_stream().map_err(std::io::Error::other),
                info.id,
                info.block,
                self.backend.events().clone(),
            ));

            // Read the block into the provided buffer
            reader
                .read_exact(&mut info.buffer[0..block_size as usize])
                .map_err(Error::from)
                .await?;

            // Write the block to the file
            spawn_blocking(move || -> Result<_> {
                crate::os::write_at(&info.file, &info.buffer[0..block_size as usize], start)
                    .map_err(Error::from)
            })
            .await
            .expect("failed to join blocking task")?;

            Ok(())
        };

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block,
                    size: Some(block_size),
                })
                .ok();
        }

        let result = transfer().await;

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockCompleted {
                    id,
                    block,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Uploads a file with blocks.
    async fn upload(
        self: Arc<Self>,
        source: &Path,
        destination: Url,
        info: UploadInfo,
    ) -> Result<()> {
        // Create the upload (retryable)
        let upload = Arc::new(
            Retry::spawn_notify(
                self.backend.config().retry_durations(),
                || {
                    self.backend
                        .new_upload(destination.clone())
                        .map_err(Error::into_retry_error)
                },
                notify_retry,
            )
            .await?,
        );

        // Create a stream of tasks for uploading the blocks
        let file = Arc::new(File::open(source)?);
        let offset = Arc::new(AtomicU64::new(0));
        let mut stream = stream::iter(0..info.num_blocks)
            .map(|_| {
                let inner = self.clone();
                let upload = upload.clone();
                let file = file.clone();
                let offset = offset.clone();

                tokio::spawn(async move {
                    // Read the block (do not retry if this fails)
                    let block = inner
                        .pool
                        .read_block(file, info.block_size, info.file_size, &offset)
                        .await?;

                    let block_num = block.num();
                    let bytes = block.into_bytes();

                    // Spawn a retryable operation to put the block
                    Retry::spawn_notify(
                        inner.backend.config().retry_durations(),
                        || {
                            inner
                                .upload_block(info.id, block_num, bytes.clone(), upload.as_ref())
                                .map_ok(|p| (block_num, p))
                                .map_err(Error::into_retry_error)
                        },
                        notify_retry,
                    )
                    .await
                })
                .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(self.backend.config().parallelism());

        // Collect the parts that were uploaded
        let mut parts = vec![Default::default(); info.num_blocks as usize];

        loop {
            let result = stream.next().await;
            match result {
                Some(result) => {
                    let (block, part) = result?;
                    parts[block as usize] = part;
                }
                None => break,
            }
        }

        drop(stream);

        // Spawn a retryable operation to finalize the upload
        Retry::spawn_notify(
            self.backend.config().retry_durations(),
            || upload.finalize(&parts).map_err(Error::into_retry_error),
            notify_retry,
        )
        .await
    }

    /// Uploads a block of a file and returns the part that was uploaded.
    async fn upload_block<U: Upload>(
        &self,
        id: u64,
        block: u64,
        bytes: Bytes,
        upload: &U,
    ) -> Result<U::Part> {
        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockStarted {
                    id,
                    block,
                    size: Some(bytes.len() as u64),
                })
                .ok();
        }

        let part = upload.put(id, block, bytes).await;

        if let Some(events) = self.backend.events() {
            events
                .send(TransferEvent::BlockCompleted {
                    id,
                    block,
                    failed: part.is_err(),
                })
                .ok();
        }

        part
    }
}

/// Represents a file transfer.
#[derive(Clone)]
pub struct FileTransfer<B>(Arc<FileTransferInner<B>>);

impl<B> FileTransfer<B>
where
    B: StorageBackend + Send + Sync + 'static,
{
    /// Constructs a new file transfer with the given storage backend.
    pub fn new(backend: B) -> Self {
        let pool = BufferPool::new(backend.config().parallelism());
        Self(Arc::new(FileTransferInner {
            backend,
            pool,
            next_id: AtomicU64::new(0),
        }))
    }

    /// Downloads from the given source URL to the given destination path.
    ///
    /// If the source URL is a "directory", the files in the directory will be
    /// downloaded relative to the destination path.
    pub async fn download(&self, source: Url, destination: impl AsRef<Path>) -> Result<()> {
        let destination = destination.as_ref();

        // Start by walking the given URL for files to download
        let paths = Retry::spawn_notify(
            self.0.backend.config().retry_durations(),
            || {
                self.0
                    .backend
                    .walk(source.clone())
                    .map_err(Error::into_retry_error)
            },
            notify_retry,
        )
        .await?;

        // If there are no files relative to the given URL, download the URL as a file
        if paths.is_empty() {
            return self.download_file(source, destination.to_path_buf()).await;
        }

        // Otherwise, download each file in turn
        for path in paths {
            let mut source = source.clone();
            let mut destination = destination.to_path_buf();

            // Adjust source and destination based on the provided relative URL path
            {
                let mut segments = source.path_segments_mut().expect("URL should have a path");
                for segment in path.split('/') {
                    segments.push(segment);
                    destination.push(segment);
                }
            }

            self.download_file(source, destination).await?;
        }

        Ok(())
    }

    /// Downloads a file from the given URL.
    async fn download_file(&self, source: Url, destination: PathBuf) -> Result<()> {
        info!(
            "downloading `{source}` to `{destination}`",
            source = source.display(),
            destination = destination.display(),
        );

        // Start by sending a HEAD request for the file
        let response = Retry::spawn_notify(
            self.0.backend.config().retry_durations(),
            || {
                self.0
                    .backend
                    .head(source.clone())
                    .map_err(Error::into_retry_error)
            },
            notify_retry,
        )
        .await?;

        let file_size = response
            .headers()
            .get(header::CONTENT_LENGTH)
            .and_then(|v| v.to_str().ok()?.parse().ok());

        let accept_ranges = response
            .headers()
            .get(header::ACCEPT_RANGES)
            .and_then(|v| v.to_str().ok());

        let etag = response
            .headers()
            .get(header::ETAG)
            .and_then(|v| v.to_str().ok());

        let id = self.0.next_id();

        // Check to see if we can download the file with blocks
        let result = match (file_size, accept_ranges, etag) {
            (Some(file_size), Some("bytes"), Some(etag)) => {
                // Calculate the block size and the number of blocks to download
                let block_size = self.0.backend.block_size(file_size)?;
                let num_blocks = file_size.div_ceil(block_size);

                debug!(
                    "file `{source}` is {file_size} bytes and will be downloaded with \
                     {num_blocks} block(s) of size {block_size}",
                    source = source.display()
                );

                if let Some(events) = self.0.backend.events() {
                    events
                        .send(TransferEvent::TransferStarted {
                            id,
                            path: destination.clone(),
                            blocks: num_blocks,
                            size: Some(file_size),
                        })
                        .ok();
                }

                let info = DownloadInfo {
                    id,
                    file_size,
                    block_size,
                    num_blocks,
                    etag: Arc::new(etag.to_string()),
                };

                // Download the file with multiple blocks
                Retry::spawn_notify(
                    self.0.backend.config().retry_durations(),
                    || {
                        self.download_in_blocks(source.clone(), &destination, info.clone())
                            .map_err(|e| match e {
                                // Only retry on modified content errors; otherwise, we've already
                                // internally retried downloads of the individual blocks
                                Error::RemoteContentModified => RetryError::transient(e),
                                _ => RetryError::permanent(e),
                            })
                    },
                    notify_retry,
                )
                .await
            }
            _ => {
                if file_size.is_none() {
                    debug!(
                        "file `{source}` will be downloaded as a single request because its size \
                         could not be determined",
                        source = source.display()
                    );
                } else if accept_ranges.is_none() {
                    debug!(
                        "file `{source}` will be downloaded as a single request because the \
                         server does not accept ranged requests",
                        source = source.display()
                    );
                } else if etag.is_none() {
                    debug!(
                        "file `{source}` will be downloaded as a single request because the \
                         server did not respond with an ETAG",
                        source = source.display()
                    );
                }

                // Download the file as a single block
                if let Some(events) = self.0.backend.events() {
                    events
                        .send(TransferEvent::TransferStarted {
                            id,
                            path: destination.clone(),
                            blocks: 1,
                            size: file_size,
                        })
                        .ok();
                }

                Retry::spawn_notify(
                    self.0.backend.config().retry_durations(),
                    || {
                        self.0
                            .download(id, source.clone(), &destination, file_size)
                            .map_err(Error::into_retry_error)
                    },
                    notify_retry,
                )
                .await
            }
        };

        if let Some(events) = self.0.backend.events() {
            events
                .send(TransferEvent::TransferCompleted {
                    id,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }

    /// Downloads a file in blocks using ranged GET requests.
    ///
    /// The blocks are downloaded in parallel and retried individually upon
    /// errors, leading to faster error recovery.
    async fn download_in_blocks(
        &self,
        source: Url,
        destination: &Path,
        info: DownloadInfo,
    ) -> Result<()> {
        // Start by creating the destination's parent directory
        let parent = destination.parent().ok_or(Error::InvalidPath)?;
        fs::create_dir_all(parent)
            .await
            .map_err(|error| Error::DirectoryCreationFailed {
                path: parent.to_path_buf(),
                error,
            })?;

        // Use a temp file that will be atomically renamed when the download completes
        // Set its initial size up front so that individual writes don't go past EOF
        let mut temp = NamedTempFile::with_prefix_in(".", parent)
            .map_err(|error| Error::CreateTempFile { error })?;
        temp.as_file_mut().set_len(info.file_size)?;

        let (file, temp) = temp.into_parts();
        let file = Arc::new(file);
        let mut stream = stream::iter(0..info.num_blocks)
            .map(|block| {
                let inner = self.0.clone();
                let file = file.clone();
                let source = source.clone();
                let info = info.clone();

                tokio::spawn(async move {
                    Retry::spawn_notify(
                        inner.backend.config().retry_durations(),
                        || {
                            inner
                                .download_block(
                                    source.clone(),
                                    BlockDownloadInfo {
                                        id: info.id,
                                        etag: info.etag.clone(),
                                        file: file.clone(),
                                        block,
                                        buffer: inner.pool.alloc(info.block_size as usize),
                                        range: (block * info.block_size)
                                            ..((block + 1) * info.block_size).min(info.file_size),
                                    },
                                )
                                .map_err(Error::into_retry_error)
                        },
                        notify_retry,
                    )
                    .await
                })
                .map(|r| r.expect("task panicked"))
            })
            .buffer_unordered(self.0.backend.config().parallelism());

        loop {
            let result = stream.next().await;
            match result {
                Some(r) => r?,
                None => break,
            }
        }

        drop(stream);

        // Persist the temp file to the destination
        temp.persist(destination)
            .map_err(|e| Error::PersistTempFile { error: e.error })?;
        Ok(())
    }

    /// Uploads the given source path to the given destination URL.
    ///
    /// If the path is a directory, each file the directory recursively contains
    /// will be uploaded.
    pub async fn upload(&self, source: impl AsRef<Path>, destination: Url) -> Result<()> {
        let source = source.as_ref();

        // Recursively walk the path looking for files to upload
        for entry in WalkDir::new(source) {
            let entry = entry?;
            let metadata = entry.metadata()?;

            // We're recursively walking the directory; ignore directory entries
            if metadata.is_dir() {
                continue;
            }

            // Calculate the relative path for the file
            let relative_path = entry
                .path()
                .strip_prefix(source)
                .expect("failed to strip path prefix");

            let destination = self.0.backend.join_url(
                destination.clone(),
                relative_path
                    .components()
                    .map(|c| c.as_os_str().to_str().expect("path not UTF-8")),
            )?;

            // Perform the upload
            let result = self
                .upload_file(entry.path(), destination, metadata.len())
                .await;

            // Send the transfer completed event
            result?;
        }

        Ok(())
    }

    /// Uploads the given file to the given destination.
    async fn upload_file(&self, source: &Path, destination: Url, file_size: u64) -> Result<()> {
        info!(
            "uploading `{source}` to `{destination}`",
            source = source.display(),
            destination = destination.display(),
        );

        // Calculate the block size and the number of blocks to upload
        let block_size = self.0.backend.block_size(file_size)?;
        let num_blocks = if file_size == 0 {
            0
        } else {
            file_size.div_ceil(block_size)
        };

        let id = self.0.next_id();

        debug!(
            "file `{source}` is {file_size} bytes and will be uploaded with {num_blocks} block(s) \
             of size {block_size}",
            source = source.display()
        );

        if let Some(events) = self.0.backend.events() {
            events
                .send(TransferEvent::TransferStarted {
                    id,
                    path: source.to_path_buf(),
                    blocks: num_blocks,
                    size: Some(file_size),
                })
                .ok();
        }

        let info = UploadInfo {
            id,
            file_size,
            block_size,
            num_blocks,
        };

        let result = self.0.clone().upload(source, destination, info).await;

        if let Some(events) = self.0.backend.events() {
            events
                .send(TransferEvent::TransferCompleted {
                    id,
                    failed: result.is_err(),
                })
                .ok();
        }

        result
    }
}
