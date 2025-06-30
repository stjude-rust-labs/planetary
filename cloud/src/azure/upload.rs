//! Implements blob uploading.

use std::fs::File;
use std::ops::Deref;
use std::path::Path;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use base64::prelude::*;
use bytes::Bytes;
use chrono::Utc;
use crc64fast_nvme::Digest;
use futures::FutureExt;
use futures::Stream;
use futures::StreamExt;
use futures::stream;
use opool::Pool;
use opool::PoolAllocator;
use opool::RcGuard;
use reqwest::Body;
use reqwest::Client;
use reqwest::StatusCode;
use reqwest::header;
use serde::Serialize;
use tokio::sync::broadcast;
use tokio::task::spawn_blocking;
use tracing::info;
use url::Url;
use walkdir::WalkDir;

use super::AZURE_BLOB_TYPE;
use super::AZURE_BLOB_TYPE_HEADER;
use super::AZURE_CONTENT_CRC_HEADER;
use super::AZURE_STORAGE_VERSION;
use super::AZURE_VERSION_HEADER;
use super::MAX_BLOCK_COUNT;
use super::error_response;
use crate::CopyConfig;
use crate::CopyEvent;
use crate::Result;
use crate::USER_AGENT;
use crate::UrlExt;
use crate::generator::Alphanumeric;

/// The block buffer allocator.
struct BufferAllocator;

impl PoolAllocator<Vec<u8>> for BufferAllocator {
    fn allocate(&self) -> Vec<u8> {
        Vec::default()
    }
}

/// Represents a pool of buffers used in uploading blocks.
#[derive(Clone)]
struct BufferPool(Arc<Pool<BufferAllocator, Vec<u8>>>);

impl BufferPool {
    /// Constructs a buffer pool of the given size.
    fn new(size: usize) -> Self {
        Self(Pool::new(size, BufferAllocator).into())
    }

    /// Allocates a buffer from the pool.
    ///
    /// If a buffer is already available in the pool, an existing buffer is
    /// returned.
    fn alloc_buffer(&self, size: usize) -> RcGuard<BufferAllocator, Vec<u8>> {
        let mut buffer = self.0.clone().get_rc();

        // Resize the buffer; this will only allocate if the buffer is new or if the
        // block size has increased from the last allocation; this can occur if we're
        // uploading multiple files and a later file has a larger calculated block size.
        buffer.resize(size, 0);
        buffer
    }
}

/// Represents a block that has been read from a file as is ready to upload.
///
/// This wraps the guard returned from the buffer pool.
///
/// When the block is dropped, the buffer is returned to the pool.
struct Block {
    /// The block index.
    index: usize,
    /// The filled buffer for the block.
    buffer: RcGuard<BufferAllocator, Vec<u8>>,
}

impl Block {
    /// Constructs a new block with the given index and filled buffer.
    fn new(index: usize, buffer: RcGuard<BufferAllocator, Vec<u8>>) -> Self {
        Self { index, buffer }
    }
}

impl AsRef<[u8]> for Block {
    fn as_ref(&self) -> &[u8] {
        self.buffer.as_slice()
    }
}

impl Deref for Block {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.buffer.as_slice()
    }
}

/// A stream used for transferring a block.
///
/// This stream handles sending progress updates.
struct TransferStream {
    /// The bytes of the block.
    bytes: Bytes,
    /// The id of the file for the block.
    id: Arc<String>,
    /// The offset into the bytes to read from.
    offset: usize,
    /// The number of bytes transferred since the last progress update.
    transferred: usize,
    /// The last time an update was made.
    last: Option<SystemTime>,
    /// The events sender to send progress updates to.
    events: Option<broadcast::Sender<CopyEvent>>,
}

impl TransferStream {
    /// Constructs a new block stream given the file id, block being uploaded,
    /// and events sender.
    fn new(id: Arc<String>, block: Block, events: Option<broadcast::Sender<CopyEvent>>) -> Self {
        Self {
            bytes: Bytes::from_owner(block),
            id,
            offset: 0,
            transferred: 0,
            last: None,
            events,
        }
    }
}

impl Stream for TransferStream {
    type Item = Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const CHUNK_SIZE: usize = 16 * 1024;
        const UPDATE_INTERVAL: Duration = Duration::from_millis(50);

        // Check for end of stream
        if self.offset == self.bytes.len() {
            return Poll::Ready(None);
        }

        // Get the next chunk of bytes
        let bytes = self
            .bytes
            .slice(self.offset..(self.offset + CHUNK_SIZE).min(self.bytes.len()));

        self.offset += bytes.len();
        self.transferred += bytes.len();

        let now = SystemTime::now();
        let update = self
            .last
            .and_then(|last| now.duration_since(last).ok().map(|d| d >= UPDATE_INTERVAL))
            .unwrap_or(true);

        // Send a progress update if enough time has elapsed
        if update {
            if let Some(events) = &self.events {
                events
                    .send(CopyEvent::TransferProgress {
                        id: self.id.clone(),
                        transferred: self.transferred.try_into().unwrap(),
                    })
                    .ok();

                self.transferred = 0;
                self.last = Some(now);
            }
        }

        Poll::Ready(Some(Ok(bytes)))
    }
}

/// Represents a block uploader.
#[derive(Clone)]
struct BlockUploader {
    /// The reqwest client to use for putting blocks.
    client: Client,
    /// The file being uploaded.
    file: Arc<File>,
    /// The size of the source file being uploaded (in bytes).
    source_size: u64,
    /// The size of a block (in bytes).
    block_size: u64,
    /// The current read offset of the file.
    offset: Arc<AtomicU64>,
}

impl BlockUploader {
    /// Creates a new block uploader given the file, size of the source file (in
    /// bytes), and block size (in bytes).
    fn new(client: Client, file: File, source_size: u64, block_size: u64) -> Self {
        Self {
            client,
            file: Arc::new(file),
            source_size,
            block_size,
            offset: Arc::default(),
        }
    }

    /// Uploads a block to the given URL.
    ///
    /// Upon success, returns the index and identifier of the block that was
    /// uploaded.
    async fn upload_block(
        &self,
        pool: &BufferPool,
        id: Arc<String>,
        mut destination: Url,
        events: Option<broadcast::Sender<CopyEvent>>,
    ) -> Result<(usize, String)> {
        // Read the block
        let block = self
            .read_block(pool)
            .await
            .expect("requested to upload more blocks than the file contained");

        let index = block.index;

        // The encoded block id must be fixed-width
        let block_id = BASE64_STANDARD.encode(format!("{id}:{index:05}"));
        info!(
            "uploading block {index} with id `{block_id}` for `{destination}`",
            destination = destination.display()
        );

        {
            // Append the operation and block id to the URL
            let mut pairs = destination.query_pairs_mut();
            pairs.append_pair("comp", "block");
            pairs.append_pair("blockid", &block_id);
        }

        // Calculate the CRC64 checksum
        let mut crc64 = Digest::new();
        crc64.write(&block);

        // Upload the block
        let response = self
            .client
            .put(destination)
            .header(header::USER_AGENT, USER_AGENT)
            .header(header::CONTENT_LENGTH, block.len())
            .header(header::CONTENT_TYPE, "application/octet-stream")
            .header(header::DATE, Utc::now().to_rfc2822())
            .header(AZURE_VERSION_HEADER, AZURE_STORAGE_VERSION)
            .header(AZURE_BLOB_TYPE_HEADER, AZURE_BLOB_TYPE)
            .header(
                AZURE_CONTENT_CRC_HEADER,
                BASE64_STANDARD.encode(crc64.sum64().to_le_bytes()),
            )
            .body(Body::wrap_stream(TransferStream::new(id, block, events)))
            .send()
            .await?;

        if response.status() == StatusCode::CREATED {
            Ok((index, block_id))
        } else {
            Err(error_response(response).await)
        }
    }

    /// Reads the next block from the file and returns its contents.
    ///
    /// Panics if there are no more blocks to read from the file.
    async fn read_block(&self, pool: &BufferPool) -> Result<Block> {
        // Allocate a buffer from the pool for the read
        let mut buffer =
            pool.alloc_buffer(self.block_size.try_into().expect("block size too large"));

        // Increment the file offset that will be read
        let block_size = self.block_size;
        let source_size = self.source_size;
        let offset = match self
            .offset
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |offset| {
                if offset >= source_size {
                    None
                } else {
                    Some(offset + block_size)
                }
            }) {
            Ok(offset) => offset,
            Err(_) => panic!("more reads than there were blocks"),
        };

        // Perform the read at the offset
        let index = (offset / block_size) as usize;
        let file = self.file.clone();
        let buffer = spawn_blocking(move || -> Result<_> {
            assert_eq!(buffer.len(), usize::try_from(block_size).unwrap());
            let len = crate::os::fill_buffer(&file, &mut buffer, offset)?;
            buffer.truncate(len);
            Ok(buffer)
        })
        .await
        .expect("failed to join blocking task")?;

        // A block should never be empty
        assert!(!buffer.is_empty(), "an empty block was read");

        Ok(Block::new(index, buffer))
    }
}

/// Uploads a block list to complete a blob upload.
async fn upload_block_list(client: &Client, mut destination: Url, ids: &[String]) -> Result<()> {
    info!(
        "uploading block list for `{destination}`",
        destination = destination.display()
    );

    {
        // Append the operation to the URL
        let mut pairs = destination.query_pairs_mut();
        pairs.append_pair("comp", "blocklist");
    }

    /// Represents a block list that comprises an Azure blob.
    #[derive(Serialize)]
    #[serde(rename = "BlockList")]
    struct BlockList<'a> {
        /// Use the latest block.
        #[serde(rename = "Latest")]
        latest: &'a [String],
    }

    let body = serde_xml_rs::to_string(&BlockList { latest: ids }).expect("should serialize");

    let response = client
        .put(destination)
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
        Err(error_response(response).await)
    }
}

/// Uploads a file to Azure blob storage.
async fn upload_file(
    config: CopyConfig,
    client: Client,
    pool: BufferPool,
    path: &Path,
    size: u64,
    destination: Url,
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
    // Calculate the block size and the number of blocks to upload
    let block_size = config.azure().block_size(size)?;
    let num_blocks = if size == 0 {
        0
    } else {
        size.div_ceil(block_size) as usize
    };
    assert!(
        num_blocks < MAX_BLOCK_COUNT as usize,
        "calculation of blocks is more than the maximum"
    );

    info!(
        "uploading {size} bytes from `{path}` to `{destination}` with {num_blocks} block(s) of \
         maximum {block_size} bytes",
        path = path.display(),
        destination = destination.display()
    );

    let file = File::open(path)?;
    let uploader = BlockUploader::new(client.clone(), file, size, block_size);
    let id = Arc::new(format!("{id}", id = Alphanumeric::new(16)));

    // Send the transfer started event
    if let Some(events) = &events {
        events
            .send(CopyEvent::TransferStarted {
                id: id.clone(),
                path: path.to_path_buf(),
                size,
            })
            .ok();
    }

    // Create a stream of tasks for uploading the blocks
    let mut stream = stream::iter(0..num_blocks)
        .map(|_| {
            let uploader = uploader.clone();
            let pool = pool.clone();
            let id = id.clone();
            let destination = destination.clone();
            let events = events.clone();

            tokio::spawn(async move { uploader.upload_block(&pool, id, destination, events).await })
                .map(|r| r.expect("task panicked"))
        })
        .buffer_unordered(config.azure().parallelism());

    // Collect the block ids that were uploaded
    let mut block_ids = vec![String::default(); num_blocks];

    loop {
        let result = stream.next().await;
        match result {
            Some(result) => {
                let (index, block_id) = result?;
                block_ids[index] = block_id;
            }
            None => break,
        }
    }

    drop(stream);

    // There should be no empty id in the list
    debug_assert!(
        block_ids.iter().all(|id| !id.is_empty()),
        "an empty ID was found"
    );

    // Upload the block list
    upload_block_list(&client, destination, &block_ids).await?;

    // Send the transfer completed event
    if let Some(events) = &events {
        events.send(CopyEvent::TransferComplete { id }).ok();
    }

    Ok(())
}

/// Uploads a file or directory to Azure blob storage.
///
/// This works by recursively walking the given path and uploading each file it
/// contains.
pub async fn upload(
    config: CopyConfig,
    source: &Path,
    destination: Url,
    events: Option<broadcast::Sender<CopyEvent>>,
) -> Result<()> {
    info!(
        "searching for files in `{source}` to upload to `{destination}`",
        source = source.display(),
        destination = destination.display()
    );

    // Recursively walk the path looking for files to upload
    let client = Client::new();
    let pool = BufferPool::new(config.azure().parallelism());
    let path = Arc::new(source.to_path_buf());

    let mut stream = stream::iter(WalkDir::new(path.as_ref()))
        .map(|entry| {
            let path = path.clone();
            let client = client.clone();
            let pool = pool.clone();
            let events = events.clone();
            let mut destination = destination.clone();

            tokio::spawn(async move {
                let entry = entry?;

                let metadata = entry.metadata()?;

                // We're recursively walking the directory; ignore directory entries
                if metadata.is_dir() {
                    return Ok(());
                }

                // Calculate the relative path for the file
                let relative_path = entry
                    .path()
                    .strip_prefix(path.as_ref())
                    .expect("failed to strip path prefix");

                {
                    // Prefer extending the segments over `Url::join` as this always treats the URL
                    // as a "directory"
                    let mut segments = destination.path_segments_mut().expect("expected Azure URL");
                    segments.pop_if_empty();
                    segments.extend(
                        relative_path
                            .components()
                            .map(|c| c.as_os_str().to_str().expect("should be utf-8")),
                    );
                }

                // Upload the file
                upload_file(
                    config,
                    client,
                    pool,
                    entry.path(),
                    metadata.len(),
                    destination,
                    events,
                )
                .await
            })
            .map(|r| r.expect("task panicked"))
        })
        .buffer_unordered(config.azure().parallelism());

    loop {
        let result = stream.next().await;
        match result {
            Some(result) => result?,
            None => break,
        }
    }

    Ok(())
}
