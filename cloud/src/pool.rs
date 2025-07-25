//! Implementation a pool of buffers used in I/O operations.
//!
//! The buffer pool stores reusable buffers that are only allocated once or when
//! they need to be reallocated due to increasing block sizes when transferring
//! multiple files.

use std::fs::File;
use std::ops::Deref;
use std::sync::Arc;
use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use opool::Pool;
use opool::PoolAllocator;
use opool::RcGuard;
use tokio::task::spawn_blocking;

use crate::Result;

/// The block buffer allocator.
///
/// Responsible for allocating new `Vec<u8>` buffers.
pub struct BufferAllocator;

impl PoolAllocator<Vec<u8>> for BufferAllocator {
    fn allocate(&self) -> Vec<u8> {
        Vec::default()
    }
}

/// Represents a allocated buffer that will be returned to the pool when
/// dropped.
pub type BufferGuard = RcGuard<BufferAllocator, Vec<u8>>;

/// Represents a pool of buffers used in uploading files.
#[derive(Clone)]
pub struct BufferPool(Arc<Pool<BufferAllocator, Vec<u8>>>);

impl BufferPool {
    /// Constructs a buffer pool of the given size.
    pub fn new(size: usize) -> Self {
        Self(Pool::new(size, BufferAllocator).into())
    }

    /// Allocates a buffer from the pool.
    ///
    /// If a buffer is already available in the pool, an existing buffer is
    /// returned.
    pub fn alloc(&self, size: usize) -> BufferGuard {
        let mut buffer = self.0.clone().get_rc();

        // Resize the buffer; this will only allocate if the buffer is new or if the
        // block size has increased from the last allocation; this can occur if we're
        // uploading multiple files and a later file has a larger calculated block size.
        buffer.resize(size, 0);
        buffer
    }

    /// Reads the next block from the given file and returns its contents in a
    /// buffer from the pool.
    ///
    /// Panics if there are no more blocks to read from the file.
    pub async fn read_block(
        &self,
        file: Arc<File>,
        block_size: u64,
        source_size: u64,
        offset: &AtomicU64,
    ) -> Result<Block> {
        // Allocate a buffer from the pool for the read
        let mut buffer = self.alloc(block_size.try_into().expect("block size too large"));

        // Increment the file offset that will be read
        let offset = match offset.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |offset| {
            if offset >= source_size {
                None
            } else {
                Some(offset + block_size)
            }
        }) {
            Ok(offset) => offset,
            Err(_) => panic!("more reads than there were blocks"),
        };

        assert_eq!(
            offset % block_size,
            0,
            "expected offset to be a multiple of the block size"
        );

        // Perform the read at the offset
        let block_num = offset / block_size;
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
        Ok(Block::new(block_num, buffer))
    }
}

/// Represents a block that has been read from a file as is ready to upload.
pub struct Block {
    /// The block number.
    num: u64,
    /// The filled buffer for the block.
    ///
    /// When the block is dropped, the buffer is returned to the pool.
    buffer: BufferGuard,
}

impl Block {
    /// Constructs a new block with the given number and filled buffer.
    fn new(num: u64, buffer: BufferGuard) -> Self {
        Self { num, buffer }
    }

    /// Gets the block index.
    pub fn num(&self) -> u64 {
        self.num
    }

    /// Consumes the block and returns its bytes.
    pub fn into_bytes(self) -> Bytes {
        Bytes::from_owner(self)
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
