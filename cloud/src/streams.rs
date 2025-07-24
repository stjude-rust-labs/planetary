//! Implementation of utility stream types.

use std::pin::Pin;
use std::task::Context;
use std::task::Poll;
use std::time::Duration;
use std::time::SystemTime;

use bytes::Bytes;
use futures::Stream;
use pin_project_lite::pin_project;
use tokio::sync::broadcast;

use crate::TransferEvent;

/// A stream implementation for `Bytes` that reads in 64K chunks.
pub struct ByteStream {
    /// The bytes of the block.
    bytes: Bytes,
    /// The offset into the bytes to read from.
    offset: usize,
}

impl ByteStream {
    /// Constructs a new bytes stream from the given `Bytes`.
    pub fn new(bytes: Bytes) -> Self {
        Self { bytes, offset: 0 }
    }
}

impl Stream for ByteStream {
    type Item = std::io::Result<Bytes>;

    fn poll_next(mut self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        const CHUNK_SIZE: usize = 64 * 1024;

        // Check for end of stream
        if self.offset == self.bytes.len() {
            return Poll::Ready(None);
        }

        // Get the next chunk of bytes
        let bytes = self
            .bytes
            .slice(self.offset..(self.offset + CHUNK_SIZE).min(self.bytes.len()));

        self.offset += bytes.len();
        Poll::Ready(Some(Ok(bytes)))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, Some(self.bytes.len()))
    }
}

pin_project! {
    /// A wrapper around a stream that sends progress events.
    pub struct TransferStream<S> {
        #[pin]
        stream: S,
        id: u64,
        block: u64,
        transferred: u64,
        last: Option<SystemTime>,
        events: Option<broadcast::Sender<TransferEvent>>,
        finished: bool,
    }
}

impl<S> TransferStream<S> {
    /// Constructs a new transfer stream responsible for sending progress
    /// events.
    pub fn new(
        stream: S,
        id: u64,
        block: u64,
        events: Option<broadcast::Sender<TransferEvent>>,
    ) -> Self
    where
        S: Stream<Item = std::io::Result<Bytes>>,
    {
        Self {
            stream,
            id,
            block,
            transferred: 0,
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
                            .send(TransferEvent::BlockProgress {
                                id: *this.id,
                                block: *this.block,
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

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}
