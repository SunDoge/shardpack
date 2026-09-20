use std::{
    pin::Pin,
    task::{Context, Poll},
};

use tokio::io::{AsyncRead, ReadBuf};

/// Streaming exact-byte identity calculation for a shard input.
pub struct Blake3Reader<R> {
    inner: R,
    hasher: blake3::Hasher,
    bytes_read: u64,
}

impl<R> Blake3Reader<R> {
    #[must_use]
    pub fn new(inner: R) -> Self {
        Self {
            inner,
            hasher: blake3::Hasher::new(),
            bytes_read: 0,
        }
    }

    #[must_use]
    pub fn digest(&self) -> [u8; 32] {
        *self.hasher.clone().finalize().as_bytes()
    }

    #[must_use]
    pub const fn bytes_read(&self) -> u64 {
        self.bytes_read
    }

    #[must_use]
    pub fn into_inner(self) -> R {
        self.inner
    }
}

impl<R: AsyncRead + Unpin> AsyncRead for Blake3Reader<R> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        context: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        let before = buffer.filled().len();
        match Pin::new(&mut self.inner).poll_read(context, buffer) {
            Poll::Ready(Ok(())) => {
                let filled = buffer.filled();
                let new_bytes = &filled[before..];
                self.hasher.update(new_bytes);
                self.bytes_read = self.bytes_read.saturating_add(new_bytes.len() as u64);
                Poll::Ready(Ok(()))
            }
            other => other,
        }
    }
}
