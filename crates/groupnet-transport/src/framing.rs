//! Length-prefixed framing primitives shared by every stream protocol
//! (feature `framing`).
//!
//! This module is the single owner of the stream-frame ceiling
//! ([`MAX_FRAME_BYTES`]), the typed big-endian length prefix
//! ([`LengthHeader`]), and the partial-write-safe vectored writer
//! ([`write_vectored`]). Protocols layer their own headers, validation and
//! authentication on top; none restates these.

use std::future::poll_fn;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::AsyncWrite;
use zerocopy::byteorder::big_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// The largest frame payload any length-prefixed stream protocol accepts:
/// 256 MiB. Per-protocol and per-operation limits may only be smaller.
pub const MAX_FRAME_BYTES: usize = 256 << 20;

/// I/O descriptors submitted per vectored write; longer part lists are
/// written in successive batches without allocating.
const MAX_IO_SLICES: usize = 16;

/// A 4-byte, network-byte-order frame length prefix, parsed and emitted via
/// [`mod@zerocopy`] with no copies. Embed it at the front of a protocol's own
/// `#[repr(C)]` header, or write it alone ahead of the payload.
#[derive(
    FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug, PartialEq, Eq,
)]
#[repr(C)]
pub struct LengthHeader {
    length: U32,
}

impl LengthHeader {
    /// Encoded size in bytes.
    pub const SIZE: usize = size_of::<Self>();

    /// Encodes a frame length.
    ///
    /// # Errors
    /// Returns `InvalidInput` above [`MAX_FRAME_BYTES`], before anything is
    /// written.
    pub fn new(length: usize) -> io::Result<Self> {
        if length > MAX_FRAME_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "frame exceeds the stream frame ceiling",
            ));
        }
        let length = u32::try_from(length)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "frame length exceeds u32"))?;
        Ok(Self {
            length: U32::new(length),
        })
    }

    /// Encodes the combined length of `parts`, as one frame written by
    /// [`write_vectored`].
    ///
    /// # Errors
    /// Returns `InvalidInput` when the sum overflows or exceeds
    /// [`MAX_FRAME_BYTES`].
    pub fn for_parts(parts: &[&[u8]]) -> io::Result<Self> {
        let length = parts
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "frame length overflow"))?;
        Self::new(length)
    }

    /// The raw decoded length, before any bound is applied.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.length.get()
    }

    /// The decoded length, checked against the receiver's limit (itself
    /// capped at [`MAX_FRAME_BYTES`]) before any payload storage is allocated.
    ///
    /// # Errors
    /// Returns `InvalidData` when the declared length exceeds the limit.
    pub fn length_within(self, max_bytes: usize) -> io::Result<usize> {
        usize::try_from(self.get())
            .ok()
            .filter(|length| *length <= max_bytes.min(MAX_FRAME_BYTES))
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    "frame length exceeds the receive limit",
                )
            })
    }
}

/// Writes every byte of `parts`, in order, using vectored writes and no
/// intermediate buffer.
///
/// A partial write may end inside any part; the next write resumes at exactly
/// that byte. Writers without native vectored I/O receive one part per call
/// through the trait's default. Empty parts are skipped, an `Interrupted` error
/// is retried, and nothing is flushed. Not cancel-safe: a cancelled write
/// leaves an unknown prefix written, so the caller must discard the stream.
///
/// # Errors
/// Propagates write errors; a writer accepting zero bytes yields `WriteZero`.
pub async fn write_vectored<W>(writer: &mut W, parts: &[&[u8]]) -> io::Result<()>
where
    W: AsyncWrite + Unpin + ?Sized,
{
    write_parts(parts, |cx, slices| {
        Pin::new(&mut *writer).poll_write_vectored(cx, slices)
    })
    .await
}

/// [`write_vectored`] over any `poll_write_vectored`, so the `futures-io`
/// data plane shares the exact loop.
pub(crate) async fn write_parts<F>(parts: &[&[u8]], mut poll_write: F) -> io::Result<()>
where
    F: FnMut(&mut Context<'_>, &[IoSlice<'_>]) -> Poll<io::Result<usize>>,
{
    let mut cursor = Cursor::new(parts);
    while !cursor.is_done() {
        let mut slices = [IoSlice::new(&[]); MAX_IO_SLICES];
        let count = cursor.fill(&mut slices);
        match poll_fn(|cx| poll_write(cx, &slices[..count])).await {
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "stream accepted no frame bytes",
                ));
            }
            Ok(written) => cursor.advance(written)?,
            Err(error) if error.kind() == io::ErrorKind::Interrupted => {}
            Err(error) => return Err(error),
        }
    }
    Ok(())
}

/// The unwritten suffix of a part list: `parts[index][offset..]` onward.
/// Always positioned on a non-empty part, or past the end.
struct Cursor<'a> {
    parts: &'a [&'a [u8]],
    index: usize,
    offset: usize,
}

impl<'a> Cursor<'a> {
    fn new(parts: &'a [&'a [u8]]) -> Self {
        let mut cursor = Self {
            parts,
            index: 0,
            offset: 0,
        };
        cursor.skip_written();
        cursor
    }

    fn is_done(&self) -> bool {
        self.index == self.parts.len()
    }

    fn fill(&self, slices: &mut [IoSlice<'a>]) -> usize {
        let first = &self.parts[self.index][self.offset..];
        let rest = self.parts[self.index + 1..]
            .iter()
            .copied()
            .filter(|part| !part.is_empty());
        let mut count = 0;
        for (slot, part) in slices.iter_mut().zip(std::iter::once(first).chain(rest)) {
            *slot = IoSlice::new(part);
            count += 1;
        }
        count
    }

    fn advance(&mut self, mut written: usize) -> io::Result<()> {
        while written > 0 {
            let Some(part) = self.parts.get(self.index) else {
                return Err(io::Error::other("writer reported more bytes than offered"));
            };
            let step = written.min(part.len() - self.offset);
            self.offset += step;
            written -= step;
            self.skip_written();
        }
        Ok(())
    }

    fn skip_written(&mut self) {
        while self
            .parts
            .get(self.index)
            .is_some_and(|part| self.offset == part.len())
        {
            self.index += 1;
            self.offset = 0;
        }
    }
}

#[cfg(test)]
mod tests;
