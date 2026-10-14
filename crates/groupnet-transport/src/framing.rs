//! Length-prefixed framing primitives shared by every stream protocol
//! (feature `framing`).
//!
//! This module is the single owner of the stream-frame ceiling
//! ([`MAX_FRAME_BYTES`]), the typed big-endian length prefix
//! ([`LengthHeader`]), and the partial-write-safe vectored writer
//! ([`write_vectored`]). Protocols layer their own headers, validation and
//! authentication on top; none restates these.
//!
//! ## Tokio-only
//!
//! [`write_vectored`] takes Tokio's [`AsyncWrite`]: it serves the Tokio-based
//! adapters in this workspace. There is deliberately no `futures-io`
//! counterpart here. Runtime-agnostic code frames its streams through
//! `bulk::DataStream` (feature `bulk`), which accepts any `futures-io`
//! stream and shares this module's ceiling, header and partial-write loop;
//! a Tokio stream adapts to it with `tokio_util::compat`.

use std::future::poll_fn;
use std::io::{self, IoSlice};
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{error, fmt};

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
    /// Returns [`FrameLengthError::AboveCeiling`] above [`MAX_FRAME_BYTES`],
    /// before anything is written.
    pub fn new(length: usize) -> Result<Self, FrameLengthError> {
        let encoded = u32::try_from(length)
            .ok()
            .filter(|_| length <= MAX_FRAME_BYTES)
            .ok_or(FrameLengthError::AboveCeiling { length })?;
        Ok(Self {
            length: U32::new(encoded),
        })
    }

    /// Encodes the combined length of `parts`, as one frame written by
    /// [`write_vectored`].
    ///
    /// # Errors
    /// Returns [`FrameLengthError::Overflow`] when the sum overflows `usize`
    /// and [`FrameLengthError::AboveCeiling`] above [`MAX_FRAME_BYTES`].
    pub fn for_parts(parts: &[&[u8]]) -> Result<Self, FrameLengthError> {
        let length = parts
            .iter()
            .try_fold(0usize, |total, part| total.checked_add(part.len()))
            .ok_or(FrameLengthError::Overflow)?;
        Self::new(length)
    }

    /// The raw decoded length, before any bound is applied.
    #[must_use]
    pub const fn get(self) -> u32 {
        self.length.get()
    }

    /// The decoded length, checked against the receiver's limit before any
    /// payload storage is allocated. The limit itself must be at most
    /// [`MAX_FRAME_BYTES`]; a larger one is refused, not capped.
    ///
    /// # Errors
    /// Returns [`FrameLengthError::LimitAboveCeiling`] for an over-ceiling
    /// `max_bytes`, and [`FrameLengthError::ReceivedAboveLimit`] when the
    /// declared length exceeds it.
    pub fn length_within(self, max_bytes: usize) -> Result<usize, FrameLengthError> {
        if max_bytes > MAX_FRAME_BYTES {
            return Err(FrameLengthError::LimitAboveCeiling { max_bytes });
        }
        let length = self.get();
        usize::try_from(length)
            .ok()
            .filter(|length| *length <= max_bytes)
            .ok_or(FrameLengthError::ReceivedAboveLimit { length, max_bytes })
    }
}

/// Why a frame length or a frame limit was refused.
///
/// Converts into an `io::Error` at the stream boundary: a declared length
/// over the receive limit is `InvalidData` (the peer sent it), every other
/// variant `InvalidInput` (the caller supplied it). The typed error stays the
/// inner error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum FrameLengthError {
    /// A frame to encode exceeds [`MAX_FRAME_BYTES`].
    AboveCeiling {
        /// The frame's length in bytes.
        length: usize,
    },
    /// The summed lengths of a frame's parts overflow `usize`.
    Overflow,
    /// A frame limit is zero where a frame must carry at least one byte.
    ZeroLimit,
    /// A frame limit exceeds [`MAX_FRAME_BYTES`].
    LimitAboveCeiling {
        /// The refused limit.
        max_bytes: usize,
    },
    /// A frame to send exceeds the caller's limit.
    SendAboveLimit {
        /// The frame's length in bytes.
        length: usize,
        /// The limit in force.
        max_bytes: usize,
    },
    /// A received length prefix exceeds the receiver's limit.
    ReceivedAboveLimit {
        /// The declared length.
        length: u32,
        /// The limit in force.
        max_bytes: usize,
    },
}

impl fmt::Display for FrameLengthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AboveCeiling { length } => write!(
                f,
                "frame of {length} bytes exceeds the {MAX_FRAME_BYTES}-byte stream ceiling"
            ),
            Self::Overflow => f.write_str("frame length overflows usize"),
            Self::ZeroLimit => f.write_str("frame limit must be nonzero"),
            Self::LimitAboveCeiling { max_bytes } => write!(
                f,
                "frame limit {max_bytes} exceeds the {MAX_FRAME_BYTES}-byte stream ceiling"
            ),
            Self::SendAboveLimit { length, max_bytes } => write!(
                f,
                "frame of {length} bytes exceeds the {max_bytes}-byte send limit"
            ),
            Self::ReceivedAboveLimit { length, max_bytes } => write!(
                f,
                "received frame length {length} exceeds the {max_bytes}-byte receive limit"
            ),
        }
    }
}

impl error::Error for FrameLengthError {}

impl From<FrameLengthError> for io::Error {
    fn from(error: FrameLengthError) -> Self {
        let kind = match error {
            FrameLengthError::ReceivedAboveLimit { .. } => io::ErrorKind::InvalidData,
            _ => io::ErrorKind::InvalidInput,
        };
        Self::new(kind, error)
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
