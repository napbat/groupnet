//! Length-prefixed, optionally MAC-authenticated and sequenced frame I/O.
use bytes::Bytes;
use groupnet_transport::framing::{self, LengthHeader};
use ring::hmac;
use std::{io, sync::atomic::Ordering};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite};
use zerocopy::byteorder::big_endian::{U16, U64};
use zerocopy::{FromBytes, FromZeros, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::codec::{Cursor, decode, encode};
use super::{Auth, Message};
use crate::tcp::{MAX_TCP_MESSAGE, invalid};

pub(super) const MAX_FRAME: usize = MAX_TCP_MESSAGE + 4096;
pub(super) const VERSION: u8 = 2;
const TAG: usize = 32;
/// A frame header followed by at least the message kind.
const MIN_FRAME: usize = size_of::<FrameHeader>() + 1;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(super) struct FrameHeader {
    pub(super) version: u8,
    pub(super) authenticated: u8,
    pub(super) sequence: U64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(super) struct DataHeader {
    pub(super) kind: u8,
    pub(super) length: U16,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(super) struct RelayHeader {
    pub(super) kind: u8,
    pub(super) identity_length: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
pub(super) struct RelayTarget {
    pub(super) session: super::Token,
    pub(super) length: U16,
}

pub(in crate::tcp) async fn write<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    message: &Message,
) -> io::Result<()> {
    write_buffered(stream, auth, message, &mut Vec::new()).await
}

pub(in crate::tcp) async fn write_buffered<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    message: &Message,
    scratch: &mut Vec<u8>,
) -> io::Result<()> {
    let sequence = if let Some(auth) = auth {
        auth.sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| invalid("TCP write sequence exhausted"))?
            + 1
    } else {
        0
    };
    let header = FrameHeader {
        version: VERSION,
        authenticated: u8::from(auth.is_some()),
        sequence: U64::new(sequence),
    };
    match message {
        Message::Data(data) => {
            let length = payload_length(data)?;
            let prefix = DataHeader {
                kind: 11,
                length: U16::new(length),
            };
            write_parts(stream, auth, &[header.as_bytes(), prefix.as_bytes(), data]).await
        }
        Message::Relay {
            node,
            session,
            data,
        } => {
            let length = payload_length(data)?;
            let id = node.as_str().as_bytes();
            if id.is_empty() || id.len() > 64 {
                return Err(invalid("TCP identity length"));
            }
            let prefix = RelayHeader {
                kind: 6,
                identity_length: u8::try_from(id.len()).expect("bounded identity"),
            };
            let target = RelayTarget {
                session: *session,
                length: U16::new(length),
            };
            write_parts(
                stream,
                auth,
                &[
                    header.as_bytes(),
                    prefix.as_bytes(),
                    id,
                    target.as_bytes(),
                    data,
                ],
            )
            .await
        }
        _ => {
            scratch.clear();
            scratch.extend_from_slice(header.as_bytes());
            encode(message, scratch)?;
            write_parts(stream, auth, &[scratch]).await
        }
    }
}

fn payload_length(data: &[u8]) -> io::Result<u16> {
    if data.len() > MAX_TCP_MESSAGE {
        return Err(invalid("TCP payload exceeds bound"));
    }
    u16::try_from(data.len()).map_err(|_| invalid("TCP payload length"))
}

async fn write_parts<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    parts: &[&[u8]],
) -> io::Result<()> {
    let length =
        parts.iter().map(|part| part.len()).sum::<usize>() + if auth.is_some() { TAG } else { 0 };
    if length > MAX_FRAME {
        return Err(invalid("TCP frame exceeds bound"));
    }
    let length = LengthHeader::new(length)?;
    let tag = auth.as_ref().map(|auth| {
        let mut context = hmac::Context::with_key(&auth.key);
        for part in parts {
            context.update(part);
        }
        context.sign()
    });
    // At most length + five relay segments + MAC, all borrowed in place;
    // the shared writer resumes partial writes inside the original buffers.
    let mut frame: [&[u8]; 7] = [&[]; 7];
    frame[0] = length.as_bytes();
    frame[1..=parts.len()].copy_from_slice(parts);
    frame[parts.len() + 1] = tag.as_ref().map_or(&[], |tag| tag.as_ref());
    framing::write_vectored(stream, &frame[..parts.len() + 2]).await
}

pub(in crate::tcp) async fn read<R: AsyncRead + Unpin>(
    stream: &mut R,
    auth: &Auth,
) -> io::Result<Message> {
    let mut length = LengthHeader::new_zeroed();
    stream.read_exact(length.as_mut_bytes()).await?;
    let length = length.length_within(MAX_FRAME)?;
    if length < MIN_FRAME {
        return Err(invalid("TCP frame length outside bound"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    let header = FrameHeader::ref_from_bytes(&bytes[..size_of::<FrameHeader>()])
        .map_err(|_| invalid("TCP frame header"))?;
    if header.version != VERSION || header.authenticated != u8::from(auth.is_some()) {
        return Err(invalid("TCP authentication mode/version mismatch"));
    }
    let sequence = header.sequence.get();
    let end = if let Some(auth) = auth {
        let end = length
            .checked_sub(TAG)
            .filter(|end| *end >= MIN_FRAME)
            .ok_or_else(|| invalid("truncated keyed TCP frame"))?;
        hmac::verify(&auth.key, &bytes[..end], &bytes[end..])
            .map_err(|_| invalid("TCP authentication failed"))?;
        auth.sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                (previous.checked_add(1) == Some(sequence)).then_some(sequence)
            })
            .map_err(|_| invalid("replayed or out-of-order TCP frame"))?;
        end
    } else {
        if sequence != 0 {
            return Err(invalid("unkeyed TCP sequence"));
        }
        length
    };
    let bytes = Bytes::from(bytes);
    let mut cursor = Cursor(&bytes[size_of::<FrameHeader>()..end]);
    let message = decode(&mut cursor, &bytes, end)?;
    if !cursor.0.is_empty() {
        return Err(invalid("trailing TCP frame bytes"));
    }
    Ok(message)
}
