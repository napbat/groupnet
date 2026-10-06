//! The raw self-attribution prefix, preserving its little-endian wire layout.

use std::io;

use groupnet_core::NodeId;
use zerocopy::byteorder::little_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

/// Largest receive buffer and accepted raw datagram, including its prefix.
pub(super) const MAX_DATAGRAM: usize = 65_535;

/// Longest accepted sender id in the self-attribution prefix.
const MAX_ID_LEN: usize = 1024;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned, Clone, Copy, Debug)]
#[repr(C)]
struct DatagramHeader {
    id_len: U32,
}

const HEADER_SIZE: usize = core::mem::size_of::<DatagramHeader>();

/// One socket's reusable concatenation buffer. The immutable sender prefix is
/// constructed once; only the payload is copied on each send. A caller must
/// retain exclusive access until the socket has finished sending the datagram.
#[derive(Debug)]
pub(super) struct SendBuffer {
    bytes: Vec<u8>,
    prefix_len: usize,
}

impl SendBuffer {
    pub(super) fn new(local: &NodeId) -> io::Result<Self> {
        let id = local.as_str().as_bytes();
        if id.len() > MAX_ID_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP sender id exceeds the wire limit",
            ));
        }
        let id_len = u32::try_from(id.len()).map_err(|_| {
            io::Error::new(io::ErrorKind::InvalidInput, "UDP sender id exceeds u32")
        })?;
        let header = DatagramHeader {
            id_len: U32::new(id_len),
        };
        let prefix_len = HEADER_SIZE + id.len();
        let mut bytes = Vec::with_capacity(prefix_len);
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(id);
        Ok(Self { bytes, prefix_len })
    }

    pub(super) fn frame(&mut self, msg: &[u8]) -> io::Result<&[u8]> {
        if msg.len() > MAX_DATAGRAM - self.prefix_len {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "UDP datagram exceeds the wire limit",
            ));
        }
        self.bytes.truncate(self.prefix_len);
        self.bytes.extend_from_slice(msg);
        Ok(&self.bytes)
    }
}

/// Every datagram carries `[u32 sender-id length][sender id][frame]`. The
/// claimed id has the same trust as a source address on this raw cluster fabric.
/// Malformed prefixes are rejected, including from registered source addresses.
pub(super) fn unframe(datagram: &[u8]) -> Option<(NodeId, &[u8])> {
    if datagram.len() > MAX_DATAGRAM {
        return None;
    }
    let (header, rest) = DatagramHeader::ref_from_prefix(datagram).ok()?;
    let len = usize::try_from(header.id_len.get()).ok()?;
    if len > MAX_ID_LEN {
        return None;
    }
    let id = std::str::from_utf8(rest.get(..len)?).ok()?;
    Some((NodeId::new(id), rest.get(len..)?))
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{HEADER_SIZE, MAX_DATAGRAM, MAX_ID_LEN, SendBuffer, unframe};

    #[test]
    fn prefix_layout_and_roundtrip_preserve_little_endian() {
        assert_eq!(HEADER_SIZE, 4);
        let sender = NodeId::new("peer-\u{03b1}");
        let mut buffer = SendBuffer::new(&sender).expect("sender prefix");
        let datagram = buffer.frame(b"payload").expect("frame");
        assert_eq!(&datagram[..4], &[7, 0, 0, 0]);
        let (from, msg) = unframe(datagram).expect("valid prefix");
        assert_eq!(from, sender);
        assert_eq!(msg, b"payload");
        let (_, msg) = unframe(buffer.frame(b"").expect("empty frame")).expect("empty payload");
        assert_eq!(msg, b"");
    }

    #[test]
    fn malformed_prefixes_are_rejected() {
        for truncated in [b"".as_slice(), &[1], &[1, 0, 0], &[2, 0, 0, 0, b'a']] {
            assert!(unframe(truncated).is_none());
        }
        assert!(unframe(&[1, 0, 0, 0, 0xff]).is_none(), "invalid UTF-8");
        assert!(unframe(&[0xff; 4]).is_none(), "absurd id length");
        assert!(unframe(&[1, 4, 0, 0]).is_none(), "id length above cap");
    }

    #[test]
    fn sender_identity_and_total_datagram_limits_are_symmetric() {
        let sender = NodeId::new("x".repeat(MAX_ID_LEN));
        let mut buffer = SendBuffer::new(&sender).expect("maximum sender id");
        let payload = vec![42; MAX_DATAGRAM - HEADER_SIZE - MAX_ID_LEN];
        let datagram = buffer.frame(&payload).expect("maximum datagram");
        assert_eq!(datagram.len(), MAX_DATAGRAM);
        let (from, received) = unframe(datagram).expect("maximum roundtrip");
        assert_eq!(from, sender);
        assert_eq!(received, payload);
        assert_eq!(
            buffer
                .frame(&vec![0; payload.len() + 1])
                .expect_err("oversized payload")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        assert!(unframe(&vec![0; MAX_DATAGRAM + 1]).is_none());
        assert_eq!(
            SendBuffer::new(&NodeId::new("x".repeat(MAX_ID_LEN + 1)))
                .expect_err("oversized identity")
                .kind(),
            std::io::ErrorKind::InvalidInput
        );
        let mut oversized_id = vec![0; HEADER_SIZE + MAX_ID_LEN + 1];
        oversized_id[..4].copy_from_slice(&[1, 4, 0, 0]);
        assert!(unframe(&oversized_id).is_none());
    }

    #[test]
    fn repeated_sends_reuse_storage_and_do_not_retain_stale_payload() {
        let mut buffer = SendBuffer::new(&NodeId::new("sender")).expect("sender prefix");
        let initial = buffer.frame(&[7; 128]).expect("large frame").as_ptr();
        let capacity = buffer.bytes.capacity();
        let short = buffer.frame(b"short").expect("short frame");
        assert_eq!(short.as_ptr(), initial);
        assert_eq!(unframe(short).expect("short roundtrip").1, b"short");
        assert_eq!(buffer.bytes.capacity(), capacity);
        assert!(buffer.frame(&vec![0; MAX_DATAGRAM]).is_err());
        assert_eq!(
            unframe(buffer.frame(b"next").expect("next frame"))
                .expect("next roundtrip")
                .1,
            b"next"
        );
        assert_eq!(buffer.bytes.capacity(), capacity);
    }
}
