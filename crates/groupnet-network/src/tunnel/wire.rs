//! Typed bounded reliability packets carrying only TLS ciphertext.

use crate::PacketBuffer;
use bytes::Bytes;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::network_endian::{U16, U64},
};

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    id: [u8; 16],
    kind: u8,
    sequence: U64,
    ack: U64,
    window: U16,
}

pub(super) const HEADER: usize = size_of::<Header>();

/// Protocol-wide segment ceiling: the ciphertext a default routing envelope
/// carries between one-byte identities. Every receiver accepts segments up to
/// this bound, independent of its own configured send segment.
pub(super) const MAX_SEGMENT: usize =
    crate::wire::MAX_FRAME - crate::wire::MIN_TUNNEL_HEADER - HEADER;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct SessionId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Open,
    Data,
    Ack,
    Fin,
    Reset,
}

#[derive(Debug)]
pub(super) struct Packet {
    pub id: SessionId,
    pub kind: Kind,
    pub sequence: u64,
    pub ack: u64,
    pub window: u16,
    pub encoded: Bytes,
}

impl Packet {
    pub fn decode(bytes: Bytes) -> Option<Self> {
        let (header, payload) = Header::ref_from_prefix(bytes.as_ref()).ok()?;
        if payload.len() > MAX_SEGMENT {
            return None;
        }
        let kind = match header.kind {
            0 => Kind::Open,
            1 => Kind::Data,
            2 => Kind::Ack,
            3 => Kind::Fin,
            4 => Kind::Reset,
            _ => return None,
        };
        // Peer credit describes its receive capacity, not this receiver's window.
        if (kind == Kind::Data) == payload.is_empty() {
            return None;
        }
        Some(Self {
            id: SessionId(header.id),
            kind,
            sequence: header.sequence.get(),
            ack: header.ack.get(),
            window: header.window.get(),
            encoded: bytes,
        })
    }

    pub fn encode(
        id: SessionId,
        kind: Kind,
        sequence: u64,
        ack: u64,
        window: u16,
        payload: &[u8],
        bytes: &mut PacketBuffer,
    ) {
        let header = Header {
            id: id.0,
            kind: match kind {
                Kind::Open => 0,
                Kind::Data => 1,
                Kind::Ack => 2,
                Kind::Fin => 3,
                Kind::Reset => 4,
            },
            sequence: sequence.into(),
            ack: ack.into(),
            window: window.into(),
        };
        bytes.extend_from_slice(header.as_bytes());
        bytes.extend_from_slice(payload);
    }

    pub fn stamp(
        id: SessionId,
        kind: Kind,
        sequence: u64,
        ack: u64,
        window: u16,
        bytes: &mut [u8],
    ) {
        let header = Header {
            id: id.0,
            kind: match kind {
                Kind::Open => 0,
                Kind::Data => 1,
                Kind::Ack => 2,
                Kind::Fin => 3,
                Kind::Reset => 4,
            },
            sequence: sequence.into(),
            ack: ack.into(),
            window: window.into(),
        };
        bytes[..HEADER].copy_from_slice(header.as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Router, RouterConfig};
    use groupnet_core::NodeId;

    #[tokio::test]
    async fn typed_header_roundtrip_retains_owned_ciphertext_and_enforces_bounds() {
        let router = Router::new(NodeId::new("a"), RouterConfig::default()).unwrap();
        let mut encoded = router
            .tunnel_packet_buffer(&NodeId::new("b"), HEADER + 512)
            .unwrap();
        Packet::encode(
            SessionId([9; 16]),
            Kind::Data,
            u64::MAX,
            42,
            32,
            &[3; 512],
            &mut encoded,
        );
        assert_eq!(encoded.len(), HEADER + 512);
        let bytes = Bytes::copy_from_slice(encoded.payload());
        let ptr = bytes.as_ptr();
        let packet = Packet::decode(bytes).unwrap();
        assert_eq!(packet.encoded.as_ptr(), ptr);
        assert_eq!(
            (
                packet.id,
                packet.kind,
                packet.sequence,
                packet.ack,
                packet.window
            ),
            (SessionId([9; 16]), Kind::Data, u64::MAX, 42, 32)
        );
        assert_eq!(&packet.encoded[HEADER..], &[3; 512]);
        assert!(Packet::decode(Bytes::from_static(&[0; HEADER - 1])).is_none());
        router.close().await;
    }

    #[tokio::test]
    async fn receive_bound_is_the_largest_segment_a_default_envelope_carries() {
        let router = Router::new(NodeId::new("a"), RouterConfig::default()).unwrap();
        let peer = NodeId::new("b");
        assert!(
            router
                .validate_tunnel_payload(&peer, HEADER + MAX_SEGMENT)
                .is_ok()
        );
        assert!(
            router
                .validate_tunnel_payload(&peer, HEADER + MAX_SEGMENT + 1)
                .is_err()
        );
        let mut encoded = router
            .tunnel_packet_buffer(&peer, HEADER + MAX_SEGMENT)
            .unwrap();
        let segment = vec![5; MAX_SEGMENT];
        Packet::encode(
            SessionId([1; 16]),
            Kind::Data,
            7,
            0,
            1,
            &segment,
            &mut encoded,
        );
        let largest = Packet::decode(Bytes::copy_from_slice(encoded.payload())).unwrap();
        assert_eq!(&largest.encoded[HEADER..], segment.as_slice());
        let mut oversized = encoded.payload().to_vec();
        oversized.push(5);
        assert!(Packet::decode(Bytes::from(oversized)).is_none());
        router.close().await;
    }
}
