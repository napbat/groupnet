//! Typed bounded reliability packets carrying only TLS ciphertext.

use crate::PacketBuffer;
use bytes::Bytes;
use zerocopy::{
    FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned,
    byteorder::network_endian::{U32, U64},
};

/// Tunnel protocol version: the first byte of every reliability packet and the
/// digit of the authenticated preamble. Bump it whenever a kind or field changes.
pub(super) const VERSION: u8 = 3;

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    version: u8,
    id: [u8; 16],
    kind: u8,
    sequence: U64,
    ack: U64,
    credit: U32,
}

pub(super) const HEADER: usize = size_of::<Header>();

/// Protocol-wide segment ceiling: the ciphertext a default routing envelope
/// carries between one-byte identities. Every receiver accepts segments up to
/// this bound, independent of its own configured send segment.
pub(super) const MAX_SEGMENT: usize =
    crate::wire::MAX_FRAME - crate::wire::MIN_TUNNEL_HEADER - HEADER;

/// Credit and memory charged for one segment: its ciphertext and its header.
pub(super) const fn cost(ciphertext: usize) -> usize {
    HEADER + ciphertext
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(super) struct SessionId(pub [u8; 16]);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) enum Kind {
    Open,
    Data,
    Ack,
    Fin,
    Reset,
    /// Sequenced, empty: the sender relinquishes its receive credit.
    Idle,
}

impl Kind {
    const fn tag(self) -> u8 {
        match self {
            Self::Open => 0,
            Self::Data => 1,
            Self::Ack => 2,
            Self::Fin => 3,
            Self::Reset => 4,
            Self::Idle => 5,
        }
    }

    const fn from_tag(tag: u8) -> Option<Self> {
        Some(match tag {
            0 => Self::Open,
            1 => Self::Data,
            2 => Self::Ack,
            3 => Self::Fin,
            4 => Self::Reset,
            5 => Self::Idle,
            _ => return None,
        })
    }
}

/// Header fields stamped on every outgoing packet.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(super) struct Fields {
    pub id: SessionId,
    pub kind: Kind,
    pub sequence: u64,
    pub ack: u64,
    pub credit: u32,
}

impl Fields {
    fn header(self) -> Header {
        Header {
            version: VERSION,
            id: self.id.0,
            kind: self.kind.tag(),
            sequence: self.sequence.into(),
            ack: self.ack.into(),
            credit: self.credit.into(),
        }
    }
}

#[derive(Debug)]
pub(super) struct Packet {
    pub id: SessionId,
    pub kind: Kind,
    pub sequence: u64,
    pub ack: u64,
    /// Receive credit in segment-cost bytes beyond `ack`.
    pub credit: u32,
    pub encoded: Bytes,
}

impl Packet {
    pub fn decode(bytes: Bytes) -> Option<Self> {
        let (header, payload) = Header::ref_from_prefix(bytes.as_ref()).ok()?;
        if header.version != VERSION || payload.len() > MAX_SEGMENT {
            return None;
        }
        let kind = Kind::from_tag(header.kind)?;
        if (kind == Kind::Data) == payload.is_empty() {
            return None;
        }
        Some(Self {
            id: SessionId(header.id),
            kind,
            sequence: header.sequence.get(),
            ack: header.ack.get(),
            credit: header.credit.get(),
            encoded: bytes,
        })
    }

    /// Appends a header and `payload` to an empty routing buffer.
    pub fn encode(fields: Fields, payload: &[u8], bytes: &mut PacketBuffer) {
        bytes.extend_from_slice(fields.header().as_bytes());
        bytes.extend_from_slice(payload);
    }

    /// Writes the header into the first [`HEADER`] bytes of a filled segment.
    pub fn stamp(fields: Fields, bytes: &mut [u8]) {
        bytes[..HEADER].copy_from_slice(fields.header().as_bytes());
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Router, RouterConfig};
    use groupnet_core::NodeId;

    fn fields(kind: Kind, credit: u32) -> Fields {
        Fields {
            id: SessionId([9; 16]),
            kind,
            sequence: u64::MAX,
            ack: 42,
            credit,
        }
    }

    #[tokio::test]
    async fn typed_header_roundtrip_retains_owned_ciphertext_and_byte_credit() {
        let router = Router::new(NodeId::new("a"), RouterConfig::default()).unwrap();
        for credit in [0, 1, 16 << 20, u32::MAX] {
            let mut encoded = router
                .tunnel_packet_buffer(&NodeId::new("b"), HEADER + 512)
                .unwrap();
            Packet::encode(fields(Kind::Data, credit), &[3; 512], &mut encoded);
            assert_eq!(encoded.len(), cost(512));
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
                    packet.credit
                ),
                (SessionId([9; 16]), Kind::Data, u64::MAX, 42, credit)
            );
            assert_eq!(&packet.encoded[HEADER..], &[3; 512]);
        }
        assert!(Packet::decode(Bytes::from_static(&[VERSION; HEADER - 1])).is_none());
        router.close().await;
    }

    #[test]
    fn kinds_roundtrip_and_payload_presence_is_enforced() {
        for kind in [
            Kind::Open,
            Kind::Data,
            Kind::Ack,
            Kind::Fin,
            Kind::Reset,
            Kind::Idle,
        ] {
            let mut bytes = [0; HEADER + 1];
            Packet::stamp(fields(kind, 7), &mut bytes);
            let empty = Packet::decode(Bytes::copy_from_slice(&bytes[..HEADER]));
            let full = Packet::decode(Bytes::copy_from_slice(&bytes));
            if kind == Kind::Data {
                assert!(empty.is_none());
                assert_eq!(full.unwrap().kind, kind);
            } else {
                assert_eq!(empty.unwrap().kind, kind);
                assert!(full.is_none());
            }
        }
        let mut unknown = [0; HEADER];
        Packet::stamp(fields(Kind::Ack, 0), &mut unknown);
        unknown[17] = 6;
        assert!(Packet::decode(Bytes::copy_from_slice(&unknown)).is_none());
    }

    #[test]
    fn other_tunnel_versions_are_rejected_instead_of_misparsed() {
        let mut bytes = [0; HEADER];
        Packet::stamp(fields(Kind::Ack, 1), &mut bytes);
        assert!(Packet::decode(Bytes::copy_from_slice(&bytes)).is_some());
        for version in [0, VERSION - 1, VERSION + 1] {
            bytes[0] = version;
            assert!(Packet::decode(Bytes::copy_from_slice(&bytes)).is_none());
        }
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
        Packet::encode(fields(Kind::Data, 1), &segment, &mut encoded);
        let largest = Packet::decode(Bytes::copy_from_slice(encoded.payload())).unwrap();
        assert_eq!(&largest.encoded[HEADER..], segment.as_slice());
        let mut oversized = encoded.payload().to_vec();
        oversized.push(5);
        assert!(Packet::decode(Bytes::from(oversized)).is_none());
        router.close().await;
    }
}
