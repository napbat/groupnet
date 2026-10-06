//! Bounded packets. Only TLS ciphertext is carried in data frames.

pub(super) const PAYLOAD: usize = 512;
pub(super) const WINDOW: usize = 32;
pub(super) const HEADER: usize = 35;

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
    pub encoded: Vec<u8>,
}

impl Packet {
    pub fn decode(bytes: Vec<u8>) -> Option<Self> {
        if bytes.len() < HEADER || bytes.len() > HEADER + PAYLOAD {
            return None;
        }
        let kind = match bytes[16] {
            0 => Kind::Open,
            1 => Kind::Data,
            2 => Kind::Ack,
            3 => Kind::Fin,
            4 => Kind::Reset,
            _ => return None,
        };
        if (kind == Kind::Data) == (bytes.len() == HEADER) {
            return None;
        }
        let window = u16::from_be_bytes(bytes[33..35].try_into().ok()?);
        if usize::from(window) > WINDOW {
            return None;
        }
        Some(Self {
            id: SessionId(bytes[..16].try_into().ok()?),
            kind,
            sequence: u64::from_be_bytes(bytes[17..25].try_into().ok()?),
            ack: u64::from_be_bytes(bytes[25..33].try_into().ok()?),
            window,
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
        bytes: &mut Vec<u8>,
    ) {
        bytes.clear();
        bytes.extend_from_slice(&id.0);
        bytes.push(match kind {
            Kind::Open => 0,
            Kind::Data => 1,
            Kind::Ack => 2,
            Kind::Fin => 3,
            Kind::Reset => 4,
        });
        bytes.extend_from_slice(&sequence.to_be_bytes());
        bytes.extend_from_slice(&ack.to_be_bytes());
        bytes.extend_from_slice(&window.to_be_bytes());
        bytes.extend_from_slice(payload);
    }
}
