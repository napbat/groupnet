//! Bounded session-bound datagrams; keyed HMAC verification precedes parsing.

mod candidates;
pub(super) use candidates::CandidateList;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use ring::hmac;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::NetworkKey;
use super::candidates::{Candidates, MAX_CANDIDATES, valid};

pub(super) const MAX_PACKET: usize = 1200;
const TAG: usize = 32;
const MAGIC: &[u8; 4] = b"GNP4";
pub(super) type Session = [u8; 16];

/// The fixed prefix before the variable-length sender in the GNP4 envelope.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct EnvelopeHeader {
    magic: [u8; 4],
    kind: u8,
    sender_len: u8,
}

/// The fixed session suffix after the sender; byte arrays preserve network order.
#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SessionHeader {
    session: Session,
    sequence: [u8; 8],
}

#[derive(Clone, Copy)]
pub(super) enum Body<'a> {
    Hello {
        nonce: Session,
    },
    Challenge {
        nonce: Session,
        cookie: Session,
    },
    Register {
        nonce: Session,
        cookie: Session,
        relay_only: bool,
        credential: &'a [u8],
    },
    Registered {
        proof: Session,
    },
    Discover {
        proof: Session,
    },
    Heartbeat {
        proof: Session,
    },
    Depart {
        proof: Session,
    },
    Denied {
        proof: Session,
    },
    Query {
        proof: Session,
        peer: &'a str,
    },
    Offer {
        proof: Session,
        peer: &'a str,
        session: Session,
        address: Option<SocketAddr>,
        relay_only: bool,
        candidates: CandidateList<'a>,
        secret: Session,
    },
    Probe {
        target: Session,
        nonce: Session,
        secret: Session,
    },
    ProbeAck {
        target: Session,
        nonce: Session,
        capability: Session,
        secret: Session,
    },
    Confirm {
        target: Session,
        capability: Session,
        secret: Session,
    },
    Direct {
        peer: &'a str,
        target: Session,
        capability: Session,
        message: &'a [u8],
        secret: Session,
    },
    Relay {
        proof: Session,
        peer: &'a str,
        target: Session,
        message: &'a [u8],
    },
    Delivered {
        proof: Session,
        peer: &'a str,
        session: Session,
        message: &'a [u8],
    },
    Candidates {
        proof: Session,
        candidates: CandidateList<'a>,
    },
    Observed {
        proof: Session,
        address: SocketAddr,
    },
}

impl Body<'_> {
    pub(super) fn request_proof(self) -> Option<Session> {
        match self {
            Self::Discover { proof }
            | Self::Heartbeat { proof }
            | Self::Depart { proof }
            | Self::Query { proof, .. }
            | Self::Relay { proof, .. }
            | Self::Candidates { proof, .. } => Some(proof),
            _ => None,
        }
    }
}

impl std::fmt::Debug for Body<'_> {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Hello { .. } => "Hello",
            Self::Challenge { .. } => "Challenge",
            Self::Register { .. } => "Register([REDACTED])",
            Self::Registered { .. } => "Registered",
            Self::Discover { .. } => "Discover",
            Self::Heartbeat { .. } => "Heartbeat",
            Self::Depart { .. } => "Depart",
            Self::Denied { .. } => "Denied",
            Self::Query { .. } => "Query",
            Self::Offer { .. } => "Offer",
            Self::Probe { .. } => "Probe",
            Self::ProbeAck { .. } => "ProbeAck",
            Self::Confirm { .. } => "Confirm",
            Self::Direct { .. } => "Direct",
            Self::Relay { .. } => "Relay",
            Self::Delivered { .. } => "Delivered",
            Self::Candidates { .. } => "Candidates",
            Self::Observed { .. } => "Observed",
        })
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) struct Packet<'a> {
    pub sender: &'a str,
    pub session: Session,
    pub sequence: u64,
    pub body: Body<'a>,
}

struct Writer<'a> {
    bytes: &'a mut [u8],
    position: usize,
}

impl Writer<'_> {
    fn put(&mut self, bytes: &[u8]) -> Option<()> {
        let end = self.position.checked_add(bytes.len())?;
        self.bytes
            .get_mut(self.position..end)?
            .copy_from_slice(bytes);
        self.position = end;
        Some(())
    }

    fn name(&mut self, name: &str) -> Option<()> {
        if name.is_empty() || name.len() > 64 {
            return None;
        }
        self.put(&[u8::try_from(name.len()).ok()?])?;
        self.put(name.as_bytes())
    }

    fn address(&mut self, address: Option<SocketAddr>) -> Option<()> {
        self.put(&[u8::from(address.is_some())])?;
        if let Some(address) = address {
            if !valid(address) {
                return None;
            }
            match address.ip() {
                IpAddr::V4(ip) => {
                    self.put(&[4])?;
                    self.put(&ip.octets())?;
                }
                IpAddr::V6(ip) => {
                    self.put(&[6])?;
                    self.put(&ip.octets())?;
                }
            }
            self.put(&address.port().to_be_bytes())?;
        }
        Some(())
    }

    fn candidates(&mut self, candidates: CandidateList<'_>) -> Option<()> {
        self.put(&[u8::try_from(candidates.len()).ok()?])?;
        for address in candidates.iter() {
            self.address(Some(address))?;
        }
        Some(())
    }

    fn message(&mut self, message: &[u8]) -> Option<()> {
        if message.len() > super::MAX_MESSAGE {
            return None;
        }
        self.put(message)
    }
}

struct Reader<'a> {
    bytes: &'a [u8],
    position: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, length: usize) -> Option<&'a [u8]> {
        let end = self.position.checked_add(length)?;
        let result = self.bytes.get(self.position..end)?;
        self.position = end;
        Some(result)
    }

    fn byte(&mut self) -> Option<u8> {
        Some(*self.take(1)?.first()?)
    }

    fn token(&mut self) -> Option<Session> {
        self.take(16)?.try_into().ok()
    }

    fn name(&mut self) -> Option<&'a str> {
        let length = usize::from(self.byte()?);
        if length == 0 || length > 64 {
            return None;
        }
        std::str::from_utf8(self.take(length)?).ok()
    }

    fn flag(&mut self) -> Option<bool> {
        match self.byte()? {
            0 => Some(false),
            1 => Some(true),
            _ => None,
        }
    }

    fn address(&mut self) -> Option<SocketAddr> {
        let ip = match self.byte()? {
            4 => IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(self.take(4)?).ok()?)),
            6 => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(self.take(16)?).ok()?)),
            _ => return None,
        };
        let port = u16::from_be_bytes(self.take(2)?.try_into().ok()?);
        if !valid(SocketAddr::new(ip, port)) {
            return None;
        }
        Some(SocketAddr::new(ip, port))
    }

    fn candidates(&mut self) -> Option<CandidateList<'a>> {
        let count = self.byte()?;
        if usize::from(count) > MAX_CANDIDATES {
            return None;
        }
        let start = self.position;
        let mut candidates = Candidates::default();
        for _ in 0..count {
            if !self.flag()? || !candidates.insert(self.address()?) {
                return None;
            }
        }
        Some(CandidateList::Encoded(
            &self.bytes[start..self.position],
            count,
        ))
    }

    fn message(&mut self) -> Option<&'a [u8]> {
        let remaining = self.bytes.len().checked_sub(self.position)?;
        if remaining > super::MAX_MESSAGE {
            return None;
        }
        self.take(remaining)
    }
}

#[cfg(test)]
pub(super) fn encode(
    packet: Packet<'_>,
    key: &NetworkKey,
    buffer: &mut [u8; MAX_PACKET],
) -> Option<usize> {
    encode_mode(packet, Some(key), buffer)
}

pub(super) fn encode_mode(
    packet: Packet<'_>,
    key: Option<&NetworkKey>,
    buffer: &mut [u8; MAX_PACKET],
) -> Option<usize> {
    let mut writer = Writer {
        bytes: &mut buffer[..MAX_PACKET - if key.is_some() { TAG } else { 0 }],
        position: 0,
    };
    if packet.sender.is_empty() || packet.sender.len() > 64 {
        return None;
    }
    let kind = match packet.body {
        Body::Hello { .. } => 1,
        Body::Challenge { .. } => 2,
        Body::Register { .. } => 3,
        Body::Registered { .. } => 4,
        Body::Query { .. } => 5,
        Body::Offer { .. } => 6,
        Body::Probe { .. } => 7,
        Body::ProbeAck { .. } => 8,
        Body::Direct { .. } => 9,
        Body::Relay { .. } => 10,
        Body::Delivered { .. } => 11,
        Body::Discover { .. } => 12,
        Body::Heartbeat { .. } => 13,
        Body::Depart { .. } => 14,
        Body::Denied { .. } => 15,
        Body::Confirm { .. } => 16,
        Body::Candidates { .. } => 17,
        Body::Observed { .. } => 18,
    };
    writer.put(
        EnvelopeHeader {
            magic: *MAGIC,
            kind,
            sender_len: u8::try_from(packet.sender.len()).ok()?,
        }
        .as_bytes(),
    )?;
    writer.put(packet.sender.as_bytes())?;
    writer.put(
        SessionHeader {
            session: packet.session,
            sequence: packet.sequence.to_be_bytes(),
        }
        .as_bytes(),
    )?;
    encode_body(packet.body, &mut writer)?;
    let length = writer.position;
    if let Some(key) = key {
        let tag = hmac::sign(&key.auth, &buffer[..length]);
        buffer[length..length + TAG].copy_from_slice(tag.as_ref());
        Some(length + TAG)
    } else {
        Some(length)
    }
}

fn encode_body(body: Body<'_>, writer: &mut Writer<'_>) -> Option<()> {
    match body {
        Body::Hello { nonce } => writer.put(&nonce)?,
        Body::Challenge { nonce, cookie } => {
            writer.put(&nonce)?;
            writer.put(&cookie)?;
        }
        Body::Register {
            nonce,
            cookie,
            relay_only,
            credential,
        } => {
            writer.put(&nonce)?;
            writer.put(&cookie)?;
            writer.put(&[u8::from(relay_only)])?;
            if credential.len() > groupnet_transport::admission::MAX_CREDENTIAL_BYTES {
                return None;
            }
            writer.put(&u16::try_from(credential.len()).ok()?.to_be_bytes())?;
            writer.put(credential)?;
        }
        Body::Registered { proof }
        | Body::Denied { proof }
        | Body::Discover { proof }
        | Body::Heartbeat { proof }
        | Body::Depart { proof } => writer.put(&proof)?,
        Body::Query { proof, peer } => {
            writer.put(&proof)?;
            writer.name(peer)?;
        }
        Body::Offer {
            proof,
            peer,
            session,
            address,
            relay_only,
            candidates,
            secret,
        } => {
            if relay_only != address.is_none() || (relay_only && !candidates.is_empty()) {
                return None;
            }
            writer.put(&proof)?;
            writer.name(peer)?;
            writer.put(&session)?;
            writer.address(address)?;
            writer.put(&[u8::from(relay_only)])?;
            writer.candidates(candidates)?;
            writer.put(&secret)?;
        }
        Body::Probe {
            target,
            nonce,
            secret,
        } => {
            writer.put(&target)?;
            writer.put(&nonce)?;
            writer.put(&secret)?;
        }
        Body::ProbeAck {
            target,
            nonce,
            capability,
            secret,
        } => {
            writer.put(&target)?;
            writer.put(&nonce)?;
            writer.put(&capability)?;
            writer.put(&secret)?;
        }
        Body::Confirm { .. }
        | Body::Direct { .. }
        | Body::Relay { .. }
        | Body::Delivered { .. } => return encode_payload(body, writer),
        Body::Candidates { proof, candidates } => {
            writer.put(&proof)?;
            writer.candidates(candidates)?;
        }
        Body::Observed { proof, address } => {
            writer.put(&proof)?;
            writer.address(Some(address))?;
        }
    }
    Some(())
}

fn encode_payload(body: Body<'_>, writer: &mut Writer<'_>) -> Option<()> {
    match body {
        Body::Confirm {
            target,
            capability,
            secret,
        } => {
            writer.put(&target)?;
            writer.put(&capability)?;
            writer.put(&secret)?;
        }
        Body::Direct {
            peer,
            target,
            capability,
            message,
            secret,
        } => {
            writer.name(peer)?;
            writer.put(&target)?;
            writer.put(&capability)?;
            writer.put(&secret)?;
            writer.message(message)?;
        }
        Body::Relay {
            proof,
            peer,
            target,
            message,
        } => {
            writer.put(&proof)?;
            writer.name(peer)?;
            writer.put(&target)?;
            writer.message(message)?;
        }
        Body::Delivered {
            proof,
            peer,
            session,
            message,
        } => {
            writer.put(&proof)?;
            writer.name(peer)?;
            writer.put(&session)?;
            writer.message(message)?;
        }
        _ => return None,
    }
    Some(())
}

#[cfg(test)]
pub(super) fn decode<'a>(bytes: &'a [u8], key: &NetworkKey) -> Option<Packet<'a>> {
    decode_mode(bytes, Some(key))
}

pub(super) fn decode_mode<'a>(bytes: &'a [u8], key: Option<&NetworkKey>) -> Option<Packet<'a>> {
    if bytes.len() > MAX_PACKET {
        return None;
    }
    let length = if let Some(key) = key {
        let length = bytes.len().checked_sub(TAG)?;
        hmac::verify(&key.auth, &bytes[..length], &bytes[length..]).ok()?;
        length
    } else {
        bytes.len()
    };
    let (header, remaining) = EnvelopeHeader::ref_from_prefix(&bytes[..length]).ok()?;
    if header.magic != *MAGIC || header.sender_len == 0 || header.sender_len > 64 {
        return None;
    }
    let sender_len = usize::from(header.sender_len);
    let sender = std::str::from_utf8(remaining.get(..sender_len)?).ok()?;
    let (session_header, body_bytes) =
        SessionHeader::ref_from_prefix(remaining.get(sender_len..)?).ok()?;
    let session = session_header.session;
    let sequence = u64::from_be_bytes(session_header.sequence);
    let mut reader = Reader {
        bytes: body_bytes,
        position: 0,
    };
    if sequence == 0 {
        return None;
    }
    let body = decode_body(header.kind, &mut reader)?;
    if reader.position != reader.bytes.len() {
        return None;
    }
    Some(Packet {
        sender,
        session,
        sequence,
        body,
    })
}

fn decode_body<'a>(kind: u8, reader: &mut Reader<'a>) -> Option<Body<'a>> {
    let body = match kind {
        1 => Body::Hello {
            nonce: reader.token()?,
        },
        2 => Body::Challenge {
            nonce: reader.token()?,
            cookie: reader.token()?,
        },
        3 => {
            let nonce = reader.token()?;
            let cookie = reader.token()?;
            let relay_only = reader.flag()?;
            let length = usize::from(u16::from_be_bytes(reader.take(2)?.try_into().ok()?));
            if length > groupnet_transport::admission::MAX_CREDENTIAL_BYTES {
                return None;
            }
            Body::Register {
                nonce,
                cookie,
                relay_only,
                credential: reader.take(length)?,
            }
        }
        4 => Body::Registered {
            proof: reader.token()?,
        },
        5 => Body::Query {
            proof: reader.token()?,
            peer: reader.name()?,
        },
        6 => {
            let proof = reader.token()?;
            let peer = reader.name()?;
            let session = reader.token()?;
            let address = if reader.flag()? {
                Some(reader.address()?)
            } else {
                None
            };
            let relay_only = reader.flag()?;
            let candidates = reader.candidates()?;
            let secret = reader.token()?;
            if relay_only != address.is_none() || (relay_only && !candidates.is_empty()) {
                return None;
            }
            Body::Offer {
                proof,
                peer,
                session,
                address,
                relay_only,
                candidates,
                secret,
            }
        }
        7 => Body::Probe {
            target: reader.token()?,
            nonce: reader.token()?,
            secret: reader.token()?,
        },
        8 => Body::ProbeAck {
            target: reader.token()?,
            nonce: reader.token()?,
            capability: reader.token()?,
            secret: reader.token()?,
        },
        9..=11 => decode_payload(kind, reader)?,
        12 => Body::Discover {
            proof: reader.token()?,
        },
        13 => Body::Heartbeat {
            proof: reader.token()?,
        },
        14 => Body::Depart {
            proof: reader.token()?,
        },
        15 => Body::Denied {
            proof: reader.token()?,
        },
        16 => Body::Confirm {
            target: reader.token()?,
            capability: reader.token()?,
            secret: reader.token()?,
        },
        17 => Body::Candidates {
            proof: reader.token()?,
            candidates: reader.candidates()?,
        },
        18 => decode_observed(reader)?,
        _ => return None,
    };
    Some(body)
}

fn decode_observed<'a>(reader: &mut Reader<'a>) -> Option<Body<'a>> {
    let proof = reader.token()?;
    if !reader.flag()? {
        return None;
    }
    Some(Body::Observed {
        proof,
        address: reader.address()?,
    })
}

fn decode_payload<'a>(kind: u8, reader: &mut Reader<'a>) -> Option<Body<'a>> {
    Some(match kind {
        9 => Body::Direct {
            peer: reader.name()?,
            target: reader.token()?,
            capability: reader.token()?,
            secret: reader.token()?,
            message: reader.message()?,
        },
        10 => Body::Relay {
            proof: reader.token()?,
            peer: reader.name()?,
            target: reader.token()?,
            message: reader.message()?,
        },
        11 => Body::Delivered {
            proof: reader.token()?,
            peer: reader.name()?,
            session: reader.token()?,
            message: reader.message()?,
        },
        _ => return None,
    })
}

// Only a MAC, never the pair key, goes to an untrusted advertised address.
pub(super) fn check_proof(secret: Session, packet: Packet<'_>) -> Session {
    let key = hmac::Key::new(hmac::HMAC_SHA256, &secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(b"groupnet-udp-check-v4");
    context.update(&[u8::try_from(packet.sender.len()).unwrap_or(0)]);
    context.update(packet.sender.as_bytes());
    context.update(&packet.session);
    context.update(&packet.sequence.to_be_bytes());
    match packet.body {
        Body::Probe { target, nonce, .. } => {
            context.update(&[1]);
            context.update(&target);
            context.update(&nonce);
        }
        Body::ProbeAck {
            target,
            nonce,
            capability,
            ..
        } => {
            context.update(&[2]);
            context.update(&target);
            context.update(&nonce);
            context.update(&capability);
        }
        Body::Confirm {
            target, capability, ..
        } => {
            context.update(&[3]);
            context.update(&target);
            context.update(&capability);
        }
        Body::Direct {
            peer,
            target,
            capability,
            message,
            ..
        } => {
            context.update(&[4]);
            context.update(&[u8::try_from(peer.len()).unwrap_or(0)]);
            context.update(peer.as_bytes());
            context.update(&target);
            context.update(&capability);
            context.update(message);
        }
        _ => {}
    }
    let tag = context.sign();
    let mut proof = [0; 16];
    proof.copy_from_slice(&tag.as_ref()[..16]);
    proof
}

pub(super) fn sign_check(mut packet: Packet<'_>) -> Packet<'_> {
    let (Body::Probe { secret, .. }
    | Body::ProbeAck { secret, .. }
    | Body::Confirm { secret, .. }
    | Body::Direct { secret, .. }) = packet.body
    else {
        return packet;
    };
    let proof = check_proof(secret, packet);
    match &mut packet.body {
        Body::Probe { secret, .. }
        | Body::ProbeAck { secret, .. }
        | Body::Confirm { secret, .. }
        | Body::Direct { secret, .. } => *secret = proof,
        _ => {}
    }
    packet
}

#[cfg(test)]
mod tests;

#[cfg(test)]
#[path = "wire/candidate_tests.rs"]
mod candidate_tests;
