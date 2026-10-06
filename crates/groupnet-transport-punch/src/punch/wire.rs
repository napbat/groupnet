//! Bounded session-bound datagrams; keyed HMAC verification precedes parsing.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use ring::hmac;

use super::NetworkKey;

pub(super) const MAX_PACKET: usize = 1200;
const TAG: usize = 32;
const MAGIC: &[u8; 4] = b"GNP3";
pub(super) type Session = [u8; 16];

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
    },
    Probe {
        target: Session,
        nonce: Session,
    },
    ProbeAck {
        target: Session,
        nonce: Session,
        capability: Session,
    },
    Confirm {
        target: Session,
        capability: Session,
    },
    Direct {
        peer: &'a str,
        target: Session,
        capability: Session,
        message: &'a [u8],
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
}

impl Body<'_> {
    pub(super) fn request_proof(self) -> Option<Session> {
        match self {
            Self::Discover { proof }
            | Self::Heartbeat { proof }
            | Self::Depart { proof }
            | Self::Query { proof, .. }
            | Self::Relay { proof, .. } => Some(proof),
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
        if port == 0 || ip.is_unspecified() || ip.is_multicast() {
            return None;
        }
        Some(SocketAddr::new(ip, port))
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
    writer.put(MAGIC)?;
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
    };
    writer.put(&[kind])?;
    writer.name(packet.sender)?;
    writer.put(&packet.session)?;
    writer.put(&packet.sequence.to_be_bytes())?;
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
        } => {
            if relay_only != address.is_none() {
                return None;
            }
            writer.put(&proof)?;
            writer.name(peer)?;
            writer.put(&session)?;
            writer.address(address)?;
            writer.put(&[u8::from(relay_only)])?;
        }
        Body::Probe { target, nonce } => {
            writer.put(&target)?;
            writer.put(&nonce)?;
        }
        Body::ProbeAck {
            target,
            nonce,
            capability,
        } => {
            writer.put(&target)?;
            writer.put(&nonce)?;
            writer.put(&capability)?;
        }
        Body::Confirm { target, capability } => {
            writer.put(&target)?;
            writer.put(&capability)?;
        }
        Body::Direct {
            peer,
            target,
            capability,
            message,
        } => {
            writer.name(peer)?;
            writer.put(&target)?;
            writer.put(&capability)?;
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
    let mut reader = Reader {
        bytes: &bytes[..length],
        position: 0,
    };
    if reader.take(4)? != MAGIC {
        return None;
    }
    let kind = reader.byte()?;
    let sender = reader.name()?;
    let session = reader.token()?;
    let sequence = u64::from_be_bytes(reader.take(8)?.try_into().ok()?);
    if sequence == 0 {
        return None;
    }
    let body = decode_body(kind, &mut reader)?;
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
            if relay_only != address.is_none() {
                return None;
            }
            Body::Offer {
                proof,
                peer,
                session,
                address,
                relay_only,
            }
        }
        7 => Body::Probe {
            target: reader.token()?,
            nonce: reader.token()?,
        },
        8 => Body::ProbeAck {
            target: reader.token()?,
            nonce: reader.token()?,
            capability: reader.token()?,
        },
        9 => Body::Direct {
            peer: reader.name()?,
            target: reader.token()?,
            capability: reader.token()?,
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
        },
        _ => return None,
    };
    Some(body)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> NetworkKey {
        NetworkKey::from_bytes([4; 32])
    }

    fn packet(body: Body<'_>) -> Packet<'_> {
        Packet {
            sender: "alpha",
            session: [2; 16],
            sequence: 7,
            body,
        }
    }

    fn control_variants() -> [Body<'static>; 12] {
        let address = SocketAddr::from(([127, 0, 0, 1], 1234));
        [
            Body::Hello { nonce: [3; 16] },
            Body::Challenge {
                nonce: [3; 16],
                cookie: [4; 16],
            },
            Body::Register {
                nonce: [3; 16],
                cookie: [4; 16],
                relay_only: true,
                credential: &[],
            },
            Body::Registered { proof: [9; 16] },
            Body::Discover { proof: [9; 16] },
            Body::Heartbeat { proof: [9; 16] },
            Body::Depart { proof: [9; 16] },
            Body::Denied { proof: [9; 16] },
            Body::Query {
                proof: [9; 16],
                peer: "beta",
            },
            Body::Offer {
                proof: [9; 16],
                peer: "beta",
                session: [5; 16],
                address: Some(address),
                relay_only: false,
            },
            Body::Offer {
                proof: [9; 16],
                peer: "beta",
                session: [5; 16],
                address: Some("[::1]:1234".parse().unwrap()),
                relay_only: false,
            },
            Body::Offer {
                proof: [9; 16],
                peer: "beta",
                session: [5; 16],
                address: None,
                relay_only: true,
            },
        ]
    }

    fn data_variants(message: &[u8]) -> [Body<'_>; 6] {
        [
            Body::Probe {
                target: [5; 16],
                nonce: [3; 16],
            },
            Body::ProbeAck {
                target: [5; 16],
                nonce: [3; 16],
                capability: [8; 16],
            },
            Body::Confirm {
                target: [5; 16],
                capability: [8; 16],
            },
            Body::Direct {
                peer: "beta",
                target: [5; 16],
                capability: [8; 16],
                message,
            },
            Body::Relay {
                proof: [9; 16],
                peer: "beta",
                target: [5; 16],
                message,
            },
            Body::Delivered {
                proof: [9; 16],
                peer: "beta",
                session: [5; 16],
                message: &[],
            },
        ]
    }

    #[test]
    fn every_typed_variant_round_trips_and_truncation_fails_closed() {
        let message = [6; super::super::MAX_MESSAGE];
        for body in control_variants()
            .into_iter()
            .chain(data_variants(&message))
        {
            let mut open = [0; MAX_PACKET];
            let length = encode_mode(packet(body), None, &mut open).unwrap();
            let decoded = decode_mode(&open[..length], None).unwrap();
            let mut round_trip = [0; MAX_PACKET];
            assert_eq!(encode_mode(decoded, None, &mut round_trip), Some(length));
            assert_eq!(&open[..length], &round_trip[..length]);
            let mut buffer = [0; MAX_PACKET];
            let length = encode(packet(body), &key(), &mut buffer).unwrap();
            assert!(length <= MAX_PACKET);
            let decoded = decode(&buffer[..length], &key()).unwrap();
            assert_eq!(decoded.sender, "alpha");
            assert_eq!(decoded.session, [2; 16]);
            assert_eq!(decoded.sequence, 7);
            let mut round_trip = [0; MAX_PACKET];
            assert_eq!(encode(decoded, &key(), &mut round_trip), Some(length));
            assert_eq!(&buffer[..length], &round_trip[..length]);
            for prefix in 0..length {
                assert!(decode(&buffer[..prefix], &key()).is_none());
            }
        }
    }

    #[test]
    fn bad_tags_wrong_keys_and_every_single_byte_tamper_are_rejected() {
        let mut bytes = [0; MAX_PACKET];
        let length = encode(packet(Body::Hello { nonce: [7; 16] }), &key(), &mut bytes).unwrap();
        assert!(decode(&bytes[..length], &NetworkKey::from_bytes([9; 32])).is_none());
        for index in 0..length {
            bytes[index] ^= 1;
            assert!(decode(&bytes[..length], &key()).is_none());
            bytes[index] ^= 1;
        }
        assert!(decode(&[0; MAX_PACKET + 1], &key()).is_none());
    }

    fn signed(payload: &[u8]) -> Vec<u8> {
        let mut bytes = payload.to_vec();
        bytes.extend_from_slice(hmac::sign(&key().auth, payload).as_ref());
        bytes
    }

    #[test]
    fn authenticated_malformed_payloads_unknown_kinds_flags_and_utf8_fail_closed() {
        let mut buffer = [0; MAX_PACKET];
        let length = encode(
            packet(Body::Register {
                nonce: [7; 16],
                cookie: [8; 16],
                relay_only: false,
                credential: &[],
            }),
            &key(),
            &mut buffer,
        )
        .unwrap();
        let payload = &buffer[..length - TAG];
        for kind in [0, 17, 255] {
            let mut malformed = payload.to_vec();
            malformed[4] = kind;
            assert!(decode(&signed(&malformed), &key()).is_none());
        }
        for name_length in [0, 65, 255] {
            let mut malformed = payload.to_vec();
            malformed[5] = name_length;
            assert!(decode(&signed(&malformed), &key()).is_none());
        }
        let mut malformed = payload.to_vec();
        malformed[6] = 255;
        assert!(decode(&signed(&malformed), &key()).is_none());
        let mut malformed = payload.to_vec();
        malformed[67] = 2; // Relay-only flag follows nonce and cookie.
        assert!(decode(&signed(&malformed), &key()).is_none());
        let mut malformed = payload.to_vec();
        malformed.extend_from_slice(&[0]);
        assert!(decode(&signed(&malformed), &key()).is_none());
        let mut malformed = payload.to_vec();
        malformed[27..35].fill(0); // Sequence after the five-byte name and session.
        assert!(decode(&signed(&malformed), &key()).is_none());
    }

    #[test]
    fn exact_limits_accept_unicode_and_reject_oversized_messages_and_names() {
        let name = "é".repeat(32);
        let message = [0; super::super::MAX_MESSAGE];
        let mut buffer = [0; MAX_PACKET];
        let value = Packet {
            sender: &name,
            session: [1; 16],
            sequence: 1,
            body: Body::Relay {
                proof: [9; 16],
                peer: &name,
                target: [2; 16],
                message: &message,
            },
        };
        let length = encode(value, &key(), &mut buffer).unwrap();
        assert_eq!(decode(&buffer[..length], &key()).unwrap().sender, name);
        let oversized = [0; super::super::MAX_MESSAGE + 1];
        assert!(
            encode(
                packet(Body::Direct {
                    peer: "beta",
                    target: [0; 16],
                    capability: [8; 16],
                    message: &oversized
                }),
                &key(),
                &mut buffer
            )
            .is_none()
        );
        let long = "x".repeat(65);
        assert!(
            encode(
                Packet {
                    sender: &long,
                    ..value
                },
                &key(),
                &mut buffer
            )
            .is_none()
        );
        // Produce an authenticated oversized body manually, without the encoder.
        let length = encode(
            packet(Body::Direct {
                peer: "beta",
                target: [0; 16],
                capability: [8; 16],
                message: &message,
            }),
            &key(),
            &mut buffer,
        )
        .unwrap();
        let mut payload = buffer[..length - TAG].to_vec();
        payload.push(0);
        assert!(decode(&signed(&payload), &key()).is_none());
    }

    #[test]
    fn clean_wire_cutover_and_keyed_parsing_never_downgrade() {
        let mut bytes = [0; MAX_PACKET];
        let length = encode_mode(packet(Body::Hello { nonce: [7; 16] }), None, &mut bytes).unwrap();
        assert!(decode_mode(&bytes[..length], Some(&key())).is_none());
        let length = encode(packet(Body::Hello { nonce: [7; 16] }), &key(), &mut bytes).unwrap();
        assert!(decode_mode(&bytes[..length], None).is_none());
        let mut old = bytes[..length - TAG].to_vec();
        old[..4].copy_from_slice(b"GNP2");
        assert!(decode(&signed(&old), &key()).is_none());
        assert!(decode_mode(&old, None).is_none());
    }

    #[test]
    fn maximum_identity_and_payload_fit_all_data_paths_in_both_modes() {
        let name = "x".repeat(64);
        let message = [42; super::super::MAX_MESSAGE];
        let bodies = [
            Body::Direct {
                peer: &name,
                target: [2; 16],
                capability: [3; 16],
                message: &message,
            },
            Body::Relay {
                proof: [4; 16],
                peer: &name,
                target: [2; 16],
                message: &message,
            },
            Body::Delivered {
                proof: [4; 16],
                peer: &name,
                session: [2; 16],
                message: &message,
            },
        ];
        for body in bodies {
            for keyed in [false, true] {
                let key = key();
                let mode = keyed.then_some(&key);
                let mut bytes = [0; MAX_PACKET];
                let length = encode_mode(
                    Packet {
                        sender: &name,
                        ..packet(body)
                    },
                    mode,
                    &mut bytes,
                )
                .unwrap();
                assert!(length <= MAX_PACKET);
                let decoded = decode_mode(&bytes[..length], mode).unwrap();
                assert!(matches!(decoded.body, Body::Direct { message: got, .. }
                    | Body::Relay { message: got, .. }
                    | Body::Delivered { message: got, .. } if got == message));
            }
        }
    }
}
