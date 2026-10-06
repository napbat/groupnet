//! Bounded, authenticated datagrams. Authentication precedes parsing or allocation.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use ring::hmac;

use super::NetworkKey;

pub(super) const MAX_PACKET: usize = 1200;
const TAG: usize = 32;
const MAGIC: &[u8; 4] = b"GNP1";
pub(super) type Session = [u8; 16];

#[derive(Clone, Copy, Debug)]
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
    },
    Registered,
    Query {
        peer: &'a str,
    },
    Offer {
        peer: &'a str,
        session: Session,
        address: SocketAddr,
        relay_only: bool,
    },
    Probe {
        target: Session,
        nonce: Session,
    },
    ProbeAck {
        target: Session,
        nonce: Session,
    },
    Direct {
        peer: &'a str,
        target: Session,
        message: &'a [u8],
    },
    Relay {
        peer: &'a str,
        target: Session,
        message: &'a [u8],
    },
    Delivered {
        peer: &'a str,
        session: Session,
        message: &'a [u8],
    },
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
    fn message(&mut self) -> Option<&'a [u8]> {
        let remaining = self.bytes.len().checked_sub(self.position)?;
        if remaining > super::MAX_MESSAGE {
            return None;
        }
        self.take(remaining)
    }
}

pub(super) fn encode(
    packet: Packet<'_>,
    key: &NetworkKey,
    buffer: &mut [u8; MAX_PACKET],
) -> Option<usize> {
    let mut writer = Writer {
        bytes: &mut buffer[..MAX_PACKET - TAG],
        position: 0,
    };
    writer.put(MAGIC)?;
    let kind = match packet.body {
        Body::Hello { .. } => 1,
        Body::Challenge { .. } => 2,
        Body::Register { .. } => 3,
        Body::Registered => 4,
        Body::Query { .. } => 5,
        Body::Offer { .. } => 6,
        Body::Probe { .. } => 7,
        Body::ProbeAck { .. } => 8,
        Body::Direct { .. } => 9,
        Body::Relay { .. } => 10,
        Body::Delivered { .. } => 11,
    };
    writer.put(&[kind])?;
    writer.name(packet.sender)?;
    writer.put(&packet.session)?;
    writer.put(&packet.sequence.to_be_bytes())?;
    match packet.body {
        Body::Hello { nonce } => writer.put(&nonce)?,
        Body::Challenge { nonce, cookie } => {
            writer.put(&nonce)?;
            writer.put(&cookie)?;
        }
        Body::Register {
            nonce,
            cookie,
            relay_only,
        } => {
            writer.put(&nonce)?;
            writer.put(&cookie)?;
            writer.put(&[u8::from(relay_only)])?;
        }
        Body::Registered => {}
        Body::Query { peer } => writer.name(peer)?,
        Body::Offer {
            peer,
            session,
            address,
            relay_only,
        } => {
            writer.name(peer)?;
            writer.put(&session)?;
            match address.ip() {
                IpAddr::V4(ip) => {
                    writer.put(&[4])?;
                    writer.put(&ip.octets())?;
                }
                IpAddr::V6(ip) => {
                    writer.put(&[6])?;
                    writer.put(&ip.octets())?;
                }
            }
            writer.put(&address.port().to_be_bytes())?;
            writer.put(&[u8::from(relay_only)])?;
        }
        Body::Probe { target, nonce } | Body::ProbeAck { target, nonce } => {
            writer.put(&target)?;
            writer.put(&nonce)?;
        }
        Body::Direct {
            peer,
            target,
            message,
        }
        | Body::Relay {
            peer,
            target,
            message,
        } => {
            if message.len() > super::MAX_MESSAGE {
                return None;
            }
            writer.name(peer)?;
            writer.put(&target)?;
            writer.put(message)?;
        }
        Body::Delivered {
            peer,
            session,
            message,
        } => {
            if message.len() > super::MAX_MESSAGE {
                return None;
            }
            writer.name(peer)?;
            writer.put(&session)?;
            writer.put(message)?;
        }
    }
    let length = writer.position;
    let tag = hmac::sign(&key.auth, &buffer[..length]);
    buffer[length..length + TAG].copy_from_slice(tag.as_ref());
    Some(length + TAG)
}

pub(super) fn decode<'a>(bytes: &'a [u8], key: &NetworkKey) -> Option<Packet<'a>> {
    if bytes.len() > MAX_PACKET || bytes.len() < TAG {
        return None;
    }
    let length = bytes.len() - TAG;
    hmac::verify(&key.auth, &bytes[..length], &bytes[length..]).ok()?;
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
    let body = match kind {
        1 => Body::Hello {
            nonce: reader.token()?,
        },
        2 => Body::Challenge {
            nonce: reader.token()?,
            cookie: reader.token()?,
        },
        3 => Body::Register {
            nonce: reader.token()?,
            cookie: reader.token()?,
            relay_only: reader.flag()?,
        },
        4 => Body::Registered,
        5 => Body::Query {
            peer: reader.name()?,
        },
        6 => {
            let peer = reader.name()?;
            let session = reader.token()?;
            let ip = match reader.byte()? {
                4 => IpAddr::V4(Ipv4Addr::from(<[u8; 4]>::try_from(reader.take(4)?).ok()?)),
                6 => IpAddr::V6(Ipv6Addr::from(<[u8; 16]>::try_from(reader.take(16)?).ok()?)),
                _ => return None,
            };
            let port = u16::from_be_bytes(reader.take(2)?.try_into().ok()?);
            if port == 0 || ip.is_unspecified() || ip.is_multicast() {
                return None;
            }
            Body::Offer {
                peer,
                session,
                address: SocketAddr::new(ip, port),
                relay_only: reader.flag()?,
            }
        }
        7 => Body::Probe {
            target: reader.token()?,
            nonce: reader.token()?,
        },
        8 => Body::ProbeAck {
            target: reader.token()?,
            nonce: reader.token()?,
        },
        9 => Body::Direct {
            peer: reader.name()?,
            target: reader.token()?,
            message: reader.message()?,
        },
        10 => Body::Relay {
            peer: reader.name()?,
            target: reader.token()?,
            message: reader.message()?,
        },
        11 => Body::Delivered {
            peer: reader.name()?,
            session: reader.token()?,
            message: reader.message()?,
        },
        _ => return None,
    };
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

    #[test]
    fn every_typed_variant_round_trips_and_truncation_fails_closed() {
        let address = SocketAddr::from(([127, 0, 0, 1], 1234));
        let message = [6; super::super::MAX_MESSAGE];
        let variants = [
            Body::Hello { nonce: [3; 16] },
            Body::Challenge {
                nonce: [3; 16],
                cookie: [4; 16],
            },
            Body::Register {
                nonce: [3; 16],
                cookie: [4; 16],
                relay_only: true,
            },
            Body::Registered,
            Body::Query { peer: "beta" },
            Body::Offer {
                peer: "beta",
                session: [5; 16],
                address,
                relay_only: false,
            },
            Body::Offer {
                peer: "beta",
                session: [5; 16],
                address: "[::1]:1234".parse().unwrap(),
                relay_only: true,
            },
            Body::Probe {
                target: [5; 16],
                nonce: [3; 16],
            },
            Body::ProbeAck {
                target: [5; 16],
                nonce: [3; 16],
            },
            Body::Direct {
                peer: "beta",
                target: [5; 16],
                message: &message,
            },
            Body::Relay {
                peer: "beta",
                target: [5; 16],
                message: &message,
            },
            Body::Delivered {
                peer: "beta",
                session: [5; 16],
                message: &[],
            },
        ];
        for body in variants {
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
            }),
            &key(),
            &mut buffer,
        )
        .unwrap();
        let payload = &buffer[..length - TAG];
        for kind in [0, 12, 255] {
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
        *malformed.last_mut().unwrap() = 2;
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
}
