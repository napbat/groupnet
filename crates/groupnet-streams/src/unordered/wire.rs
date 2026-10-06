//! Directional AEAD, fresh packet nonces and bounded replay/dedup windows.

use std::io;

use bytes::Bytes;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};
use zerocopy::byteorder::network_endian::U64;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::UnorderedDelivery;

pub(super) type SessionId = [u8; 16];
pub(super) const HEADER: usize = size_of::<Header>();
pub(super) const TAG: usize = 16;
// Security horizon, not an operational queue capacity. Both peers use this
// fixed layout and send allocation must never advance past unresolved IDs.
pub(super) const WINDOW: usize = 1024;
const MAGIC: [u8; 4] = *b"GNU1";

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct Header {
    magic: [u8; 4],
    session: SessionId,
    counter: U64,
    kind: u8,
    message: U64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub(super) enum Kind {
    Data = 1,
    Ack = 2,
    Ping = 3,
    Pong = 4,
    Close = 5,
}

impl Kind {
    fn decode(value: u8) -> Option<Self> {
        match value {
            1 => Some(Self::Data),
            2 => Some(Self::Ack),
            3 => Some(Self::Ping),
            4 => Some(Self::Pong),
            5 => Some(Self::Close),
            _ => None,
        }
    }
}

/// A fixed-memory anti-replay window. Numbers older than the window fail closed.
#[derive(Debug, Default)]
pub(super) struct Window {
    highest: u64,
    bits: [u64; WINDOW / 64],
}

impl Window {
    pub(super) fn contains(&self, number: u64) -> bool {
        if number == 0 || number > self.highest {
            return false;
        }
        let age = self.highest - number;
        age < WINDOW as u64
            && (self.bits[usize::try_from(age).expect("age is below the fixed window") / 64]
                & (1 << (age % 64)))
                != 0
    }

    pub(super) fn too_old(&self, number: u64) -> bool {
        number == 0 || (number <= self.highest && self.highest - number >= WINDOW as u64)
    }

    pub(super) fn insert(&mut self, number: u64) {
        if number > self.highest {
            let distance = number - self.highest;
            if distance >= WINDOW as u64 {
                self.bits.fill(0);
            } else {
                let distance =
                    usize::try_from(distance).expect("distance is below the fixed window");
                let words = distance / 64;
                let shift = distance % 64;
                for index in (0..self.bits.len()).rev() {
                    self.bits[index] = if index >= words {
                        let mut value = self.bits[index - words] << shift;
                        if shift != 0 && index > words {
                            value |= self.bits[index - words - 1] >> (64 - shift);
                        }
                        value
                    } else {
                        0
                    };
                }
            }
            self.highest = number;
        }
        if !self.too_old(number) {
            let age =
                usize::try_from(self.highest - number).expect("age is below the fixed window");
            self.bits[age / 64] |= 1 << (age % 64);
        }
    }
}

pub(super) struct TxCrypto {
    key: LessSafeKey,
    next: u64,
}

pub(super) struct RxCrypto {
    key: LessSafeKey,
    replay: Window,
}

impl std::fmt::Debug for TxCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TxCrypto").finish_non_exhaustive()
    }
}

impl std::fmt::Debug for RxCrypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RxCrypto").finish_non_exhaustive()
    }
}

pub(super) fn crypto(keys: &[u8; 64], initiator: bool) -> io::Result<(TxCrypto, RxCrypto)> {
    let (first, second) = keys.split_at(32);
    let (tx, rx) = if initiator {
        (first, second)
    } else {
        (second, first)
    };
    let key = |bytes| {
        UnboundKey::new(&aead::CHACHA20_POLY1305, bytes)
            .map(LessSafeKey::new)
            .map_err(|_| invalid())
    };
    Ok((
        TxCrypto {
            key: key(tx)?,
            next: 1,
        },
        RxCrypto {
            key: key(rx)?,
            replay: Window::default(),
        },
    ))
}

impl TxCrypto {
    /// Seals an initialized header/body/tag allocation without copying the body.
    /// Callers retain retry plaintext separately; failed attempts consume a nonce.
    pub(super) fn seal(
        &mut self,
        session: SessionId,
        kind: Kind,
        message: u64,
        packet: &mut [u8],
    ) -> io::Result<()> {
        if packet.len() < HEADER + TAG {
            return Err(invalid());
        }
        let counter = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| io::Error::other("unordered nonce exhausted"))?;
        let header = Header {
            magic: MAGIC,
            session,
            counter: U64::new(counter),
            kind: kind as u8,
            message: U64::new(message),
        };
        let (prefix, body) = packet.split_at_mut(HEADER);
        prefix.copy_from_slice(header.as_bytes());
        let length = body.len() - TAG;
        let (body, suffix) = body.split_at_mut(length);
        let tag = self
            .key
            .seal_in_place_separate_tag(nonce(counter), Aad::from(&*prefix), body)
            .map_err(|_| invalid())?;
        suffix.copy_from_slice(tag.as_ref());
        Ok(())
    }
}

impl RxCrypto {
    pub(super) fn open(
        &mut self,
        packet: Bytes,
        max_payload: usize,
    ) -> io::Result<(Kind, u64, Bytes)> {
        if packet.len() < HEADER + TAG || packet.len() - HEADER - TAG > max_payload {
            return Err(invalid());
        }
        let (header, _) = Header::read_from_prefix(packet.as_ref()).map_err(|_| invalid())?;
        let counter = header.counter.get();
        if header.magic != MAGIC || self.replay.too_old(counter) || self.replay.contains(counter) {
            return Err(invalid());
        }
        let kind = Kind::decode(header.kind).ok_or_else(invalid)?;
        let message = header.message.get();
        // Unique network packets recover their mutable allocation. Shared Bytes
        // copy once so neither successful nor failed authentication mutates aliases.
        let mut packet = packet
            .try_into_mut()
            .unwrap_or_else(|shared| shared.as_ref().into());
        let (prefix, body) = packet.split_at_mut(HEADER);
        let length = self
            .key
            .open_in_place(nonce(counter), Aad::from(&*prefix), body)
            .map_err(|_| invalid())?
            .len();
        if (kind != Kind::Data && length != 0)
            || (matches!(kind, Kind::Data | Kind::Ack) && message == 0)
        {
            return Err(invalid());
        }
        // Authentication and semantic validation precede replay commitment.
        self.replay.insert(counter);
        packet.truncate(HEADER + length);
        Ok((kind, message, packet.freeze().slice(HEADER..)))
    }
}

pub(super) fn session(packet: &[u8]) -> Option<SessionId> {
    if packet.len() < HEADER + TAG {
        return None;
    }
    let (header, _) = Header::ref_from_prefix(packet).ok()?;
    (header.magic == MAGIC).then_some(header.session)
}

fn nonce(counter: u64) -> Nonce {
    let mut bytes = [0; 12];
    bytes[4..].copy_from_slice(&counter.to_be_bytes());
    Nonce::assume_unique_for_key(bytes)
}

pub(super) fn policy(delivery: UnorderedDelivery) -> u8 {
    match delivery {
        UnorderedDelivery::Reliable => 1,
        UnorderedDelivery::Unreliable => 2,
    }
}

pub(super) fn delivery(value: u8) -> io::Result<UnorderedDelivery> {
    match value {
        1 => Ok(UnorderedDelivery::Reliable),
        2 => Ok(UnorderedDelivery::Unreliable),
        _ => Err(invalid()),
    }
}

fn invalid() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidData,
        "invalid authenticated unordered packet",
    )
}
