//! Directional AEAD, fresh packet nonces and bounded replay/dedup windows.

use std::io;

use bytes::Bytes;
use ring::aead::{self, Aad, LessSafeKey, Nonce, UnboundKey};

use super::UnorderedDelivery;

pub(super) type SessionId = [u8; 16];
pub(super) const HEADER: usize = 37;
const TAG: usize = 16;
pub(super) const WINDOW: usize = 1024;
const MAGIC: &[u8; 4] = b"GNU1";

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

pub(super) struct Crypto {
    tx: LessSafeKey,
    rx: LessSafeKey,
    next: u64,
    replay: Window,
}

impl std::fmt::Debug for Crypto {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Crypto").finish_non_exhaustive()
    }
}

impl Crypto {
    pub(super) fn new(keys: &[u8; 64], initiator: bool) -> io::Result<Self> {
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
        Ok(Self {
            tx: key(tx)?,
            rx: key(rx)?,
            next: 1,
            replay: Window::default(),
        })
    }

    pub(super) fn seal(
        &mut self,
        session: SessionId,
        kind: Kind,
        message: u64,
        body: &[u8],
    ) -> io::Result<Vec<u8>> {
        let counter = self.next;
        self.next = self
            .next
            .checked_add(1)
            .ok_or_else(|| io::Error::other("unordered nonce exhausted"))?;
        let mut packet = Vec::with_capacity(HEADER + body.len() + TAG);
        packet.extend_from_slice(MAGIC);
        packet.extend_from_slice(&session);
        packet.extend_from_slice(&counter.to_be_bytes());
        packet.push(kind as u8);
        packet.extend_from_slice(&message.to_be_bytes());
        let mut header = [0; HEADER];
        header.copy_from_slice(&packet);
        packet.extend_from_slice(body);
        let tag = self
            .tx
            .seal_in_place_separate_tag(nonce(counter), Aad::from(&header), &mut packet[HEADER..])
            .map_err(|_| invalid())?;
        packet.extend_from_slice(tag.as_ref());
        Ok(packet)
    }

    pub(super) fn open(
        &mut self,
        packet: &[u8],
        max_payload: usize,
    ) -> io::Result<(Kind, u64, Bytes)> {
        if packet.len() < HEADER + TAG || packet.len() > HEADER + TAG + max_payload {
            return Err(invalid());
        }
        let counter = u64::from_be_bytes(packet[20..28].try_into().map_err(|_| invalid())?);
        if self.replay.too_old(counter) || self.replay.contains(counter) {
            return Err(invalid());
        }
        let kind = Kind::decode(packet[28]).ok_or_else(invalid)?;
        let message = u64::from_be_bytes(packet[29..37].try_into().map_err(|_| invalid())?);
        let mut body = packet[HEADER..].to_vec();
        let length = self
            .rx
            .open_in_place(nonce(counter), Aad::from(&packet[..HEADER]), &mut body)
            .map_err(|_| invalid())?
            .len();
        if (kind != Kind::Data && length != 0)
            || (matches!(kind, Kind::Data | Kind::Ack) && message == 0)
        {
            return Err(invalid());
        }
        self.replay.insert(counter);
        body.truncate(length);
        Ok((kind, message, Bytes::from(body)))
    }
}

pub(super) fn session(packet: &[u8]) -> Option<SessionId> {
    if packet.len() < HEADER + TAG || !packet.starts_with(MAGIC) {
        return None;
    }
    packet[4..20].try_into().ok()
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
