//! Bounded rendezvous registrations and authenticated, non-open relay.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use groupnet_core::NodeId;
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::task::JoinHandle;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use super::wire::{self, Body, MAX_PACKET, Packet, Session};
use super::{LEASE, NetworkKey, closed, invalid, random, transient, validate_names};

const CHALLENGE_TTL: Duration = Duration::from_secs(3);
const RATE_INTERVAL: Duration = Duration::from_secs(1);
// All authenticated packets, including rejected/replayed control packets, count.
const PACKETS_PER_INTERVAL: u16 = 256;

#[derive(Clone, Copy)]
struct Registration {
    address: SocketAddr,
    session: Session,
    sequence: u64,
    seen: Instant,
    relay_only: bool,
}

struct Challenge {
    address: SocketAddr,
    session: Session,
    nonce: Session,
    cookie: Session,
    sequence: u64,
    issued: Instant,
}

struct Entry {
    active: Option<Registration>,
    pending: Option<Challenge>,
    rate_start: Instant,
    rate_count: u16,
}

impl Entry {
    fn admit(&mut self, now: Instant) -> bool {
        if now.duration_since(self.rate_start) >= RATE_INTERVAL {
            self.rate_start = now;
            self.rate_count = 0;
        }
        if self.rate_count >= PACKETS_PER_INTERVAL {
            return false;
        }
        self.rate_count += 1;
        true
    }

    fn live(&self, now: Instant) -> Option<Registration> {
        self.active
            .filter(|registration| now.duration_since(registration.seen) < LEASE)
    }

    fn authenticate(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        now: Instant,
    ) -> Option<Registration> {
        let mut registration = self.live(now)?;
        if registration.address != address
            || registration.session != packet.session
            || packet.sequence <= registration.sequence
        {
            return None;
        }
        registration.sequence = packet.sequence;
        registration.seen = now;
        self.active = Some(registration);
        Some(registration)
    }

    fn challenge(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        nonce: Session,
        now: Instant,
    ) -> io::Result<Option<Body<'static>>> {
        if self.active.is_some_and(|active| {
            active.session == packet.session && packet.sequence <= active.sequence
        }) || self.pending.as_ref().is_some_and(|pending| {
            pending.session == packet.session && packet.sequence <= pending.sequence
        }) {
            return Ok(None);
        }
        let cookie = random()?;
        self.pending = Some(Challenge {
            address,
            session: packet.session,
            nonce,
            cookie,
            sequence: packet.sequence,
            issued: now,
        });
        Ok(Some(Body::Challenge { nonce, cookie }))
    }

    fn register(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        nonce: Session,
        cookie: Session,
        relay_only: bool,
        now: Instant,
    ) -> bool {
        let Some(pending) = self.pending.as_ref() else {
            return false;
        };
        if pending.address != address
            || pending.session != packet.session
            || pending.nonce != nonce
            || pending.cookie != cookie
            || packet.sequence <= pending.sequence
            || now.duration_since(pending.issued) >= CHALLENGE_TTL
            || self.active.is_some_and(|active| {
                active.session == packet.session && packet.sequence <= active.sequence
            })
        {
            return false;
        }
        self.pending = None;
        self.active = Some(Registration {
            address,
            session: packet.session,
            sequence: packet.sequence,
            seen: now,
            relay_only,
        });
        true
    }
}

#[derive(Debug)]
struct Inner {
    address: SocketAddr,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Native UDP discovery and bounded relay for an explicitly trusted fabric.
///
/// The allowlist is fixed at bind time (at most 128 identities). A fresh random
/// challenge must return from its observed UDP address before registration is
/// replaced, discovery is disclosed, or relay traffic is accepted. Challenges
/// expire after three seconds and are consumed once; registrations expire after
/// six seconds without fresh authenticated traffic. Each allowed identity may
/// submit at most 256 packets per second, each no larger than 1200 bytes.
/// Shared-key holders are trusted members, not mutually authenticated endpoints.
#[derive(Debug)]
pub struct Rendezvous {
    inner: Arc<Inner>,
}

impl Rendezvous {
    /// Binds a rendezvous socket for the fixed identity allowlist.
    ///
    /// # Errors
    /// Rejects duplicate/empty/oversized identities and allowlists over 128;
    /// propagates socket bind errors.
    pub async fn bind(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> io::Result<Self> {
        validate_names(&peers)?;
        if bind.ip().is_multicast() {
            return Err(invalid("multicast rendezvous bind is not supported"));
        }
        let socket = UdpSocket::bind(bind).await?;
        let address = socket.local_addr()?;
        let now = Instant::now();
        let entries = peers
            .into_iter()
            .map(|peer| {
                (
                    peer.as_str().to_owned(),
                    Entry {
                        active: None,
                        pending: None,
                        rate_start: now,
                        rate_count: 0,
                    },
                )
            })
            .collect();
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(socket, key, entries, cancel.clone()));
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        })
    }

    /// Returns the actual bind address, including an assigned ephemeral port.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            Err(closed())
        } else {
            Ok(self.inner.address)
        }
    }

    /// Cancels the socket runtime and waits for all socket I/O to finish.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        let mut task = self.inner.task.lock().await;
        if let Some(task) = task.take() {
            let _ = task.await;
        }
    }
}

async fn run(
    socket: UdpSocket,
    key: NetworkKey,
    entries: HashMap<String, Entry>,
    cancel: CancellationToken,
) {
    // A cancellation drops all socket I/O, not just the receive operation.
    tokio::select! {
        biased;
        () = cancel.cancelled() => {}
        () = serve(socket, key, entries) => {}
    }
    cancel.cancel();
}

async fn serve(socket: UdpSocket, key: NetworkKey, mut entries: HashMap<String, Entry>) {
    let mut buffer = [0; MAX_PACKET + 1];
    loop {
        let received = socket.recv_from(&mut buffer).await;
        let (length, address) = match received {
            Ok(received) => received,
            Err(error) if transient(&error) => continue,
            Err(_) => break,
        };
        if address.ip().is_multicast() || address.ip().is_unspecified() || address.port() == 0 {
            continue;
        }
        let Some(packet) = wire::decode(&buffer[..length], &key) else {
            continue;
        };
        let Some(entry) = entries.get_mut(packet.sender) else {
            continue;
        };
        let now = Instant::now();
        if !entry.admit(now) {
            continue;
        }
        match packet.body {
            Body::Hello { nonce } => match entry.challenge(packet, address, nonce, now) {
                Ok(Some(body)) => respond(&socket, &key, address, packet, body).await,
                Ok(None) => {}
                Err(_) => break,
            },
            Body::Register {
                nonce,
                cookie,
                relay_only,
            } => {
                if entry.register(packet, address, nonce, cookie, relay_only, now) {
                    respond(&socket, &key, address, packet, Body::Registered).await;
                }
            }
            Body::Query { peer } => {
                if entry.authenticate(packet, address, now).is_none() || peer == packet.sender {
                    continue;
                }
                let Some(target) = entries.get(peer).and_then(|entry| entry.live(now)) else {
                    continue;
                };
                respond(
                    &socket,
                    &key,
                    address,
                    packet,
                    Body::Offer {
                        peer,
                        session: target.session,
                        address: target.address,
                        relay_only: target.relay_only,
                    },
                )
                .await;
            }
            Body::Relay {
                peer,
                target,
                message,
            } => {
                if entry.authenticate(packet, address, now).is_none() || peer == packet.sender {
                    continue;
                }
                let Some(recipient) = entries.get(peer).and_then(|entry| entry.live(now)) else {
                    continue;
                };
                if recipient.session != target {
                    continue;
                }
                let delivered = Packet {
                    sender: peer,
                    session: recipient.session,
                    sequence: packet.sequence,
                    body: Body::Delivered {
                        peer: packet.sender,
                        session: packet.session,
                        message,
                    },
                };
                transmit(&socket, &key, recipient.address, delivered).await;
            }
            _ => {}
        }
    }
}

async fn respond(
    socket: &UdpSocket,
    key: &NetworkKey,
    address: SocketAddr,
    request: Packet<'_>,
    body: Body<'_>,
) {
    transmit(socket, key, address, Packet { body, ..request }).await;
}

async fn transmit(socket: &UdpSocket, key: &NetworkKey, address: SocketAddr, packet: Packet<'_>) {
    let mut buffer = [0; MAX_PACKET];
    if let Some(length) = wire::encode(packet, key, &mut buffer) {
        let _ = socket.send_to(&buffer[..length], address).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(now: Instant) -> Entry {
        Entry {
            active: None,
            pending: None,
            rate_start: now,
            rate_count: 0,
        }
    }

    #[test]
    fn per_identity_rate_budget_is_strict_and_resets_only_after_the_window() {
        let now = Instant::now();
        let mut state = entry(now);
        for _ in 0..PACKETS_PER_INTERVAL {
            assert!(state.admit(now));
        }
        assert!(!state.admit(now));
        assert!(!state.admit(now + RATE_INTERVAL - Duration::from_nanos(1)));
        assert!(state.admit(now + RATE_INTERVAL));
        assert_eq!(state.rate_count, 1);
    }

    #[test]
    fn registration_lease_address_session_and_sequence_all_fail_closed() {
        let now = Instant::now();
        let address = SocketAddr::from(([127, 0, 0, 1], 1234));
        let mut state = entry(now);
        state.active = Some(Registration {
            address,
            session: [1; 16],
            sequence: 10,
            seen: now,
            relay_only: false,
        });
        let valid = Packet {
            sender: "a",
            session: [1; 16],
            sequence: 11,
            body: Body::Query { peer: "b" },
        };
        assert!(
            state
                .authenticate(valid, SocketAddr::from(([127, 0, 0, 1], 1235)), now)
                .is_none()
        );
        assert!(
            state
                .authenticate(
                    Packet {
                        session: [2; 16],
                        ..valid
                    },
                    address,
                    now
                )
                .is_none()
        );
        assert!(
            state
                .authenticate(
                    Packet {
                        sequence: 10,
                        ..valid
                    },
                    address,
                    now
                )
                .is_none()
        );
        assert!(state.live(now + LEASE).is_none());
        assert!(state.authenticate(valid, address, now + LEASE).is_none());
        let refreshed = state
            .authenticate(valid, address, now + Duration::from_secs(1))
            .unwrap();
        assert_eq!(refreshed.sequence, 11);
        assert_eq!(refreshed.seen, now + Duration::from_secs(1));
        assert!(
            state
                .authenticate(valid, address, now + Duration::from_secs(2))
                .is_none()
        );
    }
}
