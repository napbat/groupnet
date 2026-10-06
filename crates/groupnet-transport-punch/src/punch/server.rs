//! Bounded address-verified admission, discovery, and session-bound UDP relay.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::admission::{AcceptedPeer, Admission, JoinRequest, OpenAdmission};
use tokio::net::UdpSocket;
use tokio::sync::Mutex;
use tokio::task::{JoinHandle, JoinSet};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::wire::{self, Body, MAX_PACKET, Packet, Session};
use super::{LEASE, MAX_PEERS, NetworkKey, closed, invalid, random, transient, validate_names};

const CHALLENGE_TTL: Duration = Duration::from_secs(3);
const RATE_INTERVAL: Duration = Duration::from_secs(1);
const PACKETS_PER_INTERVAL: u16 = 256;
const MAX_PENDING_POLICY: usize = 32;

#[derive(Clone, Copy)]
struct Registration {
    address: SocketAddr,
    session: Session,
    proof: Session,
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
    admitting: Option<(Session, Instant)>,
    rate_start: Instant,
    rate_count: u16,
}

impl Entry {
    fn new(now: Instant) -> Self {
        Self {
            active: None,
            admitting: None,
            rate_start: now,
            rate_count: 0,
        }
    }

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
            || packet.body.request_proof() != Some(registration.proof)
            || registration.session != packet.session
            || packet.sequence <= registration.sequence
        {
            return None;
        }
        if !self.admit(now) {
            return None;
        }
        registration.sequence = packet.sequence;
        registration.seen = now;
        self.active = Some(registration);
        Some(registration)
    }

    fn permits_registration(&self, packet: Packet<'_>, address: SocketAddr, now: Instant) -> bool {
        self.admitting.is_none()
            && self.live(now).is_none_or(|active| {
                active.session == packet.session
                    && active.address == address
                    && packet.sequence > active.sequence
            })
    }
}

/// Unproven addresses occupy only an evictable challenge pool, never peer slots.
#[derive(Default)]
struct Challenges {
    pending: HashMap<String, Challenge>,
}

impl Challenges {
    fn issue(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        nonce: Session,
        now: Instant,
    ) -> io::Result<Option<Body<'static>>> {
        if self.pending.get(packet.sender).is_some_and(|pending| {
            pending.session == packet.session
                && pending.address == address
                && packet.sequence <= pending.sequence
                && now.duration_since(pending.issued) < CHALLENGE_TTL
        }) {
            return Ok(None);
        }
        if !self.pending.contains_key(packet.sender) && self.pending.len() >= MAX_PEERS {
            let oldest = self
                .pending
                .iter()
                .min_by_key(|(_, pending)| pending.issued)
                .map(|(name, _)| name.clone());
            if let Some(oldest) = oldest {
                self.pending.remove(&oldest);
            }
        }
        let cookie = random()?;
        self.pending.insert(
            packet.sender.to_owned(),
            Challenge {
                address,
                session: packet.session,
                nonce,
                cookie,
                sequence: packet.sequence,
                issued: now,
            },
        );
        Ok(Some(Body::Challenge { nonce, cookie }))
    }

    fn prove(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        now: Instant,
    ) -> Option<Registration> {
        let Body::Register {
            nonce,
            cookie,
            relay_only,
            ..
        } = packet.body
        else {
            return None;
        };
        let pending = self.pending.get(packet.sender)?;
        if pending.address != address
            || pending.session != packet.session
            || pending.nonce != nonce
            || pending.cookie != cookie
            || packet.sequence <= pending.sequence
            || now.duration_since(pending.issued) >= CHALLENGE_TTL
        {
            return None;
        }
        self.pending.remove(packet.sender);
        Some(Registration {
            address,
            session: packet.session,
            proof: nonce,
            sequence: packet.sequence,
            seen: now,
            relay_only,
        })
    }

    fn expire(&mut self, now: Instant) {
        self.pending
            .retain(|_, pending| now.duration_since(pending.issued) < CHALLENGE_TTL);
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

/// Address-verified UDP discovery and bounded relay.
///
/// Explicit open admission provides no cryptographic identity assurance.
/// Challenges expire after three seconds, sessions after six seconds without
/// fresh traffic. Live duplicate identities are rejected, never replaced.
/// At most 128 established identities, 128 evictable address challenges, and
/// 32 concurrent policy evaluations are retained. Unproven traffic has a separate
/// 256-packet/second budget; each live session has its own 256-packet/second budget.
#[derive(Debug)]
pub struct Rendezvous {
    inner: Arc<Inner>,
}

impl Rendezvous {
    /// Binds an explicitly keyed rendezvous with a fixed identity allowlist.
    ///
    /// # Errors
    /// Rejects malformed allowlists or socket binding failures.
    pub async fn bind(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> io::Result<Self> {
        validate_names(&peers)?;
        let allowed = peers
            .into_iter()
            .map(|peer| peer.as_str().to_owned())
            .collect();
        Self::start(bind, Some(key), Arc::new(OpenAdmission), Some(allowed)).await
    }

    /// Binds explicitly keyless discovery and relay, permitting requested direct paths.
    ///
    /// # Errors
    /// Propagates socket binding errors and rejects multicast bind addresses.
    pub async fn bind_open(bind: SocketAddr) -> io::Result<Self> {
        Self::bind_with_admission(bind, None, Arc::new(OpenAdmission)).await
    }

    /// Binds dynamic discovery with application admission and optional fabric HMAC.
    ///
    /// Keyed datagrams never fall back to keyless parsing. Policies run only
    /// after address return-routability, with bounded time and concurrency.
    ///
    /// # Errors
    /// Propagates socket binding errors and rejects multicast bind addresses.
    pub async fn bind_with_admission(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
    ) -> io::Result<Self> {
        Self::start(bind, key, admission, None).await
    }

    async fn start(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
        allowed: Option<HashSet<String>>,
    ) -> io::Result<Self> {
        if bind.ip().is_multicast() {
            return Err(invalid("multicast rendezvous bind is not supported"));
        }
        let socket = UdpSocket::bind(bind).await?;
        let address = socket.local_addr()?;
        let cancel = CancellationToken::new();
        let task = tokio::spawn(serve(socket, key, admission, allowed, cancel.clone()));
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        })
    }

    /// Returns the actual bind address.
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

    /// Cancels socket I/O and drains all pending policy evaluations.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        let mut task = self.inner.task.lock().await;
        if let Some(task) = task.as_mut() {
            let _ = task.await;
        }
        task.take();
    }
}

struct Decision {
    node: NodeId,
    registration: Registration,
    accepted: io::Result<AcceptedPeer>,
}

struct ServerState {
    entries: HashMap<String, Entry>,
    challenges: Challenges,
    decisions: JoinSet<Decision>,
    pre_admission: Entry,
}

async fn serve(
    socket: UdpSocket,
    key: Option<NetworkKey>,
    admission: Arc<dyn Admission>,
    allowed: Option<HashSet<String>>,
    cancel: CancellationToken,
) {
    let mut state = ServerState {
        entries: HashMap::new(),
        challenges: Challenges::default(),
        decisions: JoinSet::new(),
        pre_admission: Entry::new(Instant::now()),
    };
    let mut buffer = [0; MAX_PACKET + 1];
    let mut maintenance = tokio::time::interval(RATE_INTERVAL);
    maintenance.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // Cancellation covers every send, not just socket reception.
    let driver = async {
        loop {
            tokio::select! {
                biased;
                result = state.decisions.join_next(), if !state.decisions.is_empty() => {
                    if let Some(Ok(decision)) = result {
                        apply_decision(&socket, key.as_ref(), &mut state.entries, decision).await;
                    }
                }
                _ = maintenance.tick() => {
                    let now = Instant::now();
                    expire_entries(&mut state.entries, now);
                    state.challenges.expire(now);
                }
                received = socket.recv_from(&mut buffer) => {
                    let (length, address) = match received {
                        Ok(received) => received,
                        Err(error) if transient(&error) => continue,
                        Err(_) => break,
                    };
                    if address.ip().is_multicast() || address.ip().is_unspecified() || address.port() == 0 {
                        continue;
                    }
                    let Some(packet) = wire::decode_mode(&buffer[..length], key.as_ref()) else { continue; };
                    handle_packet(&socket, key.as_ref(), &admission, allowed.as_ref(), &mut state, packet, address).await;
                }
            }
        }
    };
    tokio::select! {
        biased;
        () = cancel.cancelled() => {}
        () = driver => {}
    }
    state.decisions.abort_all();
    while state.decisions.join_next().await.is_some() {}
    cancel.cancel();
}

async fn handle_packet(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    admission: &Arc<dyn Admission>,
    allowed: Option<&HashSet<String>>,
    state: &mut ServerState,
    packet: Packet<'_>,
    address: SocketAddr,
) {
    if allowed.is_some_and(|names| !names.contains(packet.sender)) {
        return;
    }
    let now = Instant::now();
    match packet.body {
        Body::Hello { nonce } => {
            if !state.pre_admission.admit(now)
                || state
                    .entries
                    .get(packet.sender)
                    .is_some_and(|entry| !entry.permits_registration(packet, address, now))
            {
                return;
            }
            if let Ok(Some(body)) = state.challenges.issue(packet, address, nonce, now) {
                respond(socket, key, address, packet, body).await;
            }
        }
        Body::Register { .. } => {
            if !state.pre_admission.admit(now)
                || state
                    .entries
                    .get(packet.sender)
                    .is_some_and(|entry| !entry.permits_registration(packet, address, now))
            {
                return;
            }
            let Some(registration) = state.challenges.prove(packet, address, now) else {
                return;
            };
            register(
                socket,
                key,
                admission,
                allowed.is_some(),
                state,
                packet,
                registration,
            )
            .await;
        }
        Body::Heartbeat { .. }
        | Body::Depart { .. }
        | Body::Discover { .. }
        | Body::Query { .. }
        | Body::Relay { .. } => {
            handle_established(socket, key, &mut state.entries, packet, address, now).await;
        }
        _ => {}
    }
}

async fn handle_established(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    entries: &mut HashMap<String, Entry>,
    packet: Packet<'_>,
    address: SocketAddr,
    now: Instant,
) {
    let Some(entry) = entries.get_mut(packet.sender) else {
        return;
    };
    let Some(registration) = entry.authenticate(packet, address, now) else {
        return;
    };
    match packet.body {
        Body::Heartbeat { .. } => {
            respond(
                socket,
                key,
                address,
                packet,
                Body::Registered {
                    proof: registration.proof,
                },
            )
            .await;
        }
        Body::Depart { .. } => {
            entry.active = None;
        }
        Body::Discover { .. } | Body::Query { .. } => {
            for (name, target) in entries.iter() {
                if name == packet.sender
                    || matches!(packet.body, Body::Query { peer, .. } if peer != name)
                {
                    continue;
                }
                let Some(target) = target.live(now) else {
                    continue;
                };
                respond(
                    socket,
                    key,
                    address,
                    packet,
                    Body::Offer {
                        proof: registration.proof,
                        peer: name,
                        session: target.session,
                        address: (!registration.relay_only && !target.relay_only)
                            .then_some(target.address),
                        relay_only: registration.relay_only || target.relay_only,
                    },
                )
                .await;
            }
        }
        Body::Relay {
            peer,
            target,
            message,
            ..
        } if peer != packet.sender => {
            let Some(recipient) = entries.get(peer).and_then(|entry| entry.live(now)) else {
                return;
            };
            if recipient.session != target {
                return;
            }
            transmit(
                socket,
                key,
                recipient.address,
                Packet {
                    sender: peer,
                    session: recipient.session,
                    sequence: packet.sequence,
                    body: Body::Delivered {
                        proof: recipient.proof,
                        peer: packet.sender,
                        session: packet.session,
                        message,
                    },
                },
            )
            .await;
        }
        _ => {}
    }
}

async fn register(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    admission: &Arc<dyn Admission>,
    static_allowed: bool,
    state: &mut ServerState,
    packet: Packet<'_>,
    registration: Registration,
) {
    let Body::Register { credential, .. } = packet.body else {
        return;
    };
    let now = registration.seen;
    if static_allowed && !credential.is_empty() {
        respond(
            socket,
            key,
            registration.address,
            packet,
            Body::Denied {
                proof: registration.proof,
            },
        )
        .await;
        return;
    }
    if let Some(entry) = state.entries.get_mut(packet.sender)
        && entry.live(now).is_some()
    {
        entry.active = Some(registration);
        respond(
            socket,
            key,
            registration.address,
            packet,
            Body::Registered {
                proof: registration.proof,
            },
        )
        .await;
        return;
    }
    expire_entries(&mut state.entries, now);
    if state.entries.len() >= MAX_PEERS || state.decisions.len() >= MAX_PENDING_POLICY {
        respond(
            socket,
            key,
            registration.address,
            packet,
            Body::Denied {
                proof: registration.proof,
            },
        )
        .await;
        return;
    }
    let entry = state
        .entries
        .entry(packet.sender.to_owned())
        .or_insert_with(|| Entry::new(now));
    entry.admitting = Some((packet.session, now));
    spawn_admission(
        &mut state.decisions,
        admission,
        packet,
        registration,
        credential,
    );
}

fn expire_entries(entries: &mut HashMap<String, Entry>, now: Instant) {
    entries.retain(|_, entry| {
        if entry.live(now).is_none() {
            entry.active = None;
        }
        if entry
            .admitting
            .is_some_and(|(_, issued)| now.duration_since(issued) >= CHALLENGE_TTL)
        {
            entry.admitting = None;
        }
        entry.active.is_some() || entry.admitting.is_some()
    });
}

fn spawn_admission(
    decisions: &mut JoinSet<Decision>,
    admission: &Arc<dyn Admission>,
    packet: Packet<'_>,
    registration: Registration,
    credential: &[u8],
) {
    let node = NodeId::from(packet.sender);
    let credential = credential.to_vec();
    let policy = admission.clone();
    let address = registration.address;
    decisions.spawn(async move {
        let accepted = tokio::time::timeout_at(
            registration.seen + CHALLENGE_TTL,
            policy.admit(JoinRequest {
                claimed: &node,
                credential: &credential,
                remote: Some(address),
            }),
        )
        .await
        .unwrap_or_else(|_| {
            Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "admission timed out",
            ))
        });
        Decision {
            node,
            registration,
            accepted,
        }
    });
}

async fn apply_decision(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    entries: &mut HashMap<String, Entry>,
    decision: Decision,
) {
    let Some(entry) = entries.get_mut(decision.node.as_str()) else {
        return;
    };
    if entry.admitting != Some((decision.registration.session, decision.registration.seen)) {
        return;
    }
    entry.admitting = None;
    let accepted = decision
        .accepted
        .is_ok_and(|accepted| accepted.node == decision.node)
        && entry.live(Instant::now()).is_none()
        && Instant::now().duration_since(decision.registration.seen) < CHALLENGE_TTL;
    let registration = decision.registration;
    if accepted {
        entry.active = Some(Registration {
            seen: Instant::now(),
            ..registration
        });
    }
    transmit(
        socket,
        key,
        registration.address,
        Packet {
            sender: decision.node.as_str(),
            session: registration.session,
            sequence: registration.sequence,
            body: if accepted {
                Body::Registered {
                    proof: registration.proof,
                }
            } else {
                Body::Denied {
                    proof: registration.proof,
                }
            },
        },
    )
    .await;
}

async fn respond(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    address: SocketAddr,
    request: Packet<'_>,
    body: Body<'_>,
) {
    transmit(socket, key, address, Packet { body, ..request }).await;
}

async fn transmit(
    socket: &UdpSocket,
    key: Option<&NetworkKey>,
    address: SocketAddr,
    packet: Packet<'_>,
) {
    let mut buffer = [0; MAX_PACKET];
    if let Some(length) = wire::encode_mode(packet, key, &mut buffer) {
        let _ = socket.send_to(&buffer[..length], address).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(now: Instant) -> Entry {
        Entry {
            active: None,
            rate_start: now,
            admitting: None,
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
            proof: [3; 16],
            sequence: 10,
            seen: now,
            relay_only: false,
        });
        let valid = Packet {
            sender: "a",
            session: [1; 16],
            sequence: 11,
            body: Body::Query {
                proof: [3; 16],
                peer: "b",
            },
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

#[cfg(test)]
mod admission_regressions;
