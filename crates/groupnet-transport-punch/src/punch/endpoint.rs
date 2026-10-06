//! One bounded socket-owning endpoint task.

mod candidates;
mod direct;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use groupnet_transport::admission::{AcceptedPeer, SessionRegistry};
use groupnet_transport::link::AdmittedInbound;
use tokio::net::UdpSocket;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::wire::{self, Body, MAX_PACKET, Packet, Session};
use super::{
    Capability, HEARTBEAT, Outbound, PathPolicy, Peer, PunchConfig, lock, random, transient,
};

struct Endpoint {
    config: PunchConfig,
    socket: UdpSocket,
    extra_sockets: Vec<UdpSocket>,
    local_candidates: super::candidates::Candidates,
    observed: Arc<Mutex<Option<SocketAddr>>>,
    announcement: u64,
    session: Session,
    nonce: Session,
    sequence: u64,
    hello: u64,
    registration: u64,
    queries: HashMap<String, u64>,
    discovery: u64,
    heartbeat: u64,
    registered: Option<Instant>,
    sessions: SessionRegistry,
    ready: Option<oneshot::Sender<std::io::Result<()>>>,
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    incoming: mpsc::Sender<AdmittedInbound>,
}

pub(super) type Channels = (
    mpsc::Receiver<Outbound>,
    mpsc::Sender<AdmittedInbound>,
    oneshot::Sender<std::io::Result<()>>,
);

pub(super) type SocketLeases = (
    Vec<UdpSocket>,
    super::candidates::Candidates,
    Arc<Mutex<Option<SocketAddr>>>,
);

pub(super) async fn run(
    config: PunchConfig,
    sockets: SocketLeases,
    seed: (Session, Session),
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    channels: Channels,
    sessions: SessionRegistry,
    cancel: CancellationToken,
) {
    // Cancellation wraps the entire driver, including any pending send_to.
    tokio::select! {
        biased;
        () = cancel.cancelled() => {}
        () = drive(config, sockets, seed, peers.clone(), channels, sessions) => {}
    }
    cancel.cancel();
    lock(&peers).clear();
}

async fn drive(
    config: PunchConfig,
    sockets: SocketLeases,
    seed: (Session, Session),
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    channels: Channels,
    sessions: SessionRegistry,
) {
    let (mut outgoing, incoming, ready) = channels;
    let (mut sockets, local_candidates, observed) = sockets;
    let socket = sockets.remove(0);
    let queries = config
        .peers
        .iter()
        .map(|node| (node.as_str().to_owned(), 0))
        .collect();
    let mut endpoint = Endpoint {
        config,
        socket,
        extra_sockets: sockets,
        local_candidates,
        observed,
        announcement: 0,
        session: seed.0,
        nonce: seed.1,
        sequence: 0,
        hello: 0,
        registration: 0,
        queries,
        discovery: 0,
        heartbeat: 0,
        registered: None,
        sessions,
        ready: Some(ready),
        peers,
        incoming,
    };
    let mut interval = tokio::time::interval(HEARTBEAT);
    interval.set_missed_tick_behavior(MissedTickBehavior::Skip);
    // One extra byte detects oversize/truncation instead of accepting a prefix.
    let mut buffer = [0; MAX_PACKET + 1];
    let mut first_socket = 0;
    loop {
        tokio::select! {
            received = candidates::receive(&endpoint.socket, &endpoint.extra_sockets, &mut buffer, first_socket) => {
                match received {
                    Ok((length, address, socket)) => {
                        first_socket = (socket + 1) % (1 + endpoint.extra_sockets.len());
                        if let Some(packet) = wire::decode_mode(&buffer[..length], endpoint.config.key.as_ref()) {
                            endpoint.receive_on(packet, address, socket);
                        }
                    }
                    Err(error) if transient(&error) => {}
                    Err(_) => break,
                }
            }
            message = outgoing.recv() => {
                if let Some(message) = message { endpoint.send_message(&message); }
                else { break; }
            }
            _ = interval.tick() => endpoint.maintain(),
        }
        if endpoint.sequence == u64::MAX {
            break;
        }
    }
}

impl Endpoint {
    fn transmit(&mut self, address: SocketAddr, body: Body<'_>) -> u64 {
        self.transmit_on(0, address, body)
    }

    fn transmit_on(&mut self, socket: usize, address: SocketAddr, body: Body<'_>) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.emit(socket, address, body);
        self.sequence
    }

    fn emit(&self, socket: usize, address: SocketAddr, body: Body<'_>) {
        let mut buffer = [0; MAX_PACKET];
        let packet = wire::sign_check(Packet {
            sender: self.config.local.as_str(),
            session: self.session,
            sequence: self.sequence,
            body,
        });
        if let Some(length) = wire::encode_mode(packet, self.config.key.as_ref(), &mut buffer) {
            let socket = if socket == 0 {
                Some(&self.socket)
            } else {
                self.extra_sockets.get(socket - 1)
            };
            if let Some(socket) = socket {
                // Never let one unreachable/would-block pair stall relay or other checks.
                // Borrow the nonblocking socket directly: cached Tokio writable
                // readiness may not yet exist for a newly bound candidate.
                let _ = socket2::SockRef::from(socket).send_to(&buffer[..length], &address.into());
            }
        }
    }

    fn maintain(&mut self) {
        let now = Instant::now();
        lock(&self.peers).retain(|_, peer| {
            peer.path(now).is_some()
                && peer.lease.is_active()
                && (!self.config.dynamic || now.duration_since(peer.offered) < super::LEASE)
        });
        if self
            .registered
            .is_none_or(|seen| now.duration_since(seen) >= super::LEASE)
        {
            if self.config.dynamic || self.config.key.is_none() {
                lock(&self.peers).clear();
            }
            *lock(&self.observed) = None;
            self.hello = self.transmit(self.config.rendezvous, Body::Hello { nonce: self.nonce });
        } else {
            self.heartbeat = self.transmit(
                self.config.rendezvous,
                Body::Heartbeat { proof: self.nonce },
            );
            if self.config.dynamic {
                self.discovery =
                    self.transmit(self.config.rendezvous, Body::Discover { proof: self.nonce });
            }
            if self.config.policy != PathPolicy::RelayOnly {
                self.sequence = self.sequence.saturating_add(1);
                self.announcement = self.sequence;
                self.emit(
                    0,
                    self.config.rendezvous,
                    Body::Candidates {
                        proof: self.nonce,
                        candidates: (&self.local_candidates).into(),
                    },
                );
            }
        }
        // NodeId clones are Arc clones. Do not hold a path-state lock over I/O.
        for index in 0..self.config.peers.len() {
            let node = self.config.peers[index].clone();
            let sequence = self.transmit(
                self.config.rendezvous,
                Body::Query {
                    proof: self.nonce,
                    peer: node.as_str(),
                },
            );
            if let Some(query) = self.queries.get_mut(node.as_str()) {
                *query = sequence;
            }
            self.probe(&node);
        }
        if self.config.dynamic && self.config.policy != PathPolicy::RelayOnly {
            let nodes: Vec<_> = lock(&self.peers)
                .values()
                .map(|peer| peer.node.clone())
                .collect();
            for node in nodes {
                self.probe(&node);
            }
        }
    }

    fn probe(&mut self, node: &NodeId) {
        let mut peers = lock(&self.peers);
        let Some(peer) = peers.get_mut(node.as_str()) else {
            return;
        };
        let now = Instant::now();
        if self.config.policy == PathPolicy::RelayOnly
            || peer.relay_only
            || peer.path(now).is_none()
            || !peer.lease.is_active()
        {
            return;
        }
        self.rotate_check(peer, now);
        // A live selected pair is sticky. Independent alternatives stay checked
        // so expiry immediately has another validated path or relay available.
        if peer
            .selected
            .is_none_or(|index| peer.checks[index].direct.is_none_or(|cap| !cap.live(now)))
        {
            peer.selected = peer
                .checks
                .iter()
                .position(|check| check.direct.is_some_and(|cap| cap.live(now)));
        }
        let target = peer.session;
        let secret = peer.secret;
        let mut probes = [None; 32];
        for (slot, check) in probes.iter_mut().zip(&mut peer.checks) {
            if now < check.next_probe || check.probe.is_some_and(|probe| probe.live(now)) {
                continue;
            }
            if check.attempts >= 3 {
                check.attempts = 0;
                check.next_probe = now + std::time::Duration::from_secs(10);
                continue;
            }
            let Ok(nonce) = random() else {
                continue;
            };
            check.probe = Some(Capability {
                token: nonce,
                issued: now,
            });
            check.attempts += 1;
            check.next_probe = now + HEARTBEAT;
            *slot = Some((check.socket, check.address, nonce));
        }
        drop(peers);
        for (socket, address, nonce) in probes.into_iter().flatten() {
            self.transmit_on(
                socket,
                address,
                Body::Probe {
                    target,
                    nonce,
                    secret,
                },
            );
        }
    }

    fn send_message(&mut self, message: &Outbound) {
        let route = {
            let peers = lock(&self.peers);
            peers
                .get(message.to.as_str())
                .filter(|peer| {
                    peer.lease.id() == message.session
                        && peer.lease.is_active()
                        && peer.session == message.target
                })
                .and_then(|peer| {
                    Some((
                        peer.path(Instant::now())?,
                        peer.direct_path(Instant::now())
                            .map(|check| (check.socket, check.address, check.direct)),
                        message.target,
                        peer.secret,
                    ))
                })
        };
        let Some((path, direct, target, secret)) = route else {
            return;
        };
        if path == super::PeerPath::Direct
            && let Some((socket, address, Some(capability))) = direct
        {
            self.transmit_on(
                socket,
                address,
                Body::Direct {
                    peer: message.to.as_str(),
                    target,
                    capability: capability.token,
                    secret,
                    message: &message.message,
                },
            );
        } else {
            self.transmit(
                self.config.rendezvous,
                Body::Relay {
                    proof: self.nonce,
                    peer: message.to.as_str(),
                    target,
                    message: &message.message,
                },
            );
        }
    }

    fn accept_offer(
        &mut self,
        packet: Packet<'_>,
        peer: &str,
        session: Session,
        address: Option<SocketAddr>,
        relay_only: bool,
        introduction: (wire::CandidateList<'_>, Session),
    ) {
        let (candidates, secret) = introduction;
        if peer == self.config.local.as_str()
            || relay_only != address.is_none()
            || (self.config.policy == PathPolicy::RelayOnly && !relay_only)
            || self
                .registered
                .is_none_or(|seen| seen.elapsed() >= super::LEASE)
            || if self.config.dynamic {
                packet.sequence != self.discovery || self.discovery == 0
            } else {
                self.queries.get(peer).copied() != Some(packet.sequence)
            }
        {
            return;
        }
        let mut addresses = super::candidates::PeerCandidates::default();
        if let Some(address) = address
            && !addresses.insert(address)
        {
            return;
        }
        for candidate in candidates.iter() {
            addresses.insert(candidate);
        }
        if relay_only && !addresses.is_empty() {
            return;
        }
        if !self.config.dynamic
            && let Some(query) = self.queries.get_mut(peer)
        {
            *query = 0;
        }
        let mut peers = lock(&self.peers);
        peers.retain(|_, peer| peer.lease.is_active() && peer.path(Instant::now()).is_some());
        if !peers.contains_key(peer) && peers.len() >= self.config.max_peers {
            return;
        }
        if let Some(previous) = peers.get_mut(peer) {
            if previous.session == session
                && previous.secret == secret
                && previous.relay_only == relay_only
            {
                if previous.addresses != addresses {
                    let mut checks = self.make_checks(addresses);
                    for check in &mut checks {
                        if let Some(index) = previous.checks.iter().position(|old| {
                            old.address == check.address && old.socket == check.socket
                        }) {
                            std::mem::swap(check, &mut previous.checks[index]);
                        }
                    }
                    let selected = previous
                        .selected
                        .and_then(|index| previous.checks.get(index))
                        .map(|check| (check.address, check.socket));
                    previous.selected = selected.and_then(|(address, socket)| {
                        checks
                            .iter()
                            .position(|check| check.address == address && check.socket == socket)
                    });
                    previous.checks = checks;
                    previous.addresses = addresses;
                }
                previous.offered = Instant::now();
                return;
            }
            previous.lease.revoke();
        }
        let node = if self.config.dynamic {
            NodeId::from(peer)
        } else if let Some(node) = self.config.peers.iter().find(|node| node.as_str() == peer) {
            node.clone()
        } else {
            return;
        };
        let Ok(lease) = self.sessions.try_admit(AcceptedPeer { node: node.clone() }) else {
            return;
        };
        peers.insert(
            peer.to_owned(),
            Peer {
                node,
                session,
                addresses,
                candidate_cursor: 32,
                secret,
                relay_only,
                offered: Instant::now(),
                checks: self.make_checks(addresses),
                selected: None,
                sequence: 0,
                lease,
            },
        );
    }

    fn receive_rendezvous(&mut self, packet: Packet<'_>, address: SocketAddr) {
        if packet.sender != self.config.local.as_str() || packet.session != self.session {
            return;
        }
        match packet.body {
            Body::Challenge { nonce, cookie }
                if nonce == self.nonce && self.hello != 0 && packet.sequence == self.hello =>
            {
                self.registered = None;
                self.hello = 0;
                let credential = std::mem::take(&mut self.config.credential);
                self.registration = self.transmit(
                    address,
                    Body::Register {
                        nonce,
                        cookie,
                        relay_only: self.config.policy == PathPolicy::RelayOnly,
                        credential: &credential,
                    },
                );
                self.config.credential = credential;
            }
            Body::Registered { proof }
                if proof == self.nonce
                    && ((self.registration != 0 && packet.sequence == self.registration)
                        || (self.heartbeat != 0 && packet.sequence == self.heartbeat)) =>
            {
                self.heartbeat = 0;
                self.registration = 0;
                self.registered = Some(Instant::now());
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(Ok(()));
                }
            }
            Body::Denied { proof }
                if proof == self.nonce
                    && self.registration != 0
                    && packet.sequence == self.registration =>
            {
                self.registration = 0;
                if let Some(ready) = self.ready.take() {
                    let _ = ready.send(Err(std::io::Error::new(
                        std::io::ErrorKind::PermissionDenied,
                        "rendezvous admission denied",
                    )));
                }
            }
            Body::Offer {
                proof,
                peer,
                session,
                address,
                relay_only,
                candidates,
                secret,
            } if proof == self.nonce => {
                self.accept_offer(
                    packet,
                    peer,
                    session,
                    address,
                    relay_only,
                    (candidates, secret),
                );
            }
            Body::Delivered { proof, .. }
                if proof == self.nonce
                    && self
                        .registered
                        .is_some_and(|seen| seen.elapsed() < super::LEASE) =>
            {
                self.deliver_relay(&packet);
            }
            Body::Observed { proof, address }
                if proof == self.nonce
                    && packet.sequence == self.announcement
                    && self.announcement != 0
                    && self.config.policy != PathPolicy::RelayOnly =>
            {
                *lock(&self.observed) = Some(address);
                self.announcement = 0;
            }
            _ => {}
        }
    }

    fn deliver_relay(&self, packet: &Packet<'_>) {
        let Body::Delivered {
            peer,
            session,
            message,
            ..
        } = packet.body
        else {
            return;
        };
        let mut peers = lock(&self.peers);
        let Some(peer) = peers.get_mut(peer) else {
            return;
        };
        if peer.session != session
            || peer.path(Instant::now()).is_none()
            || packet.sequence <= peer.sequence
            || !peer.lease.is_active()
        {
            return;
        }
        peer.sequence = packet.sequence;
        if let Ok(permit) = self.incoming.try_reserve() {
            permit.send(AdmittedInbound {
                packet: Inbound {
                    from: peer.node.clone(),
                    msg: Bytes::copy_from_slice(message),
                },
                session: Some(peer.lease.id()),
            });
        }
    }

    #[cfg(test)]
    fn receive(&mut self, packet: Packet<'_>, address: SocketAddr) {
        self.receive_on(packet, address, 0);
    }

    fn receive_on(&mut self, packet: Packet<'_>, address: SocketAddr, socket: usize) {
        if socket == 0 && address == self.config.rendezvous {
            self.receive_rendezvous(packet, address);
            return;
        }
        self.receive_direct(packet, address, socket);
    }
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod candidate_review_tests;

#[cfg(test)]
mod candidate_state_tests;
