//! One bounded socket-owning endpoint task.

mod direct;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

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

pub(super) async fn run(
    config: PunchConfig,
    socket: UdpSocket,
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
        () = drive(config, socket, seed, peers.clone(), channels, sessions) => {}
    }
    cancel.cancel();
    lock(&peers).clear();
}

async fn drive(
    config: PunchConfig,
    socket: UdpSocket,
    seed: (Session, Session),
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    channels: Channels,
    sessions: SessionRegistry,
) {
    let (mut outgoing, incoming, ready) = channels;
    let queries = config
        .peers
        .iter()
        .map(|node| (node.as_str().to_owned(), 0))
        .collect();
    let mut endpoint = Endpoint {
        config,
        socket,
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
    loop {
        tokio::select! {
            received = endpoint.socket.recv_from(&mut buffer) => {
                match received {
                    Ok((length, address)) => {
                        if let Some(packet) = wire::decode_mode(&buffer[..length], endpoint.config.key.as_ref()) {
                            endpoint.receive(packet, address).await;
                        }
                    }
                    Err(error) if transient(&error) => {}
                    Err(_) => break,
                }
            }
            message = outgoing.recv() => {
                if let Some(message) = message { endpoint.send_message(&message).await; }
                else { break; }
            }
            _ = interval.tick() => endpoint.maintain().await,
        }
        if endpoint.sequence == u64::MAX {
            break;
        }
    }
}

impl Endpoint {
    async fn transmit(&mut self, address: SocketAddr, body: Body<'_>) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        let mut buffer = [0; MAX_PACKET];
        let packet = Packet {
            sender: self.config.local.as_str(),
            session: self.session,
            sequence: self.sequence,
            body,
        };
        if let Some(length) = wire::encode_mode(packet, self.config.key.as_ref(), &mut buffer) {
            let _ = self.socket.send_to(&buffer[..length], address).await;
        }
        self.sequence
    }

    async fn maintain(&mut self) {
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
            self.hello = self
                .transmit(self.config.rendezvous, Body::Hello { nonce: self.nonce })
                .await;
        } else {
            self.heartbeat = self
                .transmit(
                    self.config.rendezvous,
                    Body::Heartbeat { proof: self.nonce },
                )
                .await;
            if self.config.dynamic {
                self.discovery = self
                    .transmit(self.config.rendezvous, Body::Discover { proof: self.nonce })
                    .await;
            }
        }
        // NodeId clones are Arc clones. Do not hold a path-state lock over I/O.
        for index in 0..self.config.peers.len() {
            let node = self.config.peers[index].clone();
            let sequence = self
                .transmit(
                    self.config.rendezvous,
                    Body::Query {
                        proof: self.nonce,
                        peer: node.as_str(),
                    },
                )
                .await;
            if let Some(query) = self.queries.get_mut(node.as_str()) {
                *query = sequence;
            }
            self.probe(&node).await;
        }
        if self.config.dynamic && self.config.policy != PathPolicy::RelayOnly {
            let nodes: Vec<_> = lock(&self.peers)
                .values()
                .map(|peer| peer.node.clone())
                .collect();
            for node in nodes {
                self.probe(&node).await;
            }
        }
    }

    async fn probe(&mut self, node: &NodeId) {
        let probe = {
            let mut peers = lock(&self.peers);
            peers.get_mut(node.as_str()).and_then(|peer| {
                if self.config.policy == PathPolicy::RelayOnly
                    || peer.relay_only
                    || peer.path(Instant::now()).is_none()
                    || !peer.lease.is_active()
                {
                    return None;
                }
                let nonce = random().ok()?;
                let address = peer.address?;
                peer.probe = Some(Capability {
                    token: nonce,
                    issued: Instant::now(),
                });
                Some((address, peer.session, nonce))
            })
        };
        if let Some((address, target, nonce)) = probe {
            self.transmit(address, Body::Probe { target, nonce }).await;
        }
    }

    async fn send_message(&mut self, message: &Outbound) {
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
                        peer.address,
                        message.target,
                        peer.direct,
                    ))
                })
        };
        let Some((path, address, target, capability)) = route else {
            return;
        };
        if path == super::PeerPath::Direct
            && let (Some(address), Some(capability)) = (address, capability)
        {
            self.transmit(
                address,
                Body::Direct {
                    peer: message.to.as_str(),
                    target,
                    capability: capability.token,
                    message: &message.message,
                },
            )
            .await;
        } else {
            self.transmit(
                self.config.rendezvous,
                Body::Relay {
                    proof: self.nonce,
                    peer: message.to.as_str(),
                    target,
                    message: &message.message,
                },
            )
            .await;
        }
    }

    fn accept_offer(
        &mut self,
        packet: Packet<'_>,
        peer: &str,
        session: Session,
        address: Option<SocketAddr>,
        relay_only: bool,
    ) {
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
        if !self.config.dynamic
            && let Some(query) = self.queries.get_mut(peer)
        {
            *query = 0;
        }
        let mut peers = lock(&self.peers);
        if let Some(previous) = peers.get_mut(peer) {
            if previous.session == session
                && previous.address == address
                && previous.relay_only == relay_only
            {
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
                address,
                relay_only,
                offered: Instant::now(),
                direct: None,
                probe: None,
                pending: None,
                confirmed: None,
                confirmed_probe: 0,
                sequence: 0,
                lease,
            },
        );
    }

    async fn receive_rendezvous(&mut self, packet: Packet<'_>, address: SocketAddr) {
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
                self.registration = self
                    .transmit(
                        address,
                        Body::Register {
                            nonce,
                            cookie,
                            relay_only: self.config.policy == PathPolicy::RelayOnly,
                            credential: &credential,
                        },
                    )
                    .await;
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
            } if proof == self.nonce => {
                self.accept_offer(packet, peer, session, address, relay_only);
            }
            Body::Delivered {
                proof,
                peer,
                session,
                message,
            } if proof == self.nonce
                && self
                    .registered
                    .is_some_and(|seen| seen.elapsed() < super::LEASE) =>
            {
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
                            msg: message.to_vec(),
                        },
                        session: Some(peer.lease.id()),
                    });
                }
            }
            _ => {}
        }
    }

    async fn receive(&mut self, packet: Packet<'_>, address: SocketAddr) {
        if address == self.config.rendezvous {
            self.receive_rendezvous(packet, address).await;
            return;
        }
        self.receive_direct(packet, address).await;
    }
}

#[cfg(test)]
mod tests;
