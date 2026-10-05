//! One bounded socket-owning endpoint task.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use groupnet_transport::Inbound;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};
use tokio_util::sync::CancellationToken;

use super::wire::{self, Body, MAX_PACKET, Packet, Session};
use super::{HEARTBEAT, Outbound, PathPolicy, Peer, PunchConfig, lock, random, transient};

struct Endpoint {
    config: PunchConfig,
    socket: UdpSocket,
    session: Session,
    nonce: Session,
    sequence: u64,
    hello: u64,
    registration: u64,
    queries: HashMap<String, u64>,
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    incoming: mpsc::Sender<Inbound>,
}

pub(super) async fn run(
    config: PunchConfig,
    socket: UdpSocket,
    seed: (Session, Session),
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    outgoing: mpsc::Receiver<Outbound>,
    incoming: mpsc::Sender<Inbound>,
    cancel: CancellationToken,
) {
    // Cancellation wraps the entire driver, including any pending send_to.
    tokio::select! {
        biased;
        () = cancel.cancelled() => {}
        () = drive(config, socket, seed, peers, outgoing, incoming) => {}
    }
    cancel.cancel();
}

async fn drive(
    config: PunchConfig,
    socket: UdpSocket,
    seed: (Session, Session),
    peers: Arc<Mutex<HashMap<String, Peer>>>,
    mut outgoing: mpsc::Receiver<Outbound>,
    incoming: mpsc::Sender<Inbound>,
) {
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
                        if let Some(packet) = wire::decode(&buffer[..length], &endpoint.config.key) {
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
        if let Some(length) = wire::encode(packet, &self.config.key, &mut buffer) {
            let _ = self.socket.send_to(&buffer[..length], address).await;
        }
        self.sequence
    }

    async fn maintain(&mut self) {
        self.hello = self
            .transmit(self.config.rendezvous, Body::Hello { nonce: self.nonce })
            .await;
        // NodeId clones are Arc clones. Do not hold a path-state lock over I/O.
        for index in 0..self.config.peers.len() {
            let node = self.config.peers[index].clone();
            let sequence = self
                .transmit(
                    self.config.rendezvous,
                    Body::Query {
                        peer: node.as_str(),
                    },
                )
                .await;
            if let Some(query) = self.queries.get_mut(node.as_str()) {
                *query = sequence;
            }
            let probe = {
                let mut peers = lock(&self.peers);
                peers.get_mut(node.as_str()).and_then(|peer| {
                    if self.config.policy == PathPolicy::RelayOnly
                        || peer.relay_only
                        || peer.path(Instant::now()).is_none()
                    {
                        return None;
                    }
                    let nonce = random().ok()?;
                    peer.probe = Some(nonce);
                    Some((peer.address, peer.session, nonce))
                })
            };
            if let Some((address, target, nonce)) = probe {
                self.transmit(address, Body::Probe { target, nonce }).await;
            }
        }
    }

    async fn send_message(&mut self, message: &Outbound) {
        let route = {
            let peers = lock(&self.peers);
            peers
                .get(message.to.as_str())
                .and_then(|peer| Some((peer.path(Instant::now())?, peer.address, peer.session)))
        };
        let Some((path, address, target)) = route else {
            return;
        };
        if path == super::PeerPath::Direct {
            self.transmit(
                address,
                Body::Direct {
                    peer: message.to.as_str(),
                    target,
                    message: &message.message,
                },
            )
            .await;
        } else {
            self.transmit(
                self.config.rendezvous,
                Body::Relay {
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
        address: SocketAddr,
        relay_only: bool,
    ) {
        if self.queries.get(peer).copied() != Some(packet.sequence) {
            return;
        }
        if let Some(query) = self.queries.get_mut(peer) {
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
            *previous = Peer {
                node: previous.node.clone(),
                session,
                address,
                relay_only,
                offered: Instant::now(),
                direct: None,
                probe: None,
                sequence: 0,
            };
            return;
        }
        let Some(node) = self
            .config
            .peers
            .iter()
            .find(|node| node.as_str() == peer)
            .cloned()
        else {
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
                sequence: 0,
            },
        );
    }

    async fn receive_rendezvous(&mut self, packet: Packet<'_>, address: SocketAddr) {
        if packet.sender != self.config.local.as_str() || packet.session != self.session {
            return;
        }
        match packet.body {
            Body::Challenge { nonce, cookie }
                if nonce == self.nonce && packet.sequence == self.hello =>
            {
                self.hello = 0;
                self.registration = self
                    .transmit(
                        address,
                        Body::Register {
                            nonce,
                            cookie,
                            relay_only: self.config.policy == PathPolicy::RelayOnly,
                        },
                    )
                    .await;
            }
            Body::Registered if packet.sequence == self.registration => {
                self.registration = 0;
            }
            Body::Offer {
                peer,
                session,
                address,
                relay_only,
            } => {
                self.accept_offer(packet, peer, session, address, relay_only);
            }
            Body::Delivered {
                peer,
                session,
                message,
            } => {
                let mut peers = lock(&self.peers);
                let Some(peer) = peers.get_mut(peer) else {
                    return;
                };
                if peer.session != session
                    || peer.path(Instant::now()).is_none()
                    || packet.sequence <= peer.sequence
                {
                    return;
                }
                peer.sequence = packet.sequence;
                if let Ok(permit) = self.incoming.try_reserve() {
                    permit.send(Inbound {
                        from: peer.node.clone(),
                        msg: message.to_vec(),
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
        if self.config.policy == PathPolicy::RelayOnly {
            return;
        }
        let response = {
            let mut peers = lock(&self.peers);
            let Some(peer) = peers.get_mut(packet.sender) else {
                return;
            };
            if peer.relay_only
                || peer.address != address
                || peer.session != packet.session
                || peer.path(Instant::now()).is_none()
                || packet.sequence <= peer.sequence
            {
                return;
            }
            match packet.body {
                Body::Probe { target, nonce } if target == self.session => {
                    peer.sequence = packet.sequence;
                    Some((peer.session, nonce))
                }
                Body::ProbeAck { target, nonce }
                    if target == self.session && peer.probe == Some(nonce) =>
                {
                    peer.sequence = packet.sequence;
                    peer.probe = None;
                    peer.direct = Some(Instant::now());
                    None
                }
                Body::Direct {
                    peer: target_name,
                    target,
                    message,
                } if target_name == self.config.local.as_str() && target == self.session => {
                    peer.sequence = packet.sequence;
                    if let Ok(permit) = self.incoming.try_reserve() {
                        permit.send(Inbound {
                            from: peer.node.clone(),
                            msg: message.to_vec(),
                        });
                    }
                    None
                }
                _ => None,
            }
        };
        if let Some((target, nonce)) = response {
            self.transmit(address, Body::ProbeAck { target, nonce })
                .await;
        }
    }
}
