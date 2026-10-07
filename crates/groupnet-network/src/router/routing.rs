//! Path-vector learning, bounded replay suppression, and packet forwarding.

use std::io;
use std::sync::{Arc, atomic::Ordering};

use bytes::{Buf, Bytes};
use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use groupnet_transport::admission::SessionId;
use groupnet_transport::link::AdmittedInbound;
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

use super::{
    AdvertisedRoute, ApplicationPacket, Candidate, Event, PacketBuffer, Queued, Route,
    RouterConfig, Shared, Table, TransportId, closed, replay::ReplayWindows,
};
use crate::wire::{self, Frame, PayloadKind, Reassembly};

impl Shared {
    pub(super) fn id(&self) -> [u8; 16] {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.nonce);
        id[8..].copy_from_slice(&self.sequence.fetch_add(1, Ordering::Relaxed).to_be_bytes());
        id
    }

    pub(super) fn send(&self, to: &NodeId, payload: &[u8], kind: PayloadKind) -> io::Result<()> {
        let mut packet = PacketBuffer::new(self, to, payload.len(), kind)?;
        packet.extend_from_slice(payload);
        self.send_packet(to, packet, kind)
    }

    pub(super) fn send_owned(
        &self,
        to: &NodeId,
        payload: Bytes,
        kind: PayloadKind,
    ) -> io::Result<()> {
        if self.cancel.is_cancelled() {
            return Err(closed());
        }
        if !wire::id_valid(to)
            || payload.len()
                > self
                    .config
                    .max_frame
                    .saturating_sub(kind.header_len(&self.local, to))
        {
            return Err(wire::invalid("routed message exceeds bound"));
        }
        if to == &self.local {
            return self.deliver_owned(kind, self.local.clone(), payload);
        }
        let mut packet = PacketBuffer::new(self, to, payload.len(), kind)?;
        packet.extend_from_slice(&payload);
        self.send_packet(to, packet, kind)
    }

    pub(super) fn send_packet(
        &self,
        to: &NodeId,
        packet: PacketBuffer,
        kind: PayloadKind,
    ) -> io::Result<()> {
        if self.cancel.is_cancelled() {
            return Err(closed());
        }
        let (bytes, offset) = packet.finish(self, to, kind)?;
        if to == &self.local {
            self.deliver_owned(kind, self.local.clone(), bytes.slice(offset..))
        } else if matches!(kind, PayloadKind::Application(_)) {
            self.forward_application(to, bytes)
        } else {
            self.forward(to, bytes);
            Ok(())
        }
    }

    pub(super) fn deliver_owned(
        &self,
        kind: PayloadKind,
        from: NodeId,
        payload: Bytes,
    ) -> io::Result<()> {
        match kind {
            PayloadKind::Application(id) => self
                .protocols
                .lock()
                .map_err(|_| io::Error::other("protocol registry poisoned"))?
                .deliver(id, ApplicationPacket { from, payload }),
            PayloadKind::Message => {
                let _ = self.messages.try_send(Inbound { from, msg: payload });
                Ok(())
            }
            PayloadKind::Tunnel => {
                if let Some(inbox) = self.tunnel.get() {
                    inbox.deliver(from, payload);
                }
                Ok(())
            }
        }
    }

    pub(super) fn forward(&self, to: &NodeId, bytes: Bytes) {
        let table = self.table.lock().expect("router table poisoned");
        if let Some(candidate) = table.candidate(to, self.config.route_ttl)
            && let Some(link) = table.links.get(&candidate.route.transport)
        {
            let _ = link.outgoing.try_send(Queued {
                peer: candidate.route.next_hop.clone(),
                session: candidate.session,
                bytes,
            });
        }
    }

    fn forward_application(&self, to: &NodeId, bytes: Bytes) -> io::Result<()> {
        let table = self
            .table
            .lock()
            .map_err(|_| io::Error::other("router table poisoned"))?;
        let candidate = table
            .candidate(to, self.config.route_ttl)
            .ok_or_else(|| io::Error::new(io::ErrorKind::NotConnected, "no application route"))?;
        let link = table
            .links
            .get(&candidate.route.transport)
            .ok_or_else(closed)?;
        link.outgoing
            .try_send(Queued {
                peer: candidate.route.next_hop.clone(),
                session: candidate.session,
                bytes,
            })
            .map_err(|error| io::Error::new(io::ErrorKind::WouldBlock, error.to_string()))
    }

    fn announce(&self) {
        self.refresh_reachable();
        let mut table = self.table.lock().expect("router table poisoned");
        table.candidates.retain(|_, choices| {
            choices.retain(|_, entry| entry.updated.elapsed() < self.config.route_ttl);
            !choices.is_empty()
        });
        let mut routes = vec![AdvertisedRoute {
            path: vec![self.local.clone()],
            frame: wire::advert(0, std::slice::from_ref(&self.local)).into(),
        }];
        if self.config.forwarding {
            for route in table
                .candidates
                .keys()
                .filter_map(|id| table.route(id, self.config.route_ttl))
            {
                if route.path.len() < self.config.max_hops
                    && 10
                        + route
                            .path
                            .iter()
                            .map(|node| 1 + node.as_str().len())
                            .sum::<usize>()
                        <= self.config.max_frame
                {
                    routes.push(AdvertisedRoute {
                        path: route.path.clone(),
                        frame: wire::advert(route.cost, &route.path).into(),
                    });
                }
            }
        }
        self.advertisements.send_replace(Arc::new(routes));
    }

    pub(super) fn refresh_reachable(&self) {
        let table = self.table.lock().expect("router table poisoned");
        let mut peers: Vec<_> = table
            .candidates
            .keys()
            .filter(|id| table.route(id, self.config.route_ttl).is_some())
            .cloned()
            .collect();
        peers.sort();
        self.reachable.send_if_modified(|current| {
            if current.as_ref() == &peers {
                return false;
            }
            *current = Arc::new(peers);
            true
        });
    }

    fn neighbors_changed(&self, id: TransportId) {
        let mut table = self.table.lock().expect("router table poisoned");
        let Table {
            links, candidates, ..
        } = &mut *table;
        candidates.retain(|_, choices| {
            choices.retain(|(link, peer), candidate| {
                *link != id
                    || links
                        .get(link)
                        .is_some_and(|adapter| adapter.admits(peer, candidate.session))
            });
            !choices.is_empty()
        });
        drop(table);
        self.announce();
    }
}

/// Reassembly and replay-suppression state shared by every link's inbound
/// path. Each link worker processes its own frames inline; the lock covers only
/// reassembly, decoding and the replay check, never delivery or forwarding.
pub(super) struct InboundState {
    fragments: Reassembly,
    replay: ReplayWindows,
}

impl InboundState {
    pub(super) fn new(config: &RouterConfig) -> Self {
        Self {
            fragments: Reassembly::new(config.reassembly.clone(), config.max_frame),
            replay: ReplayWindows::new(config.replay_capacity),
        }
    }
}

/// Owns the announcement clock and link lifecycle events. Data frames never
/// pass through here: link workers process them inline.
pub(super) async fn drive(shared: Arc<Shared>, mut events: mpsc::Receiver<Event>) {
    let mut clock = tokio::time::interval(shared.config.announce_interval);
    clock.set_missed_tick_behavior(MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = shared.cancel.cancelled() => break,
            _ = clock.tick() => shared.announce(),
            event = events.recv() => match event {
                Some(Event::Neighbors(link)) => shared.neighbors_changed(link),
                Some(Event::Down(link)) => { shared.table.lock().expect("router table poisoned").remove(link); shared.announce(); }
                Some(Event::Announce) => shared.announce(),
                None => break,
            },
        }
    }
}

impl Shared {
    fn admitted_cost(&self, link: TransportId, admitted: &AdmittedInbound) -> Option<u32> {
        if admitted.packet.from == self.local {
            return None;
        }
        let table = self.table.lock().expect("router table poisoned");
        let adapter = table.links.get(&link)?;
        adapter
            .admits(&admitted.packet.from, admitted.session)
            .then_some(adapter.config.cost)
    }

    /// Processes one admitted physical frame on the link worker that read it:
    /// reassembly, route learning, replay suppression, then local delivery or
    /// forwarding. Every hand-off is a bounded `try_send`, so this never waits.
    pub(super) fn receive(&self, link: TransportId, admitted: AdmittedInbound) {
        let Some(cost) = self.admitted_cost(link, &admitted) else {
            return;
        };
        let AdmittedInbound { packet, session } = admitted;
        let mut inbound = self.inbound.lock().expect("router inbound state poisoned");
        let Some(bytes) = inbound
            .fragments
            .receive(link.0, session, &packet.from, packet.msg)
        else {
            return;
        };
        let Ok(frame) = wire::decode_bounded(&bytes, self.config.max_frame, self.config.max_hops)
        else {
            return;
        };
        if let Frame::Data { from, id, .. } = &frame
            && !inbound.replay.first_sighting(from, *id)
        {
            return;
        }
        drop(inbound);
        match frame {
            Frame::Advert {
                cost: advertised,
                path,
            } => self.learn(link, packet.from, session, cost, advertised, path),
            Frame::Data {
                kind,
                hops,
                id: _,
                from,
                to,
                payload,
            } => {
                if to == self.local {
                    let offset = bytes.len() - payload.len();
                    let mut bytes = bytes;
                    bytes.advance(offset);
                    let _ = self.deliver_owned(kind, from, bytes);
                } else if self.config.forwarding && hops > 1 {
                    let mut forwarded = bytes
                        .try_into_mut()
                        .unwrap_or_else(|shared| bytes::BytesMut::from(shared.as_ref()));
                    wire::decrement_hops(&mut forwarded, kind);
                    self.forward(&to, forwarded.freeze());
                }
            }
        }
    }

    fn learn(
        &self,
        link: TransportId,
        neighbor: NodeId,
        session: Option<SessionId>,
        cost: u32,
        advertised: u32,
        path: Vec<NodeId>,
    ) {
        if path.first() != Some(&neighbor)
            || path.contains(&self.local)
            || path.len() >= self.config.max_hops
        {
            return;
        }
        let Some(cost) = cost.checked_add(advertised) else {
            return;
        };
        let Some(destination) = path.last().cloned() else {
            return;
        };
        let mut table = self.table.lock().expect("router table poisoned");
        if !table.candidates.contains_key(&destination)
            && table.candidates.len() >= self.config.max_routes
        {
            return;
        }
        let mut full_path = Vec::with_capacity(path.len() + 1);
        full_path.push(self.local.clone());
        full_path.extend(path);
        table
            .candidates
            .entry(destination.clone())
            .or_default()
            .insert(
                (link, neighbor.clone()),
                Candidate {
                    route: Route {
                        destination,
                        next_hop: neighbor,
                        transport: link,
                        cost,
                        path: full_path,
                    },
                    updated: Instant::now(),
                    session,
                },
            );
        drop(table);
        self.refresh_reachable();
    }
}
