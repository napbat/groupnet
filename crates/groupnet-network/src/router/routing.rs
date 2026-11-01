//! Path-vector learning, bounded replay suppression, and packet forwarding.

use std::io;
use std::sync::{Arc, atomic::Ordering};

use bytes::{Buf, Bytes};
use groupnet_core::NodeId;
use groupnet_transport::admission::SessionId;
use groupnet_transport::link::{AdmittedInbound, LinkControl};
use groupnet_transport::{Inbound, MAX_NODE_ID_BYTES, validate_node_id};
use tokio::sync::mpsc;
use tokio::time::{Instant, MissedTickBehavior};

use super::{
    AdvertisedRoute, ApplicationPacket, Candidate, Event, PacketBuffer, Queued, Route,
    RoutedSendError, RouterConfig, Shared, Table, TransportId, closed, freshness::AdvertFreshness,
    replay::ReplayWindows, seal::Sealing,
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
        validate_node_id(to, MAX_NODE_ID_BYTES).map_err(RoutedSendError::Destination)?;
        let max = self
            .config
            .max_frame
            .get()
            .saturating_sub(kind.header_len(&self.local, to));
        if payload.len() > max {
            return Err(RoutedSendError::PayloadTooLarge {
                len: payload.len(),
                max,
            }
            .into());
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
                let _ = self.messages.try_send(Inbound {
                    from,
                    msg: payload,
                    source: None,
                });
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
            frame: self.advert(0, std::slice::from_ref(&self.local)),
        }];
        if self.config.forwarding {
            for route in table
                .candidates
                .keys()
                .filter_map(|id| table.route(id, self.config.route_ttl))
            {
                if route.path.len() < usize::from(self.config.max_hops.get())
                    && wire::ADVERT_HEADER
                        + route
                            .path
                            .iter()
                            .map(|node| 1 + node.as_str().len())
                            .sum::<usize>()
                        <= self.config.max_frame.get()
                {
                    routes.push(AdvertisedRoute {
                        path: route.path.clone(),
                        frame: self.advert(route.cost, &route.path),
                    });
                }
            }
        }
        self.advertisements.send_replace(Arc::new(routes));
    }

    /// One advert envelope under this boot's next advert identity, so
    /// receivers can refuse it once a newer one arrived; sealed when this
    /// router seals. Adverts draw from their own counter, never the data one.
    fn advert(&self, cost: u32, path: &[NodeId]) -> Bytes {
        let id = wire::AdvertId {
            epoch: self.boot_epoch,
            nonce: self.nonce,
            sequence: self.advert_sequence.fetch_add(1, Ordering::Relaxed),
        };
        let mut frame = wire::advert(id, cost, path);
        self.sealing.seal(&self.local, &mut frame);
        frame.into()
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

/// Reassembly, replay-suppression and advert-freshness state shared by every
/// link's inbound path. Each link worker processes its own frames inline; the
/// lock covers only reassembly, decoding and the replay and freshness checks,
/// never delivery, forwarding or address confirmation.
pub(super) struct InboundState {
    fragments: Reassembly,
    replay: ReplayWindows,
    adverts: AdvertFreshness,
}

impl InboundState {
    pub(super) fn new(config: &RouterConfig, sealing: &Sealing) -> Self {
        Self {
            fragments: Reassembly::new(
                config.reassembly.clone(),
                config.max_frame.get() + sealing.overhead(),
                sealing.magic(),
                sealing.fragments().magic,
            ),
            replay: ReplayWindows::new(config.replay_capacity.get()),
            adverts: AdvertFreshness::new(
                config
                    .max_routes
                    .get()
                    .saturating_mul(config.max_transports.get()),
            ),
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
    /// The link's cost for an admitted frame, and its control when the frame
    /// carries a source address to confirm once it authenticates.
    fn admitted(
        &self,
        link: TransportId,
        admitted: &AdmittedInbound,
    ) -> Option<(u32, Option<LinkControl>)> {
        if admitted.packet.from == self.local {
            return None;
        }
        let table = self.table.lock().expect("router table poisoned");
        let adapter = table.links.get(&link)?;
        adapter
            .admits(&admitted.packet.from, admitted.session)
            .then(|| {
                let control = admitted.packet.source.map(|_| adapter.control.clone());
                (adapter.config.cost.get(), control)
            })
    }

    /// Processes one admitted physical frame on the link worker that read it:
    /// fragment tag, reassembly, seal verification, replay suppression and
    /// advert freshness, address confirmation, then route learning, local
    /// delivery or forwarding. Every hand-off is a bounded `try_send`, so
    /// this never waits.
    ///
    /// The link's address book changes only here, through
    /// [`LinkControl::confirm_peer`], and only for a frame that passed every
    /// check before it: a datagram that claims a neighbor's id without its
    /// seal, or replays one of its frames, never moves that neighbor's
    /// address.
    pub(super) fn receive(&self, link: TransportId, admitted: AdmittedInbound) {
        let Some((cost, control)) = self.admitted(link, &admitted) else {
            return;
        };
        let AdmittedInbound { packet, session } = admitted;
        let Inbound { from, msg, source } = packet;
        if self.sealing.unsealed(&msg) {
            self.auth_failures.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Some(msg) = self.sealing.open_fragment(&from, msg) else {
            self.auth_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let mut inbound = self.inbound.lock().expect("router inbound state poisoned");
        let Some(bytes) = inbound.fragments.receive(link.0, session, &from, msg) else {
            return;
        };
        let Some(length) = self.sealing.open(&from, &bytes) else {
            drop(inbound);
            self.auth_failures.fetch_add(1, Ordering::Relaxed);
            return;
        };
        let Ok(frame) = wire::decode_bounded(
            &bytes[..length],
            self.sealing.magic(),
            self.config.max_frame.get(),
            usize::from(self.config.max_hops.get()),
        ) else {
            return;
        };
        let fresh = match &frame {
            Frame::Data { from, id, .. } => inbound.replay.first_sighting(from, *id),
            Frame::Advert { id, .. } => inbound.adverts.accept(link.0, &from, *id),
        };
        drop(inbound);
        if !fresh {
            return;
        }
        if let (Some(control), Some(source)) = (control, source) {
            control.confirm_peer(&from, source);
        }
        match frame {
            Frame::Advert {
                id: _,
                cost: advertised,
                path,
            } => self.learn(link, from, session, cost, advertised, path),
            Frame::Data {
                kind,
                hops,
                id: _,
                from,
                to,
                payload,
            } => {
                if to == self.local {
                    let offset = length - payload.len();
                    let mut bytes = bytes;
                    bytes.truncate(length);
                    bytes.advance(offset);
                    let _ = self.deliver_owned(kind, from, bytes);
                } else if self.config.forwarding && hops > 1 {
                    let mut forwarded = bytes
                        .try_into_mut()
                        .unwrap_or_else(|shared| bytes::BytesMut::from(shared.as_ref()));
                    wire::decrement_hops(&mut forwarded, kind);
                    self.sealing.reseal(&self.local, &mut forwarded);
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
            || path.len() >= usize::from(self.config.max_hops.get())
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
            && table.candidates.len() >= self.config.max_routes.get()
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::router::Router;

    /// Adverts draw from their own counter, so announcing any number of
    /// routes never moves the data sequence a receiver's replay window
    /// tracks.
    #[tokio::test]
    async fn adverts_leave_the_data_sequence_alone() -> io::Result<()> {
        let router = Router::new(NodeId::new("local"))?;
        let shared = &router.inner.shared;
        let path = [NodeId::new("local")];
        let before = shared.sequence.load(Ordering::Relaxed);
        let ids: Vec<_> = (0..3)
            .map(|_| match wire::decode(&shared.advert(0, &path)) {
                Ok(Frame::Advert { id, .. }) => id,
                _ => panic!("an advert"),
            })
            .collect();
        assert_eq!(shared.sequence.load(Ordering::Relaxed), before);
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "{ids:?}");
        router.close().await;
        Ok(())
    }
}
