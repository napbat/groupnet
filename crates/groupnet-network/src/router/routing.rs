//! Path-vector learning, bounded replay suppression, and packet forwarding.

use std::collections::{HashSet, VecDeque};
use std::io;
use std::sync::{Arc, atomic::Ordering};
use std::time::Instant;

use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use groupnet_transport::link::AdmittedInbound;
use tokio::sync::mpsc;
use tokio::time::MissedTickBehavior;

use super::{AdvertisedRoute, Candidate, Event, Queued, Route, Shared, Table, TransportId, closed};
use crate::wire::{self, Frame, PayloadKind, Reassembly};

impl Shared {
    pub(super) fn id(&self) -> [u8; 16] {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.nonce);
        id[8..].copy_from_slice(&self.sequence.fetch_add(1, Ordering::Relaxed).to_be_bytes());
        id
    }

    pub(super) fn send(&self, to: &NodeId, payload: &[u8], kind: PayloadKind) -> io::Result<()> {
        if self.cancel.is_cancelled() {
            return Err(closed());
        }
        if !wire::id_valid(to)
            || payload.len() + self.local.as_str().len() + to.as_str().len() + 24 > wire::MAX_FRAME
        {
            return Err(wire::invalid("routed message exceeds bound"));
        }
        if to == &self.local {
            let queue = if kind == PayloadKind::Message {
                &self.messages
            } else {
                &self.tunnels
            };
            let _ = queue.try_send(Inbound {
                from: self.local.clone(),
                msg: payload.to_vec(),
            });
        } else {
            self.forward(
                to,
                wire::data(kind, 16, self.id(), &self.local, to, payload).into(),
            );
        }
        Ok(())
    }

    fn forward(&self, to: &NodeId, bytes: Arc<[u8]>) {
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
                if route.path.len() < wire::MAX_HOPS {
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

pub(super) async fn drive(shared: Arc<Shared>, mut events: mpsc::Receiver<Event>) {
    let mut clock = tokio::time::interval(shared.config.announce_interval);
    clock.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut fragments = Reassembly::default();
    let mut seen = HashSet::new();
    let mut order = VecDeque::new();
    loop {
        tokio::select! {
            biased;
            () = shared.cancel.cancelled() => break,
            _ = clock.tick() => shared.announce(),
            event = events.recv() => match event {
                Some(Event::Received { link, packet }) => {
                    process(&shared, link, packet, &mut fragments, &mut seen, &mut order);
                }
                Some(Event::Neighbors(link)) => shared.neighbors_changed(link),
                Some(Event::Down(link)) => { shared.table.lock().expect("router table poisoned").remove(link); shared.announce(); }
                Some(Event::Announce) => shared.announce(),
                None => break,
            },
        }
    }
}

fn admitted_cost(shared: &Shared, link: TransportId, admitted: &AdmittedInbound) -> Option<u32> {
    if admitted.packet.from == shared.local {
        return None;
    }
    let table = shared.table.lock().expect("router table poisoned");
    let adapter = table.links.get(&link)?;
    adapter
        .admits(&admitted.packet.from, admitted.session)
        .then_some(adapter.config.cost)
}

fn process(
    shared: &Shared,
    link: TransportId,
    admitted: AdmittedInbound,
    fragments: &mut Reassembly,
    seen: &mut HashSet<(NodeId, [u8; 16])>,
    order: &mut VecDeque<(NodeId, [u8; 16])>,
) {
    let Some(cost) = admitted_cost(shared, link, &admitted) else {
        return;
    };
    let AdmittedInbound { packet, session } = admitted;
    let Some(bytes) = fragments.receive(link.0, session, &packet.from, packet.msg) else {
        return;
    };
    let Ok(frame) = wire::decode(&bytes) else {
        return;
    };
    match frame {
        Frame::Advert {
            cost: advertised,
            path,
        } => {
            if path.first() != Some(&packet.from)
                || path.contains(&shared.local)
                || path.len() >= wire::MAX_HOPS
            {
                return;
            }
            let Some(cost) = cost.checked_add(advertised) else {
                return;
            };
            let Some(destination) = path.last().cloned() else {
                return;
            };
            let mut table = shared.table.lock().expect("router table poisoned");
            if !table.candidates.contains_key(&destination)
                && table.candidates.len() >= shared.config.max_routes
            {
                return;
            }
            let mut full_path = Vec::with_capacity(path.len() + 1);
            full_path.push(shared.local.clone());
            full_path.extend(path);
            table
                .candidates
                .entry(destination.clone())
                .or_default()
                .insert(
                    (link, packet.from.clone()),
                    Candidate {
                        route: Route {
                            destination,
                            next_hop: packet.from,
                            transport: link,
                            cost,
                            path: full_path,
                        },
                        updated: Instant::now(),
                        session,
                    },
                );
            drop(table);
            shared.refresh_reachable();
        }
        Frame::Data {
            kind,
            hops,
            id,
            from,
            to,
            payload,
        } => {
            let key = (from.clone(), id);
            if !seen.insert(key.clone()) {
                return;
            }
            order.push_back(key);
            if order.len() > 4096
                && let Some(old) = order.pop_front()
            {
                seen.remove(&old);
            }
            if to == shared.local {
                let queue = if kind == PayloadKind::Message {
                    &shared.messages
                } else {
                    &shared.tunnels
                };
                let _ = queue.try_send(Inbound {
                    from,
                    msg: payload.to_vec(),
                });
            } else if shared.config.forwarding && hops > 1 {
                shared.forward(
                    &to,
                    wire::data(kind, hops - 1, id, &from, &to, payload).into(),
                );
            }
        }
    }
}
