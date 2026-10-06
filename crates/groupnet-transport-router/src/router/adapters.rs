//! Router scheduling and framing, exposed through protocol-neutral worker I/O.

use super::{AdvertisedRoute, Event, Shared, TransportId};
use crate::wire::{self, Fragments};
use futures_util::{SinkExt, stream};
use groupnet_core::NodeId;
use groupnet_transport::{
    Inbound,
    link::{LinkIo, Outbound},
};
use std::{io, sync::Arc, time::Duration};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::{CancellationToken, PollSender};

pub(super) fn io(
    shared: Arc<Shared>,
    id: TransportId,
    peers: Vec<NodeId>,
    mtu: usize,
    outgoing: mpsc::Receiver<(NodeId, Arc<[u8]>)>,
    cancel: CancellationToken,
) -> LinkIo {
    let incoming = PollSender::new(shared.events.clone())
        .sink_map_err(|_| io::Error::new(io::ErrorKind::BrokenPipe, "router event queue closed"))
        .with(move |packet: Option<Inbound>| {
            std::future::ready(Ok(match packet {
                Some(packet) => Event::Received { link: id, packet },
                None => Event::Down(id),
            }))
        });
    let mut advertisements = shared.advertisements.subscribe();
    let snapshot = advertisements.borrow_and_update().clone();
    let remaining = snapshot.len() * peers.len();
    let state = Outgoing {
        shared,
        peers,
        mtu,
        queued: outgoing,
        advertisements,
        snapshot,
        remaining,
        position: 0,
        fragments: None,
    };
    let outgoing = stream::unfold(state, |mut state| async move {
        let packet = state.next().await?;
        Some((packet, state))
    });
    LinkIo {
        outgoing: Box::pin(outgoing),
        incoming: Box::pin(incoming),
        cancel,
        mtu,
    }
}

struct Outgoing {
    shared: Arc<Shared>,
    peers: Vec<NodeId>,
    mtu: usize,
    queued: mpsc::Receiver<(NodeId, Arc<[u8]>)>,
    advertisements: watch::Receiver<Arc<Vec<AdvertisedRoute>>>,
    snapshot: Arc<Vec<AdvertisedRoute>>,
    remaining: usize,
    position: usize,
    fragments: Option<(NodeId, Fragments, Instant)>,
}

impl Outgoing {
    async fn next(&mut self) -> Option<Outbound> {
        loop {
            if let Some((peer, fragments, deadline)) = &mut self.fragments {
                if Instant::now() < *deadline
                    && let Some(bytes) = fragments.next()
                {
                    return Some(Outbound::owned(peer.clone(), bytes, *deadline));
                }
                self.fragments = None;
            }
            let message = tokio::select! {
                result = self.advertisements.changed() => {
                    if result.is_err() { return None; }
                    self.snapshot = self.advertisements.borrow_and_update().clone();
                    self.remaining = self.snapshot.len() * self.peers.len();
                    continue;
                }
                item = self.queued.recv() => item,
                () = tokio::task::yield_now(), if self.remaining > 0 => {
                    let index = self.position % (self.snapshot.len() * self.peers.len());
                    self.position = self.position.wrapping_add(1);
                    self.remaining -= 1;
                    let peer = &self.peers[index % self.peers.len()];
                    let route = &self.snapshot[index / self.peers.len()];
                    if route.path.contains(peer) { continue; }
                    Some((peer.clone(), route.frame.clone()))
                }
            };
            let (peer, bytes) = message?;
            let deadline = Instant::now() + Duration::from_secs(5);
            if bytes.len() <= self.mtu {
                return Some(Outbound::shared(peer, bytes, deadline));
            }
            self.fragments = Some((
                peer,
                wire::fragment(bytes, self.mtu, self.shared.id()),
                deadline,
            ));
        }
    }
}
