//! Router scheduling and framing, exposed through protocol-neutral worker I/O.

use super::{AdvertisedRoute, Event, Queued, Shared, TransportId};
use crate::wire::{self, Fragments};
use futures_util::{Sink, stream};
use groupnet_core::NodeId;
use groupnet_transport::admission::{SessionId, SessionPeer, SessionRegistry};
use groupnet_transport::link::{AdmittedInbound, InboundDelivery, InboundSink, LinkIo, Outbound};
use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
};
use tokio::{
    sync::{mpsc, watch},
    time::Instant,
};
use tokio_util::sync::{CancellationToken, PollSender};

pub(super) fn io(
    shared: Arc<Shared>,
    id: TransportId,
    peers: Vec<NodeId>,
    sessions: Option<&SessionRegistry>,
    mtu: usize,
    outgoing: mpsc::Receiver<Queued>,
    cancel: CancellationToken,
) -> LinkIo {
    let direct = InboundSink::new(
        Arc::new(Direct {
            shared: shared.clone(),
            link: id,
        }),
        mtu,
    );
    let incoming = Incoming {
        shared: shared.clone(),
        link: id,
        events: PollSender::new(shared.events.clone()),
        down: false,
    };
    let mut advertisements = shared.advertisements.subscribe();
    let mut neighbors = sessions.map(SessionRegistry::subscribe);
    let peers: Vec<_> = neighbors.as_mut().map_or_else(
        || peers.into_iter().map(|peer| (peer, None)).collect(),
        |neighbors| {
            neighbors
                .borrow_and_update()
                .iter()
                .filter(|peer| peer.node != shared.local)
                .map(|peer| (peer.node.clone(), Some(peer.id)))
                .collect()
        },
    );
    let snapshot = advertisements.borrow_and_update().clone();
    let remaining = snapshot.len() * peers.len();
    let state = Outgoing {
        shared,
        peers,
        neighbors,
        sessions: sessions.cloned(),
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
        direct: Some(direct),
        cancel,
        mtu,
    }
}

/// Frames a transport's own reader tasks hand over through [`InboundSink`]:
/// processed inline on the reading task, exactly as [`Incoming`] processes
/// frames the link worker received.
struct Direct {
    shared: Arc<Shared>,
    link: TransportId,
}

impl InboundDelivery for Direct {
    fn deliver(&self, packet: AdmittedInbound) {
        self.shared.receive(self.link, packet);
    }
}

/// The link worker's inbound sink. Frames are processed synchronously on the
/// worker that read them; only the terminal transport failure becomes a router
/// event, sent with the event queue's backpressure on flush.
struct Incoming {
    shared: Arc<Shared>,
    link: TransportId,
    events: PollSender<Event>,
    down: bool,
}

impl Sink<Option<AdmittedInbound>> for Incoming {
    type Error = io::Error;

    fn poll_ready(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn start_send(mut self: Pin<&mut Self>, packet: Option<AdmittedInbound>) -> io::Result<()> {
        match packet {
            Some(packet) => self.shared.receive(self.link, packet),
            None => self.down = true,
        }
        Ok(())
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        if self.down {
            let closed = |_| io::Error::new(io::ErrorKind::BrokenPipe, "router event queue closed");
            ready!(self.events.poll_reserve(cx)).map_err(closed)?;
            let link = self.link;
            self.events.send_item(Event::Down(link)).map_err(closed)?;
            self.down = false;
        }
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.poll_flush(cx)
    }
}

struct Outgoing {
    shared: Arc<Shared>,
    peers: Vec<(NodeId, Option<SessionId>)>,
    mtu: usize,
    neighbors: Option<watch::Receiver<Arc<Vec<SessionPeer>>>>,
    sessions: Option<SessionRegistry>,
    queued: mpsc::Receiver<Queued>,
    advertisements: watch::Receiver<Arc<Vec<AdvertisedRoute>>>,
    snapshot: Arc<Vec<AdvertisedRoute>>,
    remaining: usize,
    position: usize,
    fragments: Option<Fragmented>,
}

struct Fragmented {
    peer: NodeId,
    session: Option<SessionId>,
    frames: Fragments,
    deadline: Instant,
}

impl Outgoing {
    fn admitted(&self, peer: &NodeId, session: Option<SessionId>) -> bool {
        match (&self.sessions, session) {
            (Some(registry), Some(id)) => registry.is_active(peer, id),
            (None, None) => true,
            _ => false,
        }
    }

    async fn next(&mut self) -> Option<Outbound> {
        loop {
            if let Some(mut pending) = self.fragments.take()
                && Instant::now() < pending.deadline
                && self.admitted(&pending.peer, pending.session)
                && let Some(bytes) = pending.frames.next()
            {
                let packet = Outbound::owned(pending.peer.clone(), bytes, pending.deadline)
                    .with_session(pending.session);
                self.fragments = Some(pending);
                return Some(packet);
            }
            let message = tokio::select! {
                result = self.advertisements.changed() => {
                    if result.is_err() { return None; }
                    self.snapshot = self.advertisements.borrow_and_update().clone();
                    self.remaining = self.snapshot.len() * self.peers.len();
                    continue;
                }
                () = async {
                    match &mut self.neighbors {
                        Some(neighbors) => { let _ = neighbors.changed().await; }
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(neighbors) = &mut self.neighbors {
                        self.peers = neighbors.borrow_and_update().iter()
                            .filter(|peer| peer.node != self.shared.local)
                            .map(|peer| (peer.node.clone(), Some(peer.id))).collect();
                        self.remaining = self.snapshot.len() * self.peers.len();
                        self.fragments = None;
                    }
                    continue;
                }
                item = self.queued.recv() => item,
                () = tokio::task::yield_now(), if self.remaining > 0 => {
                    let index = self.position % (self.snapshot.len() * self.peers.len());
                    self.position = self.position.wrapping_add(1);
                    self.remaining -= 1;
                    let (peer, session) = &self.peers[index % self.peers.len()];
                    let route = &self.snapshot[index / self.peers.len()];
                    if route.path.contains(peer) { continue; }
                    Some(Queued { peer: peer.clone(), session: *session, bytes: route.frame.clone() })
                }
            };
            let Queued {
                peer,
                bytes,
                session,
            } = message?;
            if !self.admitted(&peer, session) {
                continue;
            }
            let deadline = Instant::now() + self.shared.config.send_timeout;
            if bytes.len() <= self.mtu {
                return Some(Outbound::shared(peer, bytes, deadline).with_session(session));
            }
            self.fragments = Some(Fragmented {
                peer,
                session,
                frames: wire::fragment(bytes, self.mtu, self.shared.id()),
                deadline,
            });
        }
    }
}

pub(super) async fn neighbors(
    shared: Arc<Shared>,
    id: TransportId,
    mut neighbors: watch::Receiver<Arc<Vec<SessionPeer>>>,
    cancel: CancellationToken,
) {
    loop {
        neighbors.borrow_and_update();
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            result = shared.events.send(Event::Neighbors(id)) => {
                if result.is_err() { return; }
            }
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => return,
            result = neighbors.changed() => {
                if result.is_err() { return; }
            }
        }
    }
}
