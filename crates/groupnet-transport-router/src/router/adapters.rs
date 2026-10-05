//! Typed per-adapter I/O workers and fair, bounded announcement scheduling.

use std::sync::Arc;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::Transport;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

use super::{Event, Shared, TransportId};
use crate::wire;

pub(super) async fn receive_adapter<T: Transport>(
    transport: Arc<T>,
    id: TransportId,
    mtu: usize,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
) {
    loop {
        let packet = tokio::select! { biased; () = cancel.cancelled() => break, result = transport.recv() => result };
        let Ok(packet) = packet else {
            let _ = events.send(Event::Down(id)).await;
            break;
        };
        if packet.msg.len() > mtu {
            continue;
        }
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            result = events.send(Event::Received { link: id, packet }) => if result.is_err() { break; },
        }
    }
}

pub(super) async fn send_adapter<T: Transport>(
    transport: Arc<T>,
    mut outgoing: mpsc::Receiver<(NodeId, Arc<[u8]>)>,
    mtu: usize,
    peers: Vec<NodeId>,
    shared: Arc<Shared>,
    cancel: CancellationToken,
) {
    let mut advertisements = shared.advertisements.subscribe();
    let mut snapshot = advertisements.borrow_and_update().clone();
    let mut position = 0;
    let mut remaining = snapshot.len() * peers.len();
    loop {
        let message = tokio::select! {
            () = cancel.cancelled() => break,
            result = advertisements.changed() => {
                if result.is_err() { break; }
                snapshot = advertisements.borrow_and_update().clone();
                remaining = snapshot.len() * peers.len();
                continue;
            }
            item = outgoing.recv() => item,
            () = tokio::task::yield_now(), if remaining > 0 => {
                let index = position % (snapshot.len() * peers.len());
                position = position.wrapping_add(1);
                remaining -= 1;
                let peer = &peers[index % peers.len()];
                let route = &snapshot[index / peers.len()];
                if route.path.contains(peer) { continue; }
                Some((peer.clone(), route.frame.clone()))
            }
        };
        let Some((peer, bytes)) = message else {
            break;
        };
        let operation = async {
            if bytes.len() <= mtu {
                let _ = transport.send(&peer, &bytes).await;
            } else {
                for fragment in wire::fragment(&bytes, mtu, shared.id()) {
                    let _ = transport.send(&peer, &fragment).await;
                }
            }
        };
        tokio::select! { biased; () = cancel.cancelled() => break,
        _ = tokio::time::timeout(Duration::from_secs(5), operation) => {} }
    }
}
