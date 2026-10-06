//! Admitted routed destinations bootstrap already-joined membership engines.

use super::Inner;
use crate::driver::Event;
use groupnet_core::Command;
use std::sync::Arc;

pub(super) async fn discover_peers(inner: Arc<Inner>) {
    let mut reachable = inner.transport.reachable();
    loop {
        let peers = reachable.borrow_and_update().clone();
        let groups: Vec<_> = inner
            .routes
            .lock()
            .expect("routes mutex poisoned")
            .values()
            .map(crate::Group::command_sender)
            .collect();
        for commands in groups {
            tokio::select! {
                biased;
                () = inner.transport.cancelled() => return,
                _ = commands.send(Event::Local(Command::SetBootstrapContacts(
                    peers.as_ref().clone(),
                ))) => {}
            }
        }
        tokio::select! {
            biased;
            () = inner.transport.cancelled() => return,
            result = reachable.changed() => {
                if result.is_err() { return; }
            }
        }
    }
}
