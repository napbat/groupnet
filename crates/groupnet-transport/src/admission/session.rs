//! Bounded, atomic live-neighbor ownership.

use super::AcceptedPeer;
use groupnet_core::NodeId;
use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

/// Opaque generation of one admitted adjacent-peer lifetime.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(u64);

/// A current neighbor and the generation which admitted it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SessionPeer {
    /// Authorized adjacent routing identity.
    pub node: NodeId,
    /// Lifetime generation; never reused within a registry.
    pub id: SessionId,
}

#[derive(Debug, Default)]
struct State {
    peers: BTreeMap<NodeId, SessionId>,
    next: u64,
}

#[derive(Debug)]
struct Shared {
    max: usize,
    state: Mutex<State>,
    neighbors: watch::Sender<Arc<Vec<SessionPeer>>>,
    /// Sticky: set (before the final empty neighbor snapshot is published)
    /// exactly once, by [`SessionRegistry::close`].
    closed: watch::Sender<bool>,
}

impl Shared {
    fn publish(&self, state: &State) {
        self.neighbors.send_replace(Arc::new(
            state
                .peers
                .iter()
                .map(|(node, id)| SessionPeer {
                    node: node.clone(),
                    id: *id,
                })
                .collect(),
        ));
    }

    fn remove(&self, node: &NodeId, id: SessionId) {
        let mut state = self.state.lock().expect("session registry poisoned");
        if state.peers.get(node) == Some(&id) {
            state.peers.remove(node);
            self.publish(&state);
        }
    }
}

/// Bounded registry of live admitted sessions, shared by adapter and router.
/// Cloning retains registry state, not session leases or sockets.
///
/// Lifecycle is observable: an open registry with no peers and a closed one
/// both publish an empty neighbor snapshot, so [`Self::is_closed`] and
/// [`Self::closed`] distinguish them. Closing marks the registry closed
/// *before* publishing that final empty snapshot, so a subscriber woken by it
/// already observes `is_closed() == true`.
#[derive(Clone, Debug)]
pub struct SessionRegistry {
    shared: Arc<Shared>,
}

impl SessionRegistry {
    /// Creates a registry with an explicit established-peer bound.
    ///
    /// # Errors
    /// Rejects zero capacity or an unrepresentable neighbor snapshot allocation.
    pub fn new(max_peers: usize) -> io::Result<Self> {
        if max_peers == 0 || max_peers > (isize::MAX as usize) / size_of::<SessionPeer>() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid session capacity",
            ));
        }
        let (neighbors, _) = watch::channel(Arc::new(Vec::new()));
        let (closed, _) = watch::channel(false);
        Ok(Self {
            shared: Arc::new(Shared {
                max: max_peers,
                state: Mutex::new(State::default()),
                neighbors,
                closed,
            }),
        })
    }

    /// Atomically reserves an authorized identity without evicting an incumbent.
    /// The adapter must first check the policy result matches the remote claim.
    ///
    /// # Errors
    /// Rejects invalid IDs, duplicate live identities, exhausted capacity/generations.
    /// # Panics
    /// Propagates a poisoned registry lock.
    pub fn try_admit(&self, accepted: AcceptedPeer) -> io::Result<SessionLease> {
        let length = accepted.node.as_str().len();
        if length == 0 || length > crate::MAX_NODE_ID_BYTES {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid admitted identity",
            ));
        }
        let mut state = self.shared.state.lock().expect("session registry poisoned");
        // `close` sets the flag under this lock, so the check is atomic with it.
        if *self.shared.closed.borrow() {
            return Err(io::Error::new(
                io::ErrorKind::BrokenPipe,
                "session registry closed",
            ));
        }
        if state.peers.contains_key(&accepted.node) {
            return Err(io::Error::new(
                io::ErrorKind::AlreadyExists,
                "identity already admitted",
            ));
        }
        if state.peers.len() >= self.shared.max {
            return Err(io::Error::other("session capacity reached"));
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or_else(|| io::Error::other("session generation exhausted"))?;
        let id = SessionId(state.next);
        state.peers.insert(accepted.node.clone(), id);
        self.shared.publish(&state);
        Ok(SessionLease {
            inner: Arc::new(Lease {
                shared: Arc::downgrade(&self.shared),
                node: accepted.node,
                id,
            }),
        })
    }

    /// Watches complete current neighbor snapshots, coalescing rapid churn.
    /// An empty snapshot means "no peers"; pair it with [`Self::is_closed`]
    /// (or await [`Self::closed`]) to tell an idle registry from a closed one.
    #[must_use]
    pub fn subscribe(&self) -> watch::Receiver<Arc<Vec<SessionPeer>>> {
        self.shared.neighbors.subscribe()
    }

    /// Whether [`Self::close`] has run. Sticky: once `true`, always `true`.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        *self.shared.closed.borrow()
    }

    /// Resolves once the registry is closed — immediately if it already is —
    /// or once every registry handle has been dropped, which equally ends
    /// admission. The future owns its subscription, so it may outlive `self`
    /// and be polled from any task.
    pub fn closed(&self) -> impl Future<Output = ()> + Send + 'static {
        let mut closed = self.shared.closed.subscribe();
        async move {
            // An error means the registry itself is gone: equally closed.
            let _ = closed.wait_for(|closed| *closed).await;
        }
    }

    /// Checks that both identity and generation remain admitted.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    #[must_use]
    pub fn is_active(&self, node: &NodeId, id: SessionId) -> bool {
        self.shared
            .state
            .lock()
            .expect("session registry poisoned")
            .peers
            .get(node)
            == Some(&id)
    }

    /// Immediately withdraws a peer; old leases cannot remove its later replacement.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    pub fn revoke(&self, node: &NodeId) {
        let mut state = self.shared.state.lock().expect("session registry poisoned");
        if state.peers.remove(node).is_some() {
            self.shared.publish(&state);
        }
    }

    /// Permanently stops admission and withdraws every live generation.
    /// Idempotent; adapters should invoke this before draining shutdown tasks.
    /// The closed state is published before the final empty neighbor snapshot.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    pub fn close(&self) {
        let mut state = self.shared.state.lock().expect("session registry poisoned");
        if !self.shared.closed.send_replace(true) {
            state.peers.clear();
            self.shared.publish(&state);
        }
    }
}

#[derive(Debug)]
struct Lease {
    shared: Weak<Shared>,
    node: NodeId,
    id: SessionId,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(shared) = self.shared.upgrade() {
            shared.remove(&self.node, self.id);
        }
    }
}

/// RAII session ownership. The final clone's drop withdraws its exact generation.
/// Queued packets should retain only [`SessionId`], not this lease, so queues do
/// not prolong admission after a connection closes.
#[derive(Clone, Debug)]
pub struct SessionLease {
    inner: Arc<Lease>,
}

impl SessionLease {
    /// Authorized adjacent routing identity.
    #[must_use]
    pub fn node(&self) -> &NodeId {
        &self.inner.node
    }

    /// Generation to attach when producing an inbound frame.
    #[must_use]
    pub fn id(&self) -> SessionId {
        self.inner.id
    }

    /// Whether this exact generation remains admitted.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    #[must_use]
    pub fn is_active(&self) -> bool {
        self.inner.shared.upgrade().is_some_and(|shared| {
            shared
                .state
                .lock()
                .expect("session registry poisoned")
                .peers
                .get(&self.inner.node)
                == Some(&self.inner.id)
        })
    }

    /// Withdraws this exact generation without affecting a subsequent reconnect.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    pub fn revoke(&self) {
        if let Some(shared) = self.inner.shared.upgrade() {
            shared.remove(&self.inner.node, self.inner.id);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn established_peer_capacity_has_no_arbitrary_routing_ceiling() {
        assert!(SessionRegistry::new(8192).is_ok());
        assert!(SessionRegistry::new(0).is_err());
        assert!(SessionRegistry::new(usize::MAX).is_err());
    }
}
