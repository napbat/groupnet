//! Bounded, atomic live-neighbor ownership.

use super::AcceptedPeer;
use crate::identity::{MAX_NODE_ID_BYTES, NodeIdError, validate_node_id};
use groupnet_core::NodeId;
use std::{
    collections::BTreeMap,
    fmt, io,
    num::NonZeroUsize,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::watch;

/// Why [`SessionRegistry::try_admit`] refused an authorized identity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SessionError {
    /// The accepted identity is outside the shared identity bound.
    Identity(NodeIdError),
    /// [`SessionRegistry::close`] has run; admission is permanently over.
    Closed,
    /// The identity already holds a live session; incumbents are never evicted.
    AlreadyAdmitted(NodeId),
    /// Every one of the registry's `max_peers` slots is live.
    CapacityReached {
        /// The registry's established-peer bound.
        max_peers: NonZeroUsize,
    },
    /// The registry issued its last session generation.
    GenerationsExhausted,
}

impl fmt::Display for SessionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "admitted identity rejected: {error}"),
            Self::Closed => f.write_str("session registry closed"),
            Self::AlreadyAdmitted(node) => write!(f, "identity {node} is already admitted"),
            Self::CapacityReached { max_peers } => {
                write!(f, "all {max_peers} session slots (max_peers) are live")
            }
            Self::GenerationsExhausted => f.write_str("session generations exhausted"),
        }
    }
}

impl std::error::Error for SessionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            _ => None,
        }
    }
}

impl From<SessionError> for io::Error {
    /// Maps each refusal to the kind adapters report it with: invalid input,
    /// broken pipe after close, already-exists for a duplicate, otherwise
    /// `Other`; the typed error stays the inner error.
    fn from(error: SessionError) -> Self {
        let kind = match &error {
            SessionError::Identity(_) => io::ErrorKind::InvalidInput,
            SessionError::Closed => io::ErrorKind::BrokenPipe,
            SessionError::AlreadyAdmitted(_) => io::ErrorKind::AlreadyExists,
            SessionError::CapacityReached { .. } | SessionError::GenerationsExhausted => {
                io::ErrorKind::Other
            }
        };
        Self::new(kind, error)
    }
}

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
    max: NonZeroUsize,
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
    /// Creates a registry holding at most `max_peers` live sessions.
    #[must_use]
    pub fn new(max_peers: NonZeroUsize) -> Self {
        let (neighbors, _) = watch::channel(Arc::new(Vec::new()));
        let (closed, _) = watch::channel(false);
        Self {
            shared: Arc::new(Shared {
                max: max_peers,
                state: Mutex::new(State::default()),
                neighbors,
                closed,
            }),
        }
    }

    /// Atomically reserves an authorized identity without evicting an incumbent.
    /// The adapter must first check the policy result matches the remote claim.
    ///
    /// # Errors
    /// Returns the [`SessionError`] naming why: an invalid identity, a closed
    /// registry, a duplicate live identity, exhausted capacity or generations.
    ///
    /// # Panics
    /// Propagates a poisoned registry lock.
    pub fn try_admit(&self, accepted: AcceptedPeer) -> Result<SessionLease, SessionError> {
        validate_node_id(&accepted.node, MAX_NODE_ID_BYTES).map_err(SessionError::Identity)?;
        let mut state = self.shared.state.lock().expect("session registry poisoned");
        // `close` sets the flag under this lock, so the check is atomic with it.
        if *self.shared.closed.borrow() {
            return Err(SessionError::Closed);
        }
        if state.peers.contains_key(&accepted.node) {
            return Err(SessionError::AlreadyAdmitted(accepted.node));
        }
        if state.peers.len() >= self.shared.max.get() {
            return Err(SessionError::CapacityReached {
                max_peers: self.shared.max,
            });
        }
        state.next = state
            .next
            .checked_add(1)
            .ok_or(SessionError::GenerationsExhausted)?;
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
