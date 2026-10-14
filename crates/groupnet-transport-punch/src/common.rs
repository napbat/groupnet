//! Crate-private helpers shared by the UDP and TCP traversal endpoints.

use std::collections::HashSet;
use std::io;
use std::net::{Ipv4Addr, Ipv6Addr, SocketAddr};
use std::sync::{Mutex, MutexGuard};

use groupnet_core::NodeId;
use groupnet_transport::{NodeIdError, validate_node_id};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinHandle;

/// Identity bound of both native wire encodings.
pub(crate) const MAX_IDENTITY_BYTES: usize = 64;

/// The first rejected entry of a configured identity list.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum PeerListFault {
    /// The entry at `index` is not a valid native identity.
    Invalid { index: usize, error: NodeIdError },
    /// The entry at `index` repeats an earlier entry.
    Duplicate { index: usize },
}

/// Checks every identity against the native wire bound and rejects duplicates.
pub(crate) fn check_peers(peers: &[NodeId]) -> Result<(), PeerListFault> {
    let mut seen = HashSet::with_capacity(peers.len());
    for (index, node) in peers.iter().enumerate() {
        validate_node_id(node, MAX_IDENTITY_BYTES)
            .map_err(|error| PeerListFault::Invalid { index, error })?;
        if !seen.insert(node) {
            return Err(PeerListFault::Duplicate { index });
        }
    }
    Ok(())
}

/// The ephemeral wildcard bind of `peer`'s address family.
pub(crate) fn wildcard_for(peer: SocketAddr) -> SocketAddr {
    if peer.is_ipv4() {
        SocketAddr::from((Ipv4Addr::UNSPECIFIED, 0))
    } else {
        SocketAddr::from((Ipv6Addr::UNSPECIFIED, 0))
    }
}

pub(crate) fn invalid(message: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(crate) fn closed() -> io::Error {
    io::Error::new(
        io::ErrorKind::NotConnected,
        "native traversal connection closed",
    )
}

/// Locks `mutex`, recovering the guard if a panicking holder poisoned it.
pub(crate) fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
}

pub(crate) fn random<const N: usize>() -> io::Result<[u8; N]> {
    let mut bytes = [0; N];
    SystemRandom::new()
        .fill(&mut bytes)
        .map_err(|_| io::Error::other("secure random source failed"))?;
    Ok(bytes)
}

/// Waits for the owned task to finish. Cancellation-safe: a cancelled waiter
/// leaves the handle in place for the next caller.
pub(crate) async fn join(task: &AsyncMutex<Option<JoinHandle<()>>>) {
    let mut task = task.lock().await;
    if let Some(running) = task.as_mut() {
        let _ = running.await;
    }
    task.take();
}
