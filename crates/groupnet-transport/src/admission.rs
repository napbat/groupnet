//! Application admission and generation-fenced adjacent-peer lifetimes.
//!
//! Admission authorizes a claimed routing identity, not a cryptographic identity.
//! Adapters must bound pending work and handshake duration before invoking policy.

use crate::link::LinkFuture;
use groupnet_core::NodeId;
use std::{fmt, io, net::SocketAddr};

mod session;
pub use session::{SessionId, SessionLease, SessionPeer, SessionRegistry};

/// Largest opaque application credential accepted by a managed handshake.
pub const MAX_CREDENTIAL_BYTES: usize = 1024;

/// An untrusted claim supplied to an application admission policy.
/// Debug output deliberately excludes credential contents.
pub struct JoinRequest<'a> {
    /// Routing identity requested by the remote participant.
    pub claimed: &'a NodeId,
    /// Bounded, opaque application credential; never log these bytes.
    pub credential: &'a [u8],
    /// Physical remote endpoint, when the adapter has one.
    pub remote: Option<SocketAddr>,
}

impl<'a> JoinRequest<'a> {
    /// Creates a claim; the adapter must reject credentials above the byte bound.
    #[must_use]
    pub const fn new(
        claimed: &'a NodeId,
        credential: &'a [u8],
        remote: Option<SocketAddr>,
    ) -> Self {
        Self {
            claimed,
            credential,
            remote,
        }
    }
}

impl fmt::Debug for JoinRequest<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("JoinRequest")
            .field("claimed", self.claimed)
            .field("remote", &self.remote)
            .finish_non_exhaustive()
    }
}

/// The routing identity an application permits for an admitted session.
/// Adapters must reject a result differing from the claimed/local identity;
/// admission cannot silently rename an already-running node.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AcceptedPeer {
    /// Permitted routing identity.
    pub node: NodeId,
}

impl AcceptedPeer {
    /// Permits the supplied routing identity.
    #[must_use]
    pub const fn new(node: NodeId) -> Self {
        Self { node }
    }
}
/// Object-safe application policy, erased once per handshake, never per packet.
pub trait Admission: Send + Sync + fmt::Debug + 'static {
    /// Authorizes one bounded identity claim.
    ///
    /// # Errors
    /// Returns an error to deny admission; adapters must fail closed.
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>>;
}

/// Explicitly unauthenticated admission, requiring no key or shared secret.
/// This provides neither proof of identity nor transport encryption.
#[derive(Clone, Copy, Debug, Default)]
pub struct OpenAdmission;

impl Admission for OpenAdmission {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            if request.credential.len() > MAX_CREDENTIAL_BYTES
                || request.claimed.as_str().is_empty()
                || request.claimed.as_str().len() > crate::MAX_NODE_ID_BYTES
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "invalid join request",
                ));
            }
            Ok(AcceptedPeer {
                node: request.claimed.clone(),
            })
        })
    }
}

#[cfg(test)]
mod tests;
