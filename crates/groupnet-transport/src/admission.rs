//! Application admission and generation-fenced adjacent-peer lifetimes.
//!
//! Admission authorizes a claimed routing identity, not a cryptographic identity.
//! Adapters must bound pending work and handshake duration before invoking policy.

use crate::identity::{MAX_NODE_ID_BYTES, NodeIdError, validate_node_id};
use crate::link::LinkFuture;
use groupnet_core::NodeId;
use std::{error, fmt, io, net::SocketAddr};

mod session;
pub use session::{SessionError, SessionId, SessionLease, SessionPeer, SessionRegistry};

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

    /// Checks the claim against the bounds every adapter enforces: a valid
    /// routing identity and at most [`MAX_CREDENTIAL_BYTES`] of credential.
    /// Policies call it before judging the claim itself.
    ///
    /// # Errors
    /// Returns the [`JoinRequestError`] naming the offending field.
    pub fn validate(&self) -> Result<(), JoinRequestError> {
        validate_node_id(self.claimed, MAX_NODE_ID_BYTES).map_err(JoinRequestError::Claimed)?;
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(JoinRequestError::CredentialTooLong {
                len: self.credential.len(),
            });
        }
        Ok(())
    }
}

/// Why a [`JoinRequest`] is malformed, from [`JoinRequest::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JoinRequestError {
    /// The `claimed` identity is empty or too long.
    Claimed(NodeIdError),
    /// The `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLong {
        /// The credential's length in bytes.
        len: usize,
    },
}

impl fmt::Display for JoinRequestError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Claimed(error) => write!(f, "claimed identity rejected: {error}"),
            Self::CredentialTooLong { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte limit"
            ),
        }
    }
}

impl error::Error for JoinRequestError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Claimed(error) => Some(error),
            Self::CredentialTooLong { .. } => None,
        }
    }
}

impl From<JoinRequestError> for io::Error {
    fn from(error: JoinRequestError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
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
///
/// Implementations return a boxed [`LinkFuture`] so adapters can hold any
/// policy as `Arc<dyn Admission>`. Wrap the body in `Box::pin(async move {..})`;
/// the future may borrow both the policy and the request for `'a`. Deny with
/// an `io::Error` whose kind is `PermissionDenied` and whose message names the
/// claim and the reason, and reject malformed claims with
/// [`JoinRequest::validate`] (convertible with `?`), so operators can tell a
/// refused identity from a broken request:
///
/// ```
/// use std::{collections::HashSet, io};
///
/// use groupnet_core::NodeId;
/// use groupnet_transport::admission::{AcceptedPeer, Admission, JoinRequest};
/// use groupnet_transport::link::LinkFuture;
///
/// /// Admits only identities on a fixed list.
/// #[derive(Debug)]
/// struct Allowlist(HashSet<NodeId>);
///
/// impl Admission for Allowlist {
///     fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
///         Box::pin(async move {
///             request.validate()?;
///             if self.0.contains(request.claimed) {
///                 Ok(AcceptedPeer::new(request.claimed.clone()))
///             } else {
///                 Err(io::Error::new(
///                     io::ErrorKind::PermissionDenied,
///                     format!("{} is not on the allowlist", request.claimed),
///                 ))
///             }
///         })
///     }
/// }
///
/// let policy = Allowlist(HashSet::from([NodeId::new("a")]));
/// let admitted = futures::executor::block_on(
///     policy.admit(JoinRequest::new(&NodeId::new("a"), &[], None)),
/// );
/// assert!(admitted.is_ok());
/// ```
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
            request.validate()?;
            Ok(AcceptedPeer {
                node: request.claimed.clone(),
            })
        })
    }
}

#[cfg(test)]
mod tests;
