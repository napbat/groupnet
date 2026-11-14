//! Admitted sessions: application admission on both sides of each
//! connection, one live generation per identity, and revocation.
//!
//! An admitted endpoint routes only through identities its policy accepted
//! and its [`SessionRegistry`] holds. Configured peers are seeds that the
//! endpoint dials on its own; any other identity joins when it dials in and
//! the policy admits it (`docs/client-role.md`, section 2).

use std::sync::Arc;
use std::{error, fmt, io};

use groupnet_core::NodeId;
use groupnet_transport::admission::{
    AcceptedPeer, Admission, JoinRequest, MAX_CREDENTIAL_BYTES, SessionLease, SessionRegistry,
};
use tokio::sync::mpsc;

use super::{Session, State, join, platform};

/// Application admission for an admitted [`IpcTransport`](super::IpcTransport):
/// the policy judging every remote claim and the credential this endpoint
/// presents to remote policies.
///
/// There is no default policy: membership is an explicit choice. `IpcLink`
/// admits exactly its configured peers unless it is given another policy;
/// `OpenAdmission` admits every local peer.
#[derive(Clone)]
pub struct IpcAdmission {
    /// Judges every remote claim, inbound and outbound; the endpoint fails
    /// closed on its errors.
    pub policy: Arc<dyn Admission>,
    /// Opaque application bytes presented to remote policies, at most
    /// [`MAX_CREDENTIAL_BYTES`]. Never printed by `Debug`. Default: empty.
    pub credential: Vec<u8>,
}

impl IpcAdmission {
    /// Admits through `policy`, presenting no credential.
    #[must_use]
    pub fn new(policy: Arc<dyn Admission>) -> Self {
        Self {
            policy,
            credential: Vec::new(),
        }
    }

    /// Checks the credential size.
    ///
    /// # Errors
    /// Returns [`IpcAdmissionError::CredentialTooLong`].
    pub fn validate(&self) -> Result<(), IpcAdmissionError> {
        if self.credential.len() > MAX_CREDENTIAL_BYTES {
            return Err(IpcAdmissionError::CredentialTooLong {
                len: self.credential.len(),
            });
        }
        Ok(())
    }
}

impl fmt::Debug for IpcAdmission {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Policies can hold secrets too: neither policy nor credential is printed.
        f.debug_struct("IpcAdmission").finish_non_exhaustive()
    }
}

/// A violated [`IpcAdmission`] rule, from [`IpcAdmission::validate`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcAdmissionError {
    /// `credential` exceeds [`MAX_CREDENTIAL_BYTES`].
    CredentialTooLong {
        /// The credential's length in bytes.
        len: usize,
    },
}

impl fmt::Display for IpcAdmissionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::CredentialTooLong { len } => write!(
                f,
                "credential is {len} bytes, above the {MAX_CREDENTIAL_BYTES}-byte limit"
            ),
        }
    }
}

impl error::Error for IpcAdmissionError {}

impl From<IpcAdmissionError> for io::Error {
    fn from(error: IpcAdmissionError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why [`IpcTransport::connect_peer`](super::IpcTransport::connect_peer)
/// started nothing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcConnectPeerError {
    /// The transport was bound without an [`IpcAdmission`]: it has no
    /// admission exchange to start, and its first send opens a session.
    AdmissionNotConfigured,
}

impl fmt::Display for IpcConnectPeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AdmissionNotConfigured => f.write_str(
                "connect_peer needs an admitted IPC transport (bind_admitted or \
                 dial_only_admitted)",
            ),
        }
    }
}

impl error::Error for IpcConnectPeerError {}

impl From<IpcConnectPeerError> for io::Error {
    fn from(error: IpcConnectPeerError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// The admission state one admitted transport shares between its handles
/// and its connection tasks.
pub(super) struct Managed {
    policy: Arc<dyn Admission>,
    credential: Vec<u8>,
    pub(super) sessions: SessionRegistry,
    /// Configured peers, dialed while they hold no session.
    pub(super) seeds: Vec<NodeId>,
}

impl fmt::Debug for Managed {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Managed")
            .field("sessions", &self.sessions)
            .field("seeds", &self.seeds)
            .finish_non_exhaustive()
    }
}

impl Managed {
    pub(super) fn new(
        admission: IpcAdmission,
        registry: SessionRegistry,
        seeds: Vec<NodeId>,
    ) -> Self {
        Self {
            policy: admission.policy,
            credential: admission.credential,
            sessions: registry,
            seeds,
        }
    }

    /// Runs the policy on a remote hello. A result naming another identity
    /// than the claim is refused: admission cannot rename a peer.
    async fn authorize(&self, hello: &join::Hello) -> io::Result<AcceptedPeer> {
        let accepted = self
            .policy
            .admit(JoinRequest {
                claimed: &hello.node,
                credential: &hello.credential,
                remote: None,
            })
            .await?;
        if accepted.node != hello.node {
            return Err(denied());
        }
        Ok(accepted)
    }

    /// The dialing side of the join (steps 1 and 3 of the wire): returns the
    /// lease once the acceptor admitted this endpoint and the local policy
    /// admitted the acceptor. Fails when a simultaneous inbound connection
    /// replaced the provisional dial of `generation`.
    pub(super) async fn dial(
        &self,
        state: &State,
        stream: &mut platform::Stream,
        peer: &NodeId,
        generation: u64,
    ) -> io::Result<SessionLease> {
        join::write_hello(stream, &state.local, &self.credential).await?;
        let hello = join::read_hello(stream).await?;
        let echoed = join::read_id(stream).await?;
        if echoed != state.local || hello.node != *peer {
            return Err(denied());
        }
        let accepted = self.authorize(&hello).await?;
        let lease = {
            let mut book = state.book.lock().map_err(|_| poisoned())?;
            let Some(session) = book
                .sessions
                .get_mut(peer)
                .filter(|session| session.generation == generation)
            else {
                return Err(denied());
            };
            let lease = self.sessions.try_admit(accepted)?;
            session.admitted = Some(lease.id());
            session.provisional = None;
            lease
        };
        join::write_id(stream, lease.node()).await?;
        Ok(lease)
    }

    /// The accepting side of the join (steps 2 and 4 of the wire). The
    /// returned claim owns the identity's entry in the address book until it
    /// drops.
    pub(super) async fn accept<'a>(
        &self,
        state: &'a State,
        stream: &mut platform::Stream,
    ) -> io::Result<Accepted<'a>> {
        let hello = join::read_hello(stream).await?;
        if hello.node == state.local {
            return Err(denied());
        }
        let accepted = self.authorize(&hello).await?;
        let (claim, frames) = claim_inbound(state, &hello.node)?;
        let lease = {
            let mut book = state.book.lock().map_err(|_| poisoned())?;
            let Some(session) = book
                .sessions
                .get_mut(&claim.node)
                .filter(|session| session.generation == claim.generation)
            else {
                return Err(denied());
            };
            let lease = self.sessions.try_admit(accepted)?;
            session.admitted = Some(lease.id());
            lease
        };
        join::write_hello(stream, &state.local, &self.credential).await?;
        join::write_id(stream, lease.node()).await?;
        if join::read_id(stream).await? != state.local {
            return Err(denied());
        }
        Ok(Accepted {
            claim,
            lease,
            frames,
        })
    }
}

/// An inbound connection that finished its join.
pub(super) struct Accepted<'a> {
    pub(super) claim: Claim<'a>,
    pub(super) lease: SessionLease,
    pub(super) frames: mpsc::Receiver<bytes::Bytes>,
}

/// Ownership of one identity's address-book entry by one connection; the
/// drop removes the entry if it still belongs to that connection.
pub(super) struct Claim<'a> {
    state: &'a State,
    pub(super) node: NodeId,
    pub(super) generation: u64,
}

impl Drop for Claim<'_> {
    fn drop(&mut self) {
        if let Ok(mut book) = self.state.book.lock() {
            book.remove_session(&self.node, self.generation);
        }
    }
}

/// Takes the address-book entry of `peer` for an inbound connection. Only a
/// provisional outbound dial is replaceable, and only when the remote
/// identity is the lower one: the lower identity's outbound connection wins
/// at both ends, as on TCP.
fn claim_inbound<'a>(
    state: &'a State,
    peer: &NodeId,
) -> io::Result<(Claim<'a>, mpsc::Receiver<bytes::Bytes>)> {
    let mut book = state.book.lock().map_err(|_| poisoned())?;
    if let Some(existing) = book.sessions.get(peer) {
        let Some(cancel) = existing
            .provisional
            .as_ref()
            .filter(|_| state.local.as_str() > peer.as_str())
        else {
            return Err(denied());
        };
        let _ = cancel.send(true);
    }
    let (sender, frames) = mpsc::channel(state.config.session_queue.get());
    let generation = book.generation();
    book.sessions.insert(
        peer.clone(),
        Session {
            generation,
            sender,
            admitted: None,
            provisional: None,
        },
    );
    Ok((
        Claim {
            state,
            node: peer.clone(),
            generation,
        },
        frames,
    ))
}

fn denied() -> io::Error {
    io::Error::new(io::ErrorKind::PermissionDenied, "IPC admission refused")
}

fn poisoned() -> io::Error {
    io::Error::other("IPC book poisoned")
}
