//! Persistent native local IPC with bounded, shared framing.
//!
//! Unix sockets require a private directory (no group/other permissions), owned
//! by the socket creator. Existing paths are never removed to make binding work.
//! Windows uses local named pipes, rejects remote clients, and reserves the first
//! pipe instance. Peer identities are trusted within this OS security boundary;
//! this transport is not a cryptographic identity layer.
//!
//! A transport is static ([`IpcTransport::bind`]) or admitted
//! ([`IpcTransport::bind_admitted`], [`IpcTransport::dial_only_admitted`]). A
//! static transport talks to any local peer that introduces itself and leaves
//! admission to the router's configured peer list. An admitted transport runs
//! an [`Admission`](groupnet_transport::admission::Admission) policy on both
//! sides of every connection and records live peers in a
//! [`SessionRegistry`], so its peers grow and shrink at run time.

mod address;
mod admitted;
mod config;
mod frame;
mod join;
mod platform;
mod runtime;

use std::collections::HashMap;
use std::num::NonZeroUsize;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::{error, fmt, io};

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::NodeIdError;
use groupnet_transport::admission::{SessionId, SessionRegistry};
use groupnet_transport::link::{AdmittedInbound, LinkConfig, LinkFuture, LinkLifecycle};
use groupnet_transport::{Inbound, Transport};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Largest message accepted by the IPC framing protocol, in bytes.
pub const MAX_FRAME: usize = 65_000;
pub use address::IpcAddressError;
pub use admitted::{IpcAdmission, IpcAdmissionError, IpcConnectPeerError};
pub use config::{IpcConfig, IpcConfigError};

/// A native, local-only IPC listener address.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum IpcAddress {
    /// A Unix socket path inside a private, owner-only directory.
    #[cfg(unix)]
    Unix(PathBuf),
    /// A local Windows pipe name of the form `\\.\pipe\name`.
    #[cfg(windows)]
    NamedPipe(String),
}

#[derive(Debug)]
struct Session {
    generation: u64,
    sender: mpsc::Sender<Bytes>,
    /// The registry generation of an admitted session, once its join
    /// reserved the identity. Always `None` on a static transport.
    admitted: Option<SessionId>,
    /// Cancels a provisional outbound dial of an admitted transport; only
    /// such an entry can be replaced by a simultaneous inbound connection.
    provisional: Option<watch::Sender<bool>>,
}

#[derive(Debug, Default)]
struct Book {
    peers: HashMap<NodeId, IpcAddress>,
    sessions: HashMap<NodeId, Session>,
    generation: u64,
}

impl Book {
    fn generation(&mut self) -> u64 {
        self.generation = self.generation.wrapping_add(1);
        self.generation
    }

    fn remove_session(&mut self, node: &NodeId, generation: u64) {
        if self
            .sessions
            .get(node)
            .is_some_and(|s| s.generation == generation)
        {
            self.sessions.remove(node);
        }
    }
}

#[derive(Debug)]
struct State {
    local: NodeId,
    config: IpcConfig,
    book: Mutex<Book>,
    slots: Arc<Semaphore>,
    inbox: AsyncMutex<mpsc::Receiver<AdmittedInbound>>,
    incoming: mpsc::Sender<AdmittedInbound>,
    commands: mpsc::Sender<runtime::Dial>,
    cancel: CancellationToken,
    /// Admission state of an admitted transport; `None` for a static one.
    admission: Option<admitted::Managed>,
}

impl State {
    /// Whether `session` belongs to an admitted generation that is no
    /// longer live: its connection is ending and must not take new frames.
    fn is_stale(&self, node: &NodeId, session: &Session) -> bool {
        match (&self.admission, session.admitted) {
            (Some(managed), Some(id)) => !managed.sessions.is_active(node, id),
            _ => false,
        }
    }

    /// Queues `msg` on the session to `to`, dialing a registered address when
    /// there is none. `None` only starts the session. Every drop is silent,
    /// under the best-effort contract: an unknown peer, a full queue, no free
    /// session slot, or a session that is being revoked.
    fn enqueue<F: FnOnce() -> Bytes>(
        &self,
        to: &NodeId,
        length: usize,
        msg: Option<F>,
    ) -> io::Result<()> {
        let state = self;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if length > MAX_FRAME {
            return Err(IpcSendError::PayloadTooLarge {
                len: length,
                max: MAX_FRAME,
            }
            .into());
        }
        let mut book = state
            .book
            .lock()
            .map_err(|_| io::Error::other("IPC book poisoned"))?;
        if let Some(session) = book.sessions.get(to) {
            if state.is_stale(to, session) {
                return Ok(());
            }
            if msg.is_none() {
                return Ok(());
            }
            match session.sender.try_reserve() {
                Ok(permit) => {
                    if let Some(msg) = msg {
                        permit.send(msg());
                    }
                    return Ok(());
                }
                Err(mpsc::error::TrySendError::Full(())) => return Ok(()),
                Err(mpsc::error::TrySendError::Closed(())) => {}
            }
            book.sessions.remove(to);
        }
        let Some(address) = book.peers.get(to).cloned() else {
            return Ok(());
        };
        let Ok(slot) = Arc::clone(&state.slots).try_acquire_owned() else {
            return Ok(());
        };
        let Ok(command) = state.commands.try_reserve() else {
            return Ok(());
        };
        let (sender, frames) = mpsc::channel(state.config.session_queue.get());
        if let Some(msg) = msg {
            // The newly allocated queue is empty, so reservation cannot fail.
            sender
                .try_send(msg())
                .map_err(|_| io::Error::other("new IPC queue unavailable"))?;
        }
        let generation = book.generation();
        let (provisional, cancelled) = if state.admission.is_some() {
            let (provisional, cancelled) = watch::channel(false);
            (Some(provisional), Some(cancelled))
        } else {
            (None, None)
        };
        book.sessions.insert(
            to.clone(),
            Session {
                generation,
                sender,
                admitted: None,
                provisional,
            },
        );
        command.send(runtime::Dial {
            node: to.clone(),
            address,
            generation,
            frames,
            slot,
            cancelled,
        });
        Ok(())
    }

    /// Queues an owned frame only on the admitted session of `expected`.
    fn send_session(&self, to: &NodeId, msg: Bytes, expected: Option<SessionId>) -> io::Result<()> {
        let state = self;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if msg.len() > MAX_FRAME {
            return Err(IpcSendError::PayloadTooLarge {
                len: msg.len(),
                max: MAX_FRAME,
            }
            .into());
        }
        let Some(expected) = expected else {
            return Ok(());
        };
        let book = state
            .book
            .lock()
            .map_err(|_| io::Error::other("IPC book poisoned"))?;
        // A queue belongs to one connection for its whole life, so a frame
        // selected for one generation never reaches a later one.
        if let Some(session) = book
            .sessions
            .get(to)
            .filter(|session| session.admitted == Some(expected) && !state.is_stale(to, session))
        {
            let _ = session.sender.try_send(msg);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Handle {
    state: Arc<State>,
    done: watch::Receiver<bool>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.state.cancel.cancel();
    }
}

/// A cloneable, persistent, full-duplex native IPC message transport.
///
/// By default there are at most 128 registered peers, 64 sessions (including
/// setup), 16 queued messages per session, and 64 queued inbound messages,
/// with 5 s setup and per-frame write deadlines and a 60 s idle read deadline;
/// [`IpcConfig`] selects other finite limits. Unknown peers, busy connections
/// and full outbound queues drop messages under the best-effort [`Transport`]
/// contract; a message longer than [`MAX_FRAME`] is refused as `InvalidInput`
/// carrying [`IpcSendError`]. Each session introduces its node ID
/// once and can carry messages in either direction without a second connection.
/// Dropping the last handle cancels all workers; [`Self::close`] also waits for
/// workers to stop and for the owned listener to be released.
#[derive(Clone, Debug)]
pub struct IpcTransport {
    handle: Arc<Handle>,
}

impl IpcTransport {
    /// Binds a native listener with the default [`IpcConfig`] and starts its
    /// bounded Tokio workers.
    ///
    /// Synchronous by design: a local listener needs no name resolution or
    /// network round trip, but the current Tokio runtime must exist to spawn
    /// the workers.
    ///
    /// # Errors
    /// Returns an error for empty/over-64-byte node IDs, an address rejected
    /// with its [`IpcAddressError`] (malformed, insecure), occupied addresses,
    /// missing Tokio runtime, and native listener failures. Unix callers must
    /// create their own private directory before binding.
    pub fn bind(local: NodeId, address: &IpcAddress) -> io::Result<Self> {
        Self::bind_with(local, address, IpcConfig::default())
    }

    /// Binds a native listener with explicit finite operational budgets.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcConfigError`] before binding,
    /// in addition to [`Self::bind`] errors.
    pub fn bind_with(local: NodeId, address: &IpcAddress, config: IpcConfig) -> io::Result<Self> {
        Self::start(Some(address), Preflight::new(local, config)?, None)
    }

    /// Binds an admitted listener: every connection, inbound or outbound,
    /// runs `admission`'s policy on the remote claim, and live peers are
    /// recorded in a [`SessionRegistry`] of at most [`IpcConfig::max_peers`]
    /// identities. Peers the policy admits join at run time; registered
    /// addresses are only where this endpoint dials.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcAdmissionError`] before
    /// binding, in addition to [`Self::bind_with`] errors.
    pub fn bind_admitted(
        local: NodeId,
        address: &IpcAddress,
        config: IpcConfig,
        admission: IpcAdmission,
    ) -> io::Result<Self> {
        admission.validate()?;
        Self::start(
            Some(address),
            Preflight::new(local, config)?,
            Some(admission),
        )
    }

    /// Starts an admitted endpoint without a listener: a client that dials
    /// registered peers ([`Self::register_peer`], [`Self::connect_peer`]) and
    /// is reachable only over the sessions it opened.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcAdmissionError`],
    /// [`IpcConfigError`] or local [`NodeIdError`] before any worker starts,
    /// and fails without a Tokio runtime.
    pub fn dial_only_admitted(
        local: NodeId,
        config: IpcConfig,
        admission: IpcAdmission,
    ) -> io::Result<Self> {
        admission.validate()?;
        Self::start(None, Preflight::new(local, config)?, Some(admission))
    }

    /// Opens the listener, when there is one, for an already validated
    /// configuration, local identity, initial peer book and admission, and
    /// starts the workers.
    pub(crate) fn start(
        address: Option<&IpcAddress>,
        preflight: Preflight,
        admission: Option<IpcAdmission>,
    ) -> io::Result<Self> {
        let Preflight {
            local,
            config,
            peers,
            seeds,
        } = preflight;
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("IPC requires a Tokio runtime"))?;
        let listener = address
            .map(|address| platform::Listener::bind(address, config.max_sessions.get()))
            .transpose()?;
        let admission = admission.map(|admission| {
            admitted::Managed::new(admission, SessionRegistry::new(config.max_peers), seeds)
        });
        let (incoming, inbox) = mpsc::channel(config.inbound_queue.get());
        let (commands, receiver) = mpsc::channel(config.max_sessions.get());
        let (finished, done) = watch::channel(false);
        let state = Arc::new(State {
            local,
            config,
            book: Mutex::new(Book {
                peers,
                ..Book::default()
            }),
            slots: Arc::new(Semaphore::new(config.max_sessions.get())),
            inbox: AsyncMutex::new(inbox),
            incoming,
            commands,
            cancel: CancellationToken::new(),
            admission,
        });
        tokio::spawn(runtime::run(
            Arc::clone(&state),
            listener,
            receiver,
            finished,
        ));
        Ok(Self {
            handle: Arc::new(Handle { state, done }),
        })
    }

    /// Registers or replaces the local address of a peer.
    ///
    /// Replacing an address invalidates the existing outbound queue; the next
    /// send opens a connection to the new address. Advertised strings are not
    /// automatically trusted or added to this local-only address book.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`IpcPeerError`] for an invalid
    /// identity or platform address, the local node or a full peer book, and
    /// fails once the transport is closed.
    pub fn register_peer(&self, node: NodeId, address: IpcAddress) -> io::Result<()> {
        frame::validate_id(&node).map_err(IpcPeerError::Identity)?;
        platform::validate_address(&address).map_err(IpcPeerError::Address)?;
        let state = &self.handle.state;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if node == state.local {
            return Err(IpcPeerError::Local.into());
        }
        let mut book = state
            .book
            .lock()
            .map_err(|_| io::Error::other("IPC book poisoned"))?;
        if !book.peers.contains_key(&node) && book.peers.len() >= state.config.max_peers.get() {
            return Err(IpcPeerError::PeerLimit {
                max_peers: state.config.max_peers,
            }
            .into());
        }
        if book.peers.get(&node) != Some(&address) {
            book.sessions.remove(&node);
        }
        book.peers.insert(node, address);
        Ok(())
    }

    /// Starts the join with a registered peer without sending a frame, unless
    /// a session or a dial to it already exists. An unregistered peer is
    /// ignored, following the best-effort send contract.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying [`IpcConnectPeerError`] for a static
    /// transport, and `BrokenPipe` once the transport is closed.
    pub fn connect_peer(&self, node: &NodeId) -> io::Result<()> {
        if self.handle.state.admission.is_none() {
            return Err(IpcConnectPeerError::AdmissionNotConfigured.into());
        }
        self.handle.state.enqueue(node, 0, None::<fn() -> Bytes>)
    }

    /// The peers registered in the address book so far.
    pub(crate) fn registered_peers(&self) -> Vec<NodeId> {
        self.handle
            .state
            .book
            .lock()
            .map(|book| book.peers.keys().cloned().collect())
            .unwrap_or_default()
    }

    /// The live admitted peers of an admitted transport; `None` for a static
    /// one. Revoking a peer here ends its connection.
    #[must_use]
    pub fn sessions(&self) -> Option<&SessionRegistry> {
        self.handle
            .state
            .admission
            .as_ref()
            .map(|managed| &managed.sessions)
    }

    /// Cancels listener, connection setup, reads and writes and waits for cleanup.
    ///
    /// Closing one handle closes every clone. Pending and future receives return
    /// `BrokenPipe`; queued messages are discarded.
    pub async fn close(&self) {
        self.handle.state.cancel.cancel();
        let mut done = self.handle.done.clone();
        while !*done.borrow_and_update() {
            if done.changed().await.is_err() {
                break;
            }
        }
    }
}

impl LinkLifecycle for IpcTransport {
    fn shutdown(&self) {
        self.handle.state.cancel.cancel();
    }

    fn close(&self) -> LinkFuture<'_, ()> {
        Box::pin(Self::close(self))
    }
}

impl Transport for IpcTransport {
    type Error = io::Error;

    fn send(&self, to: &NodeId, msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(self.handle.state.enqueue(
            to,
            msg.len(),
            Some(|| Bytes::copy_from_slice(msg)),
        ))
    }

    fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(match (&self.handle.state.admission, session) {
            (Some(_), session) => self.handle.state.send_session(to, msg, session),
            (None, Some(_)) => Ok(()),
            (None, None) => self.handle.state.enqueue(to, msg.len(), Some(|| msg)),
        })
    }

    async fn recv(&self) -> io::Result<Inbound> {
        self.recv_admitted().await.map(|admitted| admitted.packet)
    }

    async fn recv_admitted(&self) -> io::Result<AdmittedInbound> {
        let state = &self.handle.state;
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => Err(closed()),
            result = async { state.inbox.lock().await.recv().await } => result.ok_or_else(closed),
        }
    }
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "IPC transport closed")
}

/// A configuration, local identity and initial peer book that passed every
/// check, so [`IpcTransport::start`] opens the listener without validating
/// anything a second time.
#[derive(Debug)]
pub(crate) struct Preflight {
    local: NodeId,
    config: IpcConfig,
    peers: HashMap<NodeId, IpcAddress>,
    /// Configured link peers in their configured order, which an admitted
    /// transport dials while they hold no session.
    seeds: Vec<NodeId>,
}

impl Preflight {
    /// Checks `config` and the local identity's 64-byte IPC bound.
    fn new(local: NodeId, config: IpcConfig) -> io::Result<Self> {
        config.validate()?;
        frame::validate_id(&local)?;
        Ok(Self {
            local,
            config,
            peers: HashMap::new(),
            seeds: Vec::new(),
        })
    }

    /// Checks a link's peers for `local` with [`LinkConfig::validate_for`]
    /// (valid, distinct, not local), `config` and the local identity, then
    /// every peer as [`IpcTransport::register_peer`] would on a fresh
    /// transport: the 64-byte identity bound, `max_peers` and the platform
    /// address. `addresses[i]` belongs to `link.peers[i]`.
    pub(crate) fn for_link(
        local: NodeId,
        config: IpcConfig,
        link: &LinkConfig,
        addresses: Vec<IpcAddress>,
    ) -> io::Result<Self> {
        link.validate_for(&local)?;
        let mut preflight = Self::new(local, config)?;
        if link.peers.len() > config.max_peers.get() {
            return Err(IpcPeerError::PeerLimit {
                max_peers: config.max_peers,
            }
            .into());
        }
        for (peer, address) in link.peers.iter().zip(addresses) {
            frame::validate_id(peer).map_err(IpcPeerError::Identity)?;
            platform::validate_address(&address).map_err(IpcPeerError::Address)?;
            preflight.peers.insert(peer.clone(), address);
        }
        preflight.seeds.clone_from(&link.peers);
        Ok(preflight)
    }
}

/// Why a peer cannot enter an [`IpcTransport`]'s address book.
#[derive(Debug)]
pub enum IpcPeerError {
    /// The peer's identity is empty or longer than the 64-byte IPC bound.
    Identity(NodeIdError),
    /// The peer's address breaks the platform's local-only rules.
    Address(IpcAddressError),
    /// The peer is the transport's own identity.
    Local,
    /// The address book already holds `max_peers` other peers, or a link
    /// configures more peers than that.
    PeerLimit {
        /// The configured [`IpcConfig::max_peers`].
        max_peers: NonZeroUsize,
    },
}

impl fmt::Display for IpcPeerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Identity(error) => write!(f, "peer identity rejected: {error}"),
            Self::Address(error) => write!(f, "peer address rejected: {error}"),
            Self::Local => f.write_str("an IPC transport cannot register itself as a peer"),
            Self::PeerLimit { max_peers } => {
                write!(f, "IPC peers exceed the max_peers bound of {max_peers}")
            }
        }
    }
}

impl error::Error for IpcPeerError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Address(error) => Some(error),
            Self::Local | Self::PeerLimit { .. } => None,
        }
    }
}

impl From<IpcPeerError> for io::Error {
    fn from(error: IpcPeerError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why an [`IpcTransport`] refused to send. Converts into an `InvalidInput`
/// `io::Error` that keeps this error as its inner error.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IpcSendError {
    /// The message is longer than [`MAX_FRAME`].
    PayloadTooLarge {
        /// The message's length in bytes.
        len: usize,
        /// The largest message accepted, [`MAX_FRAME`].
        max: usize,
    },
}

impl fmt::Display for IpcSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PayloadTooLarge { len, max } => {
                write!(
                    f,
                    "IPC message of {len} bytes exceeds the {max}-byte frame limit"
                )
            }
        }
    }
}

impl error::Error for IpcSendError {}

impl From<IpcSendError> for io::Error {
    fn from(error: IpcSendError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests;
