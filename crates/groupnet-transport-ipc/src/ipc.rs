//! Persistent native local IPC with bounded, shared framing.
//!
//! Unix sockets require a private directory (no group/other permissions), owned
//! by the socket creator. Existing paths are never removed to make binding work.
//! Windows uses local named pipes, rejects remote clients, and reserves the first
//! pipe instance. Peer identities are trusted within this OS security boundary;
//! this transport is not a cryptographic identity layer.

mod address;
mod config;
mod frame;
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
use groupnet_transport::admission::SessionId;
use groupnet_transport::link::{LinkConfig, LinkFuture, LinkLifecycle};
use groupnet_transport::{Inbound, Transport};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Largest message accepted by the IPC framing protocol, in bytes.
pub const MAX_FRAME: usize = 65_000;
pub use address::IpcAddressError;
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
    inbox: AsyncMutex<mpsc::Receiver<Inbound>>,
    incoming: mpsc::Sender<Inbound>,
    commands: mpsc::Sender<runtime::Dial>,
    cancel: CancellationToken,
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
/// contract. Each session introduces its node ID
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
        Self::start(address, Preflight::new(local, config)?)
    }

    /// Opens the listener for an already validated configuration, local
    /// identity and initial peer book, and starts the workers.
    pub(crate) fn start(address: &IpcAddress, preflight: Preflight) -> io::Result<Self> {
        let Preflight {
            local,
            config,
            peers,
        } = preflight;
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("IPC requires a Tokio runtime"))?;
        let listener = platform::Listener::bind(address, config.max_sessions.get())?;
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

    fn enqueue(&self, to: &NodeId, length: usize, msg: impl FnOnce() -> Bytes) -> io::Result<()> {
        let state = &self.handle.state;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if length > MAX_FRAME {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "IPC message exceeds frame limit",
            ));
        }
        let mut book = state
            .book
            .lock()
            .map_err(|_| io::Error::other("IPC book poisoned"))?;
        if let Some(session) = book.sessions.get(to) {
            match session.sender.try_reserve() {
                Ok(permit) => {
                    permit.send(msg());
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
        let generation = book.generation();
        // The newly allocated queue is empty, so reservation cannot fail.
        sender
            .try_send(msg())
            .map_err(|_| io::Error::other("new IPC queue unavailable"))?;
        book.sessions
            .insert(to.clone(), Session { generation, sender });
        command.send(runtime::Dial {
            node: to.clone(),
            address,
            generation,
            frames,
            slot,
        });
        Ok(())
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
        std::future::ready(self.enqueue(to, msg.len(), || Bytes::copy_from_slice(msg)))
    }

    fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<SessionId>,
    ) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(if session.is_some() {
            Ok(())
        } else {
            self.enqueue(to, msg.len(), || msg)
        })
    }

    async fn recv(&self) -> io::Result<Inbound> {
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

#[cfg(test)]
mod tests;
