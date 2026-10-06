//! Persistent native local IPC with bounded, shared framing.
//!
//! Unix sockets require a private directory (no group/other permissions), owned
//! by the socket creator. Existing paths are never removed to make binding work.
//! Windows uses local named pipes, rejects remote clients, and reserves the first
//! pipe instance. Peer identities are trusted within this OS security boundary;
//! this transport is not a cryptographic identity layer.

mod frame;
mod platform;
mod runtime;

use std::collections::HashMap;
use std::io;
#[cfg(unix)]
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use groupnet_core::NodeId;
use groupnet_transport::link::{LinkFuture, LinkLifecycle};
use groupnet_transport::{Inbound, Transport};
use tokio::sync::{Mutex as AsyncMutex, Semaphore, mpsc, watch};
use tokio_util::sync::CancellationToken;

/// Largest message accepted by the IPC framing protocol, in bytes.
pub const MAX_FRAME: usize = 65_000;
const MAX_PEERS: usize = 128;
const MAX_SESSIONS: usize = 64;
const SESSION_QUEUE: usize = 16;
const INBOUND_QUEUE: usize = 64;

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
    sender: mpsc::Sender<Vec<u8>>,
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
/// There are at most 128 registered peers, 64 sessions (including connection
/// setup), 16 queued messages per session, and 64 queued inbound messages.
/// Unknown peers, busy connections and full outbound queues drop messages under
/// the best-effort [`Transport`] contract. Each session introduces its node ID
/// once and can carry messages in either direction without a second connection.
/// Dropping the last handle cancels all workers; [`Self::close`] also waits for
/// workers to stop and for the owned listener to be released.
#[derive(Clone, Debug)]
pub struct IpcTransport {
    handle: Arc<Handle>,
}

impl IpcTransport {
    /// Binds a native listener and starts its bounded Tokio runtime.
    ///
    /// # Errors
    /// Returns an error for empty/over-64-byte node IDs, insecure or occupied
    /// addresses, missing Tokio runtime, and native listener failures. Unix
    /// callers must create their own private directory before binding.
    pub fn bind(local: NodeId, address: &IpcAddress) -> io::Result<Self> {
        frame::validate_id(&local)?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::other("IPC requires a Tokio runtime"))?;
        let listener = platform::Listener::bind(address)?;
        let (incoming, inbox) = mpsc::channel(INBOUND_QUEUE);
        let (commands, receiver) = mpsc::channel(MAX_SESSIONS);
        let (finished, done) = watch::channel(false);
        let state = Arc::new(State {
            local,
            book: Mutex::new(Book::default()),
            slots: Arc::new(Semaphore::new(MAX_SESSIONS)),
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
    /// Rejects invalid IDs/addresses, the local node, a full peer book, or a
    /// closed transport.
    pub fn register_peer(&self, node: NodeId, address: IpcAddress) -> io::Result<()> {
        frame::validate_id(&node)?;
        platform::validate_address(&address)?;
        let state = &self.handle.state;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if node == state.local {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "cannot register self",
            ));
        }
        let mut book = state
            .book
            .lock()
            .map_err(|_| io::Error::other("IPC book poisoned"))?;
        if !book.peers.contains_key(&node) && book.peers.len() == MAX_PEERS {
            return Err(io::Error::other("IPC peer limit reached"));
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

    fn enqueue(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        let state = &self.handle.state;
        if state.cancel.is_cancelled() {
            return Err(closed());
        }
        if msg.len() > MAX_FRAME {
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
                    permit.send(msg.to_vec());
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
        let (sender, frames) = mpsc::channel(SESSION_QUEUE);
        let generation = book.generation();
        // The newly allocated queue is empty, so reservation cannot fail.
        sender
            .try_send(msg.to_vec())
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
        std::future::ready(self.enqueue(to, msg))
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
