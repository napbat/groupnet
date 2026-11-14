//! One bounded supervisor owns every connection task and listener cleanup.

use std::io;
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use groupnet_transport::admission::SessionLease;
use groupnet_transport::link::AdmittedInbound;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::{MissedTickBehavior, interval, timeout};

use super::{IpcAddress, Session, State, frame, platform};

/// How often an admitted transport dials the seeds that hold no session.
const SEED_INTERVAL: Duration = Duration::from_secs(1);

#[derive(Debug)]
pub(super) struct Dial {
    pub node: NodeId,
    pub address: IpcAddress,
    pub generation: u64,
    pub frames: mpsc::Receiver<Bytes>,
    pub slot: OwnedSemaphorePermit,
    /// Fires when a simultaneous inbound connection replaced this provisional
    /// admitted dial; `None` on a static transport.
    pub cancelled: Option<watch::Receiver<bool>>,
}

pub(super) async fn run(
    state: Arc<State>,
    mut listener: Option<platform::Listener>,
    mut commands: mpsc::Receiver<Dial>,
    finished: watch::Sender<bool>,
) {
    let mut tasks = JoinSet::new();
    let seeding = state
        .admission
        .as_ref()
        .is_some_and(|managed| !managed.seeds.is_empty());
    let mut seeds = interval(SEED_INTERVAL);
    seeds.set_missed_tick_behavior(MissedTickBehavior::Skip);
    let mut first_seed = 0;
    loop {
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => break,
            result = tasks.join_next(), if !tasks.is_empty() => {
                if result.is_some_and(|result| result.is_err()) {
                    state.cancel.cancel();
                }
            }
            accepted = accept_next(listener.as_mut()) => {
                if let Ok(stream) = accepted {
                    if let Ok(slot) = Arc::clone(&state.slots).try_acquire_owned() {
                        tasks.spawn(accept(Arc::clone(&state), stream, slot));
                    }
                } else {
                    state.cancel.cancel();
                    break;
                }
            }
            command = commands.recv() => {
                if let Some(command) = command {
                    tasks.spawn(dial(Arc::clone(&state), command));
                } else {
                    state.cancel.cancel();
                    break;
                }
            }
            _ = seeds.tick(), if seeding => first_seed = dial_seeds(&state, first_seed),
        }
    }
    state.cancel.cancel();
    if let Some(managed) = &state.admission {
        managed.sessions.close();
    }
    commands.close();
    // Setup and session futures all select cancellation, so no worker can
    // retain the listener or an outstanding OS operation past close().
    while tasks.join_next().await.is_some() {}
    if let Ok(mut book) = state.book.lock() {
        book.sessions.clear();
    }
    let mut inbox = state.inbox.lock().await;
    inbox.close();
    while inbox.try_recv().is_ok() {}
    drop(inbox);
    drop(commands);
    drop(listener);
    finished.send_replace(true);
}

/// The next inbound connection, or never for a transport without a listener.
async fn accept_next(listener: Option<&mut platform::Listener>) -> io::Result<platform::Stream> {
    match listener {
        Some(listener) => listener.accept().await,
        None => std::future::pending().await,
    }
}

/// Starts a join with every seed that holds no session, beginning at
/// `first` so an unreachable prefix cannot hold every dial slot. Returns where
/// the next round starts.
fn dial_seeds(state: &State, first: usize) -> usize {
    let Some(managed) = &state.admission else {
        return first;
    };
    let seeds = &managed.seeds;
    for offset in 0..seeds.len() {
        let _ = state.enqueue(
            &seeds[(first + offset) % seeds.len()],
            0,
            None::<fn() -> Bytes>,
        );
    }
    let step = state.config.max_sessions.get().min(seeds.len());
    (first + step) % seeds.len()
}

async fn dial(state: Arc<State>, command: Dial) {
    let Dial {
        node,
        address,
        generation,
        frames,
        slot,
        cancelled,
    } = command;
    let operation = async {
        let mut stream = platform::connect(&address).await?;
        if let Some(managed) = &state.admission {
            let lease = managed.dial(&state, &mut stream, &node, generation).await?;
            return Ok((stream, Some(lease)));
        }
        let remote = frame::introduce(&mut stream, &state.local).await?;
        if remote != node || remote == state.local {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "IPC peer ID mismatch",
            ));
        }
        Ok((stream, None))
    };
    let established = tokio::select! {
        biased;
        () = state.cancel.cancelled() => None,
        () = replaced(cancelled) => None,
        result = timeout(state.config.setup_timeout, operation) => result.ok().and_then(Result::ok),
    };
    if let Some((stream, lease)) = established {
        session(&state, stream, node.clone(), frames, lease).await;
    }
    remove(&state, &node, generation);
    drop(slot);
}

/// Resolves once a simultaneous inbound connection replaced a provisional
/// dial; never for a static dial. The sender also drops when the join
/// completes and the entry stops being provisional, which is no replacement:
/// a replacement always sends `true` first.
async fn replaced(cancelled: Option<watch::Receiver<bool>>) {
    if let Some(mut cancelled) = cancelled
        && cancelled.wait_for(|cancelled| *cancelled).await.is_ok()
    {
        return;
    }
    std::future::pending::<()>().await;
}

async fn accept(state: Arc<State>, mut stream: platform::Stream, slot: OwnedSemaphorePermit) {
    if let Some(managed) = &state.admission {
        let accepted = tokio::select! {
            biased;
            () = state.cancel.cancelled() => None,
            result = timeout(state.config.setup_timeout, managed.accept(&state, &mut stream)) => {
                result.ok().and_then(Result::ok)
            }
        };
        if let Some(accepted) = accepted {
            let remote = accepted.claim.node.clone();
            session(
                &state,
                stream,
                remote,
                accepted.frames,
                Some(accepted.lease),
            )
            .await;
            // The claim drops here and removes its own entry only.
            drop(accepted.claim);
        }
        drop(slot);
        return;
    }
    let remote = tokio::select! {
        biased;
        () = state.cancel.cancelled() => None,
        result = timeout(
            state.config.setup_timeout,
            frame::introduce(&mut stream, &state.local),
        ) => {
            result.ok().and_then(Result::ok)
        }
    };
    if let Some(remote) = remote.filter(|node| *node != state.local) {
        let (sender, frames) = mpsc::channel(state.config.session_queue.get());
        let generation = {
            let Ok(mut book) = state.book.lock() else {
                return;
            };
            let generation = book.generation();
            // Preserve a simultaneous outbound connection's queue. The inbound
            // socket still reads, but only one session is selected for sends.
            if let std::collections::hash_map::Entry::Vacant(entry) =
                book.sessions.entry(remote.clone())
            {
                entry.insert(Session {
                    generation,
                    sender: sender.clone(),
                    admitted: None,
                    provisional: None,
                });
            }
            generation
        };
        // Keep the unused writer queue open if an outbound connection won the
        // race, so bidirectional reads do not terminate prematurely.
        session(&state, stream, remote.clone(), frames, None).await;
        drop(sender);
        remove(&state, &remote, generation);
    }
    drop(slot);
}

fn remove(state: &State, node: &NodeId, generation: u64) {
    if let Ok(mut book) = state.book.lock() {
        book.remove_session(node, generation);
    }
}

/// Runs one established connection until either direction ends, the
/// transport closes, or (for an admitted session) its generation is revoked.
async fn session(
    state: &State,
    stream: platform::Stream,
    remote: NodeId,
    mut frames: mpsc::Receiver<Bytes>,
    lease: Option<SessionLease>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let read = read_messages(state, &mut reader, remote, lease.as_ref());
    let write = async {
        while let Some(msg) = frames.recv().await {
            timeout(state.config.write_timeout, frame::write(&mut writer, &msg))
                .await
                .map_err(io::Error::other)??;
        }
        Ok::<(), io::Error>(())
    };
    tokio::select! {
        biased;
        () = state.cancel.cancelled() => {}
        () = revoked(state, lease.as_ref()) => {}
        _ = read => {}
        _ = write => {}
    }
}

/// Resolves once an admitted session's generation is no longer live; never
/// for a static session.
async fn revoked(state: &State, lease: Option<&SessionLease>) {
    let (Some(managed), Some(lease)) = (&state.admission, lease) else {
        return std::future::pending().await;
    };
    let mut neighbors = managed.sessions.subscribe();
    while lease.is_active() {
        if neighbors.changed().await.is_err() {
            return;
        }
    }
}

async fn read_messages<R: tokio::io::AsyncRead + Unpin>(
    state: &State,
    reader: &mut R,
    remote: NodeId,
    lease: Option<&SessionLease>,
) -> io::Result<()> {
    loop {
        let msg = timeout(state.config.read_timeout, frame::read(reader))
            .await
            .map_err(io::Error::other)??;
        // A frame read after revocation never reaches the router: it carries
        // a generation that no longer admits its sender.
        if lease.is_some_and(|lease| !lease.is_active()) {
            return Ok(());
        }
        state
            .incoming
            .send(AdmittedInbound {
                packet: Inbound {
                    from: remote.clone(),
                    msg,
                    source: None,
                },
                session: lease.map(SessionLease::id),
            })
            .await
            .map_err(io::Error::other)?;
    }
}
