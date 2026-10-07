//! One bounded supervisor owns every connection task and listener cleanup.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::Inbound;
use tokio::sync::{OwnedSemaphorePermit, mpsc, watch};
use tokio::task::JoinSet;
use tokio::time::timeout;

use super::{IpcAddress, Session, State, frame, platform};

#[derive(Debug)]
pub(super) struct Dial {
    pub node: NodeId,
    pub address: IpcAddress,
    pub generation: u64,
    pub frames: mpsc::Receiver<Bytes>,
    pub slot: OwnedSemaphorePermit,
}

pub(super) async fn run(
    state: Arc<State>,
    mut listener: platform::Listener,
    mut commands: mpsc::Receiver<Dial>,
    finished: watch::Sender<bool>,
) {
    let mut tasks = JoinSet::new();
    loop {
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => break,
            result = tasks.join_next(), if !tasks.is_empty() => {
                if result.is_some_and(|result| result.is_err()) {
                    state.cancel.cancel();
                }
            }
            accepted = listener.accept() => {
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
        }
    }
    state.cancel.cancel();
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

async fn dial(state: Arc<State>, command: Dial) {
    let Dial {
        node,
        address,
        generation,
        frames,
        slot,
    } = command;
    let operation = async {
        let mut stream = platform::connect(&address).await?;
        let remote = frame::introduce(&mut stream, &state.local).await?;
        if remote != node || remote == state.local {
            return Err(io::Error::new(
                io::ErrorKind::PermissionDenied,
                "IPC peer ID mismatch",
            ));
        }
        Ok(stream)
    };
    let stream = tokio::select! {
        biased;
        () = state.cancel.cancelled() => None,
        result = timeout(state.config.setup_timeout, operation) => result.ok().and_then(Result::ok),
    };
    if let Some(stream) = stream {
        session(&state, stream, node.clone(), frames).await;
    }
    remove(&state, &node, generation);
    drop(slot);
}

async fn accept(state: Arc<State>, mut stream: platform::Stream, slot: OwnedSemaphorePermit) {
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
                });
            }
            generation
        };
        // Keep the unused writer queue open if an outbound connection won the
        // race, so bidirectional reads do not terminate prematurely.
        session(&state, stream, remote.clone(), frames).await;
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

async fn session(
    state: &State,
    stream: platform::Stream,
    remote: NodeId,
    mut frames: mpsc::Receiver<Bytes>,
) {
    let (mut reader, mut writer) = tokio::io::split(stream);
    let read = read_messages(state, &mut reader, remote);
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
        _ = read => {}
        _ = write => {}
    }
}

async fn read_messages<R: tokio::io::AsyncRead + Unpin>(
    state: &State,
    reader: &mut R,
    remote: NodeId,
) -> io::Result<()> {
    loop {
        let msg = timeout(state.config.read_timeout, frame::read(reader))
            .await
            .map_err(io::Error::other)??;
        state
            .incoming
            .send(Inbound {
                from: remote.clone(),
                msg,
            })
            .await
            .map_err(io::Error::other)?;
    }
}
