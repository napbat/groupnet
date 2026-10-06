//! Message acceptance, independent retransmission and session lifetime binding.

use std::{
    collections::BTreeMap,
    io,
    sync::{Arc, Mutex},
};

use bytes::Bytes;
use futures_util::io::AsyncReadExt;
use groupnet_core::NodeId;
use groupnet_network::{ProtocolIo, tunnel::TunneledStream};
use tokio::{
    sync::{Mutex as AsyncMutex, Semaphore, mpsc, oneshot},
    time::Instant,
};
use tokio_util::sync::CancellationToken;

use super::{
    UnorderedConfig, UnorderedDelivery, aborted,
    endpoint::Lease,
    wire::{Crypto, Kind, SessionId, Window},
};

#[derive(Debug)]
struct ReceiveState {
    accepted: Window,
    last_seen: Instant,
}

#[derive(Debug)]
struct SendState {
    next_message: u64,
    pending: BTreeMap<u64, oneshot::Sender<()>>,
}

#[derive(Debug)]
pub(super) struct State {
    pub(super) peer: NodeId,
    pub(super) id: SessionId,
    pub(super) delivery: UnorderedDelivery,
    pub(super) config: UnorderedConfig,
    pub(super) max_payload: usize,
    pub(super) io: ProtocolIo,
    pub(super) cancel: CancellationToken,
    pub(super) endpoint_cancel: CancellationToken,
    stopped: CancellationToken,
    crypto: Mutex<Crypto>,
    receive: Mutex<ReceiveState>,
    inbox: mpsc::Sender<Bytes>,
    send: Mutex<SendState>,
    slots: Arc<Semaphore>,
    lease: Mutex<Option<Lease>>,
}

#[derive(Debug)]
struct Handle {
    state: Arc<State>,
    inbox: AsyncMutex<mpsc::Receiver<Bytes>>,
}

impl Drop for Handle {
    fn drop(&mut self) {
        self.state.cancel.cancel();
    }
}

/// A shared authenticated message-oriented session. The last handle's drop closes it.
/// Concurrent sends are independent; cloning does not create another peer socket.
#[derive(Clone, Debug)]
pub struct UnorderedSession {
    handle: Arc<Handle>,
}

impl UnorderedSession {
    /// Original pinned peer identity, not a forwarding neighbor.
    #[must_use]
    pub fn peer(&self) -> &NodeId {
        &self.handle.state.peer
    }

    pub(super) fn handle_cancelled(&self) -> bool {
        self.handle.state.cancel.is_cancelled() || self.handle.state.endpoint_cancel.is_cancelled()
    }

    /// Fresh authenticated session identity shared by both endpoints.
    #[must_use]
    pub fn session_id(&self) -> [u8; 16] {
        self.handle.state.id
    }

    /// Explicitly agreed policy; no implicit downgrade occurs.
    #[must_use]
    pub fn delivery(&self) -> UnorderedDelivery {
        self.handle.state.delivery
    }

    /// Sends one message. Reliable success means bounded peer inbox acceptance.
    /// Unreliable success means local route acceptance only. Dropping this future
    /// stops its retries and cannot report success; timeout leaves delivery unknown.
    /// Reliable sends return `WouldBlock` before allocating a logical ID when
    /// advancing it would evict an unresolved send from the peer's dedup window.
    ///
    /// # Errors
    /// Returns payload/capacity errors, local route errors, cancellation or a finite deadline.
    pub async fn send(&self, payload: Bytes) -> io::Result<()> {
        let state = &self.handle.state;
        state.ensure_open()?;
        if payload.len() > state.max_payload {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unordered message too large",
            ));
        }
        let _slot = state.slots.clone().try_acquire_owned().map_err(|_| {
            io::Error::new(io::ErrorKind::WouldBlock, "unordered pending sends full")
        })?;
        // Allocation and pending registration share the same lock: no concurrent
        // sender can advance the peer's horizon before this ID is protected.
        let (message, accepted) = {
            let mut send = state.send.lock().map_err(|_| poisoned())?;
            let message = send.next_message;
            if send
                .pending
                .first_key_value()
                .is_some_and(|(&oldest, _)| message - oldest >= super::wire::WINDOW as u64)
            {
                return Err(io::Error::new(
                    io::ErrorKind::WouldBlock,
                    "unordered reliable send horizon full",
                ));
            }
            let next = message
                .checked_add(1)
                .ok_or_else(|| io::Error::other("unordered message ids exhausted"))?;
            let accepted = if state.delivery == UnorderedDelivery::Reliable {
                let (ack, accepted) = oneshot::channel();
                send.pending.insert(message, ack);
                Some(accepted)
            } else {
                None
            };
            send.next_message = next;
            (message, accepted)
        };
        let Some(mut accepted) = accepted else {
            return state.transmit(Kind::Data, message, &payload);
        };
        let _pending = Pending {
            state: state.clone(),
            message,
        };
        let deadline = Instant::now() + state.config.send_timeout;
        for _ in 0..state.config.max_attempts {
            state.ensure_open()?;
            if Instant::now() >= deadline {
                return Err(send_timeout());
            }
            match accepted.try_recv() {
                Ok(()) => return Ok(()),
                Err(oneshot::error::TryRecvError::Closed) => return Err(aborted()),
                Err(oneshot::error::TryRecvError::Empty) => {}
            }
            let _attempt = state.transmit(Kind::Data, message, &payload);
            let retry = (Instant::now() + state.config.retry_interval).min(deadline);
            tokio::select! {
                biased;
                () = state.cancel.cancelled() => return Err(aborted()),
                () = state.endpoint_cancel.cancelled() => return Err(aborted()),
                result = &mut accepted => return result.map_err(|_| aborted()),
                () = tokio::time::sleep_until(retry) => {},
            }
        }
        Err(send_timeout())
    }

    /// Receives the next accepted message without waiting for any earlier message.
    ///
    /// # Errors
    /// Returns cancellation/closure even when messages remain buffered after revocation.
    pub async fn recv(&self) -> io::Result<Bytes> {
        let state = &self.handle.state;
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => Err(aborted()),
            () = state.endpoint_cancel.cancelled() => Err(aborted()),
            result = async { self.handle.inbox.lock().await.recv().await } => result.ok_or_else(aborted),
        }
    }

    /// Sends a best-effort authenticated close and waits for local session teardown.
    ///
    /// # Errors
    /// Reports a local close packet error; local cancellation always occurs.
    pub async fn close(&self) -> io::Result<()> {
        let state = &self.handle.state;
        // Cancel before any await, so dropping close cannot leave a live session.
        state.cancel.cancel();
        let result = state.transmit_close();
        state.stopped.cancelled().await;
        result
    }
}

pub(super) struct Parameters {
    pub(super) peer: NodeId,
    pub(super) id: SessionId,
    pub(super) delivery: UnorderedDelivery,
    pub(super) config: UnorderedConfig,
    pub(super) max_payload: usize,
    pub(super) io: ProtocolIo,
    pub(super) endpoint_cancel: CancellationToken,
    pub(super) lease: Lease,
}

pub(super) fn create(
    params: Parameters,
    stream: &TunneledStream,
    keys: &[u8; 64],
    initiator: bool,
) -> io::Result<(Arc<State>, UnorderedSession)> {
    let (inbox, receiver) = mpsc::channel(params.config.inbox_capacity);
    let slots = Arc::new(Semaphore::new(params.config.pending_sends));
    let state = Arc::new(State {
        peer: params.peer,
        id: params.id,
        delivery: params.delivery,
        config: params.config,
        max_payload: params.max_payload,
        io: params.io,
        cancel: stream.cancellation(),
        endpoint_cancel: params.endpoint_cancel,
        stopped: CancellationToken::new(),
        crypto: Mutex::new(Crypto::new(keys, initiator)?),
        receive: Mutex::new(ReceiveState {
            accepted: Window::default(),
            last_seen: Instant::now(),
        }),
        inbox,
        send: Mutex::new(SendState {
            next_message: 1,
            pending: BTreeMap::new(),
        }),
        slots,
        lease: Mutex::new(Some(params.lease)),
    });
    let handle = Arc::new(Handle {
        state: state.clone(),
        inbox: AsyncMutex::new(receiver),
    });
    Ok((state, UnorderedSession { handle }))
}

impl State {
    fn ensure_open(&self) -> io::Result<()> {
        if self.cancel.is_cancelled() || self.endpoint_cancel.is_cancelled() {
            Err(aborted())
        } else {
            Ok(())
        }
    }

    pub(super) fn transmit(&self, kind: Kind, message: u64, body: &[u8]) -> io::Result<()> {
        self.ensure_open()?;
        let packet = self
            .crypto
            .lock()
            .map_err(|_| poisoned())?
            .seal(self.id, kind, message, body)?;
        self.ensure_open()?;
        self.io.send(&self.peer, &packet)
    }

    fn transmit_close(&self) -> io::Result<()> {
        let packet =
            self.crypto
                .lock()
                .map_err(|_| poisoned())?
                .seal(self.id, Kind::Close, 0, &[])?;
        self.io.send(&self.peer, &packet)
    }

    pub(super) fn receive_packet(&self, packet: &[u8]) -> io::Result<()> {
        self.ensure_open()?;
        let (kind, message, body) = self
            .crypto
            .lock()
            .map_err(|_| poisoned())?
            .open(packet, self.max_payload)?;
        let mut reply = None;
        {
            let mut receive = self.receive.lock().map_err(|_| poisoned())?;
            receive.last_seen = Instant::now();
            match kind {
                Kind::Data => {
                    if receive.accepted.too_old(message) {
                        return Ok(());
                    }
                    let already = receive.accepted.contains(message);
                    if already || self.inbox.try_send(body).is_ok() {
                        receive.accepted.insert(message);
                        if self.delivery == UnorderedDelivery::Reliable {
                            reply = Some((Kind::Ack, message));
                        }
                    }
                }
                Kind::Ack if self.delivery == UnorderedDelivery::Reliable => {
                    if let Some(accepted) = self
                        .send
                        .lock()
                        .map_err(|_| poisoned())?
                        .pending
                        .remove(&message)
                    {
                        let _sent = accepted.send(());
                    }
                }
                Kind::Ack => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "data ACK on unreliable session",
                    ));
                }
                Kind::Ping => reply = Some((Kind::Pong, 0)),
                Kind::Pong => {}
                Kind::Close => self.cancel.cancel(),
            }
        }
        if let Some((kind, message)) = reply {
            self.transmit(kind, message, &[])?;
        }
        Ok(())
    }
}

pub(super) async fn lifetime(state: Arc<State>, mut control: TunneledStream) {
    let mut heartbeat = tokio::time::interval(state.config.heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut close = [0; 1];
    loop {
        tokio::select! {
            biased;
            () = state.cancel.cancelled() => break,
            () = state.endpoint_cancel.cancelled() => break,
            _result = control.read(&mut close) => break,
            _tick = heartbeat.tick() => {
                let expired = state.receive.lock().map_or(true, |receive| receive.last_seen.elapsed() >= state.config.idle_timeout);
                if expired { break; }
                let _sent = state.transmit(Kind::Ping, 0, &[]);
            },
        }
    }
    state.cancel.cancel();
    let _closed = state.transmit_close();
    if let Ok(mut send) = state.send.lock() {
        send.pending.clear();
    }
    if let Ok(mut lease) = state.lease.lock() {
        lease.take();
    }
    drop(control);
    state.stopped.cancel();
}

struct Pending {
    state: Arc<State>,
    message: u64,
}

impl Drop for Pending {
    fn drop(&mut self) {
        if let Ok(mut send) = self.state.send.lock() {
            send.pending.remove(&self.message);
        }
    }
}

fn poisoned() -> io::Error {
    io::Error::other("unordered state lock poisoned")
}

fn send_timeout() -> io::Error {
    io::Error::new(
        io::ErrorKind::TimedOut,
        "unordered acceptance unknown at retry deadline",
    )
}
