//! Bounded, opaque application frames over the trusted routing fabric.
//!
//! Raw source attribution trusts admitted peers and transit routers: this plane
//! does not authenticate or encrypt application data. Use pinned TLS tunnels for
//! confidentiality/authentication. `Delivered` means queue reservation; `Applied`
//! means explicit application success, never durable/exactly-once execution.
//! Timeout and route loss after dispatch leave the remote outcome unknown.
//! Retained duplicate records cover the configured retry window, not arbitrarily
//! delayed traffic or process crashes. Abandoned pending receipts are interrupted
//! after an inactive retention horizon and retained for another horizon.
//! No ordering is promised.
//!
//! The [`codec`] module is pure sans-IO; this crate owns all acknowledgement,
//! retry and duplicate state. The network router carries its packets opaquely.

/// Sans-IO application message identities, receipt transitions and bounded codec.
pub mod codec;

mod config;
pub use config::MessagingConfig;
mod received;
mod worker;

#[cfg(test)]
mod tests;

use groupnet_core::{GroupId, NodeId};
use groupnet_network::{ProtocolId, ProtocolIo, Router};
use ring::rand::{SecureRandom, SystemRandom};
use std::{
    collections::HashMap,
    io,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, mpsc, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub use bytes::Bytes;
pub use codec::{
    DEFAULT_DEDUP_RETENTION_MS, DEFAULT_MAX_MESSAGE_BYTES, DEFAULT_MAX_TIMEOUT_MS, DecodeError,
    Delivery, MessageId, Outcome, Packet, ReceiptState, Rejection, ack, completes, data, decode,
    destination_valid,
};

/// Static-dispatch application messaging with protocol-specific delivery receipts.
///
/// Implementations own a single shared inbox; cloning an endpoint must not create
/// a competing dispatcher. Receipt semantics belong to the implementation.
pub trait MessageProtocol: Send + Sync {
    /// Exclusive application namespace carried by the router.
    const ID: ProtocolId;
    /// Explicit protocol-specific send policy.
    type SendOptions: Send;
    /// Owned receipt retained independently of payload ownership.
    type Receipt: Send;

    /// Sends owned bytes to a logical destination and optional group.
    fn send(
        &self,
        to: &NodeId,
        group: Option<&GroupId>,
        payload: Bytes,
        options: Self::SendOptions,
    ) -> impl Future<Output = io::Result<MessageId>> + Send;

    /// Receives the complete owned frame, including its receipt.
    fn recv(&self) -> impl Future<Output = io::Result<Frame<Self::Receipt>>> + Send;

    /// Initiates endpoint-local shutdown.
    fn shutdown(&self);

    /// Waits for shutdown and owned worker termination.
    fn closed(&self) -> impl Future<Output = ()> + Send;
}
/// Delivery boundary and finite acknowledgement deadline.
#[derive(Clone, Copy, Debug)]
pub struct SendOptions {
    /// Receiver acknowledgement boundary; defaults to local best effort.
    pub delivery: Delivery,
    /// Acknowledged sends require a nonzero deadline within the endpoint's configuration.
    pub timeout: Duration,
}

impl Default for SendOptions {
    fn default() -> Self {
        Self {
            delivery: Delivery::BestEffort,
            timeout: Duration::from_secs(5),
        }
    }
}

/// Owned application frame; callbacks may move the buffer without cloning it.
#[derive(Debug)]
pub struct Frame<R = Receipt> {
    /// Original application identity, stable across acknowledged retries.
    pub id: MessageId,
    /// Routed origin attributed by the trusted fabric, not an authenticated identity.
    pub from: NodeId,
    /// Group destination, or `None` for the node inbox.
    pub group: Option<GroupId>,
    /// Owned, reference-counted opaque payload.
    pub payload: Bytes,
    /// Protocol-specific receipt, independent of payload ownership.
    pub receipt: R,
}

impl<R> Frame<R> {
    /// Splits this frame into its complete context and owned payload.
    ///
    /// Moves every field without copying the payload or cloning metadata.
    /// The context retains the receipt independently of the payload's lifetime.
    #[must_use]
    pub fn into_parts(self) -> (MessageContext<R>, Bytes) {
        let Self {
            id,
            from,
            group,
            payload,
            receipt,
        } = self;
        (
            MessageContext {
                id,
                from,
                group,
                receipt,
            },
            payload,
        )
    }
}

impl<R: Clone> Frame<R> {
    /// Clones the protocol-specific receipt without cloning the payload.
    #[must_use]
    pub fn receipt(&self) -> R {
        self.receipt.clone()
    }
}

impl Frame {
    /// The sender's requested delivery boundary.
    #[must_use]
    pub fn delivery(&self) -> Delivery {
        self.receipt.delivery()
    }

    /// Explicitly acknowledges successful application of this frame.
    /// # Errors
    /// Returns shutdown/state errors; transient ACK enqueue failure is replayed on retry.
    pub fn applied(&self) -> io::Result<()> {
        self.receipt.applied()
    }
}

/// Complete frame context, independent of ownership of the payload.
///
/// Returned with `Bytes` by runtime buffer receives. Retains all frame metadata
/// and its receipt; moving or dropping the payload does not acknowledge work.
#[derive(Debug)]
pub struct MessageContext<R = Receipt> {
    /// Original message identity, stable across acknowledged retries.
    pub id: MessageId,
    /// Original routed sender, not the forwarding bridge.
    /// Attribution trusts the fabric; this is not an authenticated identity.
    pub from: NodeId,
    /// Group destination, or `None` for the node inbox.
    pub group: Option<GroupId>,
    /// Protocol-specific receipt retained independently of the payload.
    pub receipt: R,
}

impl<R: Clone> MessageContext<R> {
    /// Clones the protocol-specific receipt independently of payload ownership.
    #[must_use]
    pub fn receipt(&self) -> R {
        self.receipt.clone()
    }
}

impl MessageContext {
    /// The sender's requested delivery boundary.
    #[must_use]
    pub fn delivery(&self) -> Delivery {
        self.receipt.delivery()
    }

    /// Explicitly acknowledges successful application processing.
    ///
    /// # Errors
    /// Returns shutdown/state errors; transient ACK enqueue failure is replayed on retry.
    pub fn applied(&self) -> io::Result<()> {
        self.receipt.applied()
    }

    /// Rejects processing while preserving an earlier terminal outcome.
    ///
    /// # Errors
    /// Returns shutdown/state errors, not transient ACK enqueue failure.
    /// Error kinds without a dedicated wire representation become `Other` remotely.
    pub fn reject(&self, kind: io::ErrorKind) -> io::Result<()> {
        self.receipt.reject(kind)
    }
}

#[derive(Debug)]
struct Record {
    from: NodeId,
    id: MessageId,
    delivery: Delivery,
    retry_horizon_ms: u64,
    group: Option<GroupId>,
    fingerprint: [u8; 32],
    state: Mutex<ReceiptState>,
}

/// Idempotent queue/application receipt preserving the first terminal outcome.
/// A receipt does not retain the messaging owner's lifetime. Best-effort frames
/// carry no record: their receipt actions are no-ops.
#[derive(Clone, Debug)]
pub struct Receipt {
    tracked: Option<Tracked>,
}

/// An acknowledged frame's retained receive record.
#[derive(Clone, Debug)]
struct Tracked {
    owner: Weak<Inner>,
    record: Arc<Record>,
}

impl Receipt {
    /// Acknowledges that the runtime has reserved its application queue.
    /// # Errors
    /// Returns shutdown/state errors, not transient ACK enqueue failure.
    pub fn accepted(&self) -> io::Result<()> {
        self.act(Outcome::Accepted)
    }

    /// Acknowledges successful application; implicitly includes queue acceptance.
    /// # Errors
    /// Returns shutdown/state errors, not transient ACK enqueue failure.
    pub fn applied(&self) -> io::Result<()> {
        self.act(Outcome::Applied)
    }

    /// Rejects the frame, preserving an earlier terminal success or failure.
    /// Error kinds without a dedicated wire representation become `Other` remotely.
    /// # Errors
    /// Returns shutdown/state errors, not transient ACK enqueue failure.
    pub fn reject(&self, kind: io::ErrorKind) -> io::Result<()> {
        let rejection = match kind {
            io::ErrorKind::WouldBlock => Rejection::Full,
            io::ErrorKind::NotConnected | io::ErrorKind::BrokenPipe => Rejection::Closed,
            io::ErrorKind::PermissionDenied | io::ErrorKind::NotFound => Rejection::Permission,
            io::ErrorKind::InvalidInput | io::ErrorKind::InvalidData => Rejection::Invalid,
            io::ErrorKind::Interrupted => Rejection::Interrupted,
            _ => Rejection::Other,
        };
        self.act(Outcome::Rejected(rejection))
    }

    fn delivery(&self) -> Delivery {
        self.tracked
            .as_ref()
            .map_or(Delivery::BestEffort, |tracked| tracked.record.delivery)
    }

    fn act(&self, action: Outcome) -> io::Result<()> {
        let Some(Tracked { owner, record }) = &self.tracked else {
            return Ok(());
        };
        let owner = owner.upgrade().ok_or_else(closed)?;
        if owner.cancel.is_cancelled() {
            return Err(closed());
        }
        let outcome = {
            let now = owner.now();
            let mut state = record.state.lock().map_err(|_| poisoned())?;
            state.retire(now);
            if state.terminal() {
                return Ok(());
            }
            state.act(action, now)
        };
        if let Some(outcome) = outcome {
            // The outcome is recorded before transmission. Transient ACK
            // backpressure/route loss must not fail successful application work;
            // a duplicate data packet replays this retained receipt.
            let _ = owner.acknowledge(&record.from, record.id, outcome);
        }
        Ok(())
    }
}

#[derive(Debug)]
struct Pending {
    to: NodeId,
    delivery: Delivery,
    result: oneshot::Sender<io::Result<()>>,
}

#[derive(Debug, Default)]
struct State {
    pending: HashMap<MessageId, Pending>,
    received: received::Received,
}

#[derive(Debug)]
struct Inner {
    io: ProtocolIo,
    state: Mutex<State>,
    incoming: mpsc::Sender<Frame>,
    receiver: AsyncMutex<mpsc::Receiver<Frame>>,
    cancel: CancellationToken,
    tasks: TaskTracker,
    nonce: [u8; 8],
    sequence: AtomicU64,
    epoch: tokio::time::Instant,
    config: MessagingConfig,
    retention_ms: u64,
    max_timeout_ms: u64,
}

impl Inner {
    fn now(&self) -> u64 {
        u64::try_from(self.epoch.elapsed().as_millis()).unwrap_or(u64::MAX)
    }

    /// Encodes and routes one attempt of an already validated data packet.
    fn dispatch(&self, to: &NodeId, frame: &codec::DataFrame<'_>) -> io::Result<()> {
        let mut packet = self.io.packet_buffer(to, frame.len())?;
        frame.encode(|part| packet.extend_from_slice(part));
        self.io.send_packet(to, packet)
    }

    fn acknowledge(&self, to: &NodeId, id: MessageId, outcome: Outcome) -> io::Result<()> {
        let mut packet = self.io.packet_buffer(to, codec::ack_len(outcome))?;
        codec::encode_ack(id, outcome, |part| packet.extend_from_slice(part));
        self.io.send_packet(to, packet)
    }

    fn next_id(&self) -> io::Result<MessageId> {
        let sequence = self
            .sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_add(1))
            .map_err(|_| {
                error(
                    io::ErrorKind::Other,
                    "application message identity exhausted",
                )
            })?;
        let mut id = [0; 16];
        id[..8].copy_from_slice(&self.nonce);
        id[8..].copy_from_slice(&sequence.to_be_bytes());
        Ok(MessageId(id))
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// Shared application-plane owner; the last public clone cancels its worker.
#[derive(Clone, Debug)]
pub struct Messaging {
    inner: Arc<Inner>,
}

impl Messaging {
    /// Exclusively binds the messaging application namespace.
    /// # Errors
    /// Rejects a missing executor, closed router, unavailable randomness or claimed inbox.
    pub fn new(router: &Router) -> io::Result<Self> {
        Self::with_config(router, MessagingConfig::default())
    }

    /// Binds messaging with node-wide queue, retry and retention bounds.
    ///
    /// # Errors
    /// Rejects invalid configuration, unavailable executor/randomness or claimed namespace.
    pub fn with_config(router: &Router, config: MessagingConfig) -> io::Result<Self> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|_| closed())?;
        if router.is_closed() {
            return Err(closed());
        }
        let mut nonce = [0; 8];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| io::Error::other("OS randomness unavailable"))?;
        let io = router.bind_protocol(Self::ID)?;
        Ok(Self::bind(io, nonce, config))
    }

    /// Creates a messaging implementation on its pre-bound namespace.
    /// # Errors
    /// Rejects a wrong namespace, unavailable executor or OS randomness.
    pub fn from_io(io: ProtocolIo) -> io::Result<Self> {
        Self::from_io_with_config(io, MessagingConfig::default())
    }

    /// Creates messaging on a pre-bound namespace with explicit endpoint bounds.
    ///
    /// # Errors
    /// Rejects invalid bounds, wrong namespace, unavailable executor or randomness.
    pub fn from_io_with_config(io: ProtocolIo, config: MessagingConfig) -> io::Result<Self> {
        config.validate()?;
        tokio::runtime::Handle::try_current().map_err(|_| closed())?;
        if io.id() != Self::ID {
            return Err(error(
                io::ErrorKind::InvalidInput,
                "wrong messaging namespace",
            ));
        }
        let mut nonce = [0; 8];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| io::Error::other("OS randomness unavailable"))?;
        Ok(Self::bind(io, nonce, config))
    }

    fn bind(io: ProtocolIo, nonce: [u8; 8], config: MessagingConfig) -> Self {
        let cancel = io.cancellation();
        let receive = io.clone();
        let (incoming, receiver) = mpsc::channel(config.inbox_capacity.get());
        let inner = Arc::new(Inner {
            io,
            state: Mutex::new(State::default()),
            incoming,
            receiver: AsyncMutex::new(receiver),
            cancel: cancel.clone(),
            tasks: TaskTracker::new(),
            nonce,
            sequence: AtomicU64::new(0),
            epoch: tokio::time::Instant::now(),
            retention_ms: config.retention_ms(),
            max_timeout_ms: config.max_timeout_ms(),
            config,
        });
        inner
            .tasks
            .spawn(worker::drive(Arc::downgrade(&inner), receive, cancel));
        Self { inner }
    }

    /// The shared endpoint's configured operational bounds.
    #[must_use]
    pub fn config(&self) -> &MessagingConfig {
        &self.inner.config
    }

    /// Validates an operation against this shared endpoint's settings.
    ///
    /// # Errors
    /// Returns `InvalidInput` when the payload/deadline exceeds configured bounds.
    pub fn validate_send(&self, options: SendOptions, payload_len: usize) -> io::Result<()> {
        self.inner.config.validate_send(options, payload_len)
    }

    /// Dispatches owned bytes, optionally awaiting queue/application acknowledgement.
    /// Retries retain application identity but allocate fresh router packet IDs.
    /// Cancelling this future removes its bounded outstanding-send reservation.
    /// # Errors
    /// Reports invalid input, local backpressure/no route, explicit rejection,
    /// shutdown, or `TimedOut` (remote outcome unknown).
    pub async fn send(
        &self,
        to: &NodeId,
        group: Option<&GroupId>,
        payload: Bytes,
        options: SendOptions,
    ) -> io::Result<MessageId> {
        let retry_horizon_ms = self.inner.config.retry_horizon_ms(options, payload.len())?;
        if !codec::destination_valid(to) {
            return Err(error(
                io::ErrorKind::InvalidInput,
                "invalid application destination",
            ));
        }
        if self.inner.cancel.is_cancelled() {
            return Err(closed());
        }
        let id = self.inner.next_id()?;
        // Validated once; every retry re-encodes the same packet unchecked.
        let frame = codec::DataFrame::new(id, options.delivery, retry_horizon_ms, group, &payload)
            .map_err(|_| {
                error(
                    io::ErrorKind::InvalidInput,
                    "invalid application group or payload",
                )
            })?;
        if options.delivery == Delivery::BestEffort {
            self.inner.dispatch(to, &frame)?;
            return Ok(id);
        }
        let start = tokio::time::Instant::now();
        let deadline = start.checked_add(options.timeout).ok_or_else(|| {
            error(
                io::ErrorKind::InvalidInput,
                "acknowledgement deadline exceeds the monotonic clock",
            )
        })?;
        let (result, mut receive) = oneshot::channel();
        {
            let mut state = self.inner.state.lock().map_err(|_| poisoned())?;
            if self.inner.cancel.is_cancelled() {
                return Err(closed());
            }
            if state.pending.len() >= self.inner.config.pending_sends.get() {
                return Err(error(
                    io::ErrorKind::WouldBlock,
                    "outstanding application send limit",
                ));
            }
            state.pending.insert(
                id,
                Pending {
                    to: to.clone(),
                    delivery: options.delivery,
                    result,
                },
            );
        }
        let _reservation = Reservation {
            owner: &self.inner,
            id,
        };
        self.inner.dispatch(to, &frame)?;
        let retry_interval = self.inner.config.retry_interval;
        let first_retry = start.checked_add(retry_interval).unwrap_or(deadline);
        let mut retry = tokio::time::interval_at(first_retry, retry_interval);
        retry.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = self.inner.cancel.cancelled() => return Err(closed()),
                () = tokio::time::sleep_until(deadline) => return Err(error(io::ErrorKind::TimedOut, "application acknowledgement deadline; remote outcome unknown")),
                result = &mut receive => { result.map_err(|_| closed())??; return Ok(id); }
                _ = retry.tick() => self.inner.dispatch(to, &frame)?,
            }
        }
    }

    /// Receives frames for runtime dispatch; receipt acceptance is explicit.
    /// # Errors
    /// Returns `NotConnected` after messaging/router cancellation.
    pub async fn recv(&self) -> io::Result<Frame> {
        tokio::select! {
            biased;
            () = self.inner.cancel.cancelled() => Err(closed()),
            frame = async { self.inner.receiver.lock().await.recv().await } => frame.ok_or_else(closed),
        }
    }

    /// A child token cancelled by messaging endpoint or router shutdown.
    #[must_use]
    pub fn cancellation(&self) -> CancellationToken {
        self.inner.cancel.child_token()
    }

    /// Cancels this endpoint across all clones without shutting down the router.
    pub fn shutdown(&self) {
        self.inner.cancel.cancel();
        self.inner.io.shutdown();
    }

    /// Waits for shutdown and then drains the endpoint's owned workers and queues.
    pub async fn closed(&self) {
        self.inner.cancel.cancelled().await;
        self.close().await;
    }

    /// Cancels and drains the dispatcher and queued frames across all clones.
    pub async fn close(&self) {
        self.shutdown();
        self.inner.tasks.close();
        self.inner.tasks.wait().await;
        if let Ok(mut state) = self.inner.state.lock() {
            state.pending.clear();
            state.received.clear();
        }
        let mut receiver = self.inner.receiver.lock().await;
        receiver.close();
        while receiver.try_recv().is_ok() {}
    }
}

impl MessageProtocol for Messaging {
    const ID: ProtocolId = 1;
    type SendOptions = SendOptions;
    type Receipt = Receipt;

    async fn send(
        &self,
        to: &NodeId,
        group: Option<&GroupId>,
        payload: Bytes,
        options: SendOptions,
    ) -> io::Result<MessageId> {
        Self::send(self, to, group, payload, options).await
    }

    async fn recv(&self) -> io::Result<Frame> {
        Self::recv(self).await
    }

    fn shutdown(&self) {
        Self::shutdown(self);
    }

    async fn closed(&self) {
        Self::closed(self).await;
    }
}

struct Reservation<'a> {
    owner: &'a Inner,
    id: MessageId,
}

impl Drop for Reservation<'_> {
    fn drop(&mut self) {
        if let Ok(mut state) = self.owner.state.lock() {
            state.pending.remove(&self.id);
        }
    }
}

fn error(kind: io::ErrorKind, message: &str) -> io::Error {
    io::Error::new(kind, message)
}

fn closed() -> io::Error {
    error(io::ErrorKind::NotConnected, "application messaging closed")
}

fn poisoned() -> io::Error {
    io::Error::other("application messaging state poisoned")
}
