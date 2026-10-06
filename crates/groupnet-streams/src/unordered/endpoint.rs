//! Bounded endpoint admission and the pinned TLS-only setup/control plane.

use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex, Weak},
};

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_core::NodeId;
use groupnet_network::{
    ProtocolIo,
    tunnel::{TunnelTransport, TunneledStream},
};
use ring::rand::{SecureRandom, SystemRandom};
use tokio::{
    sync::{Mutex as AsyncMutex, mpsc},
    time::timeout,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

use super::{
    UnorderedConfig, UnorderedDelivery, UnorderedOptions, UnorderedProtocol, UnorderedSession,
    aborted,
    session::{self, Parameters, State},
    wire::{self, SessionId},
};

const PREAMBLE: &[u8; 8] = b"GNUORD01";
const REQUEST: usize = 29;
const REPLY: usize = 30;
const READY: u8 = 0xa5;
type Key = (NodeId, SessionId);

#[derive(Debug, Default)]
struct Registry {
    sessions: HashMap<Key, Option<Weak<State>>>,
    setups: HashMap<NodeId, usize>,
}

#[derive(Debug)]
pub(super) struct Inner {
    pub(super) io: ProtocolIo,
    tunnels: TunnelTransport,
    config: UnorderedConfig,
    pub(super) cancel: CancellationToken,
    registry: Mutex<Registry>,
    incoming: [mpsc::Sender<(NodeId, UnorderedSession)>; 2],
    accepts: [AsyncMutex<mpsc::Receiver<(NodeId, UnorderedSession)>>; 2],
    tasks: TaskTracker,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
        self.io.shutdown();
    }
}

#[derive(Debug)]
pub(super) struct Lease {
    owner: Weak<Inner>,
    peer: NodeId,
    id: Option<SessionId>,
}

impl Drop for Lease {
    fn drop(&mut self) {
        if let Some(inner) = self.owner.upgrade()
            && let Ok(mut registry) = inner.registry.lock()
        {
            if let Some(id) = self.id {
                registry.sessions.remove(&(self.peer.clone(), id));
            } else {
                release_setup(&mut registry, &self.peer);
            }
        }
    }
}

fn release_setup(registry: &mut Registry, peer: &NodeId) {
    if let Some(count) = registry.setups.get_mut(peer) {
        *count -= 1;
        if *count == 0 {
            registry.setups.remove(peer);
        }
    }
}

fn identify(inner: &Inner, lease: &mut Lease, id: SessionId) -> io::Result<()> {
    let mut registry = inner.registry.lock().map_err(|_| poisoned())?;
    if inner.cancel.is_cancelled() {
        return Err(aborted());
    }
    let key = (lease.peer.clone(), id);
    if registry.sessions.contains_key(&key) {
        return Err(invalid("duplicate unordered session id"));
    }
    registry.sessions.insert(key, None);
    release_setup(&mut registry, &lease.peer);
    lease.id = Some(id);
    Ok(())
}

pub(super) fn new(
    io: ProtocolIo,
    tunnels: TunnelTransport,
    config: UnorderedConfig,
) -> UnorderedProtocol {
    let cancel = io.cancellation();
    // Global session admission bounds the combined queues, not each separately.
    let (reliable_incoming, reliable_accepts) = mpsc::channel(config.max_sessions);
    let (unreliable_incoming, unreliable_accepts) = mpsc::channel(config.max_sessions);
    let inner = Arc::new(Inner {
        io: io.clone(),
        tunnels: tunnels.clone(),
        config,
        cancel: cancel.clone(),
        registry: Mutex::new(Registry::default()),
        incoming: [reliable_incoming, unreliable_incoming],
        accepts: [
            AsyncMutex::new(reliable_accepts),
            AsyncMutex::new(unreliable_accepts),
        ],
        tasks: TaskTracker::new(),
    });
    inner
        .tasks
        .spawn(dispatch(Arc::downgrade(&inner), io, cancel.clone()));
    inner
        .tasks
        .spawn(accept_controls(Arc::downgrade(&inner), tunnels, cancel));
    UnorderedProtocol { inner }
}

fn reserve(inner: &Arc<Inner>, peer: NodeId, id: Option<SessionId>) -> io::Result<Lease> {
    let mut registry = inner.registry.lock().map_err(|_| poisoned())?;
    if inner.cancel.is_cancelled() {
        return Err(aborted());
    }
    if id.is_some_and(|id| registry.sessions.contains_key(&(peer.clone(), id))) {
        return Err(invalid("duplicate unordered session id"));
    }
    if registry.sessions.len() + registry.setups.values().sum::<usize>()
        >= inner.config.max_sessions
        || registry
            .sessions
            .keys()
            .filter(|(node, _)| node == &peer)
            .count()
            + registry.setups.get(&peer).copied().unwrap_or(0)
            >= inner.config.sessions_per_peer
    {
        return Err(io::Error::new(
            io::ErrorKind::WouldBlock,
            "unordered session admission full",
        ));
    }
    if let Some(id) = id {
        registry.sessions.insert((peer.clone(), id), None);
    } else {
        *registry.setups.entry(peer.clone()).or_default() += 1;
    }
    Ok(Lease {
        owner: Arc::downgrade(inner),
        peer,
        id,
    })
}

fn register(inner: &Inner, state: &Arc<State>) -> io::Result<()> {
    if inner.cancel.is_cancelled() {
        return Err(aborted());
    }
    let mut registry = inner.registry.lock().map_err(|_| poisoned())?;
    let entry = registry
        .sessions
        .get_mut(&(state.peer.clone(), state.id))
        .ok_or_else(aborted)?;
    *entry = Some(Arc::downgrade(state));
    Ok(())
}

pub(super) fn validate_options(inner: &Inner, options: &UnorderedOptions) -> io::Result<()> {
    if options.timeout.is_zero() || !inner.config.allows(options.delivery) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unordered policy or setup timeout disallowed locally",
        ));
    }
    Ok(())
}

fn policy_index(delivery: UnorderedDelivery) -> usize {
    match delivery {
        UnorderedDelivery::Reliable => 0,
        UnorderedDelivery::Unreliable => 1,
    }
}

pub(super) async fn connect(
    inner: &Arc<Inner>,
    to: &NodeId,
    options: UnorderedOptions,
) -> io::Result<UnorderedSession> {
    let mut id = [0; 16];
    SystemRandom::new()
        .fill(&mut id)
        .map_err(|_| io::Error::other("session randomness unavailable"))?;
    let lease = reserve(inner, to.clone(), Some(id))?;
    let request = request(id, options.delivery, inner.config.max_payload)?;
    let operation = async {
        let mut control = inner.tunnels.connect_control(to).await?;
        control.write_all(&request).await?;
        control.flush().await?;
        let mut reply = [0; REPLY];
        control.read_exact(&mut reply).await?;
        if reply[..25] != request[..25] {
            return Err(invalid("unordered peer changed session identity or policy"));
        }
        match reply[29] {
            0 => {}
            1 => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "unordered delivery policy rejected by peer",
                ));
            }
            _ => return Err(invalid("invalid unordered setup status")),
        }
        let maximum = payload_bound(&reply[25..29])?;
        if maximum > inner.config.max_payload {
            return Err(invalid("invalid peer message bound"));
        }
        let (state, session) = establish(
            inner,
            to.clone(),
            id,
            options.delivery,
            maximum,
            lease,
            &control,
            &request,
            true,
        )?;
        register(inner, &state)?;
        control.write_all(&[READY]).await?;
        control.flush().await?;
        let mut done = [0];
        control.read_exact(&mut done).await?;
        if done[0] != READY {
            return Err(invalid("unordered acceptance incomplete"));
        }
        if state.cancel.is_cancelled() || inner.cancel.is_cancelled() {
            return Err(aborted());
        }
        inner.tasks.spawn(session::lifetime(state, control));
        Ok(session)
    };
    tokio::select! {
        biased;
        () = inner.cancel.cancelled() => Err(aborted()),
        result = timeout(options.timeout.min(inner.config.setup_timeout), operation) => {
            result.map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "unordered setup deadline"))?
        },
    }
}

pub(super) async fn accept(inner: &Inner) -> io::Result<(NodeId, UnorderedSession)> {
    tokio::select! {
        accepted = accept_policy(inner, UnorderedDelivery::Reliable) => accepted,
        accepted = accept_policy(inner, UnorderedDelivery::Unreliable) => accepted,
    }
}

pub(super) async fn accept_policy(
    inner: &Inner,
    delivery: UnorderedDelivery,
) -> io::Result<(NodeId, UnorderedSession)> {
    let mut queue = tokio::select! {
        biased;
        () = inner.cancel.cancelled() => return Err(endpoint_closed()),
        queue = inner.accepts[policy_index(delivery)].lock() => queue,
    };
    loop {
        let accepted = tokio::select! {
            biased;
            () = inner.cancel.cancelled() => return Err(endpoint_closed()),
            session = queue.recv() => session.ok_or_else(endpoint_closed)?,
        };
        if !accepted.1.handle_cancelled() {
            return Ok(accepted);
        }
    }
}

pub(super) async fn closed(inner: &Inner) {
    inner.cancel.cancelled().await;
    inner.io.shutdown();
    inner.tasks.close();
    inner.tasks.wait().await;
    for queue in &inner.accepts {
        let mut queue = queue.lock().await;
        queue.close();
        while queue.try_recv().is_ok() {}
    }
}

async fn dispatch(owner: Weak<Inner>, io: ProtocolIo, cancel: CancellationToken) {
    loop {
        let packet = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            result = io.recv() => match result { Ok(packet) => packet, Err(_) => break },
        };
        let Some(id) = wire::session(&packet.payload) else {
            continue;
        };
        let state = owner.upgrade().and_then(|inner| {
            let registry = inner.registry.lock().ok()?;
            registry
                .sessions
                .get(&(packet.from.clone(), id))?
                .as_ref()?
                .upgrade()
        });
        if let Some(state) = state {
            let _accepted = state.receive_packet(&packet.payload);
        }
    }
    cancel.cancel();
}

async fn accept_controls(owner: Weak<Inner>, tunnels: TunnelTransport, cancel: CancellationToken) {
    loop {
        let accepted = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            result = tunnels.accept_control() => match result { Ok(accepted) => accepted, Err(_) => break },
        };
        let Some(inner) = owner.upgrade() else {
            break;
        };
        // Account for authenticated peers before reading any peer-controlled
        // request bytes. Slow setups hold only their own bounded admission.
        let Ok(lease) = reserve(&inner, accepted.0.clone(), None) else {
            continue;
        };
        let setup = inner.config.setup_timeout;
        let owner = owner.clone();
        let cancel = cancel.clone();
        inner.tasks.spawn(async move {
            let operation = receive_control(&owner, accepted.0, accepted.1, lease);
            tokio::select! {
                biased;
                () = cancel.cancelled() => {},
                _result = timeout(setup, operation) => {},
            }
        });
    }
    cancel.cancel();
}

async fn receive_control(
    owner: &Weak<Inner>,
    peer: NodeId,
    mut control: TunneledStream,
    mut lease: Lease,
) -> io::Result<()> {
    let mut request = [0; REQUEST];
    control.read_exact(&mut request).await?;
    if &request[..8] != PREAMBLE {
        return Err(invalid("invalid unordered control version"));
    }
    let delivery = wire::delivery(request[8])?;
    let id = request[9..25]
        .try_into()
        .map_err(|_| invalid("invalid unordered session identity"))?;
    let remote_max = payload_bound(&request[25..29])?;
    let inner = owner.upgrade().ok_or_else(aborted)?;
    let maximum = remote_max.min(inner.config.max_payload);
    let mut reply = [0; REPLY];
    reply[..25].copy_from_slice(&request[..25]);
    reply[25..29].copy_from_slice(
        &u32::try_from(maximum)
            .map_err(|_| invalid("payload bound overflow"))?
            .to_be_bytes(),
    );
    if !inner.config.allows(delivery) {
        reply[29] = 1;
        drop(inner);
        control.write_all(&reply).await?;
        control.flush().await?;
        let _closed = control.close().await;
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "unordered policy rejected",
        ));
    }
    identify(&inner, &mut lease, id)?;
    let (state, session) = establish(
        &inner,
        peer.clone(),
        id,
        delivery,
        maximum,
        lease,
        &control,
        &request,
        false,
    )?;
    register(&inner, &state)?;
    drop(inner);
    control.write_all(&reply).await?;
    control.flush().await?;
    let mut ready = [0];
    control.read_exact(&mut ready).await?;
    if ready[0] != READY {
        return Err(invalid("invalid unordered ready marker"));
    }
    let inner = owner.upgrade().ok_or_else(aborted)?;
    if state.cancel.is_cancelled() || inner.cancel.is_cancelled() {
        return Err(aborted());
    }
    let accepted = inner.incoming[policy_index(delivery)]
        .clone()
        .try_reserve_owned()
        .map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "unordered accept queue full"))?;
    drop(inner);
    control.write_all(&[READY]).await?;
    control.flush().await?;
    let inner = owner.upgrade().ok_or_else(aborted)?;
    inner.tasks.spawn(session::lifetime(state, control));
    accepted.send((peer, session));
    Ok(())
}

#[expect(
    clippy::too_many_arguments,
    reason = "setup carries the authenticated context and its admission lease"
)]
fn establish(
    inner: &Inner,
    peer: NodeId,
    id: SessionId,
    delivery: super::UnorderedDelivery,
    max_payload: usize,
    lease: Lease,
    control: &TunneledStream,
    request: &[u8; REQUEST],
    initiator: bool,
) -> io::Result<(Arc<State>, UnorderedSession)> {
    let mut keys = [0; 64];
    control.export_keying_material(
        &mut keys,
        b"EXPORTER-groupnet-unordered-v1",
        Some(&request[..25]),
    )?;
    let result = session::create(
        Parameters {
            peer,
            id,
            delivery,
            max_payload,
            lease,
            config: inner.config.clone(),
            io: inner.io.clone(),
            endpoint_cancel: inner.cancel.clone(),
        },
        control,
        &keys,
        initiator,
    );
    keys.fill(0);
    result
}

fn request(
    id: SessionId,
    delivery: super::UnorderedDelivery,
    maximum: usize,
) -> io::Result<[u8; REQUEST]> {
    let mut bytes = [0; REQUEST];
    bytes[..8].copy_from_slice(PREAMBLE);
    bytes[8] = wire::policy(delivery);
    bytes[9..25].copy_from_slice(&id);
    bytes[25..].copy_from_slice(
        &u32::try_from(maximum)
            .map_err(|_| invalid("payload bound overflow"))?
            .to_be_bytes(),
    );
    Ok(bytes)
}

fn payload_bound(bytes: &[u8]) -> io::Result<usize> {
    let bound = u32::from_be_bytes(
        bytes
            .try_into()
            .map_err(|_| invalid("invalid payload bound"))?,
    );
    if !(1..=48 * 1024).contains(&bound) {
        return Err(invalid("invalid payload bound"));
    }
    usize::try_from(bound).map_err(|_| invalid("payload bound overflow"))
}

fn invalid(message: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

fn poisoned() -> io::Error {
    io::Error::other("unordered endpoint state poisoned")
}

fn endpoint_closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "unordered endpoint closed")
}
