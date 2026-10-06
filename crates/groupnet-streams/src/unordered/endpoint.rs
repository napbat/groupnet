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
use zerocopy::byteorder::network_endian::U32;
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

use super::{
    UnorderedConfig, UnorderedDelivery, UnorderedOptions, UnorderedProtocol, UnorderedSession,
    aborted,
    session::{self, Parameters, State},
    wire::{self, SessionId},
};

const PREAMBLE: [u8; 8] = *b"GNUORD01";
const REQUEST: usize = size_of::<SetupRequest>();
const REPLY: usize = size_of::<SetupReply>();

#[derive(
    Clone, Copy, Debug, PartialEq, Eq, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned,
)]
#[repr(C)]
struct SetupIdentity {
    preamble: [u8; 8],
    delivery: u8,
    session: SessionId,
}

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SetupRequest {
    identity: SetupIdentity,
    max_payload: U32,
}

#[derive(Clone, Copy, Debug, FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct SetupReply {
    request: SetupRequest,
    status: u8,
}
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
    let maximum = inner.config.max_payload.min(
        inner
            .io
            .max_payload(to)
            .saturating_sub(wire::HEADER + wire::TAG),
    );
    let request = request(id, options.delivery, maximum)?;
    let operation = async {
        let mut control = inner.tunnels.connect_control(to).await?;
        control.write_all(request.as_bytes()).await?;
        control.flush().await?;
        let mut reply = [0; REPLY];
        control.read_exact(&mut reply).await?;
        let reply = SetupReply::read_from_bytes(&reply)
            .map_err(|_| invalid("invalid unordered setup reply"))?;
        if reply.request.identity != request.identity {
            return Err(invalid("unordered peer changed session identity or policy"));
        }
        match reply.status {
            0 => {}
            1 => {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "unordered delivery policy rejected by peer",
                ));
            }
            _ => return Err(invalid("invalid unordered setup status")),
        }
        let maximum = payload_bound(reply.request.max_payload)?;
        if maximum > payload_bound(request.max_payload)? {
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
            let _accepted = state.receive_packet(packet.payload);
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
    let request = SetupRequest::read_from_bytes(&request)
        .map_err(|_| invalid("invalid unordered setup request"))?;
    if request.identity.preamble != PREAMBLE {
        return Err(invalid("invalid unordered control version"));
    }
    let delivery = wire::delivery(request.identity.delivery)?;
    let id = request.identity.session;
    let remote_max = payload_bound(request.max_payload)?;
    let inner = owner.upgrade().ok_or_else(aborted)?;
    let maximum = remote_max.min(inner.config.max_payload).min(
        inner
            .io
            .max_payload(&peer)
            .saturating_sub(wire::HEADER + wire::TAG),
    );
    let mut reply = SetupReply {
        request: SetupRequest {
            identity: request.identity,
            max_payload: U32::new(
                u32::try_from(maximum).map_err(|_| invalid("payload bound overflow"))?,
            ),
        },
        status: 0,
    };
    payload_bound(reply.request.max_payload)?;
    if !inner.config.allows(delivery) {
        reply.status = 1;
        drop(inner);
        control.write_all(reply.as_bytes()).await?;
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
    control.write_all(reply.as_bytes()).await?;
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
    request: &SetupRequest,
    initiator: bool,
) -> io::Result<(Arc<State>, UnorderedSession)> {
    let mut keys = [0; 64];
    control.export_keying_material(
        &mut keys,
        b"EXPORTER-groupnet-unordered-v1",
        Some(request.identity.as_bytes()),
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
) -> io::Result<SetupRequest> {
    let max_payload =
        U32::new(u32::try_from(maximum).map_err(|_| invalid("payload bound overflow"))?);
    payload_bound(max_payload)?;
    Ok(SetupRequest {
        identity: SetupIdentity {
            preamble: PREAMBLE,
            delivery: wire::policy(delivery),
            session: id,
        },
        max_payload,
    })
}

fn payload_bound(bound: U32) -> io::Result<usize> {
    if bound.get() == 0 {
        return Err(invalid("invalid payload bound"));
    }
    usize::try_from(bound.get()).map_err(|_| invalid("payload bound overflow"))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn setup_layout_preserves_identity_policy_and_network_order_bounds() {
        let request = request([9; 16], UnorderedDelivery::Reliable, 96 * 1024).unwrap();
        assert_eq!(REQUEST, 29);
        assert_eq!(REPLY, 30);
        assert_eq!(request.identity.as_bytes().len(), 25);
        let decoded = SetupRequest::read_from_bytes(request.as_bytes()).unwrap();
        assert_eq!(decoded.identity, request.identity);
        assert_eq!(payload_bound(decoded.max_payload).unwrap(), 96 * 1024);
        let reply = SetupReply { request, status: 1 };
        let decoded = SetupReply::read_from_bytes(reply.as_bytes()).unwrap();
        assert_eq!(decoded.status, 1);
        assert_eq!(decoded.request.identity, reply.request.identity);
        assert!(payload_bound(U32::new(0)).is_err());
        for end in 0..REQUEST {
            assert!(SetupRequest::read_from_bytes(&request.as_bytes()[..end]).is_err());
        }
    }
}
