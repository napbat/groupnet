//! Bounded TCP admission, pair introductions, and independent relay writers.
use super::{
    DEADLINE, IDLE, MAX_CANDIDATES, NetworkKey, RelayPacing, TcpRendezvousConfig, closed, invalid,
    lock,
    policy::Budget,
    random, sockets,
    wire::{self, Auth, Duplex, Message, Token},
};
use groupnet_core::NodeId;
use groupnet_transport::admission::{Admission, JoinRequest, OpenAdmission};
use std::{
    collections::{HashMap, HashSet},
    io,
    net::SocketAddr,
    sync::Arc,
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream},
    sync::{Mutex, Semaphore, mpsc},
    task::{JoinHandle, JoinSet},
};
use tokio_util::sync::CancellationToken;

#[derive(Debug)]
struct Inner {
    address: SocketAddr,
    cancel: CancellationToken,
    task: Mutex<Option<JoinHandle<()>>>,
}

impl Drop for Inner {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

/// TCP-only rendezvous with bounded admission and session-fenced relay queues.
/// This service does not participate in logical routing and requires no UDP socket.
#[derive(Clone, Debug)]
pub struct TcpRendezvous {
    inner: Arc<Inner>,
}

impl TcpRendezvous {
    /// Binds a keyed rendezvous restricted to an explicit identity allowlist.
    ///
    /// # Errors
    /// Returns invalid identities, duplicate allowlist entries, or bind errors.
    pub async fn bind(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> io::Result<Self> {
        Self::bind_config(bind, key, peers, TcpRendezvousConfig::default()).await
    }

    /// Binds a keyed allowlist with explicit operational limits and relay policy.
    ///
    /// # Errors
    /// Rejects invalid limits or identities and returns socket errors.
    pub async fn bind_config(
        bind: SocketAddr,
        key: NetworkKey,
        peers: Vec<NodeId>,
        config: TcpRendezvousConfig,
    ) -> io::Result<Self> {
        let allowed = validate_peers(&peers)?;
        Self::start(
            bind,
            Some(key),
            Arc::new(OpenAdmission),
            Some(Arc::new(allowed)),
            config,
        )
        .await
    }

    /// Binds explicit unauthenticated dynamic admission.
    ///
    /// # Errors
    /// Returns invalid bind or socket errors.
    pub async fn bind_open(bind: SocketAddr) -> io::Result<Self> {
        Self::bind_open_config(bind, TcpRendezvousConfig::default()).await
    }

    /// Binds open admission with explicit operational limits and relay policy.
    ///
    /// # Errors
    /// Rejects invalid limits and returns socket errors.
    pub async fn bind_open_config(
        bind: SocketAddr,
        config: TcpRendezvousConfig,
    ) -> io::Result<Self> {
        Self::bind_with_admission_config(bind, None, Arc::new(OpenAdmission), config).await
    }

    /// Binds application-controlled admission with optional strict fabric authentication.
    ///
    /// # Errors
    /// Returns invalid bind or socket errors.
    pub async fn bind_with_admission(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
    ) -> io::Result<Self> {
        Self::bind_with_admission_config(bind, key, admission, TcpRendezvousConfig::default()).await
    }

    /// Binds application admission with explicit operational limits and relay policy.
    ///
    /// # Errors
    /// Rejects invalid limits and returns socket errors.
    pub async fn bind_with_admission_config(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
        config: TcpRendezvousConfig,
    ) -> io::Result<Self> {
        Self::start(bind, key, admission, None, config).await
    }

    async fn start(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
        allowed: Option<Arc<HashSet<NodeId>>>,
        config: TcpRendezvousConfig,
    ) -> io::Result<Self> {
        config.validate()?;
        if bind.ip().is_multicast() {
            return Err(invalid("multicast TCP rendezvous bind"));
        }
        let listener = TcpListener::bind(bind).await?;
        let address = listener.local_addr()?;
        let cancel = CancellationToken::new();
        let task = tokio::spawn(run(
            listener,
            wire::auth(key.as_ref()),
            admission,
            allowed,
            cancel.clone(),
            config,
        ));
        Ok(Self {
            inner: Arc::new(Inner {
                address,
                cancel,
                task: Mutex::new(Some(task)),
            }),
        })
    }

    /// Returns the actual TCP listening address.
    ///
    /// # Errors
    /// Returns `NotConnected` after shutdown.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        if self.inner.cancel.is_cancelled() {
            Err(closed())
        } else {
            Ok(self.inner.address)
        }
    }

    /// Cancels and drains all pending admission, reader, and writer tasks.
    pub async fn close(&self) {
        self.inner.cancel.cancel();
        let mut task = self.inner.task.lock().await;
        if let Some(running) = task.as_mut() {
            let _ = running.await;
        }
        task.take();
    }
}

pub(super) fn validate_peers(peers: &[NodeId]) -> io::Result<HashSet<NodeId>> {
    let mut seen = HashSet::with_capacity(peers.len());
    for node in peers {
        if node.as_str().is_empty() || node.as_str().len() > 64 || !seen.insert(node.clone()) {
            return Err(invalid("invalid or duplicate TCP identity"));
        }
    }
    Ok(seen)
}

struct Registration {
    node: NodeId,
    session: Token,
    peers: HashSet<NodeId>,
    dynamic: bool,
    relay_only: bool,
    candidates: Vec<SocketAddr>,
    observed: SocketAddr,
}

struct Entry {
    registration: Registration,
    writer: mpsc::Sender<Message>,
    data_writer: mpsc::Sender<Message>,
    cancel: CancellationToken,
}

enum Event {
    Join(Registration, TcpStream, Duplex),
    Closed(NodeId, Token),
}

struct Delivery {
    writer: mpsc::Sender<Message>,
    cancel: CancellationToken,
    message: Message,
}

type Entries = Arc<std::sync::Mutex<HashMap<NodeId, Entry>>>;

async fn admit(
    mut stream: TcpStream,
    address: SocketAddr,
    auth: Auth,
    admission: Arc<dyn Admission>,
    allowed: Option<Arc<HashSet<NodeId>>>,
) -> io::Result<(Registration, TcpStream, Duplex)> {
    let challenge = random()?;
    wire::write(&mut stream, &auth, &Message::Challenge(challenge)).await?;
    let Message::Register {
        node,
        session,
        challenge: echo,
        credential,
        peers,
        dynamic,
        relay_only,
        candidates,
    } = wire::read(&mut stream, &auth).await?
    else {
        return Err(invalid("expected TCP registration"));
    };
    if !wire::matches(&challenge, &echo)
        || allowed
            .as_ref()
            .is_some_and(|allowed| !allowed.contains(&node))
    {
        return Err(invalid("TCP registration denied"));
    }
    let peers = validate_peers(&peers)?;
    if peers.contains(&node)
        || candidates
            .iter()
            .any(|candidate| !sockets::valid(*candidate))
        || (relay_only && !candidates.is_empty())
    {
        return Err(invalid("invalid TCP registration candidates"));
    }
    let accepted = admission
        .admit(JoinRequest::new(&node, &credential, Some(address)))
        .await?;
    if accepted.node != node {
        return Err(invalid("TCP admission may not rename identity"));
    }
    let auth = wire::control_auth(&auth, &challenge, &session, wire::Role::Server);
    Ok((
        Registration {
            node,
            session,
            peers,
            dynamic,
            relay_only,
            candidates,
            observed: address,
        },
        stream,
        auth,
    ))
}

async fn run(
    listener: TcpListener,
    auth: Auth,
    admission: Arc<dyn Admission>,
    allowed: Option<Arc<HashSet<NodeId>>>,
    cancel: CancellationToken,
    config: TcpRendezvousConfig,
) {
    let pending = Arc::new(Semaphore::new(config.max_pending));
    let (events, mut incoming) = mpsc::channel(config.event_queue);
    let mut tasks = JoinSet::new();
    let entries: Entries = Arc::new(std::sync::Mutex::new(HashMap::new()));
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(event) = incoming.recv() => match event {
                Event::Join(registration, stream, auth) => join(registration, (stream, auth), &entries, &mut tasks, &events, &cancel, &config),
                Event::Closed(node, session) => remove(&mut lock(&entries), &node, session),
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            Ok((stream, address)) = listener.accept() => {
                if tasks.len() >= config.max_sessions + config.max_pending { continue; }
                let Ok(permit) = pending.clone().try_acquire_owned() else { continue; };
                let events = events.clone(); let auth = wire::fresh(&auth); let admission = admission.clone(); let allowed = allowed.clone();
                tasks.spawn(async move {
                    let _permit = permit;
                    if let Ok(Ok((registration, stream, auth))) = tokio::time::timeout(DEADLINE, admit(stream, address, auth, admission, allowed)).await {
                        let _ = events.send(Event::Join(registration, stream, auth)).await;
                    }
                });
            }
        }
    }
    for entry in lock(&entries).values() {
        entry.cancel.cancel();
    }
    drop(incoming);
    let drained = tokio::time::timeout(DEADLINE + Duration::from_secs(1), async {
        while tasks.join_next().await.is_some() {}
    })
    .await;
    if drained.is_err() {
        tasks.abort_all();
        while tasks.join_next().await.is_some() {}
    }
}

fn permitted(a: &Registration, b: &Registration) -> bool {
    (a.dynamic || a.peers.contains(&b.node)) && (b.dynamic || b.peers.contains(&a.node))
}

fn candidates(registration: &Registration) -> Vec<SocketAddr> {
    let mut candidates = registration.candidates.clone();
    if sockets::valid(registration.observed) && !candidates.contains(&registration.observed) {
        if candidates.len() == MAX_CANDIDATES {
            candidates.pop();
        }
        candidates.push(registration.observed);
    }
    candidates
}

fn join(
    registration: Registration,
    socket: (TcpStream, Duplex),
    shared: &Entries,
    tasks: &mut JoinSet<()>,
    events: &mpsc::Sender<Event>,
    cancel: &CancellationToken,
    config: &TcpRendezvousConfig,
) {
    let (stream, auth) = socket;
    let mut entries = lock(shared);
    if entries.contains_key(&registration.node) || entries.len() >= config.max_sessions {
        let auth = auth.clone();
        tasks.spawn(async move {
            let mut stream = stream;
            let _ = tokio::time::timeout(
                DEADLINE,
                wire::write(&mut stream, &auth.tx, &Message::Denied),
            )
            .await;
        });
        return;
    }
    let (writer, control_outgoing) = mpsc::channel(config.control_queue);
    let (data_writer, data_outgoing) = mpsc::channel(config.session_queue);
    let peer_cancel = cancel.child_token();
    let node = registration.node.clone();
    let session = registration.session;
    let _ = writer.try_send(Message::Welcome {
        observed: registration.observed,
    });
    for entry in entries.values() {
        if !permitted(&registration, &entry.registration) {
            continue;
        }
        let Ok(secret) = random() else {
            peer_cancel.cancel();
            return;
        };
        let disclose = !registration.relay_only && !entry.registration.relay_only;
        if writer
            .try_send(Message::Intro {
                node: entry.registration.node.clone(),
                session: entry.registration.session,
                secret,
                candidates: if disclose {
                    candidates(&entry.registration)
                } else {
                    Vec::new()
                },
            })
            .is_err()
        {
            peer_cancel.cancel();
            return;
        }
        // Failure is fail-closed: withdraw the congested control session instead
        // of installing an introduction only one participant received.
        if entry
            .writer
            .try_send(Message::Intro {
                node: node.clone(),
                session,
                secret,
                candidates: if disclose {
                    candidates(&registration)
                } else {
                    Vec::new()
                },
            })
            .is_err()
        {
            entry.cancel.cancel();
        }
    }
    tasks.spawn(connection(
        stream,
        auth,
        (control_outgoing, data_outgoing),
        events.clone(),
        (node.clone(), session),
        peer_cancel.clone(),
        (*config, shared.clone()),
    ));
    entries.insert(
        node,
        Entry {
            registration,
            writer,
            data_writer,
            cancel: peer_cancel,
        },
    );
}

fn remove(entries: &mut HashMap<NodeId, Entry>, node: &NodeId, session: Token) {
    if entries
        .get(node)
        .is_none_or(|entry| entry.registration.session != session)
    {
        return;
    }
    if let Some(entry) = entries.remove(node) {
        entry.cancel.cancel();
    }
    for entry in entries.values() {
        if entry
            .writer
            .try_send(Message::Gone {
                node: node.clone(),
                session,
            })
            .is_err()
        {
            entry.cancel.cancel();
        }
    }
}

fn relay(
    entries: &HashMap<NodeId, Entry>,
    source: &NodeId,
    session: Token,
    message: Message,
) -> Option<Delivery> {
    let sender = entries.get(source)?;
    if sender.registration.session != session {
        return None;
    }
    match message {
        Message::Ping => {
            let _ = sender.writer.try_send(Message::Ping);
            None
        }
        Message::Relay {
            node,
            session: target,
            data,
        } => {
            let recipient = entries.get(&node)?;
            if recipient.registration.session != target
                || !permitted(&sender.registration, &recipient.registration)
            {
                return None;
            }
            Some(Delivery {
                writer: recipient.data_writer.clone(),
                cancel: recipient.cancel.clone(),
                message: Message::Relay {
                    node: source.clone(),
                    session,
                    data,
                },
            })
        }
        _ => {
            sender.cancel.cancel();
            None
        }
    }
}

async fn connection(
    stream: TcpStream,
    auth: Duplex,
    outgoing: (mpsc::Receiver<Message>, mpsc::Receiver<Message>),
    events: mpsc::Sender<Event>,
    identity: (NodeId, Token),
    cancel: CancellationToken,
    context: (TcpRendezvousConfig, Entries),
) {
    let (node, session) = identity;
    let (config, entries) = context;
    let (mut reader, mut writer) = stream.into_split();
    let reading = read_client(&mut reader, &auth.rx, &entries, &node, session, config);
    let writing = write_client(&mut writer, &auth.tx, outgoing);
    tokio::select! { () = cancel.cancelled() => {}, _ = reading => {}, _ = writing => {} }
    cancel.cancel();
    let _ = events.send(Event::Closed(node, session)).await;
}

async fn write_client<W: tokio::io::AsyncWrite + Unpin>(
    writer: &mut W,
    auth: &Auth,
    outgoing: (mpsc::Receiver<Message>, mpsc::Receiver<Message>),
) -> io::Result<()> {
    let (mut control_outgoing, mut data_outgoing) = outgoing;
    let mut scratch = Vec::new();
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        let message = tokio::select! {
            biased;
            message = control_outgoing.recv() => message,
            message = data_outgoing.recv() => message,
            _ = heartbeat.tick() => Some(Message::Ping),
        };
        let Some(message) = message else { break };
        // A peer that stops reading is still evicted. The heartbeat is
        // independent of the source reader's valid data/backpressure waits.
        tokio::time::timeout(
            DEADLINE,
            wire::write_buffered(writer, auth, &message, &mut scratch),
        )
        .await
        .map_err(|_| closed())??;
    }
    Ok(())
}

async fn read_client<R: tokio::io::AsyncRead + Unpin>(
    reader: &mut R,
    auth: &Auth,
    entries: &Entries,
    node: &NodeId,
    session: Token,
    config: TcpRendezvousConfig,
) -> io::Result<()> {
    let now = tokio::time::Instant::now();
    let mut control = Budget::new(
        u64::from(config.control.frames_per_second.get()),
        u64::from(config.control.burst_frames.get()),
        now,
    );
    let mut data = match config.relay_pacing {
        RelayPacing::Backpressure => None,
        RelayPacing::Bytes {
            bytes_per_second,
            burst_bytes,
        } => Some(Budget::new(bytes_per_second.get(), burst_bytes.get(), now)),
    };
    loop {
        let message = tokio::time::timeout(IDLE, wire::read(reader, auth))
            .await
            .map_err(|_| closed())??;
        let is_relay = matches!(&message, Message::Relay { .. });
        if !is_relay {
            let ready = control.ready_at(tokio::time::Instant::now(), 1);
            tokio::time::sleep_until(ready).await;
        }
        let delivery = relay(&lock(entries), node, session, message);
        if let Some(delivery) = delivery {
            if let Some(budget) = &mut data {
                let Message::Relay { data, .. } = &delivery.message else {
                    unreachable!()
                };
                let ready = budget.ready_at(
                    tokio::time::Instant::now(),
                    u64::try_from(data.len()).expect("bounded payload"),
                );
                tokio::time::sleep_until(ready).await;
            }
            // This source reader owns the wait. A saturated recipient cannot
            // stall the central admission/routing task or unrelated sources.
            tokio::select! {
                () = delivery.cancel.cancelled() => {},
                result = delivery.writer.send(delivery.message) => { let _ = result; },
            }
        } else if is_relay {
            let ready = control.ready_at(tokio::time::Instant::now(), 1);
            tokio::time::sleep_until(ready).await;
        }
    }
}

#[cfg(test)]
mod tests;
