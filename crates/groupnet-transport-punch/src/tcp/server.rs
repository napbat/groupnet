//! Bounded TCP admission, pair introductions, and independent relay writers.
use super::{
    DEADLINE, IDLE, MAX_CANDIDATES, MAX_PEERS, NetworkKey, QUEUE, closed, invalid, random, sockets,
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
        let allowed = validate_peers(&peers)?;
        Self::start(
            bind,
            Some(key),
            Arc::new(OpenAdmission),
            Some(Arc::new(allowed)),
        )
        .await
    }

    /// Binds explicit unauthenticated dynamic admission.
    ///
    /// # Errors
    /// Returns invalid bind or socket errors.
    pub async fn bind_open(bind: SocketAddr) -> io::Result<Self> {
        Self::bind_with_admission(bind, None, Arc::new(OpenAdmission)).await
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
        Self::start(bind, key, admission, None).await
    }

    async fn start(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
        allowed: Option<Arc<HashSet<NodeId>>>,
    ) -> io::Result<Self> {
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
    if peers.len() > MAX_PEERS {
        return Err(invalid("TCP peer bound exceeded"));
    }
    let mut seen = HashSet::new();
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
    cancel: CancellationToken,
    rate: tokio::time::Instant,
    count: u16,
}

enum Event {
    Join(Registration, TcpStream, Duplex),
    Frame(NodeId, Token, Message),
    Closed(NodeId, Token),
}

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
) {
    let pending = Arc::new(Semaphore::new(32));
    let (events, mut incoming) = mpsc::channel(QUEUE);
    let mut tasks = JoinSet::new();
    let mut entries: HashMap<NodeId, Entry> = HashMap::new();
    loop {
        tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            Some(event) = incoming.recv() => match event {
                Event::Join(registration, stream, auth) => join(registration, stream, &mut entries, &mut tasks, &events, &auth, &cancel),
                Event::Closed(node, session) => remove(&mut entries, &node, session),
                Event::Frame(node, session, message) => relay(&mut entries, &node, session, message),
            },
            Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
            Ok((stream, address)) = listener.accept() => {
                if tasks.len() >= MAX_PEERS + 64 { continue; }
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
    for entry in entries.values() {
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
    stream: TcpStream,
    entries: &mut HashMap<NodeId, Entry>,
    tasks: &mut JoinSet<()>,
    events: &mpsc::Sender<Event>,
    auth: &Duplex,
    cancel: &CancellationToken,
) {
    if entries.contains_key(&registration.node) || entries.len() >= MAX_PEERS {
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
    let (writer, outgoing) = mpsc::channel(QUEUE);
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
        let _ = writer.try_send(Message::Intro {
            node: entry.registration.node.clone(),
            session: entry.registration.session,
            secret,
            candidates: if disclose {
                candidates(&entry.registration)
            } else {
                Vec::new()
            },
        });
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
        auth.clone(),
        outgoing,
        events.clone(),
        node.clone(),
        session,
        peer_cancel.clone(),
    ));
    entries.insert(
        node,
        Entry {
            registration,
            writer,
            cancel: peer_cancel,
            rate: tokio::time::Instant::now(),
            count: 0,
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

fn relay(entries: &mut HashMap<NodeId, Entry>, source: &NodeId, session: Token, message: Message) {
    let Some(sender) = entries.get_mut(source) else {
        return;
    };
    if sender.registration.session != session {
        return;
    }
    if sender.rate.elapsed() >= Duration::from_secs(1) {
        sender.rate = tokio::time::Instant::now();
        sender.count = 0;
    }
    if sender.count >= 256 {
        sender.cancel.cancel();
        return;
    }
    sender.count += 1;
    match message {
        Message::Ping => {
            let _ = sender.writer.try_send(Message::Ping);
        }
        Message::Relay {
            node,
            session: target,
            data,
        } => {
            let Some(recipient) = entries.get(&node) else {
                return;
            };
            let Some(sender) = entries.get(source) else {
                return;
            };
            if recipient.registration.session != target
                || !permitted(&sender.registration, &recipient.registration)
            {
                return;
            }
            let _ = recipient.writer.try_send(Message::Relay {
                node: source.clone(),
                session,
                data,
            });
        }
        _ => sender.cancel.cancel(),
    }
}

async fn connection(
    stream: TcpStream,
    auth: Duplex,
    mut outgoing: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    node: NodeId,
    session: Token,
    cancel: CancellationToken,
) {
    let (mut reader, mut writer) = stream.into_split();
    let reading = read_client(&mut reader, &auth.rx, &events, &node, session);
    let writing = async {
        while let Some(message) = outgoing.recv().await {
            tokio::time::timeout(DEADLINE, wire::write(&mut writer, &auth.tx, &message))
                .await
                .map_err(|_| closed())??;
        }
        Ok::<(), io::Error>(())
    };
    tokio::select! { () = cancel.cancelled() => {}, _ = reading => {}, _ = writing => {} }
    cancel.cancel();
    let _ = events.send(Event::Closed(node, session)).await;
}

async fn read_client(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    auth: &Auth,
    events: &mpsc::Sender<Event>,
    node: &NodeId,
    session: Token,
) -> io::Result<()> {
    loop {
        let message = tokio::time::timeout(IDLE, wire::read(reader, auth))
            .await
            .map_err(|_| closed())??;
        events
            .send(Event::Frame(node.clone(), session, message))
            .await
            .map_err(|_| closed())?;
    }
}
