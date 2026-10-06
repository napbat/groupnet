//! Endpoint admission and bounded, nonblocking multi-candidate scheduling.
mod checks;
mod direct;

#[cfg(test)]
mod recovery_tests;
use super::{
    DEADLINE, IDLE, Inner, MAX_CANDIDATES, MAX_PEERS, Outgoing, QUEUE, TcpConnection,
    TcpPunchConfig, View, closed, invalid, lock, random, server, sockets,
    wire::{self, Auth, Duplex, Message, Token},
};
use crate::PathPolicy;
use groupnet_core::NodeId;
use groupnet_transport::{
    Inbound,
    admission::{AcceptedPeer, SessionLease, SessionRegistry},
    link::AdmittedInbound,
};
use std::{
    collections::{HashMap, HashSet, VecDeque},
    io,
    net::SocketAddr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpSocket, TcpStream},
    sync::{Semaphore, mpsc},
    task::JoinSet,
};
use tokio_util::sync::CancellationToken;

#[derive(Clone)]
struct ProofPeer {
    session: Token,
    secret: Token,
}
type Proofs = Arc<Mutex<HashMap<NodeId, ProofPeer>>>;
type Views = Arc<Mutex<HashMap<NodeId, View>>>;

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
struct Rank {
    direction: u8,
    nonce: Token,
}

struct Direct {
    rank: Rank,
    writer: mpsc::Sender<Message>,
    cancel: CancellationToken,
}

struct Peer {
    session: Token,
    secret: Token,
    lease: SessionLease,
    relay_live: bool,
    checks: Vec<checks::Check>,
    next_check: usize,
    direct: Option<Direct>,
}

enum Event {
    Control(Message),
    ControlClosed,
    Ready {
        node: NodeId,
        session: Token,
        rank: Rank,
        stream: TcpStream,
        auth: Duplex,
    },
    Data {
        node: NodeId,
        session: Token,
        rank: Rank,
        data: Vec<u8>,
    },
    Closed {
        node: NodeId,
        session: Token,
        rank: Rank,
    },
    Checked {
        node: NodeId,
        session: Token,
        tuple: (SocketAddr, SocketAddr),
    },
}

pub(super) async fn bind(config: TcpPunchConfig) -> io::Result<TcpConnection> {
    validate(&config)?;
    let auth = wire::auth(config.key.as_ref());
    let session = random()?;
    let (mut stream, reusable) = tokio::time::timeout(DEADLINE, registration_socket(&config))
        .await
        .map_err(|_| {
            io::Error::new(
                io::ErrorKind::TimedOut,
                "TCP registration connect timed out",
            )
        })??;
    let actual = stream.local_addr()?;
    let address = SocketAddr::new(config.bind.ip(), actual.port());
    let mut listeners = Vec::new();
    let mut sources = Vec::new();
    let mut local_binds = Vec::new();
    if reusable && config.policy != PathPolicy::RelayOnly {
        // Registration owns the source-port mapping even when the OS cannot
        // also bind a listener. Outgoing simultaneous-open still uses this port.
        sources.push(address);
        local_binds.push(actual);
        if address != actual {
            local_binds.push(address);
        }
        if let Ok((listener, _)) = sockets::listen(address) {
            listeners.push(listener);
        }
        for bind in &config.candidate_binds {
            if let Ok((listener, bound)) = sockets::listen(*bind) {
                sources.push(bound);
                local_binds.push(bound);
                listeners.push(listener);
            }
        }
    }
    let candidates = sockets::gather(&config, &local_binds)?;
    let (observed, auth) = tokio::time::timeout(
        DEADLINE,
        register(&mut stream, &config, session, &auth, &candidates),
    )
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "TCP admission timed out"))??;
    let peers = Arc::new(Mutex::new(HashMap::new()));
    let sessions = SessionRegistry::new(MAX_PEERS)?;
    let cancel = CancellationToken::new();
    let (outbound, outgoing) = mpsc::channel(QUEUE);
    let (incoming, inbound) = mpsc::channel(QUEUE);
    let addresses = if sources.is_empty() {
        vec![address]
    } else {
        sources.clone()
    };
    let local = config.local.clone();
    let runtime = Runtime {
        config,
        session,
        sources,
        control_live: true,
        peers: HashMap::new(),
        schedule: VecDeque::new(),
        pending_checks: HashSet::new(),
        views: peers.clone(),
        proofs: Arc::new(Mutex::new(HashMap::new())),
        sessions: sessions.clone(),
        incoming,
        cancel: cancel.clone(),
    };
    let task = tokio::spawn(runtime.run(stream, auth, listeners, outgoing));
    Ok(TcpConnection {
        inner: Arc::new(Inner {
            local,
            address,
            addresses,
            observed,
            candidates,
            peers,
            outbound,
            inbound: tokio::sync::Mutex::new(inbound),
            sessions,
            cancel,
            task: tokio::sync::Mutex::new(Some(task)),
        }),
    })
}

fn validate(config: &TcpPunchConfig) -> io::Result<()> {
    server::validate_peers(&config.peers)?;
    server::validate_peers(std::slice::from_ref(&config.local))?;
    if config.peers.contains(&config.local)
        || config.credential.len() > 1024
        || config.candidate_binds.len() > 3
        || config.advertised_candidates.len() > MAX_CANDIDATES
        || config
            .advertised_candidates
            .iter()
            .any(|address| !sockets::valid(*address))
        || !sockets::valid(config.rendezvous)
        || config.bind.is_ipv4() != config.rendezvous.is_ipv4()
        || config.bind.ip().is_multicast()
        || config
            .candidate_binds
            .iter()
            .any(|address| address.ip().is_multicast())
    {
        return Err(invalid("invalid TCP traversal configuration"));
    }
    Ok(())
}

async fn registration_socket(config: &TcpPunchConfig) -> io::Result<(TcpStream, bool)> {
    if let Ok(stream) = sockets::dial(config.bind, config.rendezvous).await {
        Ok((stream, true))
    } else {
        // Safe relay fallback, never claim a reusable/direct socket exists.
        let socket = if config.bind.is_ipv4() {
            TcpSocket::new_v4()?
        } else {
            TcpSocket::new_v6()?
        };
        socket.bind(config.bind)?;
        Ok((socket.connect(config.rendezvous).await?, false))
    }
}

async fn register(
    stream: &mut TcpStream,
    config: &TcpPunchConfig,
    session: Token,
    auth: &Auth,
    candidates: &[SocketAddr],
) -> io::Result<(SocketAddr, Duplex)> {
    let Message::Challenge(challenge) = wire::read(stream, auth).await? else {
        return Err(invalid("expected TCP rendezvous challenge"));
    };
    wire::write(
        stream,
        auth,
        &Message::Register {
            node: config.local.clone(),
            session,
            challenge,
            credential: config.credential.clone(),
            peers: config.peers.clone(),
            dynamic: config.dynamic,
            relay_only: config.policy == PathPolicy::RelayOnly,
            candidates: candidates.to_vec(),
        },
    )
    .await?;
    let auth = wire::control_auth(auth, &challenge, &session, wire::Role::Client);
    match wire::read(stream, &auth.rx).await? {
        Message::Welcome { observed } => Ok((observed, auth)),
        Message::Denied => Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "TCP rendezvous rejected duplicate identity or capacity",
        )),
        _ => Err(invalid("expected TCP rendezvous admission")),
    }
}

struct Runtime {
    config: TcpPunchConfig,
    session: Token,
    sources: Vec<SocketAddr>,
    control_live: bool,
    peers: HashMap<NodeId, Peer>,
    schedule: VecDeque<NodeId>,
    pending_checks: HashSet<(SocketAddr, SocketAddr)>,
    views: Views,
    proofs: Proofs,
    sessions: SessionRegistry,
    incoming: mpsc::Sender<AdmittedInbound>,
    cancel: CancellationToken,
}

impl Runtime {
    async fn run(
        mut self,
        stream: TcpStream,
        auth: Duplex,
        listeners: Vec<TcpListener>,
        mut outgoing: mpsc::Receiver<Outgoing>,
    ) {
        let (events, mut pending) = mpsc::channel(QUEUE);
        let (control, control_outgoing) = mpsc::channel(QUEUE);
        let dials = Arc::new(Semaphore::new(16));
        let accepts = Arc::new(Semaphore::new(16));
        let mut tasks = JoinSet::new();
        tasks.spawn(control_io(
            stream,
            auth,
            control_outgoing,
            events.clone(),
            self.cancel.clone(),
        ));
        for listener in listeners {
            tasks.spawn(direct::accept_loop(
                listener,
                self.config.local.clone(),
                self.session,
                self.proofs.clone(),
                accepts.clone(),
                events.clone(),
                self.cancel.clone(),
            ));
        }
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                () = self.cancel.cancelled() => break,
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                Some(event) = pending.recv() => {
                    if !self.event(event, &events, &mut tasks) { break; }
                }
                Some(message) = outgoing.recv() => self.send(message, &control),
                _ = tick.tick() => {
                    if self.control_live {
                        if control.try_send(Message::Ping).is_err() { self.control_closed(); }
                        else { self.check(&events, &dials, &mut tasks); }
                    }
                }
            }
        }
        self.cancel.cancel();
        self.sessions.close();
        lock(&self.views).clear();
        lock(&self.proofs).clear();
        drop(pending);
        let drained = tokio::time::timeout(DEADLINE + Duration::from_secs(1), async {
            while tasks.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tasks.abort_all();
            while tasks.join_next().await.is_some() {}
        }
    }

    fn event(
        &mut self,
        event: Event,
        events: &mpsc::Sender<Event>,
        tasks: &mut JoinSet<()>,
    ) -> bool {
        if matches!(event, Event::Control(_)) && !self.control_live {
            return true;
        }
        match event {
            Event::ControlClosed => self.control_closed(),
            Event::Control(Message::Intro {
                node,
                session,
                secret,
                candidates,
            }) => self.intro(node, session, secret, candidates),
            Event::Control(Message::Gone { node, session }) => {
                self.registration_gone(&node, session);
            }
            Event::Control(Message::Relay {
                node,
                session,
                data,
            }) => {
                if self.peers.get(&node).is_some_and(|peer| peer.relay_live) {
                    self.deliver(&node, session, data);
                }
            }
            Event::Control(Message::Ping) => {}
            Event::Control(_) => return false,
            Event::Ready {
                node,
                session,
                rank,
                stream,
                auth,
            } => self.adopt((node, session, rank), stream, auth, events, tasks),
            Event::Checked {
                node,
                session,
                tuple,
            } => {
                self.pending_checks.remove(&tuple);
                if let Some(peer) = self
                    .peers
                    .get_mut(&node)
                    .filter(|peer| peer.session == session)
                    && let Some(check) = peer.checks.iter_mut().find(|check| check.tuple == tuple)
                {
                    check.finished();
                }
            }
            Event::Data {
                node,
                session,
                rank,
                data,
            } => {
                if self
                    .peers
                    .get(&node)
                    .and_then(|peer| peer.direct.as_ref())
                    .is_some_and(|direct| direct.rank == rank)
                {
                    self.deliver(&node, session, data);
                }
            }
            Event::Closed {
                node,
                session,
                rank,
            } => {
                if let Some(peer) = self.peers.get_mut(&node)
                    && peer.session == session
                    && peer
                        .direct
                        .as_ref()
                        .is_some_and(|direct| direct.rank == rank)
                {
                    peer.direct.take();
                    for check in &mut peer.checks {
                        check.rearm();
                    }
                    if let Some(view) = lock(&self.views).get_mut(&node) {
                        view.direct = None;
                    }
                }
                if self.peers.get(&node).is_some_and(|peer| {
                    peer.session == session && !peer.relay_live && peer.direct.is_none()
                }) {
                    self.remove(&node);
                }
                if !self.control_live && self.peers.is_empty() {
                    self.cancel.cancel();
                }
            }
        }
        true
    }

    fn control_closed(&mut self) {
        self.control_live = false;
        lock(&self.proofs).clear();
        self.schedule.clear();
        for peer in self.peers.values_mut() {
            peer.relay_live = false;
            peer.checks.clear();
        }
        let withdrawn: Vec<_> = self
            .peers
            .iter()
            .filter(|(_, peer)| peer.direct.is_none())
            .map(|(node, _)| node.clone())
            .collect();
        for node in withdrawn {
            self.remove(&node);
        }
        if self.peers.is_empty() {
            self.cancel.cancel();
        }
    }

    fn registration_gone(&mut self, node: &NodeId, session: Token) {
        if let Some(peer) = self.peers.get_mut(node)
            && peer.session == session
        {
            peer.relay_live = false;
            peer.checks.clear();
            lock(&self.proofs).remove(node);
            self.schedule.retain(|scheduled| scheduled != node);
            if peer.direct.is_none() {
                self.remove(node);
            }
        }
    }

    fn remove(&mut self, node: &NodeId) {
        if let Some(peer) = self.peers.remove(node) {
            if let Some(direct) = peer.direct {
                direct.cancel.cancel();
            }
            peer.lease.revoke();
        }
        lock(&self.views).remove(node);
        lock(&self.proofs).remove(node);
        self.schedule.retain(|scheduled| scheduled != node);
    }

    fn intro(&mut self, node: NodeId, session: Token, secret: Token, candidates: Vec<SocketAddr>) {
        if !self.control_live
            || node == self.config.local
            || (!self.config.dynamic && !self.config.peers.contains(&node))
            || candidates.iter().any(|address| !sockets::valid(*address))
        {
            return;
        }
        if let Some(peer) = self.peers.get(&node) {
            // A surviving orphaned direct session is superseded only by a
            // fresh rendezvous admission, never a repeated introduction.
            if peer.relay_live || peer.session == session {
                return;
            }
            self.remove(&node);
        }
        let Ok(lease) = self.sessions.try_admit(AcceptedPeer::new(node.clone())) else {
            return;
        };
        // A captured plaintext introduction is not a keyed direct credential:
        // keyed proofs additionally require possession of the fabric key.
        let secret = self.config.key.as_ref().map_or(secret, |key| {
            wire::proof(&key.to_bytes(), b"keyed pair", &[&secret])
        });
        lock(&self.views).insert(
            node.clone(),
            View {
                session,
                lease: lease.clone(),
                direct: None,
            },
        );
        lock(&self.proofs).insert(node.clone(), ProofPeer { session, secret });
        let mut checks: Vec<checks::Check> = Vec::new();
        for target in candidates {
            for source in &self.sources {
                let tuple = (*source, target);
                if source.is_ipv4() == target.is_ipv4()
                    && !checks.iter().any(|check| check.tuple == tuple)
                {
                    checks.push(checks::Check::new(tuple));
                }
            }
        }
        self.schedule.push_back(node.clone());
        self.peers.insert(
            node,
            Peer {
                session,
                secret,
                lease,
                relay_live: true,
                checks,
                next_check: 0,
                direct: None,
            },
        );
    }

    fn deliver(&self, node: &NodeId, session: Token, data: Vec<u8>) {
        let Some(peer) = self.peers.get(node) else {
            return;
        };
        if peer.session != session || !peer.lease.is_active() {
            return;
        }
        let _ = self.incoming.try_send(AdmittedInbound {
            packet: Inbound {
                from: node.clone(),
                msg: data,
            },
            session: Some(peer.lease.id()),
        });
    }

    fn send(&self, message: Outgoing, control: &mpsc::Sender<Message>) {
        let Some(peer) = self.peers.get(&message.to) else {
            return;
        };
        if peer.session != message.target
            || peer.lease.id() != message.generation
            || !peer.lease.is_active()
        {
            return;
        }
        let data = if let Some(direct) = &peer.direct {
            match direct.writer.try_send(Message::Data(message.message)) {
                Ok(()) => return,
                Err(error) => match error.into_inner() {
                    Message::Data(data) => data,
                    _ => return,
                },
            }
        } else {
            message.message
        };
        if self.control_live && peer.relay_live {
            let _ = control.try_send(Message::Relay {
                node: message.to,
                session: message.target,
                data,
            });
        }
    }

    fn adopt(
        &mut self,
        identity: (NodeId, Token, Rank),
        stream: TcpStream,
        auth: Duplex,
        events: &mpsc::Sender<Event>,
        tasks: &mut JoinSet<()>,
    ) {
        let (node, session, rank) = identity;
        if self.config.policy == PathPolicy::RelayOnly {
            return;
        }
        let Some(peer) = self.peers.get_mut(&node) else {
            return;
        };
        if !peer.relay_live
            || peer.session != session
            || !peer.lease.is_active()
            || peer
                .direct
                .as_ref()
                .is_some_and(|direct| direct.rank <= rank)
        {
            return;
        }
        let Ok(address) = stream.peer_addr() else {
            return;
        };
        let (writer, outgoing) = mpsc::channel(QUEUE);
        let cancel = self.cancel.child_token();
        if let Some(old) = peer.direct.take() {
            old.cancel.cancel();
        }
        peer.direct = Some(Direct {
            rank,
            writer,
            cancel: cancel.clone(),
        });
        if let Some(view) = lock(&self.views).get_mut(&node) {
            view.direct = Some(address);
        }
        tasks.spawn(direct::connection(
            stream,
            auth,
            outgoing,
            events.clone(),
            (node, session, rank),
            cancel,
        ));
    }
}

async fn control_io(
    stream: TcpStream,
    auth: Duplex,
    mut outgoing: mpsc::Receiver<Message>,
    events: mpsc::Sender<Event>,
    cancel: CancellationToken,
) {
    let (mut reader, mut writer) = stream.into_split();
    let reading = read_control(&mut reader, &auth.rx, &events);
    let writing = async {
        while let Some(message) = outgoing.recv().await {
            tokio::time::timeout(DEADLINE, wire::write(&mut writer, &auth.tx, &message))
                .await
                .map_err(|_| closed())??;
        }
        Ok::<(), io::Error>(())
    };
    tokio::select! { () = cancel.cancelled() => {}, _ = reading => {}, _ = writing => {} }
    let _ = events.send(Event::ControlClosed).await;
}

async fn read_control(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    auth: &Auth,
    events: &mpsc::Sender<Event>,
) -> io::Result<()> {
    loop {
        let message = tokio::time::timeout(IDLE, wire::read(reader, auth))
            .await
            .map_err(|_| closed())??;
        events
            .send(Event::Control(message))
            .await
            .map_err(|_| closed())?;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn streams() -> (TcpStream, TcpStream) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let (connecting, accepting) = tokio::join!(TcpStream::connect(address), listener.accept());
        (connecting.unwrap(), accepting.unwrap().0)
    }

    fn runtime() -> (Runtime, mpsc::Receiver<AdmittedInbound>) {
        let config = TcpPunchConfig::dynamic(
            NodeId::from("local"),
            "127.0.0.1:1".parse().unwrap(),
            Some(crate::NetworkKey::from_bytes([1; 32])),
            Vec::new(),
        );
        let (incoming, delivered) = mpsc::channel(8);
        let runtime = Runtime {
            config,
            session: [2; 32],
            sources: Vec::new(),
            control_live: true,
            peers: HashMap::new(),
            schedule: VecDeque::new(),
            pending_checks: HashSet::new(),
            views: Arc::new(Mutex::new(HashMap::new())),
            proofs: Arc::new(Mutex::new(HashMap::new())),
            sessions: SessionRegistry::new(8).unwrap(),
            incoming,
            cancel: CancellationToken::new(),
        };
        (runtime, delivered)
    }

    #[tokio::test]
    async fn duplicate_loser_never_closes_winner_and_failed_direct_falls_back_to_relay() {
        let (mut runtime, mut delivered) = runtime();
        let views = runtime.views.clone();
        let node = NodeId::from("peer");
        let session = [3; 32];
        runtime.intro(node.clone(), session, [4; 32], Vec::new());
        let generation = runtime.peers.get(&node).unwrap().lease.id();
        let (events, mut pending) = mpsc::channel(32);
        let mut tasks = JoinSet::new();
        let winner = Rank {
            direction: 0,
            nonce: [1; 32],
        };
        let loser = Rank {
            direction: 1,
            nonce: [0; 32],
        };
        let (winning, remote) = streams().await;
        runtime.adopt(
            (node.clone(), session, winner),
            winning,
            Duplex::plain(),
            &events,
            &mut tasks,
        );
        let winning_cancel = runtime
            .peers
            .get(&node)
            .unwrap()
            .direct
            .as_ref()
            .unwrap()
            .cancel
            .clone();
        let (losing, _other) = streams().await;
        runtime.adopt(
            (node.clone(), session, loser),
            losing,
            Duplex::plain(),
            &events,
            &mut tasks,
        );
        assert!(!winning_cancel.is_cancelled());
        runtime.event(
            Event::Closed {
                node: node.clone(),
                session,
                rank: loser,
            },
            &events,
            &mut tasks,
        );
        assert!(lock(&views).get(&node).unwrap().direct.is_some());
        // Closing the actual winning socket yields its exact ranked Close event.
        drop(remote);
        let event = tokio::time::timeout(DEADLINE, pending.recv())
            .await
            .unwrap()
            .unwrap();
        assert!(matches!(event, Event::Closed { .. }));
        runtime.event(event, &events, &mut tasks);
        assert!(lock(&views).get(&node).unwrap().direct.is_none());
        assert!(runtime.sessions.is_active(&node, generation));
        runtime.deliver(&node, session, b"relay after direct failure".to_vec());
        assert_eq!(
            delivered.recv().await.unwrap().packet.msg,
            b"relay after direct failure"
        );
        let (stream, _remote) = streams().await;
        runtime.adopt(
            (node.clone(), [9; 32], winner),
            stream,
            Duplex::plain(),
            &events,
            &mut tasks,
        );
        assert!(runtime.peers.get(&node).unwrap().direct.is_none());
        runtime.cancel.cancel();
        while tasks.join_next().await.is_some() {}
    }
}
