//! Consumer APIs exercised against a real rendezvous and an admitted wire peer.
use super::{
    ProofPeer, Proofs, TcpConnection, TcpPunchConfig, direct, lock, register,
    wire::{self, Message, Token},
};
use crate::{PathPolicy, PeerPath, TcpRendezvous};
use groupnet_core::NodeId;
use groupnet_testkit::cluster::eventually_within;
use std::{
    collections::HashMap,
    io,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::net::{TcpListener, TcpStream};
use tokio_util::sync::CancellationToken;

const RECOVER: Duration = Duration::from_secs(20);

async fn control_read(
    reader: &mut tokio::net::tcp::OwnedReadHalf,
    proofs: &Proofs,
) -> io::Result<()> {
    loop {
        match wire::read(reader, &None).await? {
            Message::Intro {
                node,
                session,
                secret,
                ..
            } => {
                lock(proofs).insert(node, ProofPeer { session, secret });
            }
            Message::Gone { node, .. } => {
                lock(proofs).remove(&node);
            }
            _ => {}
        }
    }
}

async fn control_write(writer: &mut tokio::net::tcp::OwnedWriteHalf) -> io::Result<()> {
    let mut heartbeat = tokio::time::interval(Duration::from_secs(1));
    loop {
        heartbeat.tick().await;
        wire::write(writer, &None, &Message::Ping).await?;
    }
}

async fn authenticated_accept(
    listener: &TcpListener,
    session: Token,
    proofs: &Proofs,
) -> (TcpStream, wire::Duplex) {
    let (stream, _) = tokio::time::timeout(RECOVER, listener.accept())
        .await
        .unwrap()
        .unwrap();
    let event = tokio::time::timeout(
        Duration::from_secs(3),
        direct::accept(stream, NodeId::from("peer"), session, proofs.clone()),
    )
    .await
    .unwrap()
    .unwrap();
    let super::Event::Ready { stream, auth, .. } = event else {
        panic!("missing validated stream");
    };
    (stream, auth)
}

async fn read_data(stream: &mut TcpStream, auth: &wire::Duplex) -> Vec<u8> {
    loop {
        match wire::read(stream, &auth.rx).await.unwrap() {
            Message::Data(data) => return data,
            Message::Ping => {}
            _ => panic!("unexpected direct message"),
        }
    }
}

struct Fixture {
    rendezvous: TcpRendezvous,
    listener: TcpListener,
    peer_session: Token,
    proofs: Proofs,
    cancelled: CancellationToken,
    control: tokio::task::JoinHandle<()>,
    client: TcpConnection,
}

async fn fixture() -> Fixture {
    let rendezvous = TcpRendezvous::bind_open("127.0.0.1:0".parse().unwrap())
        .await
        .unwrap();
    let address = rendezvous.local_addr().unwrap();
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let candidate = listener.local_addr().unwrap();
    let peer_session = [19; 32];
    let mut config = TcpPunchConfig::open(NodeId::from("peer"), address);
    config.policy = PathPolicy::DirectPreferred;
    let mut registration = TcpStream::connect(address).await.unwrap();
    register(
        &mut registration,
        &config,
        peer_session,
        &None,
        &[candidate],
    )
    .await
    .unwrap();
    let proofs: Proofs = Arc::new(Mutex::new(HashMap::new()));
    let cancelled = CancellationToken::new();
    let cancel = cancelled.clone();
    let introductions = proofs.clone();
    let control = tokio::spawn(async move {
        let (mut reader, mut writer) = registration.into_split();
        tokio::select! { () = cancel.cancelled() => {}, _ = control_read(&mut reader, &introductions) => {}, _ = control_write(&mut writer) => {} }
    });
    let mut config = TcpPunchConfig::open(NodeId::from("client"), address);
    config.bind = "127.0.0.1:0".parse().unwrap();
    config.policy = PathPolicy::DirectPreferred;
    let client = TcpConnection::bind(config).await.unwrap();
    eventually_within("real TCP peer introduced", RECOVER, || {
        client.path_to(&NodeId::from("peer")) == Some(PeerPath::Relay)
    })
    .await;
    Fixture {
        rendezvous,
        listener,
        peer_session,
        proofs,
        cancelled,
        control,
        client,
    }
}

#[tokio::test]
async fn exhausted_burst_and_winner_loss_recover_without_replacing_admission() {
    let Fixture {
        rendezvous,
        listener,
        peer_session,
        proofs,
        cancelled,
        control,
        client,
    } = fixture().await;
    let registry = client.sessions();
    assert_eq!(client.local_id(), &NodeId::from("client"));
    let same_registry = client.clone().sessions();
    let generation = registry
        .subscribe()
        .borrow()
        .iter()
        .find(|peer| peer.node == NodeId::from("peer"))
        .unwrap()
        .id;
    // Exhaust exactly three valid candidate dials without completing proof.
    // The other, rendezvous-observed registration source is not listening.
    for _ in 0..3 {
        let (mut stream, _) = tokio::time::timeout(RECOVER, listener.accept())
            .await
            .unwrap()
            .unwrap();
        let message = wire::read(&mut stream, &None).await.unwrap();
        assert!(matches!(message, Message::Hello { .. }));
    }
    assert_eq!(client.path_to(&NodeId::from("peer")), Some(PeerPath::Relay));
    assert!(
        tokio::time::timeout(Duration::from_secs(2), listener.accept())
            .await
            .is_err()
    );
    // The same public transport and session now recover after cooldown.
    let (mut stream, auth) = authenticated_accept(&listener, peer_session, &proofs).await;
    eventually_within("TCP direct recovery after exhausted burst", RECOVER, || {
        client.path_to(&NodeId::from("peer")) == Some(PeerPath::Direct)
    })
    .await;
    assert!(registry.is_active(&NodeId::from("peer"), generation));
    client
        .send(&NodeId::from("peer"), b"recovered after cooldown")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(RECOVER, read_data(&mut stream, &auth))
            .await
            .unwrap(),
        b"recovered after cooldown"
    );
    wire::write(
        &mut stream,
        &auth.tx,
        &Message::Data(b"first reply".to_vec()),
    )
    .await
    .unwrap();
    assert_eq!(client.recv().await.unwrap().msg, b"first reply");
    drop(stream);
    eventually_within("failed winning TCP stream falls back", RECOVER, || {
        client.path_to(&NodeId::from("peer")) == Some(PeerPath::Relay)
    })
    .await;
    let (mut stream, auth) = authenticated_accept(&listener, peer_session, &proofs).await;
    eventually_within("TCP direct recovery after winner loss", RECOVER, || {
        client.path_to(&NodeId::from("peer")) == Some(PeerPath::Direct)
    })
    .await;
    assert!(registry.is_active(&NodeId::from("peer"), generation));
    assert!(same_registry.is_active(&NodeId::from("peer"), generation));
    client
        .send(&NodeId::from("peer"), b"same admission, new direct stream")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(RECOVER, read_data(&mut stream, &auth))
            .await
            .unwrap(),
        b"same admission, new direct stream"
    );
    drop(stream);
    client.close().await;
    assert!(!registry.is_active(&NodeId::from("peer"), generation));
    assert!(!same_registry.is_active(&NodeId::from("peer"), generation));
    assert_eq!(client.local_id(), &NodeId::from("client"));
    cancelled.cancel();
    control.await.unwrap();
    rendezvous.close().await;
}

#[tokio::test]
async fn one_sided_registration_loss_preserves_direct_session_until_stream_closes() {
    disconnected_peer(false).await;
}

#[tokio::test]
async fn fresh_registration_supersedes_orphaned_direct_generation_and_queued_data() {
    disconnected_peer(true).await;
}

async fn disconnected_peer(replace_before_close: bool) {
    let Fixture {
        rendezvous,
        listener,
        peer_session,
        proofs,
        cancelled,
        control,
        client,
    } = fixture().await;
    let peer = NodeId::from("peer");
    let (mut stream, auth) = authenticated_accept(&listener, peer_session, &proofs).await;
    eventually_within("healthy direct session", RECOVER, || {
        client.path_to(&peer) == Some(PeerPath::Direct)
    })
    .await;
    let registry = client.sessions();
    let generation = registry
        .subscribe()
        .borrow()
        .iter()
        .find(|entry| entry.node == peer)
        .unwrap()
        .id;
    cancelled.cancel();
    control.await.unwrap();
    // Continue beyond the registration idle deadline while the server remains
    // live. Losing just this control leg must not revoke the direct generation.
    for _ in 0..7 {
        direct_request(&client, &peer, &mut stream, &auth).await;
        wire::write(
            &mut stream,
            &auth.tx,
            &Message::Data(b"direct reply".to_vec()),
        )
        .await
        .unwrap();
        let received = tokio::time::timeout(RECOVER, client.recv_admitted())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(received.packet.from, peer);
        assert_eq!(received.packet.msg, b"direct reply");
        assert_eq!(received.session, Some(generation));
        let started = tokio::time::Instant::now();
        eventually_within("control-disruption observation window", RECOVER, || {
            started.elapsed() >= Duration::from_secs(1)
        })
        .await;
        assert!(registry.is_active(&peer, generation));
    }
    let mut old_stream = Some(stream);
    if replace_before_close {
        wire::write(
            old_stream.as_mut().unwrap(),
            &auth.tx,
            &Message::Data(b"obsolete queued generation".to_vec()),
        )
        .await
        .unwrap();
    } else {
        old_stream.take();
        eventually_within(
            "lost direct peer has no stale relay fallback",
            RECOVER,
            || client.path_to(&peer).is_none() && !registry.is_active(&peer, generation),
        )
        .await;
    }
    let replacement = TcpConnection::bind(TcpPunchConfig::open(
        peer.clone(),
        rendezvous.local_addr().unwrap(),
    ))
    .await
    .unwrap();
    eventually_within(
        "live rendezvous admits a replacement generation",
        RECOVER,
        || client.path_to(&peer) == Some(PeerPath::Relay),
    )
    .await;
    assert!(!registry.is_active(&peer, generation));
    drop(old_stream);
    replacement
        .send(client.local_id(), b"fresh generation")
        .await
        .unwrap();
    let received = tokio::time::timeout(RECOVER, client.recv_admitted())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(received.packet.from, peer);
    assert_eq!(received.packet.msg, b"fresh generation");
    assert_ne!(received.session, Some(generation));
    replacement.close().await;
    client.close().await;
    rendezvous.close().await;
}

async fn direct_request(
    client: &TcpConnection,
    peer: &NodeId,
    stream: &mut TcpStream,
    auth: &wire::Duplex,
) {
    client
        .send(peer, b"direct after registration loss")
        .await
        .unwrap();
    assert_eq!(
        tokio::time::timeout(RECOVER, read_data(stream, auth))
            .await
            .unwrap(),
        b"direct after registration loss"
    );
}
