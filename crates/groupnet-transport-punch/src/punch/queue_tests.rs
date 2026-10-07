//! Owned payload, queue backpressure, and operational configuration regressions.

use super::*;
use groupnet_transport::admission::AcceptedPeer;

fn connection() -> (UdpConnection, mpsc::Receiver<Outbound>) {
    let sessions = SessionRegistry::new(1).unwrap();
    let node = NodeId::from("remote");
    let lease = sessions
        .try_admit(AcceptedPeer { node: node.clone() })
        .unwrap();
    let peer = Peer {
        node: node.clone(),
        session: [3; 16],
        addresses: candidates::PeerCandidates::default(),
        secret: [7; 16],
        relay_only: true,
        offered: Instant::now(),
        checks: Vec::new(),
        selected: None,
        candidate_cursor: 0,
        sequence: 0,
        lease,
    };
    let (outbound, outgoing) = mpsc::channel(1);
    let (_, inbound) = mpsc::channel(1);
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    let connection = UdpConnection {
        inner: Arc::new(Inner {
            local: "local".into(),
            address,
            addresses: vec![address],
            candidates: candidates::Candidates::default(),
            observed: Arc::new(Mutex::new(None)),
            configured: vec![node],
            peers: Arc::new(Mutex::new(HashMap::from([("remote".to_owned(), peer)]))),
            outbound,
            inbound: AsyncMutex::new(inbound),
            sessions,
            dynamic: false,
            cancel: CancellationToken::new(),
            task: AsyncMutex::new(None),
        }),
    };
    (connection, outgoing)
}

#[tokio::test]
async fn owned_queue_preserves_storage_and_full_queue_skips_borrowed_copy() {
    let (connection, mut outgoing) = connection();
    let remote = NodeId::from("remote");
    let session = lock(&connection.inner.peers)["remote"].lease.id();
    let payload = Bytes::from(vec![42; MAX_MESSAGE]);
    let pointer = payload.as_ptr();
    connection
        .send_owned_admitted(&remote, payload, Some(session))
        .await
        .unwrap();
    connection
        .enqueue(&remote, 1, Some(session), || {
            panic!("a full queue must not copy a borrowed payload")
        })
        .unwrap();
    connection
        .send_owned(&remote, Bytes::from_static(b"dropped"))
        .await
        .unwrap();
    let queued = outgoing.try_recv().unwrap();
    assert_eq!(queued.message.as_ptr(), pointer);
    assert_eq!(queued.message.len(), MAX_MESSAGE);
    assert_eq!(queued.session, session);
    assert_eq!(queued.target, [3; 16]);
    assert!(outgoing.try_recv().is_err());
    connection
        .send(&remote, b"after capacity returns")
        .await
        .unwrap();
    assert_eq!(
        outgoing.try_recv().unwrap().message.as_ref(),
        b"after capacity returns"
    );
}

#[tokio::test]
async fn owned_sends_reject_oversize_missing_generation_and_closed_connection() {
    let (connection, mut outgoing) = connection();
    let remote = NodeId::from("remote");
    assert_eq!(
        connection
            .send_owned(&remote, Bytes::from(vec![0; MAX_MESSAGE + 1]))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        connection
            .send_owned_admitted(&remote, Bytes::from_static(b"untagged"), None)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert!(outgoing.try_recv().is_err());
    connection.shutdown();
    assert_eq!(
        connection
            .send_owned(&remote, Bytes::from_static(b"closed"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
}

#[tokio::test]
async fn owned_admitted_send_drops_replaced_generation() {
    let (connection, mut outgoing) = connection();
    let remote = NodeId::from("remote");
    let old = lock(&connection.inner.peers)["remote"].lease.clone();
    old.revoke();
    let replacement = connection
        .inner
        .sessions
        .try_admit(AcceptedPeer {
            node: remote.clone(),
        })
        .unwrap();
    lock(&connection.inner.peers)
        .get_mut("remote")
        .unwrap()
        .lease = replacement.clone();
    connection
        .send_owned_admitted(&remote, Bytes::from_static(b"stale"), Some(old.id()))
        .await
        .unwrap();
    assert!(outgoing.try_recv().is_err());
    connection
        .send_owned_admitted(
            &remote,
            Bytes::from_static(b"current"),
            Some(replacement.id()),
        )
        .await
        .unwrap();
    assert_eq!(outgoing.try_recv().unwrap().message.as_ref(), b"current");
}

#[tokio::test]
async fn operational_limits_are_validated_before_socket_binding() {
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    // Queue capacities are valid by construction (`QueueCapacity`); the peer
    // limit is still checked before any socket is bound.
    let mut config = PunchConfig::open("local".into(), address);
    config.max_peers = 0;
    assert_eq!(
        UdpConnection::bind(config).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let mut config = PunchConfig::new(
        "local".into(),
        address,
        NetworkKey::from_bytes([1; 32]),
        vec!["a".into(), "b".into()],
    );
    config.max_peers = 1;
    assert_eq!(
        UdpConnection::bind(config).await.unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let defaults = PunchConfig::open("local".into(), address);
    assert_eq!(defaults.max_peers, DEFAULT_MAX_PEERS);
    assert_eq!(defaults.queue_capacity, DEFAULT_QUEUE_CAPACITY);
}

#[tokio::test]
async fn configured_capacities_apply_to_both_queues_and_admission_registry() {
    let address = SocketAddr::from(([127, 0, 0, 1], 1234));
    let mut config = PunchConfig::new(
        "local".into(),
        address,
        NetworkKey::from_bytes([1; 32]),
        Vec::new(),
    );
    config.bind = SocketAddr::from(([127, 0, 0, 1], 0));
    config.gather_interfaces = false;
    config.max_peers = 2;
    config.queue_capacity = QueueCapacity::of(1);
    let connection = UdpConnection::bind(config).await.unwrap();
    assert_eq!(connection.inner.outbound.max_capacity(), 1);
    assert_eq!(connection.inner.inbound.lock().await.max_capacity(), 1);
    let sessions = connection.sessions();
    let first = sessions
        .try_admit(AcceptedPeer {
            node: "first".into(),
        })
        .unwrap();
    let second = sessions
        .try_admit(AcceptedPeer {
            node: "second".into(),
        })
        .unwrap();
    assert!(
        sessions
            .try_admit(AcceptedPeer {
                node: "overflow".into()
            })
            .is_err()
    );
    assert!(first.is_active());
    assert!(second.is_active());
    connection.close().await;
    assert!(!first.is_active());
    assert!(!second.is_active());
}
