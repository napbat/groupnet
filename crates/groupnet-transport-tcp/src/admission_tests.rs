use std::sync::atomic::{AtomicUsize, Ordering};

use groupnet_transport::Transport;
use groupnet_transport::admission::{AcceptedPeer, OpenAdmission};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::*;

async fn endpoint(id: &str, policy: Arc<dyn Admission>, credential: &[u8]) -> TcpMsgTransport {
    TcpMsgTransport::bind_admitted(
        NodeId::new(id),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        policy,
        credential.to_vec(),
        TcpAdmissionConfig::default(),
    )
    .await
    .expect("admitted bind")
}

async fn eventually(mut condition: impl FnMut() -> bool) {
    timeout(Duration::from_secs(5), async {
        while !condition() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("condition converges");
}

fn peers(endpoint: &TcpMsgTransport) -> usize {
    endpoint
        .sessions()
        .expect("sessions")
        .subscribe()
        .borrow()
        .len()
}

async fn receive(endpoint: &TcpMsgTransport) -> groupnet_transport::Inbound {
    timeout(Duration::from_secs(5), endpoint.recv())
        .await
        .expect("receive deadline")
        .expect("receive")
}

fn connect(client: &TcpMsgTransport, server: &TcpMsgTransport) {
    client.register_peer(server.local_id().clone(), server.local_addr());
    client
        .connect_peer(server.local_id())
        .expect("connect bootstrap");
}

#[tokio::test]
async fn unknown_keyless_joiner_receives_server_traffic_without_reverse_dial() {
    let server = endpoint("server", Arc::new(OpenAdmission), &[]).await;
    // An unspecified listener advertises nothing: the server cannot dial it.
    let client = TcpMsgTransport::bind_admitted(
        NodeId::new("uuid-unknown-client"),
        "0.0.0.0:0",
        TcpMsgConfig::default(),
        Arc::new(OpenAdmission),
        Vec::new(),
        TcpAdmissionConfig::default(),
    )
    .await
    .expect("client");
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    assert_eq!(server.peer_addr(client.local_id()), None);
    server
        .send(client.local_id(), b"server initiated")
        .await
        .expect("send");
    assert_eq!(receive(&client).await.msg.as_ref(), b"server initiated");
    client
        .send(server.local_id(), b"reply")
        .await
        .expect("send");
    assert_eq!(receive(&server).await.msg.as_ref(), b"reply");
    client.close().await;
    eventually(|| peers(&server) == 0).await;
    server.close().await;
}

#[derive(Debug)]
struct Accounts {
    called: AtomicUsize,
}

impl Admission for Accounts {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            self.called.fetch_add(1, Ordering::SeqCst);
            assert!(request.remote.is_some());
            if request.claimed.as_str() == "account:alice" && request.credential == b"alice-token" {
                Ok(AcceptedPeer::new(request.claimed.clone()))
            } else {
                Err(denied())
            }
        })
    }
}

#[tokio::test]
async fn custom_policy_rejects_wrong_credentials_and_claims_without_leaking_secrets() {
    let policy = Arc::new(Accounts {
        called: AtomicUsize::new(0),
    });
    let server = endpoint("server", policy.clone(), &[]).await;
    for (claim, credential) in [
        ("account:alice", &b"wrong"[..]),
        ("account:mallory", &b"alice-token"[..]),
    ] {
        let client = endpoint(claim, Arc::new(OpenAdmission), credential).await;
        connect(&client, &server);
        let expected = policy.called.load(Ordering::SeqCst) + 1;
        eventually(|| policy.called.load(Ordering::SeqCst) >= expected).await;
        eventually(|| {
            client
                .direct()
                .expect("direct endpoint")
                .admission
                .as_ref()
                .expect("managed")
                .connections
                .lock()
                .expect("connections")
                .peers
                .is_empty()
        })
        .await;
        assert_eq!(peers(&server), 0);
        assert_eq!(peers(&client), 0);
        client.close().await;
    }
    let client = endpoint("account:alice", Arc::new(OpenAdmission), b"alice-token").await;
    assert!(!format!("{client:?}").contains("alice-token"));
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    server
        .send(client.local_id(), b"authorized")
        .await
        .expect("send");
    assert_eq!(receive(&client).await.msg.as_ref(), b"authorized");
    client.close().await;
    server.close().await;
}

#[tokio::test]
async fn duplicate_identity_cannot_evict_incumbent_and_clean_reconnect_works() {
    let server = endpoint("server", Arc::new(OpenAdmission), &[]).await;
    let incumbent = endpoint("same-id", Arc::new(OpenAdmission), &[]).await;
    connect(&incumbent, &server);
    eventually(|| peers(&server) == 1 && peers(&incumbent) == 1).await;
    let old = server.sessions().expect("sessions").subscribe().borrow()[0].id;
    let duplicate = endpoint("same-id", Arc::new(OpenAdmission), &[]).await;
    connect(&duplicate, &server);
    eventually(|| {
        duplicate
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .connections
            .lock()
            .expect("connections")
            .peers
            .is_empty()
    })
    .await;
    assert_eq!(peers(&duplicate), 0);
    assert!(
        server
            .sessions()
            .expect("sessions")
            .is_active(incumbent.local_id(), old)
    );
    server
        .send(incumbent.local_id(), b"incumbent")
        .await
        .expect("send");
    assert_eq!(receive(&incumbent).await.msg.as_ref(), b"incumbent");
    incumbent.close().await;
    eventually(|| peers(&server) == 0).await;
    connect(&duplicate, &server);
    eventually(|| peers(&server) == 1 && peers(&duplicate) == 1).await;
    let new = server.sessions().expect("sessions").subscribe().borrow()[0].id;
    assert_ne!(old, new);
    assert!(
        !server
            .sessions()
            .expect("sessions")
            .is_active(duplicate.local_id(), old)
    );
    server
        .send(duplicate.local_id(), b"replacement")
        .await
        .expect("send");
    assert_eq!(receive(&duplicate).await.msg.as_ref(), b"replacement");
    duplicate.close().await;
    server.close().await;
}

#[tokio::test]
async fn simultaneous_dials_converge_to_one_full_duplex_session() {
    let a = endpoint("a", Arc::new(OpenAdmission), &[]).await;
    let b = endpoint("b", Arc::new(OpenAdmission), &[]).await;
    a.register_peer(b.local_id().clone(), b.local_addr());
    b.register_peer(a.local_id().clone(), a.local_addr());
    // Both provisional queues exist before any task can execute the handshake.
    a.connect_peer(b.local_id()).expect("a dial");
    b.connect_peer(a.local_id()).expect("b dial");
    eventually(|| peers(&a) == 1 && peers(&b) == 1).await;
    a.send(b.local_id(), b"a to b").await.expect("send");
    b.send(a.local_id(), b"b to a").await.expect("send");
    assert_eq!(receive(&a).await.msg.as_ref(), b"b to a");
    assert_eq!(receive(&b).await.msg.as_ref(), b"a to b");
    a.close().await;
    b.close().await;
}

#[tokio::test]
async fn revoke_disconnects_and_fences_queued_frames_before_reconnect() {
    let server = endpoint("server", Arc::new(OpenAdmission), &[]).await;
    let client = endpoint("client", Arc::new(OpenAdmission), &[]).await;
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    let sessions = server.sessions().expect("sessions");
    let generation = sessions.subscribe().borrow()[0].id;
    client
        .send(server.local_id(), b"old queued frame")
        .await
        .expect("send");
    // Wait for the frame to queue without consuming it, then revoke its producer.
    eventually(|| {
        server
            .direct()
            .expect("direct endpoint")
            .inbox
            .try_lock()
            .is_ok_and(|inbox| !inbox.is_empty())
    })
    .await;
    sessions.revoke(client.local_id());
    eventually(|| peers(&client) == 0).await;
    eventually(|| {
        client
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .connections
            .lock()
            .expect("connections")
            .peers
            .is_empty()
    })
    .await;
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    let queued = server.recv_admitted().await.expect("queued frame");
    assert_eq!(queued.session, Some(generation));
    assert!(!sessions.is_active(client.local_id(), queued.session.expect("tag")));
    client.close().await;
    server.close().await;
}

#[derive(Debug)]
struct Stall {
    calls: AtomicUsize,
}

impl Admission for Stall {
    fn admit<'a>(&'a self, request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if request.claimed.as_str() == "stall" {
                std::future::pending::<()>().await;
            }
            Ok(AcceptedPeer::new(request.claimed.clone()))
        })
    }
}

#[tokio::test]
async fn pending_policy_capacity_and_deadline_are_bounded_and_shutdown_drains() {
    let policy = Arc::new(Stall {
        calls: AtomicUsize::new(0),
    });
    let server = TcpMsgTransport::bind_admitted(
        NodeId::new("server"),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        policy.clone(),
        Vec::new(),
        TcpAdmissionConfig {
            max_pending: 1,
            handshake_timeout: Duration::from_secs(1),
            ..TcpAdmissionConfig::default()
        },
    )
    .await
    .expect("bind");
    let stalled = endpoint("stall", Arc::new(OpenAdmission), &[]).await;
    connect(&stalled, &server);
    eventually(|| policy.calls.load(Ordering::SeqCst) == 1).await;
    let denied = endpoint("denied-during-pending", Arc::new(OpenAdmission), &[]).await;
    connect(&denied, &server);
    eventually(|| {
        denied
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .connections
            .lock()
            .expect("connections")
            .peers
            .is_empty()
    })
    .await;
    assert_eq!(policy.calls.load(Ordering::SeqCst), 1);
    eventually(|| {
        server
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .pending
            .available_permits()
            == 1
    })
    .await;
    let allowed = endpoint("allowed-after-timeout", Arc::new(OpenAdmission), &[]).await;
    connect(&allowed, &server);
    eventually(|| peers(&server) == 1 && peers(&allowed) == 1).await;
    let address = server.local_addr();
    let mut incomplete = TcpStream::connect(address)
        .await
        .expect("incomplete socket");
    timeout(Duration::from_secs(5), server.close())
        .await
        .expect("drained close");
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(5), incomplete.read(&mut byte))
            .await
            .expect("released"),
        Ok(0) | Err(_)
    ));
    let _replacement = tokio::net::TcpListener::bind(address)
        .await
        .expect("listener released");
    assert_eq!(peers(&server), 0);
    stalled.close().await;
    denied.close().await;
    allowed.close().await;
}

#[tokio::test]
async fn oversized_wire_credentials_are_rejected_before_policy_and_capacity_holds() {
    let policy = Arc::new(Stall {
        calls: AtomicUsize::new(0),
    });
    let server = TcpMsgTransport::bind_admitted(
        NodeId::new("server"),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        policy.clone(),
        Vec::new(),
        TcpAdmissionConfig {
            max_peers: 1,
            ..TcpAdmissionConfig::default()
        },
    )
    .await
    .expect("server");
    let mut malformed = TcpStream::connect(server.local_addr())
        .await
        .expect("connect");
    malformed.write_all(MAGIC).await.expect("magic");
    write_id(&mut malformed, &NodeId::new("bad"))
        .await
        .expect("id");
    write_str(&mut malformed, "").await.expect("intro");
    malformed
        .write_u32(u32::try_from(MAX_CREDENTIAL_BYTES + 1).expect("length"))
        .await
        .expect("length");
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(5), malformed.read(&mut byte))
            .await
            .expect("rejected"),
        Ok(0) | Err(_)
    ));
    assert_eq!(policy.calls.load(Ordering::SeqCst), 0);
    let first = endpoint("first", Arc::new(OpenAdmission), &[]).await;
    connect(&first, &server);
    eventually(|| peers(&server) == 1 && peers(&first) == 1).await;
    let excess = endpoint("excess", Arc::new(OpenAdmission), &[]).await;
    connect(&excess, &server);
    eventually(|| {
        excess
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .connections
            .lock()
            .expect("connections")
            .peers
            .is_empty()
    })
    .await;
    assert_eq!(peers(&server), 1);
    assert_eq!(peers(&excess), 0);
    first.close().await;
    excess.close().await;
    server.close().await;
}

#[derive(Debug)]
struct Rename;

impl Admission for Rename {
    fn admit<'a>(&'a self, _request: JoinRequest<'a>) -> LinkFuture<'a, io::Result<AcceptedPeer>> {
        Box::pin(async { Ok(AcceptedPeer::new(NodeId::new("renamed"))) })
    }
}

#[tokio::test]
async fn policy_cannot_silently_rename_local_identity() {
    let server = endpoint("server", Arc::new(Rename), &[]).await;
    let client = endpoint("client", Arc::new(OpenAdmission), &[]).await;
    connect(&client, &server);
    eventually(|| {
        client
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .connections
            .lock()
            .expect("connections")
            .peers
            .is_empty()
    })
    .await;
    assert_eq!(peers(&server), 0);
    assert_eq!(peers(&client), 0);
    client.close().await;
    server.close().await;
}

#[tokio::test]
async fn idle_pool_settings_do_not_expire_admitted_sessions() {
    let server = TcpMsgTransport::bind_admitted(
        NodeId::new("server"),
        "127.0.0.1:0",
        TcpMsgConfig {
            idle_timeout: Duration::from_millis(1),
            max_outbound: 1,
            ..TcpMsgConfig::default()
        },
        Arc::new(OpenAdmission),
        Vec::new(),
        TcpAdmissionConfig::default(),
    )
    .await
    .expect("server");
    let a = endpoint("a", Arc::new(OpenAdmission), &[]).await;
    let b = endpoint("b", Arc::new(OpenAdmission), &[]).await;
    connect(&a, &server);
    connect(&b, &server);
    eventually(|| peers(&server) == 2 && peers(&a) == 1 && peers(&b) == 1).await;
    // An unrelated bounded handshake deadline ensures several raw idle intervals
    // elapse without introducing an arbitrary sleep into this socket regression.
    let mut incomplete = TcpStream::connect(server.local_addr())
        .await
        .expect("connect");
    let mut byte = [0];
    assert!(
        timeout(Duration::from_millis(20), incomplete.read(&mut byte))
            .await
            .is_err()
    );
    assert_eq!(peers(&server), 2);
    server.send(a.local_id(), b"still a").await.expect("send");
    server.send(b.local_id(), b"still b").await.expect("send");
    assert_eq!(receive(&a).await.msg.as_ref(), b"still a");
    assert_eq!(receive(&b).await.msg.as_ref(), b"still b");
    a.close().await;
    b.close().await;
    server.close().await;
}

#[tokio::test]
async fn clean_cutover_rejects_raw_handshakes_and_invalid_local_bounds() {
    let server = endpoint("server", Arc::new(OpenAdmission), &[]).await;
    let mut raw = TcpStream::connect(server.local_addr())
        .await
        .expect("connect");
    write_id(&mut raw, &NodeId::new("raw"))
        .await
        .expect("legacy id");
    write_str(&mut raw, "").await.expect("legacy intro");
    let mut byte = [0];
    assert!(matches!(
        timeout(Duration::from_secs(5), raw.read(&mut byte))
            .await
            .expect("rejected"),
        Ok(0) | Err(_)
    ));
    assert_eq!(peers(&server), 0);
    assert!(
        TcpMsgTransport::bind_admitted(
            NodeId::new("oversized"),
            "127.0.0.1:0",
            TcpMsgConfig::default(),
            Arc::new(OpenAdmission),
            vec![0; MAX_CREDENTIAL_BYTES + 1],
            TcpAdmissionConfig::default(),
        )
        .await
        .is_err()
    );
    assert!(
        TcpMsgTransport::bind_admitted(
            NodeId::new("bad-bounds"),
            "127.0.0.1:0",
            TcpMsgConfig::default(),
            Arc::new(OpenAdmission),
            Vec::new(),
            TcpAdmissionConfig {
                max_pending: 0,
                ..TcpAdmissionConfig::default()
            },
        )
        .await
        .is_err()
    );
    server.close().await;
}

#[tokio::test]
async fn client_rejects_server_reassigning_its_local_identity() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener");
    let client = endpoint("client", Arc::new(OpenAdmission), &[]).await;
    client.register_peer(
        NodeId::new("server"),
        listener.local_addr().expect("address"),
    );
    let malicious = tokio::spawn(async move {
        let (mut socket, _) = listener.accept().await.expect("accept");
        let hello = read_hello(&mut socket).await.expect("claim");
        assert_eq!(hello.node, NodeId::new("client"));
        write_hello(&mut socket, &NodeId::new("server"), "", &[])
            .await
            .expect("server claim");
        write_id(&mut socket, &NodeId::new("renamed-client"))
            .await
            .expect("reassignment");
        let mut byte = [0];
        assert!(matches!(
            timeout(Duration::from_secs(5), socket.read(&mut byte))
                .await
                .expect("closed"),
            Ok(0) | Err(_)
        ));
    });
    client
        .connect_peer(&NodeId::new("server"))
        .expect("connect");
    timeout(Duration::from_secs(5), malicious)
        .await
        .expect("server completes")
        .expect("server task");
    eventually(|| client.outbound_connections() == 0).await;
    assert_eq!(peers(&client), 0);
    assert_eq!(client.local_id(), &NodeId::new("client"));
    client.close().await;
}

#[tokio::test]
async fn shutdown_cancels_policy_work_without_waiting_for_handshake_deadline() {
    let policy = Arc::new(Stall {
        calls: AtomicUsize::new(0),
    });
    let server = TcpMsgTransport::bind_admitted(
        NodeId::new("server"),
        "127.0.0.1:0",
        TcpMsgConfig::default(),
        policy.clone(),
        Vec::new(),
        TcpAdmissionConfig {
            handshake_timeout: Duration::from_secs(60),
            ..TcpAdmissionConfig::default()
        },
    )
    .await
    .expect("server");
    let client = endpoint("stall", Arc::new(OpenAdmission), &[]).await;
    connect(&client, &server);
    eventually(|| policy.calls.load(Ordering::SeqCst) == 1).await;
    timeout(Duration::from_secs(5), server.close())
        .await
        .expect("policy drained immediately");
    assert_eq!(
        server
            .direct()
            .expect("direct endpoint")
            .admission
            .as_ref()
            .expect("managed")
            .pending
            .available_permits(),
        64
    );
    assert_eq!(peers(&server), 0);
    eventually(|| client.outbound_connections() == 0).await;
    client.close().await;
}

#[tokio::test]
async fn blocked_old_generation_send_cannot_cross_same_identity_reconnect() {
    let server = endpoint("server", Arc::new(OpenAdmission), &[]).await;
    let incumbent = endpoint("client", Arc::new(OpenAdmission), &[]).await;
    connect(&incumbent, &server);
    eventually(|| peers(&server) == 1 && peers(&incumbent) == 1).await;
    let sessions = server.sessions().expect("sessions");
    let old = sessions.subscribe().borrow()[0].id;
    let (release, blocked) = tokio::sync::oneshot::channel();
    let (started, ready) = tokio::sync::oneshot::channel();
    let sending = server.clone();
    let target = incumbent.local_id().clone();
    let delayed = tokio::spawn(async move {
        // Capture the original route-selected generation before blocking this
        // worker. Polling its send later must not bind the replacement socket.
        let send =
            sending.send_owned_admitted(&target, Bytes::from_static(b"stale outbound"), Some(old));
        started.send(()).expect("worker ready");
        blocked.await.expect("release worker");
        send.await.expect("stale send is best-effort drop");
    });
    ready.await.expect("worker blocked");
    incumbent.close().await;
    eventually(|| peers(&server) == 0).await;
    let replacement = endpoint("client", Arc::new(OpenAdmission), &[]).await;
    connect(&replacement, &server);
    eventually(|| peers(&server) == 1 && peers(&replacement) == 1).await;
    let new = sessions.subscribe().borrow()[0].id;
    assert_ne!(old, new);
    release.send(()).expect("resume stale worker");
    timeout(Duration::from_secs(5), delayed)
        .await
        .expect("worker completes")
        .expect("worker task");
    server
        .send_owned_admitted(
            replacement.local_id(),
            Bytes::from_static(b"missing generation"),
            None,
        )
        .await
        .expect("untagged managed send drops");
    server
        .send_owned_admitted(
            replacement.local_id(),
            Bytes::from_static(b"fresh outbound"),
            Some(new),
        )
        .await
        .expect("current session send");
    // One ordered socket proves neither stale nor untagged payload entered the
    // replacement's queue ahead of the valid current-generation marker.
    assert_eq!(receive(&replacement).await.msg.as_ref(), b"fresh outbound");
    replacement.close().await;
    server.close().await;
}

#[tokio::test]
async fn owned_queue_retains_payload_storage_and_drops_at_capacity() {
    let server = endpoint("owned-server", Arc::new(OpenAdmission), &[]).await;
    let client = endpoint("owned-client", Arc::new(OpenAdmission), &[]).await;
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    let inner = server.direct().expect("direct");
    let managed = inner.admission.as_ref().expect("managed");
    let session = managed.sessions.subscribe().borrow()[0].id;
    let (frames, mut queued) = mpsc::channel(1);
    // Keep the original sender alive so the real socket writer remains parked.
    let original = {
        let mut connections = managed.connections.lock().expect("connections");
        let connection = connections
            .peers
            .get_mut(client.local_id())
            .expect("client");
        std::mem::replace(&mut connection.frames, frames)
    };
    let payload = Bytes::from(vec![0xAB; 1024]);
    let storage = payload.as_ptr();
    server
        .send_owned_admitted(client.local_id(), payload, Some(session))
        .await
        .expect("owned send");
    server
        .send_owned_admitted(
            client.local_id(),
            Bytes::from_static(b"dropped"),
            Some(session),
        )
        .await
        .expect("full queue drops");
    let received = queued.try_recv().expect("queued payload");
    assert_eq!(
        received.as_ptr(),
        storage,
        "owned payload must not be copied"
    );
    assert_eq!(received.len(), 1024);
    assert!(matches!(
        queued.try_recv(),
        Err(mpsc::error::TryRecvError::Empty)
    ));
    drop(original);
    client.close().await;
    server.close().await;
}

#[tokio::test]
async fn admitted_reader_applies_configured_frame_cap() {
    let server = TcpMsgTransport::bind_admitted(
        NodeId::new("capped-server"),
        "127.0.0.1:0",
        TcpMsgConfig {
            max_frame_bytes: 4,
            ..TcpMsgConfig::default()
        },
        Arc::new(OpenAdmission),
        Vec::new(),
        TcpAdmissionConfig::default(),
    )
    .await
    .expect("server");
    let client = endpoint("capped-client", Arc::new(OpenAdmission), &[]).await;
    connect(&client, &server);
    eventually(|| peers(&server) == 1 && peers(&client) == 1).await;
    client
        .send(server.local_id(), b"four")
        .await
        .expect("at cap");
    assert_eq!(receive(&server).await.msg.as_ref(), b"four");
    client
        .send(server.local_id(), b"too large")
        .await
        .expect("send");
    eventually(|| peers(&server) == 0 && peers(&client) == 0).await;
    assert!(
        server
            .direct()
            .expect("direct")
            .inbox
            .lock()
            .await
            .is_empty()
    );
    client.close().await;
    server.close().await;
}
