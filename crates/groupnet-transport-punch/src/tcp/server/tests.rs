use super::*;
use crate::{PeerPath, TcpConnection, TcpPunchConfig};
use bytes::Bytes;
use groupnet_testkit::cluster::eventually_within;

fn entry(
    node: &str,
    session: Token,
    capacity: usize,
) -> (Entry, mpsc::Receiver<Message>, mpsc::Receiver<Message>) {
    let (writer, control) = mpsc::channel(capacity);
    let (data_writer, receiver) = mpsc::channel(capacity);
    (
        Entry {
            registration: Registration {
                node: NodeId::new(node),
                session,
                peers: HashSet::new(),
                dynamic: true,
                relay_only: true,
                candidates: Vec::new(),
                observed: "127.0.0.1:1".parse().unwrap(),
            },
            writer,
            data_writer,
            cancel: CancellationToken::new(),
        },
        control,
        receiver,
    )
}

#[tokio::test]
async fn saturated_recipient_applies_backpressure_without_blocking_other_routes() {
    let (source, _source_control, _source_rx) = entry("source", [1; 32], 1);
    let (slow, mut slow_control, mut slow_rx) = entry("slow", [2; 32], 1);
    let (fast, _fast_control, mut fast_rx) = entry("fast", [3; 32], 1);
    slow.data_writer
        .try_send(Message::Ping)
        .unwrap_or_else(|_| panic!("empty queue"));
    let entries = HashMap::from([
        (source.registration.node.clone(), source),
        (slow.registration.node.clone(), slow),
        (fast.registration.node.clone(), fast),
    ]);
    let payload = Bytes::from(vec![9; 1024]);
    let pointer = payload.as_ptr();
    let delivery = relay(
        &entries,
        &NodeId::new("source"),
        [1; 32],
        Message::Relay {
            node: NodeId::new("slow"),
            session: [2; 32],
            data: payload,
        },
    )
    .unwrap();
    let blocked = delivery.writer.send(delivery.message);
    tokio::pin!(blocked);
    tokio::select! {
        biased;
        _ = &mut blocked => panic!("a full relay queue must backpressure"),
        () = std::future::ready(()) => {},
    }
    // Data saturation must not withdraw the incumbent's admission when a
    // control introduction or departure is queued.
    entries
        .get(&NodeId::new("slow"))
        .unwrap()
        .writer
        .send(Message::Ping)
        .await
        .unwrap_or_else(|_| panic!("independent control queue"));
    assert!(matches!(slow_control.recv().await, Some(Message::Ping)));
    let independent = relay(
        &entries,
        &NodeId::new("source"),
        [1; 32],
        Message::Relay {
            node: NodeId::new("fast"),
            session: [3; 32],
            data: Bytes::from_static(b"independent"),
        },
    )
    .unwrap();
    independent
        .writer
        .send(independent.message)
        .await
        .unwrap_or_else(|_| panic!("live recipient"));
    assert!(matches!(fast_rx.recv().await, Some(Message::Relay { .. })));
    assert!(matches!(slow_rx.recv().await, Some(Message::Ping)));
    blocked
        .await
        .unwrap_or_else(|_| panic!("drained recipient"));
    let Message::Relay { data, .. } = slow_rx.recv().await.unwrap() else {
        panic!("relay payload")
    };
    assert_eq!(data.as_ptr(), pointer);
}

#[test]
fn relay_rejects_stale_sessions_and_nonmutual_admission() {
    let (source, _source_control, _source_rx) = entry("source", [1; 32], 1);
    let (mut recipient, _recipient_control, _recipient_rx) = entry("recipient", [2; 32], 1);
    recipient.registration.dynamic = false;
    let mut entries = HashMap::from([
        (source.registration.node.clone(), source),
        (recipient.registration.node.clone(), recipient),
    ]);
    let packet = || Message::Relay {
        node: NodeId::new("recipient"),
        session: [2; 32],
        data: Bytes::new(),
    };
    assert!(relay(&entries, &NodeId::new("source"), [1; 32], packet()).is_none());
    entries
        .get_mut(&NodeId::new("recipient"))
        .unwrap()
        .registration
        .peers
        .insert(NodeId::new("source"));
    assert!(relay(&entries, &NodeId::new("source"), [9; 32], packet()).is_none());
    assert!(
        relay(
            &entries,
            &NodeId::new("source"),
            [1; 32],
            Message::Relay {
                node: NodeId::new("recipient"),
                session: [9; 32],
                data: Bytes::new(),
            }
        )
        .is_none()
    );
    assert!(relay(&entries, &NodeId::new("source"), [1; 32], packet()).is_some());
}

#[tokio::test]
async fn configured_rendezvous_forwards_data_bursts_without_revoking_admission() {
    let rendezvous = TcpRendezvous::bind_open_config(
        "127.0.0.1:0".parse().unwrap(),
        TcpRendezvousConfig {
            session_queue: 1024,
            event_queue: 1024,
            control: super::super::ControlRateLimit {
                frames_per_second: std::num::NonZeroU32::new(1).unwrap(),
                burst_frames: std::num::NonZeroU32::new(1).unwrap(),
            },
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let address = rendezvous.local_addr().unwrap();
    let mut a_config = TcpPunchConfig::open(NodeId::new("source"), address);
    a_config.queue_capacity = 1024;
    let mut b_config = TcpPunchConfig::open(NodeId::new("target"), address);
    b_config.queue_capacity = 1024;
    let a = TcpConnection::bind(a_config).await.unwrap();
    let b = TcpConnection::bind(b_config).await.unwrap();
    let target = b.local_id().clone();
    eventually_within("relay pair introduced", Duration::from_secs(3), || {
        a.path_to(&target) == Some(PeerPath::Relay)
            && b.path_to(a.local_id()) == Some(PeerPath::Relay)
    })
    .await;
    let source_generation = a.sessions().subscribe().borrow()[0].id;
    let target_generation = b.sessions().subscribe().borrow()[0].id;
    // Data bursts must not consume the independent one-control-frame/second
    // abuse budget. Performance is measured by the release benchmark, not a
    // subsecond wall-clock assertion in this lifecycle regression.
    tokio::time::timeout(Duration::from_secs(3), async {
        for sequence in 0_u32..768 {
            a.send_owned(&target, Bytes::copy_from_slice(&sequence.to_be_bytes()))
                .await
                .unwrap();
        }
        for sequence in 0_u32..768 {
            assert_eq!(
                b.recv().await.unwrap().msg.as_ref(),
                &sequence.to_be_bytes()
            );
        }
    })
    .await
    .expect("admitted data burst must remain deliverable");
    assert!(a.sessions().is_active(&target, source_generation));
    assert!(b.sessions().is_active(a.local_id(), target_generation));
    a.close().await;
    b.close().await;
    rendezvous.close().await;
}

#[tokio::test]
async fn configured_session_cap_denies_excess_admission_without_revoking_incumbent() {
    let rendezvous = TcpRendezvous::bind_open_config(
        "127.0.0.1:0".parse().unwrap(),
        TcpRendezvousConfig {
            max_sessions: 1,
            session_queue: 1,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let address = rendezvous.local_addr().unwrap();
    let incumbent = TcpConnection::bind(TcpPunchConfig::open(NodeId::new("first"), address))
        .await
        .unwrap();
    let denied = TcpConnection::bind(TcpPunchConfig::open(NodeId::new("second"), address))
        .await
        .unwrap_err();
    assert_eq!(denied.kind(), io::ErrorKind::PermissionDenied);
    assert!(incumbent.local_addr().is_ok());
    incumbent.close().await;
    rendezvous.close().await;
}

#[tokio::test(start_paused = true)]
async fn authenticated_heartbeats_keep_slow_byte_pacing_live_and_cancellation_drains_waits() {
    let (source, source_control, source_data) = entry("source", [1; 32], 2);
    let source_cancel = source.cancel.clone();
    let (target, _target_control, mut delivered) = entry("target", [2; 32], 2);
    let entries: Entries = Arc::new(std::sync::Mutex::new(HashMap::from([
        (source.registration.node.clone(), source),
        (target.registration.node.clone(), target),
    ])));
    let config = TcpRendezvousConfig {
        relay_pacing: RelayPacing::Bytes {
            bytes_per_second: std::num::NonZeroU64::new(1).unwrap(),
            burst_bytes: std::num::NonZeroU64::new(1).unwrap(),
        },
        ..Default::default()
    };
    let (server, client) = tokio::io::duplex(16);
    let (mut server_read, mut server_write) = tokio::io::split(server);
    let (client_read, mut client_write) = tokio::io::split(client);
    let cancel = CancellationToken::new();
    let stopped = cancel.clone();
    let reading = tokio::spawn(async move {
        let auth = Some(wire::keyed(&[7; 32]));
        let source = NodeId::new("source");
        tokio::select! {
            () = stopped.cancelled() => Ok(()),
            result = read_client(&mut server_read, &auth, &entries, &source, [1; 32], config) => result,
        }
    });
    let stopped = cancel.clone();
    let writing = tokio::spawn(async move {
        let auth = Some(wire::keyed(&[8; 32]));
        tokio::select! {
            () = stopped.cancelled() => Ok(()),
            result = write_client(&mut server_write, &auth, (source_control, source_data)) => result,
        }
    });
    let (outgoing, mut pending) = mpsc::channel(2);
    for _ in 0..2 {
        outgoing
            .try_send(Message::Relay {
                node: NodeId::new("target"),
                session: [2; 32],
                data: Bytes::from_static(b"12345678"),
            })
            .unwrap_or_else(|_| panic!("empty endpoint queue"));
    }
    drop(outgoing);
    let stopped = cancel.clone();
    let sending = tokio::spawn(async move {
        let auth = Some(wire::keyed(&[7; 32]));
        tokio::select! {
            () = stopped.cancelled() => Err(closed()),
            result = super::super::endpoint::write_control(&mut client_write, &auth, &mut pending) => result,
        }
    });
    let receiving = tokio::spawn(count_heartbeats(client_read, cancel.clone()));
    // Let the first frame reach the reader before stepping virtual time.
    for _ in 0..16 {
        tokio::task::yield_now().await;
    }
    for second in 1..=8 {
        tokio::time::advance(Duration::from_secs(1)).await;
        for _ in 0..16 {
            tokio::task::yield_now().await;
        }
        assert!(!reading.is_finished());
        assert!(!writing.is_finished());
        assert!(
            !receiving.is_finished(),
            "authenticated read expiry must not fire while paced"
        );
        assert!(!source_cancel.is_cancelled());
        if second == 4 {
            assert!(
                !sending.is_finished(),
                "second frame must still backpressure, not hit the old three-second deadline"
            );
        }
        if second < 7 {
            assert!(delivered.try_recv().is_err());
        }
    }
    assert!(
        sending.is_finished(),
        "paced reader must resume the bounded writer"
    );
    sending.await.unwrap().unwrap();
    let Message::Relay { data, .. } = delivered.try_recv().unwrap() else {
        panic!("paced payload")
    };
    assert_eq!(data.as_ref(), b"12345678");
    assert!(
        delivered.try_recv().is_err(),
        "second payload still awaits its byte budget"
    );
    cancel.cancel();
    reading.await.unwrap().unwrap();
    writing.await.unwrap().unwrap();
    assert!(receiving.await.unwrap().unwrap() >= 6);
}

async fn count_heartbeats(
    mut reader: impl tokio::io::AsyncRead + Unpin,
    stopped: CancellationToken,
) -> io::Result<usize> {
    let auth = Some(wire::keyed(&[8; 32]));
    let mut heartbeats = 0;
    loop {
        tokio::select! {
            // Cancellation also closes the writer; prefer it over the resulting EOF.
            biased;
            () = stopped.cancelled() => return Ok(heartbeats),
            message = tokio::time::timeout(IDLE, wire::read(&mut reader, &auth)) => {
                let message = message.map_err(|_| closed())??;
                if !matches!(message, Message::Ping) { return Err(invalid("expected heartbeat")); }
                heartbeats += 1;
            }
        }
    }
}
