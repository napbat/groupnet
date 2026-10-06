//! Real router regressions and deterministic bounded-state adversarial inputs.

use super::*;
use groupnet_network::{ApplicationPacket, RouterConfig};
use groupnet_transport::{Transport, link::LinkConfig};
use groupnet_transport_mem::Network;

const WAIT: Duration = Duration::from_secs(5);

fn router(name: &str) -> io::Result<Router> {
    Router::new(
        NodeId::new(name),
        RouterConfig {
            announce_interval: Duration::from_millis(20),
            route_ttl: Duration::from_secs(2),
            ..RouterConfig::default()
        },
    )
}

async fn pair() -> io::Result<(Router, Router, Messaging, Messaging)> {
    named_pair("a", "b").await
}

async fn named_pair(
    a_name: &str,
    b_name: &str,
) -> io::Result<(Router, Router, Messaging, Messaging)> {
    let a = router(a_name)?;
    let b = router(b_name)?;
    let net = Network::new();
    for (local, peer) in [(&a, &b), (&b, &a)] {
        local.add_transport(
            net.endpoint(local.local_id().clone()),
            LinkConfig {
                peers: vec![peer.local_id().clone()],
                cost: 1,
                mtu: 1200,
            },
        )?;
    }
    tokio::time::timeout(WAIT, async {
        while a.route_to(b.local_id()).is_none() || b.route_to(a.local_id()).is_none() {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let am = Messaging::new(&a)?;
    let bm = Messaging::new(&b)?;
    Ok((a, b, am, bm))
}

fn options(delivery: Delivery) -> SendOptions {
    SendOptions {
        delivery,
        timeout: Duration::from_secs(2),
    }
}

#[tokio::test]
async fn application_isolation_and_delivered_requires_explicit_queue_acceptance() -> io::Result<()>
{
    let (a, b, am, bm) = pair().await?;
    assert_eq!(
        Messaging::new(&b).unwrap_err().kind(),
        io::ErrorKind::AlreadyExists
    );
    a.send(b.local_id(), b"coordination only").await?;
    let sender = am.clone();
    let target = b.local_id().clone();
    let sent = tokio::spawn(async move {
        sender
            .send(
                &target,
                None,
                Bytes::from_static(b"application only"),
                options(Delivery::Delivered),
            )
            .await
    });
    let frame = tokio::time::timeout(WAIT, bm.recv()).await??;
    assert_eq!(frame.from, *a.local_id());
    assert_eq!(frame.payload.as_ref(), b"application only");
    assert!(
        !sent.is_finished(),
        "decoding data must not acknowledge queue reservation"
    );
    assert_eq!(
        tokio::time::timeout(WAIT, b.recv()).await??.msg,
        b"coordination only"
    );
    frame.receipt().accepted()?;
    assert_eq!(tokio::time::timeout(WAIT, sent).await???, frame.id);
    // Delivered is already terminal: later callback outcome cannot contradict it.
    frame.applied()?;
    frame.receipt().reject(io::ErrorKind::Other)?;
    am.close().await;
    bm.close().await;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn applied_retries_deduplicate_without_reenqueue_and_rejection_is_terminal() -> io::Result<()>
{
    let (a, b, am, bm) = pair().await?;
    let sender = am.clone();
    let target = b.local_id().clone();
    let group = GroupId::new("opaque-group");
    let sent = tokio::spawn(async move {
        sender
            .send(
                &target,
                Some(&group),
                Bytes::from(vec![0; 12_000]),
                options(Delivery::Applied),
            )
            .await
    });
    let frame = tokio::time::timeout(WAIT, bm.recv()).await??;
    assert_eq!(frame.group, Some(GroupId::new("opaque-group")));
    assert_eq!(frame.payload.len(), 12_000);
    frame.receipt().accepted()?;
    assert!(
        tokio::time::timeout(RETRY * 3, bm.recv()).await.is_err(),
        "retries may replay receipts but must not re-enqueue"
    );
    assert!(!sent.is_finished(), "accepted is not applied");
    frame.applied()?;
    assert_eq!(tokio::time::timeout(WAIT, sent).await???, frame.id);

    let sender = am.clone();
    let target = b.local_id().clone();
    let rejected = tokio::spawn(async move {
        sender
            .send(
                &target,
                None,
                Bytes::from_static(b"rejected"),
                options(Delivery::Applied),
            )
            .await
    });
    let frame = tokio::time::timeout(WAIT, bm.recv()).await??;
    frame.receipt().reject(io::ErrorKind::PermissionDenied)?;
    frame.applied()?; // First terminal rejection cannot be overwritten.
    assert_eq!(
        tokio::time::timeout(WAIT, rejected)
            .await??
            .unwrap_err()
            .kind(),
        io::ErrorKind::PermissionDenied
    );
    am.close().await;
    bm.close().await;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn ack_requires_exact_source_and_identity_and_ignores_malformed_data() -> io::Result<()> {
    let router = router("local")?;
    let messages = Messaging::new(&router)?;
    let id = MessageId([42; 16]);
    let target = NodeId::new("selected-target");
    let (result, mut received) = oneshot::channel();
    messages
        .inner
        .state
        .lock()
        .map_err(|_| poisoned())?
        .pending
        .insert(
            id,
            Pending {
                to: target.clone(),
                delivery: Delivery::Applied,
                result,
            },
        );
    let inject = |from, id, outcome| {
        worker::process(
            &messages.inner,
            ApplicationPacket {
                from,
                payload: Bytes::from(codec::ack(id, outcome)),
            },
        );
    };
    inject(NodeId::new("other-peer"), id, Outcome::Applied);
    inject(target.clone(), MessageId([43; 16]), Outcome::Applied);
    inject(target.clone(), id, Outcome::Accepted);
    assert!(matches!(
        received.try_recv(),
        Err(oneshot::error::TryRecvError::Empty)
    ));
    assert_eq!(
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .pending
            .len(),
        1
    );
    worker::process(
        &messages.inner,
        ApplicationPacket {
            from: target.clone(),
            payload: Bytes::from_static(b"GNA1\xffmalformed"),
        },
    );
    assert!(messages.inner.receiver.lock().await.try_recv().is_err());
    inject(target, id, Outcome::Rejected(Rejection::Permission));
    assert_eq!(
        received.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::PermissionDenied
    );
    messages.close().await;
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn received_record_exhaustion_never_evicts_live_identity_and_body_collision_is_not_work()
-> io::Result<()> {
    let router = router("local")?;
    let messages = Messaging::new(&router)?;
    let source = NodeId::new("source");
    let first = MessageId([0; 16]);
    for n in 0..MAX_RECEIVED {
        let mut id = [0; 16];
        id[8..].copy_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
        let packet = codec::data(MessageId(id), Delivery::Applied, None, b"original").unwrap();
        worker::process(
            &messages.inner,
            ApplicationPacket {
                from: source.clone(),
                payload: Bytes::from(packet),
            },
        );
    }
    assert_eq!(
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .received
            .len(),
        MAX_RECEIVED
    );
    let mut rejected_id = [0; 16];
    rejected_id[8..].copy_from_slice(&64_u64.to_be_bytes());
    let record = messages
        .inner
        .state
        .lock()
        .map_err(|_| poisoned())?
        .received
        .get(&(source.clone(), MessageId(rejected_id)))
        .unwrap()
        .clone();
    assert_eq!(
        record
            .state
            .lock()
            .map_err(|_| poisoned())?
            .replay(messages.inner.now()),
        Some(Outcome::Rejected(Rejection::Full)),
        "full network inbox is an explicit retained rejection"
    );
    // Drain the inbox without accepting; these live identities still cannot be evicted.
    {
        let mut incoming = messages.inner.receiver.lock().await;
        while incoming.try_recv().is_ok() {}
    }
    for (id, body) in [
        (first, b"changed".as_slice()),
        (MessageId([255; 16]), b"new".as_slice()),
        (first, b"original".as_slice()),
    ] {
        worker::process(
            &messages.inner,
            ApplicationPacket {
                from: source.clone(),
                payload: Bytes::from(codec::data(id, Delivery::Applied, None, body).unwrap()),
            },
        );
    }
    assert_eq!(
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .received
            .len(),
        MAX_RECEIVED
    );
    assert!(
        messages.inner.receiver.lock().await.try_recv().is_err(),
        "collision, exhaustion, and live duplicate must not execute"
    );
    messages.close().await;
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn cancelled_sends_cleanup_and_last_owner_drop_closes_worker() -> io::Result<()> {
    let router = router("self")?;
    let messages = Messaging::new(&router)?;
    let sender = messages.clone();
    let target = router.local_id().clone();
    let send = tokio::spawn(async move {
        sender
            .send(
                &target,
                None,
                Bytes::from_static(b"cancelled"),
                options(Delivery::Applied),
            )
            .await
    });
    let frame = tokio::time::timeout(WAIT, messages.recv()).await??;
    frame.receipt().accepted()?;
    send.abort();
    let _ = send.await;
    assert!(
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .pending
            .is_empty()
    );
    let weak = Arc::downgrade(&messages.inner);
    let cancel = messages.inner.cancel.clone();
    let tasks = messages.inner.tasks.clone();
    tasks.close();
    drop(messages);
    assert!(
        weak.upgrade().is_none(),
        "worker and retained Frame/Receipt must not keep owner alive"
    );
    assert!(cancel.is_cancelled());
    tokio::time::timeout(WAIT, tasks.wait()).await?;
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn invalid_bounds_no_route_and_outstanding_limit_fail_before_dispatch() -> io::Result<()> {
    let router = router("self")?;
    let messages = Messaging::new(&router)?;
    let target = NodeId::new("missing-route");
    assert_eq!(
        messages
            .send(&target, None, Bytes::new(), options(Delivery::Delivered))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    assert!(
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .pending
            .is_empty()
    );
    assert_eq!(
        messages
            .send(
                router.local_id(),
                None,
                Bytes::from(vec![0; MAX_MESSAGE_BYTES + 1]),
                SendOptions::default()
            )
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    for timeout in [Duration::ZERO, Duration::from_secs(31)] {
        assert_eq!(
            messages
                .send(
                    router.local_id(),
                    None,
                    Bytes::new(),
                    SendOptions {
                        delivery: Delivery::Applied,
                        timeout
                    }
                )
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
    }
    for n in 0..MAX_PENDING {
        let mut id = [0; 16];
        id[..8].copy_from_slice(&u64::try_from(n).unwrap().to_be_bytes());
        let (result, _) = oneshot::channel();
        messages
            .inner
            .state
            .lock()
            .map_err(|_| poisoned())?
            .pending
            .insert(
                MessageId(id),
                Pending {
                    to: target.clone(),
                    delivery: Delivery::Applied,
                    result,
                },
            );
    }
    assert_eq!(
        messages
            .send(
                router.local_id(),
                None,
                Bytes::new(),
                options(Delivery::Applied)
            )
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert!(messages.inner.receiver.lock().await.try_recv().is_err());
    messages.close().await;
    assert_eq!(
        messages.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn receipt_records_success_when_ack_route_is_missing_and_shutdown_wakes_recv()
-> io::Result<()> {
    let router = router("local")?;
    let messages = Messaging::new(&router)?;
    let source = NodeId::new("unreachable-ack-destination");
    let id = MessageId([8; 16]);
    worker::process(
        &messages.inner,
        ApplicationPacket {
            from: source.clone(),
            payload: Bytes::from(codec::data(id, Delivery::Applied, None, b"work").unwrap()),
        },
    );
    let frame = messages.recv().await?;
    frame.receipt().accepted()?;
    frame.applied()?;
    frame.receipt().reject(io::ErrorKind::Other)?;
    let record = messages
        .inner
        .state
        .lock()
        .map_err(|_| poisoned())?
        .received
        .get(&(source, id))
        .unwrap()
        .clone();
    assert_eq!(
        record
            .state
            .lock()
            .map_err(|_| poisoned())?
            .replay(messages.inner.now()),
        Some(Outcome::Applied)
    );
    router.shutdown();
    assert!(router.is_closed());
    assert_eq!(
        messages.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    messages.close().await;
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn maximal_identifiers_and_buffer_fit_real_routing_envelope() -> io::Result<()> {
    let (a, b, am, bm) = named_pair(&"f".repeat(255), &"t".repeat(255)).await?;
    let sender = am.clone();
    let target = b.local_id().clone();
    let group = GroupId::new("g".repeat(255));
    let expected_group = group.clone();
    let sent = tokio::spawn(async move {
        sender
            .send(
                &target,
                Some(&group),
                Bytes::from(vec![7; MAX_MESSAGE_BYTES]),
                options(Delivery::Applied),
            )
            .await
    });
    let frame = tokio::time::timeout(WAIT, bm.recv()).await??;
    assert_eq!(frame.from, *a.local_id());
    assert_eq!(frame.group, Some(expected_group));
    assert_eq!(frame.payload.len(), MAX_MESSAGE_BYTES);
    assert!(frame.payload.iter().all(|byte| *byte == 7));
    frame.applied()?;
    assert_eq!(tokio::time::timeout(WAIT, sent).await???, frame.id);
    am.close().await;
    bm.close().await;
    a.close().await;
    b.close().await;
    Ok(())
}

#[tokio::test]
async fn local_raw_application_enqueue_reports_backpressure() -> io::Result<()> {
    let router = router("local")?;
    let messages = Messaging::new(&router)?;
    // No awaits here: the current-thread worker cannot drain this bounded queue.
    for _ in 0..128 {
        messages
            .inner
            .io
            .send(router.local_id(), b"malformed fixture")?;
    }
    assert_eq!(
        messages
            .send(
                router.local_id(),
                None,
                Bytes::new(),
                SendOptions::default()
            )
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    messages.close().await;
    router.close().await;
    Ok(())
}
