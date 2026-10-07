mod boundaries;
mod fabric;

use std::{io, sync::atomic::Ordering, time::Duration};

use bytes::Bytes;
use groupnet_testkit::cluster::eventually;
use groupnet_transport::QueueCapacity;
use tokio::time::timeout;

use super::{
    UnorderedDelivery::{Reliable, Unreliable},
    UnorderedOptions,
    wire::{self, Kind, TxCrypto, Window},
};
use fabric::{DROP_ALL, DROP_FIRST, Fabric, HOLD_FIRST, PASS, TAMPER_FIRST, config};

const BOUND: Duration = Duration::from_secs(5);

fn sealed(sender: &mut TxCrypto, body: &[u8]) -> Bytes {
    let mut packet = bytes::BytesMut::with_capacity(wire::HEADER + body.len() + wire::TAG);
    packet.extend_from_slice(&[0; wire::HEADER]);
    packet.extend_from_slice(body);
    packet.extend_from_slice(&[0; wire::TAG]);
    sender.seal([3; 16], Kind::Data, 1, &mut packet).unwrap();
    packet.freeze()
}

#[test]
fn directional_aead_authenticates_identity_and_never_commits_tampering() {
    let keys = [7; 64];
    let (mut sender, _) = wire::crypto(&keys, true).unwrap();
    let (_, mut receiver) = wire::crypto(&keys, false).unwrap();
    let original = sealed(&mut sender, b"secret");
    let mut tampered = original.to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    assert!(receiver.open(tampered.into(), 1024).is_err());
    let (_, id, body) = receiver.open(original.clone(), 1024).unwrap();
    assert_eq!(id, 1);
    assert_eq!(body, Bytes::from_static(b"secret"));
    assert!(
        receiver.open(original.clone(), 1024).is_err(),
        "replayed nonce must fail"
    );
    let (_, mut wrong_session) = wire::crypto(&[8; 64], false).unwrap();
    assert!(wrong_session.open(original.clone(), 1024).is_err());
    let mut directional = [1; 64];
    directional[32..].fill(2);
    let (mut client, mut client_rx) = wire::crypto(&directional, true).unwrap();
    let reflected = sealed(&mut client, b"reflection");
    assert!(client_rx.open(reflected, 1024).is_err());
    let (_, mut fresh) = wire::crypto(&keys, false).unwrap();
    let mut changed_identity = original.to_vec();
    changed_identity[4] ^= 1;
    assert!(fresh.open(changed_identity.into(), 1024).is_err());
}

#[test]
fn owned_decryption_reuses_unique_storage_and_preserves_shared_aliases() {
    let (mut sender, _) = wire::crypto(&[7; 64], true).unwrap();
    let (_, mut receiver) = wire::crypto(&[7; 64], false).unwrap();
    let unique = sealed(&mut sender, b"unique");
    let body_pointer = unique.as_ptr().wrapping_add(wire::HEADER);
    let (_, _, body) = receiver.open(unique, 6).unwrap();
    assert_eq!(body.as_ptr(), body_pointer);
    assert_eq!(body.as_ref(), b"unique");

    let shared = sealed(&mut sender, b"shared");
    let alias = shared.clone();
    let ciphertext = shared.to_vec();
    let (_, _, body) = receiver.open(shared, 6).unwrap();
    assert_eq!(body.as_ref(), b"shared");
    assert_eq!(alias.as_ref(), ciphertext);
    assert_ne!(body.as_ptr(), alias.as_ptr().wrapping_add(wire::HEADER));

    let mut tampered = sealed(&mut sender, b"failure").to_vec();
    *tampered.last_mut().unwrap() ^= 1;
    let tampered = Bytes::from(tampered);
    let alias = tampered.clone();
    let ciphertext = alias.to_vec();
    assert!(receiver.open(tampered, 7).is_err());
    assert_eq!(alias.as_ref(), ciphertext);
}

#[test]
fn size_failure_does_not_consume_nonce_and_retry_plaintext_stays_intact() {
    let (mut sender, _) = wire::crypto(&[7; 64], true).unwrap();
    let (_, mut receiver) = wire::crypto(&[7; 64], false).unwrap();
    let plaintext = Bytes::from_static(b"retry");
    let first = sealed(&mut sender, &plaintext);
    assert!(receiver.open(first.clone(), 4).is_err());
    assert_eq!(receiver.open(first.clone(), 5).unwrap().2, plaintext);
    let retry = sealed(&mut sender, &plaintext);
    assert_ne!(first, retry);
    assert_eq!(receiver.open(retry, 5).unwrap().2, plaintext);
    for length in 0..wire::HEADER + wire::TAG {
        assert!(receiver.open(Bytes::from(vec![0; length]), 5).is_err());
    }
}

#[test]
fn operational_capacities_do_not_override_the_security_horizon() {
    let mut bounds = config();
    bounds.max_payload = 96 * 1024;
    bounds.inbox_capacity = QueueCapacity::of(2048);
    bounds.max_sessions = QueueCapacity::of(2048);
    bounds.sessions_per_peer = QueueCapacity::of(1025);
    bounds.max_attempts = 2000;
    bounds.setup_timeout = Duration::from_secs(7200);
    bounds.validate().unwrap();
    bounds.pending_sends = QueueCapacity::of(wire::WINDOW);
    bounds.validate().unwrap();
    bounds.pending_sends = bounds.pending_sends.saturating_add(1);
    assert!(bounds.validate().is_err());
    bounds.pending_sends = QueueCapacity::MIN;
    bounds.sessions_per_peer = bounds.max_sessions.saturating_add(1);
    assert!(bounds.validate().is_err());
}

#[test]
fn replay_and_dedup_windows_are_bounded_and_allow_reordering() {
    let mut window = Window::default();
    window.insert(1025);
    assert!(window.too_old(1));
    assert!(!window.too_old(2));
    assert!(!window.contains(1024));
    window.insert(1024);
    window.insert(2);
    assert!(window.contains(1025));
    assert!(window.contains(1024));
    assert!(window.contains(2));
    window.insert(1089);
    assert!(window.too_old(2));
    assert!(window.contains(1024));
    window.insert(u64::MAX);
    assert!(window.contains(u64::MAX));
    assert!(!window.contains(1089));
}

#[tokio::test]
async fn reliable_loss_does_not_block_a_later_message() {
    let mut bounds = config();
    bounds.retry_interval = Duration::from_secs(1);
    bounds.send_timeout = Duration::from_secs(4);
    bounds.max_attempts = 4;
    let fabric = Fabric::new(bounds).await;
    let (a, b) = fabric.pair(Reliable).await;
    fabric.faults.set(DROP_FIRST);
    let first = tokio::spawn({
        let a = a.clone();
        async move { a.send(Bytes::from_static(b"earlier")).await }
    });
    eventually("first unordered datagram dropped", || {
        fabric.faults.data.load(Ordering::SeqCst) >= 1
    })
    .await;
    a.send(Bytes::from_static(b"later")).await.unwrap();
    assert_eq!(
        timeout(BOUND, b.recv()).await.unwrap().unwrap(),
        b"later"[..]
    );
    assert_eq!(
        timeout(BOUND, b.recv()).await.unwrap().unwrap(),
        b"earlier"[..]
    );
    first.await.unwrap().unwrap();
    assert!(fabric.faults.data.load(Ordering::SeqCst) >= 3);
    assert!(
        timeout(Duration::from_millis(250), b.recv()).await.is_err(),
        "logical retry must not duplicate delivery"
    );
    fabric.close().await;
}

#[tokio::test]
async fn both_policies_deliver_actual_datagram_reordering() {
    for policy in [Reliable, Unreliable] {
        let fabric = Fabric::new(config()).await;
        let (a, b) = fabric.pair(policy).await;
        fabric.faults.set(HOLD_FIRST);
        let first = tokio::spawn({
            let a = a.clone();
            async move { a.send(Bytes::from_static(b"first")).await }
        });
        eventually("datagram held", || {
            fabric.faults.data.load(Ordering::SeqCst) >= 1
        })
        .await;
        a.send(Bytes::from_static(b"second")).await.unwrap();
        assert_eq!(
            timeout(BOUND, b.recv()).await.unwrap().unwrap(),
            b"second"[..]
        );
        assert_eq!(
            timeout(BOUND, b.recv()).await.unwrap().unwrap(),
            b"first"[..]
        );
        first.await.unwrap().unwrap();
        let (sent, received) = tokio::join!(b.send(Bytes::from_static(b"reverse")), a.recv());
        sent.unwrap();
        assert_eq!(received.unwrap(), b"reverse"[..]);
        fabric.close().await;
    }
}

#[tokio::test]
async fn unreliable_loss_and_tampering_are_not_retried() {
    for action in [DROP_FIRST, TAMPER_FIRST] {
        let fabric = Fabric::new(config()).await;
        let (a, b) = fabric.pair(Unreliable).await;
        fabric.faults.set(action);
        a.send(Bytes::from_static(b"lost")).await.unwrap();
        assert!(timeout(Duration::from_millis(650), b.recv()).await.is_err());
        assert_eq!(
            fabric.faults.data.load(Ordering::SeqCst),
            1,
            "unreliable must not retransmit"
        );
        a.send(Bytes::from_static(b"next")).await.unwrap();
        assert_eq!(
            timeout(BOUND, b.recv()).await.unwrap().unwrap(),
            b"next"[..]
        );
        fabric.close().await;
    }
}

#[tokio::test]
async fn reliable_tampering_is_retried_with_fresh_nonce() {
    let fabric = Fabric::new(config()).await;
    let (a, b) = fabric.pair(Reliable).await;
    fabric.faults.set(TAMPER_FIRST);
    a.send(Bytes::from_static(b"authenticated")).await.unwrap();
    assert_eq!(b.recv().await.unwrap(), b"authenticated"[..]);
    assert!(fabric.faults.data.load(Ordering::SeqCst) >= 2);
    fabric.close().await;
}

#[tokio::test]
async fn replay_wrong_peer_and_cross_session_packets_fail_closed() {
    let fabric = Fabric::new(config()).await;
    let (a, b) = fabric.pair(Unreliable).await;
    a.send(Bytes::from_static(b"one")).await.unwrap();
    assert_eq!(b.recv().await.unwrap(), b"one"[..]);
    let captured = fabric
        .faults
        .captured
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .clone()
        .unwrap();
    // A new router envelope avoids relying on the router's own packet dedup.
    fabric
        .a
        .inner
        .io
        .send(fabric.br.local_id(), &captured)
        .unwrap();
    let attacker = fabric.attacker.bind_protocol(2).unwrap();
    attacker.send(fabric.br.local_id(), &captured).unwrap();
    let (_second_a, second_b) = fabric.pair(Unreliable).await;
    let mut cross_session = captured;
    cross_session[4..20].copy_from_slice(&second_b.session_id());
    fabric
        .a
        .inner
        .io
        .send(fabric.br.local_id(), &cross_session)
        .unwrap();
    assert!(timeout(Duration::from_millis(250), b.recv()).await.is_err());
    assert!(
        timeout(Duration::from_millis(250), second_b.recv())
            .await
            .is_err()
    );
    fabric.close().await;
}

#[tokio::test]
async fn reliable_ack_is_bounded_inbox_acceptance_and_retries_are_finite() {
    let mut bounds = config();
    bounds.inbox_capacity = QueueCapacity::MIN;
    bounds.pending_sends = QueueCapacity::MIN;
    bounds.max_attempts = 3;
    let fabric = Fabric::new(bounds).await;
    let (a, b) = fabric.pair(Reliable).await;
    a.send(Bytes::from_static(b"fills inbox")).await.unwrap();
    fabric.faults.set(PASS);
    let pending = tokio::spawn({
        let a = a.clone();
        async move { a.send(Bytes::from_static(b"not accepted yet")).await }
    });
    eventually("full inbox data attempt", || {
        fabric.faults.data.load(Ordering::SeqCst) >= 1
    })
    .await;
    assert!(!pending.is_finished(), "must not ACK a full inbox");
    assert_eq!(
        a.send(Bytes::from_static(b"pending capacity"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::WouldBlock
    );
    assert_eq!(b.recv().await.unwrap(), b"fills inbox"[..]);
    pending.await.unwrap().unwrap();
    assert_eq!(b.recv().await.unwrap(), b"not accepted yet"[..]);
    fabric.faults.set(DROP_ALL);
    assert_eq!(
        a.send(Bytes::from_static(b"finite retries"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::TimedOut
    );
    assert_eq!(fabric.faults.data.load(Ordering::SeqCst), 3);
    fabric.close().await;
}

#[tokio::test]
async fn dropping_sender_future_cancels_retries_and_releases_pending_capacity() {
    let mut bounds = config();
    bounds.pending_sends = QueueCapacity::MIN;
    let fabric = Fabric::new(bounds).await;
    let (a, b) = fabric.pair(Reliable).await;
    fabric.faults.set(DROP_ALL);
    let send = tokio::spawn({
        let a = a.clone();
        async move { a.send(Bytes::from_static(b"cancelled")).await }
    });
    eventually("cancellable send started", || {
        fabric.faults.data.load(Ordering::SeqCst) >= 1
    })
    .await;
    send.abort();
    assert!(send.await.unwrap_err().is_cancelled());
    let attempts = fabric.faults.data.load(Ordering::SeqCst);
    assert!(timeout(Duration::from_millis(650), b.recv()).await.is_err());
    assert_eq!(fabric.faults.data.load(Ordering::SeqCst), attempts);
    fabric.faults.set(PASS);
    a.send(Bytes::from_static(b"slot reused")).await.unwrap();
    assert_eq!(b.recv().await.unwrap(), b"slot reused"[..]);
    fabric.close().await;
}

#[tokio::test]
async fn policy_rejection_and_session_capacity_cleanup_reuse_slots() {
    let a_config = config();
    let mut b_config = config();
    b_config.allow_unreliable = false;
    b_config.max_sessions = QueueCapacity::MIN;
    b_config.sessions_per_peer = QueueCapacity::MIN;
    let fabric = Fabric::with_configs(a_config, b_config).await;
    let rejected = fabric
        .a
        .connect(
            fabric.br.local_id(),
            UnorderedOptions {
                delivery: Unreliable,
                timeout: BOUND,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(rejected.kind(), io::ErrorKind::PermissionDenied);
    let (a, b) = fabric.pair(Reliable).await;
    let full = fabric
        .a
        .connect(
            fabric.br.local_id(),
            UnorderedOptions {
                delivery: Reliable,
                timeout: BOUND,
            },
        )
        .await
        .unwrap_err();
    assert!(capacity_rejection(&full));
    a.close().await.unwrap();
    assert!(timeout(BOUND, b.recv()).await.unwrap().is_err());
    drop(a);
    drop(b);
    // Wait for the closing TLS generation to leave the tunnel admission table.
    let retry = async {
        loop {
            match fabric
                .a
                .connect(
                    fabric.br.local_id(),
                    UnorderedOptions {
                        delivery: Reliable,
                        timeout: BOUND,
                    },
                )
                .await
            {
                Ok(session) => break session,
                Err(error) if capacity_rejection(&error) => tokio::task::yield_now().await,
                Err(error) => panic!("unexpected re-admission error: {error}"),
            }
        }
    };
    let new_a = timeout(BOUND, retry).await.unwrap();
    let (_, new_b) = fabric.b.accept().await.unwrap();
    new_a
        .send(Bytes::from_static(b"new generation"))
        .await
        .unwrap();
    assert_eq!(new_b.recv().await.unwrap(), b"new generation"[..]);
    fabric.close().await;
}

fn capacity_rejection(error: &io::Error) -> bool {
    // Capacity is reserved before parsing any request. Overload therefore
    // closes the authenticated control immediately instead of reading for a reply.
    matches!(
        error.kind(),
        io::ErrorKind::WouldBlock
            | io::ErrorKind::UnexpectedEof
            | io::ErrorKind::ConnectionAborted
            | io::ErrorKind::ConnectionReset
            | io::ErrorKind::BrokenPipe
    )
}

#[tokio::test]
async fn close_drop_revocation_and_shutdown_wake_blocked_calls() {
    for policy in [Reliable, Unreliable] {
        for mode in 0..4 {
            let fabric = Fabric::new(config()).await;
            let (a, b) = fabric.pair(policy).await;
            fabric.faults.set(DROP_ALL);
            let send = if policy == Reliable {
                let task = tokio::spawn({
                    let a = a.clone();
                    async move { a.send(Bytes::from_static(b"blocked")).await }
                });
                eventually("blocked send active", || {
                    fabric.faults.data.load(Ordering::SeqCst) >= 1
                })
                .await;
                Some(task)
            } else {
                None
            };
            match mode {
                0 => {
                    a.close().await.unwrap();
                }
                1 => {
                    drop(b);
                }
                2 => {
                    assert!(fabric.at.revoke_peer(fabric.br.local_id()));
                }
                _ => {
                    fabric.a.shutdown();
                }
            }
            if let Some(send) = send {
                assert!(timeout(BOUND, send).await.unwrap().unwrap().is_err());
            }
            assert!(timeout(BOUND, a.recv()).await.unwrap().is_err());
            fabric.close().await;
        }
    }
    let fabric = Fabric::new(config()).await;
    let accepting = tokio::spawn({
        let endpoint = fabric.a.clone();
        async move { endpoint.accept().await }
    });
    fabric.ar.close().await;
    assert_eq!(
        timeout(BOUND, accepting)
            .await
            .unwrap()
            .unwrap()
            .unwrap_err()
            .kind(),
        io::ErrorKind::NotConnected
    );
    fabric.close().await;
}

#[tokio::test]
async fn held_healthy_sessions_heartbeat_and_dead_peer_expires() {
    let fabric = Fabric::new(config()).await;
    let (a, b) = fabric.pair(Reliable).await;
    eventually("heartbeats beyond the idle expiry", || {
        fabric.faults.heartbeats.load(Ordering::SeqCst) >= 15
    })
    .await;
    a.send(Bytes::from_static(b"still alive")).await.unwrap();
    assert_eq!(b.recv().await.unwrap(), b"still alive"[..]);
    fabric.br.close().await;
    assert!(timeout(BOUND, a.recv()).await.unwrap().is_err());
    fabric.close().await;
}

#[tokio::test]
async fn last_endpoint_drop_cancels_sessions_and_releases_only_its_namespace() {
    let fabric = Fabric::new(config()).await;
    let (a_session, b_session) = fabric.pair(Unreliable).await;
    let Fabric {
        a,
        b,
        at,
        bt,
        ar,
        br,
        attacker,
        faults: _,
    } = fabric;
    drop(a);
    assert!(timeout(BOUND, a_session.recv()).await.unwrap().is_err());
    assert!(timeout(BOUND, b_session.recv()).await.unwrap().is_err());
    assert!(
        !ar.cancellation().is_cancelled(),
        "endpoint drop must not close the router"
    );
    let rebound = super::UnorderedProtocol::new(&ar, at.clone(), config()).unwrap();
    rebound.shutdown();
    rebound.closed().await;
    b.shutdown();
    b.closed().await;
    tokio::join!(at.close(), bt.close());
    tokio::join!(ar.close(), br.close(), attacker.close());
}

#[tokio::test]
async fn pinned_admission_and_negotiated_payload_bounds_are_enforced() {
    let mut remote = config();
    remote.max_payload = 4;
    let fabric = Fabric::with_configs(config(), remote).await;
    let unadmitted = fabric
        .a
        .connect(
            fabric.attacker.local_id(),
            UnorderedOptions {
                delivery: Reliable,
                timeout: BOUND,
            },
        )
        .await
        .unwrap_err();
    assert_eq!(unadmitted.kind(), io::ErrorKind::PermissionDenied);
    for policy in [Reliable, Unreliable] {
        let (a, b) = fabric.pair(policy).await;
        assert_eq!(
            a.send(Bytes::from_static(b"12345"))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidInput
        );
        a.send(Bytes::from_static(b"1234")).await.unwrap();
        assert_eq!(b.recv().await.unwrap(), b"1234"[..]);
    }
    fabric.close().await;
}

#[test]
fn invalid_capacity_policy_and_timer_configuration_is_rejected() {
    let mut bounds = config();
    bounds.allow_reliable = false;
    bounds.allow_unreliable = false;
    assert!(bounds.validate().is_err());
    bounds = config();
    bounds.sessions_per_peer = bounds.max_sessions.saturating_add(1);
    assert!(bounds.validate().is_err());
    bounds = config();
    bounds.idle_timeout = bounds.heartbeat_interval;
    assert!(bounds.validate().is_err());
    bounds = config();
    bounds.send_timeout = Duration::MAX;
    assert!(bounds.validate().is_err());
}

#[tokio::test]
async fn cancelled_setup_releases_admission_and_its_tls_control_session() {
    let mut bounds = config();
    bounds.max_sessions = QueueCapacity::MIN;
    bounds.sessions_per_peer = QueueCapacity::MIN;
    let fabric = Fabric::new(bounds).await;
    fabric.b.shutdown();
    fabric.b.closed().await;
    let (started, ready) = tokio::sync::oneshot::channel();
    let connecting = tokio::spawn({
        let endpoint = fabric.a.clone();
        let peer = fabric.br.local_id().clone();
        async move {
            started.send(()).unwrap();
            endpoint
                .connect(
                    &peer,
                    UnorderedOptions {
                        delivery: Reliable,
                        timeout: BOUND,
                    },
                )
                .await
        }
    });
    ready.await.unwrap();
    assert!(!connecting.is_finished());
    connecting.abort();
    assert!(connecting.await.unwrap_err().is_cancelled());
    let second = fabric
        .a
        .connect(
            fabric.br.local_id(),
            UnorderedOptions {
                delivery: Reliable,
                timeout: Duration::from_millis(100),
            },
        )
        .await
        .unwrap_err();
    assert_eq!(
        second.kind(),
        io::ErrorKind::TimedOut,
        "cancelled setup must release the local slot"
    );
    fabric.close().await;
}
