//! Worker integration scenarios for guarded local and peer acquisition.

use super::{
    AdmissionClass, Arc, BootId, BootstrapCapabilities, BootstrapClaim, BootstrapRuntimeConfig,
    BootstrapScope, BootstrapSession, ByteAdmission, CaptureId, ClaimIdentity, Claims,
    DonorCapture, DonorJournal, DonorPort, DonorRequest, Duration, JournalIngress, Mutex, NodeId,
    Notify, Ordering, OriginDonor, PeerDonor, ReadAdapter, RecoveryConfig, RecoveryHandle,
    RecoveryMode, SETTLE, Time, admission, bootstrap_config, eventually_within, journal_config,
};

fn captured_for_cleanup(
    serial: u64,
    selected: ClaimIdentity,
    admission: &ByteAdmission,
) -> DonorCapture<Vec<u8>> {
    let mut journal = DonorJournal::new(
        journal_config(),
        CaptureId {
            scope: BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            donor: selected.clone(),
            recovery_generation: 1,
            serial,
        },
    )
    .unwrap();
    journal
        .begin_capture(Time(0), 1, 1, vec![selected], Vec::new())
        .unwrap();
    journal.finish_capture(Time(0), 1, 1).unwrap();
    let suffix = admission.reserve(AdmissionClass::Suffix, 64).unwrap();
    let ingress = JournalIngress::new(journal, suffix, Arc::new(Notify::new())).unwrap();
    let encoded = admission.reserve(AdmissionClass::Encoded, 1).unwrap();
    let decoded = admission.reserve(AdmissionClass::Decoded, 1).unwrap();
    DonorCapture::new(vec![42], ingress, encoded, decoded).unwrap()
}

#[test]
fn delayed_old_capture_unlink_preserves_replacement_and_its_charge() {
    let admission = admission();
    let donor = OriginDonor::default();
    let selected = ClaimIdentity {
        node: NodeId::from("me"),
        incarnation: BootId(31),
        session: 14,
        attempt: 1,
    };
    let old = captured_for_cleanup(1, selected.clone(), &admission);
    let replacement = captured_for_cleanup(2, selected, &admission);
    *donor.ingress.lock().unwrap() = Some(replacement.ingress().clone());
    donor.retire_local_capture(&old);
    assert!(
        donor
            .ingress
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|live| { live.same_candidate(replacement.ingress()) })
    );
    assert!(replacement.is_active());
    assert_eq!(admission.usage().0, 132);
    drop(old);
    assert!(replacement.is_active());
    donor.retire_local_capture(&replacement);
    assert!(donor.ingress.lock().unwrap().is_none());
    drop(replacement);
    assert_eq!(admission.usage().0, 0);
}

#[tokio::test]
async fn one_worker_builds_origin_once_before_advertising_donor() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    let (driver, sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        bootstrap_config(),
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(7),
        1,
    )
    .unwrap();
    let reads = Arc::new(ReadAdapter::default());
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::clone(&reads),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        9,
        Box::new(driver),
    )
    .unwrap();
    eventually_within(
        "one guarded builder reaches independent affirmation",
        SETTLE,
        || handle.status().may_serve && donor.builds.load(Ordering::SeqCst) == 1,
    )
    .await;
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    assert!(
        claims.local.lock().unwrap().as_ref().is_some_and(
            |claim| claim.phase == groupnet_core::volatile_bootstrap::ClaimPhase::Ready
        )
    );
    assert_eq!(
        sender.current_identity(),
        claims
            .local
            .lock()
            .unwrap()
            .as_ref()
            .map(|claim| claim.identity.clone())
    );
    handle.cancel().unwrap();
    eventually_within(
        "retired capture clears donor listener identity",
        SETTLE,
        || sender.current_identity().is_none(),
    )
    .await;
}

#[tokio::test]
async fn one_coalesced_wake_serves_a_bounded_burst_without_waiting_for_renewal() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    let mut config = bootstrap_config();
    config.claim.renew_ms = 400;
    config.donor_inbox_capacity = 4;
    let (driver, sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims,
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        config,
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(23),
        11,
    )
    .unwrap();
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::new(ReadAdapter::default()),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        23,
        Box::new(driver),
    )
    .unwrap();
    eventually_within("donor capture available", SETTLE, || {
        handle.status().may_serve && donor.ingress.lock().unwrap().is_some()
    })
    .await;
    let mut replies = Vec::new();
    for _ in 0..4 {
        let request = admission
            .reserve(AdmissionClass::Inflight, 1)
            .unwrap()
            .hold(DonorRequest::Offer {
                max_metadata_bytes: 8,
            });
        replies.push(sender.try_submit(request).unwrap());
    }
    tokio::time::timeout(Duration::from_millis(250), async {
        for reply in replies {
            assert!(reply.await.unwrap().is_err());
        }
    })
    .await
    .expect("all queued requests drain before the next claim renewal");
    handle.cancel().unwrap();
    eventually_within("all burst charges released", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn stalled_ready_renewal_cannot_serve_a_request_after_capture_expiry() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    claims.pause_ready_renewal.store(true, Ordering::SeqCst);
    let donor = Arc::new(OriginDonor {
        capture_max_ms: std::sync::atomic::AtomicU64::new(65),
        ..OriginDonor::default()
    });
    let mut config = bootstrap_config();
    config.claim.renew_ms = 40;
    let (driver, sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        config,
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(32),
        15,
    )
    .unwrap();
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::new(ReadAdapter::default()),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        32,
        Box::new(driver),
    )
    .unwrap();
    eventually_within("local capture ready", SETTLE, || {
        handle.status().may_serve && donor.ingress.lock().unwrap().is_some()
    })
    .await;
    tokio::time::timeout(SETTLE, claims.renewal_started.notified())
        .await
        .expect("claim maintenance is blocked");
    let request = admission
        .reserve(AdmissionClass::Inflight, 1)
        .unwrap()
        .hold(DonorRequest::Offer {
            max_metadata_bytes: 8,
        });
    let reply = sender.try_submit(request).unwrap();
    let blocked_at = std::time::Instant::now();
    eventually_within("capture expires while maintenance awaits", SETTLE, || {
        blocked_at.elapsed() >= Duration::from_millis(50)
    })
    .await;
    claims.resume_renewal.notify_one();
    assert!(
        tokio::time::timeout(SETTLE, reply)
            .await
            .unwrap()
            .unwrap()
            .is_err()
    );
    assert_eq!(donor.follower_prepares.load(Ordering::SeqCst), 0);
    eventually_within("expired donor Ready withdrawn", SETTLE, || {
        donor.ingress.lock().unwrap().is_none() && claims.local.lock().unwrap().is_none()
    })
    .await;
    assert!(
        handle.status().may_serve,
        "local read authority is independent"
    );
    handle.cancel().unwrap();
    eventually_within("expired capture charges retired", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn cancel_while_capture_is_private_retires_charge_without_publication() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor {
        pause: std::sync::atomic::AtomicBool::new(true),
        ..OriginDonor::default()
    });
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims,
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        bootstrap_config(),
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(8),
        2,
    )
    .unwrap();
    let reads = Arc::new(ReadAdapter::default());
    let handle = RecoveryHandle::open_with_bootstrap(
        reads,
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        10,
        Box::new(driver),
    )
    .unwrap();
    tokio::time::timeout(SETTLE, donor.started.notified())
        .await
        .unwrap();
    assert!(!handle.status().may_serve);
    handle.cancel().unwrap();
    donor.resume.notify_one();
    eventually_within("cancelled capture releases every byte", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
    assert_eq!(donor.builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn expired_builder_permit_cannot_publish_after_child_timeout() {
    let admission = admission();
    let donor = Arc::new(OriginDonor {
        pause: std::sync::atomic::AtomicBool::new(true),
        ..OriginDonor::default()
    });
    let mut config = bootstrap_config();
    config.claim.total_ms = 180;
    config.claim.donor_wait_ms = 150;
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::new(Claims::default()),
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        config,
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(24),
        12,
    )
    .unwrap();
    let reads = Arc::new(ReadAdapter::default());
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::clone(&reads),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        24,
        Box::new(driver),
    )
    .unwrap();
    tokio::time::timeout(SETTLE, donor.started.notified())
        .await
        .expect("builder callback started");
    let child_permit = donor.latest_permit.lock().unwrap().clone().unwrap();
    eventually_within("child deadline triggers origin fallback", SETTLE, || {
        reads.old_origin_builds.load(Ordering::SeqCst) > 0
    })
    .await;
    assert!(child_permit.publish(|| ()).is_none());
    donor.resume.notify_one();
    assert_eq!(donor.builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("timed-out builder releases admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn donor_only_invalidation_withdraws_ready_without_revoking_local_reads() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        bootstrap_config(),
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(9),
        3,
    )
    .unwrap();
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::new(ReadAdapter::default()),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        11,
        Box::new(driver),
    )
    .unwrap();
    eventually_within("capture ready", SETTLE, || {
        handle.status().may_serve && donor.ingress.lock().unwrap().is_some()
    })
    .await;
    let old = donor.ingress.lock().unwrap().clone().unwrap();
    old.with_journal(|journal| {
        journal.invalidate(groupnet_core::volatile_bootstrap::journal::Invalidation::DonorLost);
    });
    eventually_within("donor withdraw without local read loss", SETTLE, || {
        donor.ingress.lock().unwrap().is_none() && claims.local.lock().unwrap().is_none()
    })
    .await;
    assert!(handle.status().may_serve);
    drop(old);
    handle.cancel().unwrap();
    eventually_within("all capture admission retired", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

fn peer_identity() -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from("peer"),
        incarnation: BootId(12),
        session: 5,
        attempt: 1,
    }
}

fn follower_identity() -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from("me"),
        incarnation: BootId(17),
        session: 7,
        attempt: 1,
    }
}

fn follower_setup(
    paused: bool,
) -> (
    Arc<PeerDonor>,
    ByteAdmission,
    RecoveryHandle<ReadAdapter>,
    Arc<ReadAdapter>,
) {
    follower_setup_with_config(paused, bootstrap_config())
}

fn follower_setup_with_config(
    paused: bool,
    config: BootstrapRuntimeConfig,
) -> (
    Arc<PeerDonor>,
    ByteAdmission,
    RecoveryHandle<ReadAdapter>,
    Arc<ReadAdapter>,
) {
    let peer = peer_identity();
    let claims = Arc::new(Claims {
        local: Mutex::new(None),
        peer: Some(BootstrapClaim {
            identity: peer.clone(),
            renewal: 1,
            phase: groupnet_core::volatile_bootstrap::ClaimPhase::Ready,
            remaining_ms: 500,
        }),
        ..Claims::default()
    });
    let donor = Arc::new(PeerDonor::new(peer.clone(), follower_identity()));
    donor.pause_install.store(paused, Ordering::SeqCst);
    let admission = admission();
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims,
            donor: Arc::clone(&donor),
            admission: admission.clone(),
        },
        config,
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        NodeId::from("me"),
        BootId(17),
        7,
    )
    .unwrap();
    let reads = Arc::new(ReadAdapter {
        peer: Some(peer),
        ..ReadAdapter::default()
    });
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::clone(&reads),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 1_000,
            attempt_ms: 500,
            settle_ms: 5,
            poll_ms: 5,
        },
        RecoveryMode::Unleased,
        NodeId::from("me"),
        15,
        Box::new(driver),
    )
    .unwrap();
    (donor, admission, handle, reads)
}

#[tokio::test]
async fn child_deadline_rejects_delayed_install_before_outer_episode_expires() {
    let mut config = bootstrap_config();
    config.claim.donor_wait_ms = 150;
    let (donor, admission, handle, reads) = follower_setup_with_config(true, config);
    tokio::time::timeout(SETTLE, donor.install_started.notified())
        .await
        .expect("private stage reaches install callback");
    eventually_within("child expires into origin fallback", SETTLE, || {
        reads.old_origin_builds.load(Ordering::SeqCst) > 0
    })
    .await;
    donor.resume_install.notify_one();
    assert_eq!(donor.installed.load(Ordering::SeqCst), 0);
    assert!(donor.live.lock().unwrap().is_none());
    handle.cancel().unwrap();
    eventually_within("expired child releases private admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn quiet_feed_peer_handoff_installs_once_then_independent_heads_affirm() {
    let (donor, admission, handle, reads) = follower_setup(false);
    eventually_within(
        "quiet peer transfer and fresh native handoff",
        SETTLE,
        || handle.status().may_serve && donor.installed.load(Ordering::SeqCst) == 1,
    )
    .await;
    assert_eq!(*donor.live.lock().unwrap(), Some(vec![42]));
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("follower transfer admission released", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn cancel_during_delayed_install_rejects_old_candidate_and_releases_stage() {
    let (donor, admission, handle, _reads) = follower_setup(true);
    tokio::time::timeout(SETTLE, donor.install_started.notified())
        .await
        .unwrap();
    assert!(!handle.status().may_serve);
    handle.cancel().unwrap();
    donor.resume_install.notify_one();
    eventually_within("stale private stage and callback retired", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
    assert_eq!(donor.installed.load(Ordering::SeqCst), 0);
    assert!(donor.live.lock().unwrap().is_none());
}
