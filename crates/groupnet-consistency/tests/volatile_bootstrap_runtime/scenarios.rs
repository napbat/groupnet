//! Worker integration scenarios for guarded local and peer acquisition.

use super::{
    AdmissionClass, Arc, BootId, BootstrapCapabilities, BootstrapClaim, BootstrapRuntimeConfig,
    BootstrapScope, BootstrapSession, ByteAdmission, CaptureId, ClaimIdentity, Claims,
    DonorCapture, DonorJournal, DonorPort, DonorRequest, Duration, JournalIngress, Mutex, NodeId,
    Notify, Ordering, OriginDonor, PeerDonor, ReadAdapter, RecoveryConfig, RecoveryHandle,
    RecoveryMode, RecoveryRearm, SETTLE, Time, admission, bootstrap_config, eventually_within,
    journal_config, member_from_claim,
};

fn captured_for_cleanup(
    serial: u64,
    selected: &ClaimIdentity,
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
        .begin_capture(Time(0), 1, 1, vec![member_from_claim(selected)], Vec::new())
        .unwrap();
    journal.finish_capture(Time(0), 1, 1).unwrap();
    let storage = DonorJournal::storage_bound(journal_config()).unwrap();
    let suffix = admission.reserve(AdmissionClass::Suffix, storage).unwrap();
    let ingress = JournalIngress::new(journal, suffix, Arc::new(Notify::new())).unwrap();
    let encoded = admission.reserve(AdmissionClass::Encoded, 1).unwrap();
    let decoded = admission.reserve(AdmissionClass::Decoded, 1).unwrap();
    DonorCapture::new(vec![42], ingress, encoded, decoded).unwrap()
}

#[test]
fn ingress_rejects_an_undercharged_whole_journal() {
    let admission = admission();
    let selected = ClaimIdentity {
        node: NodeId::from("me"),
        incarnation: BootId(31),
        session: 14,
        attempt: 1,
    };
    let mut journal = DonorJournal::new(
        journal_config(),
        CaptureId {
            scope: BootstrapScope {
                domain: "o".into(),
                partition: "b".into(),
            },
            donor: selected.clone(),
            recovery_generation: 1,
            serial: 3,
        },
    )
    .unwrap();
    journal
        .begin_capture(
            Time(0),
            1,
            1,
            vec![member_from_claim(&selected)],
            Vec::new(),
        )
        .unwrap();
    journal.finish_capture(Time(0), 1, 1).unwrap();
    let storage = DonorJournal::storage_bound(journal_config()).unwrap();
    let insufficient = admission
        .reserve(AdmissionClass::Suffix, storage - 1)
        .unwrap();
    assert!(JournalIngress::new(journal, insufficient, Arc::new(Notify::new())).is_err());
    assert_eq!(admission.usage().0, 0);
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
    let old = captured_for_cleanup(1, &selected, &admission);
    let replacement = captured_for_cleanup(2, &selected, &admission);
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
    assert_eq!(
        admission.usage().0,
        2 * (DonorJournal::storage_bound(journal_config()).unwrap() + 2)
    );
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
    let handle = RecoveryHandle::open_with_bootstrap_and_rearm(
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
        RecoveryRearm {
            initial_ms: 50,
            max_ms: 200,
        },
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
async fn completed_origin_without_donor_capture_serves_without_a_second_scan() {
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    donor.local_only.store(true, Ordering::SeqCst);
    let (driver, sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission(),
        },
        BootstrapRuntimeConfig {
            require_participation: true,
            ..bootstrap_config()
        },
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
    eventually_within("local-only image is independently affirmed", SETTLE, || {
        handle.status().may_serve && donor.builds.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    assert!(sender.current_identity().is_none());
    handle.cancel().unwrap();
}

#[tokio::test]
async fn required_participation_keeps_local_origin_and_declines_claim_only_donor() {
    let admission = admission();
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    let (driver, sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission,
        },
        config,
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
        "local scan can affirm without a donor advertisement",
        SETTLE,
        || handle.status().may_serve && donor.builds.load(Ordering::SeqCst) == 1,
    )
    .await;
    // The declined image is never advertised Ready. Its failed recapture is
    // retried, each retry after its backoff, only inside the claim window of
    // `donor_wait_ms` (500 ms) that opened with the local build. Then the
    // claim is withdrawn and nothing retries it under the unchanged roster.
    eventually_within(
        "claim-only donor retires but participation remains",
        SETTLE,
        || {
            claims.local.lock().unwrap().is_none()
                && claims.presence.lock().unwrap().is_some()
                && sender.current_identity().is_none()
                && donor.ingress.lock().unwrap().is_none()
                && donor.recapture_attempts.load(Ordering::SeqCst) > 0
        },
    )
    .await;
    let attempts = donor.recapture_attempts.load(Ordering::SeqCst);
    // Retries wait 100, 125, 125, 125 ms: at most five attempts fit.
    assert!((2..=5).contains(&attempts), "{attempts} attempts");
    let renewal = claims.presence.lock().unwrap().as_ref().unwrap().renewal;
    eventually_within("maintenance turns keep running", SETTLE, || {
        claims
            .presence
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|presence| presence.renewal > renewal + 2)
    })
    .await;
    assert_eq!(donor.recapture_attempts.load(Ordering::SeqCst), attempts);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("cancel withdraws the exact participation", SETTLE, || {
        claims.presence.lock().unwrap().is_none()
    })
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
    let (claims, donor, admission, handle, reads) =
        follower_setup_with_mode(paused, config, RecoveryMode::Unleased);
    drop(claims);
    (donor, admission, handle, reads)
}

fn follower_setup_with_mode(
    paused: bool,
    config: BootstrapRuntimeConfig,
    mode: RecoveryMode,
) -> (
    Arc<Claims>,
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
            progress: 0,
            remaining_ms: 500,
        }),
        ..Claims::default()
    });
    let donor = Arc::new(PeerDonor::new(&peer, &follower_identity()));
    donor.pause_install.store(paused, Ordering::SeqCst);
    let admission = admission();
    let (driver, _sender) = BootstrapSession::new(
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
        mode,
        NodeId::from("me"),
        15,
        Box::new(driver),
    )
    .unwrap();
    (claims, donor, admission, handle, reads)
}

#[tokio::test]
async fn complete_presence_enables_canonical_peer_transfer() {
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    let (donor, admission, handle, reads) = follower_setup_with_config(false, config);
    eventually_within(
        "peer transfer and fresh head barrier use the same participant roster",
        SETTLE,
        || handle.status().may_serve && donor.installed.load(Ordering::SeqCst) == 1,
    )
    .await;
    assert_eq!(*donor.live.lock().unwrap(), Some(vec![42]));
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("canonical transfer releases admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

/// A claim renewal can fall due exactly when the follower rechecks its
/// roster before accepting a Ready donor. The worker publishes that renewal
/// before reading the source back, so the source shows the worker's own
/// current claim instead of contradicting it and forcing an origin build.
#[tokio::test]
async fn renewal_due_at_donor_selection_does_not_decline_the_peer() {
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    config.claim.settle_ms = 50;
    config.claim.renew_ms = 1;
    let (claims, donor, admission, handle, reads) =
        follower_setup_with_mode(false, config, RecoveryMode::Unleased);
    // Each publication outlasts the renewal interval, so every engine tick,
    // including the one that starts the donor's roster recheck, renews.
    claims.claim_publish_delay_ms.store(2, Ordering::SeqCst);
    eventually_within(
        "the peer image installs despite a renewal at selection",
        SETTLE,
        || handle.status().may_serve && donor.installed.load(Ordering::SeqCst) == 1,
    )
    .await;
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("peer transfer releases admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

#[tokio::test]
async fn completed_peer_keeps_presence_through_lapse_for_a_third_join() {
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    let (claims, donor, _admission, handle, reads) =
        follower_setup_with_mode(false, config, RecoveryMode::Leased);
    reads.lapse_sequences.store(1, Ordering::SeqCst);
    eventually_within("peer baseline is affirmed", SETTLE, || {
        handle.status().may_serve && donor.installed.load(Ordering::SeqCst) == 1
    })
    .await;
    let before = claims
        .presence
        .lock()
        .unwrap()
        .clone()
        .expect("presence survives transfer");
    assert!(
        claims.local.lock().unwrap().is_none(),
        "candidate claim retired"
    );
    handle.lease_lapse(1).unwrap();
    eventually_within(
        "lapse reaffirms without retiring process participation",
        SETTLE,
        || {
            handle.status().may_serve
                && handle.status().state.generation == 2
                && claims
                    .presence
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|p| p.identity == before.identity)
        },
    )
    .await;
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
    eventually_within("terminal worker withdraws participation", SETTLE, || {
        claims.presence.lock().unwrap().is_none()
    })
    .await;
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

/// Without required participation no native roster can prove the handoff's
/// covered members, so the installed image is discarded for guarded origin
/// recovery instead of opening reads on an unverified roster.
#[tokio::test]
async fn peer_handoff_without_participation_roster_recovers_from_origin() {
    let (donor, admission, handle, reads) = follower_setup(false);
    eventually_within(
        "unverifiable peer handoff falls back to one guarded origin build",
        SETTLE,
        || {
            handle.status().may_serve
                && donor.installed.load(Ordering::SeqCst) == 1
                && reads.old_origin_builds.load(Ordering::SeqCst) == 1
        },
    )
    .await;
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
