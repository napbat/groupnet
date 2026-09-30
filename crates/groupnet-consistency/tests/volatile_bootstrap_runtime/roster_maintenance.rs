//! Automatic donor recapture on the recovery worker's existing maintenance timer.

use super::*;
use groupnet_core::volatile_bootstrap::ClaimPhase;

fn open_local_only_donor() -> (
    Arc<Claims>,
    Arc<OriginDonor>,
    Arc<ReadAdapter>,
    RecoveryHandle<ReadAdapter>,
) {
    open_local_only_donor_with(100, RecoveryMode::Unleased)
}

fn open_local_only_donor_with_renewal(
    renew_ms: u64,
) -> (
    Arc<Claims>,
    Arc<OriginDonor>,
    Arc<ReadAdapter>,
    RecoveryHandle<ReadAdapter>,
) {
    open_local_only_donor_with(renew_ms, RecoveryMode::Unleased)
}

fn open_local_only_donor_with(
    renew_ms: u64,
    mode: RecoveryMode,
) -> (
    Arc<Claims>,
    Arc<OriginDonor>,
    Arc<ReadAdapter>,
    RecoveryHandle<ReadAdapter>,
) {
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    donor.local_only.store(true, Ordering::SeqCst);
    donor.allow_recapture.store(true, Ordering::SeqCst);
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    config.claim.renew_ms = renew_ms;
    config.claim.claim_ttl_ms = config.claim.claim_ttl_ms.max(renew_ms * 2);
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission(),
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
        mode,
        NodeId::from("me"),
        9,
        Box::new(driver),
    )
    .unwrap();
    (claims, donor, reads, handle)
}

#[tokio::test]
async fn affirmed_lapse_resumes_local_donor_with_a_new_capture_and_no_origin_scan() {
    let (claims, donor, reads, handle) = open_local_only_donor_with(100, RecoveryMode::Leased);
    reads.lapse_sequences.store(1, Ordering::SeqCst);
    reads.lapse_hold.store(true, Ordering::SeqCst);
    wait_ready(&claims, &donor, &handle).await;
    let old = claims
        .local
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .identity
        .clone();
    let old_ingress = donor.ingress.lock().unwrap().clone().unwrap();
    handle.lease_lapse(1).unwrap();
    eventually_within("lapse withdraws old donor claim", SETTLE, || {
        !handle.status().may_serve
            && donor.ingress.lock().unwrap().is_none()
            && claims.local.lock().unwrap().is_none()
            && claims.presence.lock().unwrap().is_some()
    })
    .await;
    reads.lapse_hold.store(false, Ordering::SeqCst);
    eventually_within(
        "reaffirmed local image obtains fresh donor C",
        SETTLE,
        || {
            let status = handle.status();
            status.may_serve
                && status.state.generation == 2
                && donor.recaptures.load(Ordering::SeqCst) == 2
                && claims
                    .local
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|claim| claim.phase == ClaimPhase::Ready && claim.identity != old)
        },
    )
    .await;
    assert!(
        !donor
            .ingress
            .lock()
            .unwrap()
            .as_ref()
            .is_some_and(|current| current.same_candidate(&old_ingress))
    );
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
}

#[tokio::test]
async fn cancelled_lapse_withdraws_suspended_presence_and_never_recaptures() {
    let (claims, donor, reads, handle) = open_local_only_donor_with(100, RecoveryMode::Leased);
    reads.lapse_sequences.store(1, Ordering::SeqCst);
    reads.lapse_hold.store(true, Ordering::SeqCst);
    wait_ready(&claims, &donor, &handle).await;
    handle.lease_lapse(1).unwrap();
    eventually_within("suspended local capture withdraws Ready", SETTLE, || {
        !handle.status().may_serve
            && donor.ingress.lock().unwrap().is_none()
            && claims.local.lock().unwrap().is_none()
            && claims.presence.lock().unwrap().is_some()
    })
    .await;
    handle.cancel().unwrap();
    eventually_within(
        "terminal cancel withdraws suspended presence",
        SETTLE,
        || {
            claims.presence.lock().unwrap().is_none()
                && claims.local.lock().unwrap().is_none()
                && donor.ingress.lock().unwrap().is_none()
        },
    )
    .await;
    assert_eq!(donor.recaptures.load(Ordering::SeqCst), 1);
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn ready_affirmation_recaptures_before_the_next_presence_deadline() {
    let (claims, donor, reads, handle) = open_local_only_donor_with_renewal(5_000);
    wait_ready(&claims, &donor, &handle).await;
    assert_eq!(claims.presence.lock().unwrap().as_ref().unwrap().renewal, 1);
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
}

async fn wait_ready(claims: &Claims, donor: &OriginDonor, handle: &RecoveryHandle<ReadAdapter>) {
    eventually_within(
        "initial local-only image becomes a Ready donor",
        SETTLE,
        || {
            handle.status().may_serve
                && donor.recaptures.load(Ordering::SeqCst) == 1
                && claims
                    .local
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|claim| claim.phase == ClaimPhase::Ready)
        },
    )
    .await;
}

#[tokio::test]
async fn ready_donor_retires_changed_roster_then_recaptures_without_origin_scan() {
    let (claims, donor, reads, handle) = open_local_only_donor();
    wait_ready(&claims, &donor, &handle).await;
    let old = claims
        .local
        .lock()
        .unwrap()
        .as_ref()
        .unwrap()
        .identity
        .clone();
    let first_presence = claims.presence.lock().unwrap().as_ref().unwrap().renewal;
    eventually_within(
        "unchanged roster survives maintenance renewals",
        SETTLE,
        || {
            claims
                .presence
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|presence| presence.renewal > first_presence + 1)
        },
    )
    .await;
    assert_eq!(donor.recaptures.load(Ordering::SeqCst), 1);

    let joiner = BootstrapMemberIdentity {
        node: NodeId::from("peer"),
        presence: Some(PresenceIdentity {
            node: NodeId::from("peer"),
            boot: BootId(9),
            session: 2,
        }),
        member_incarnation: 2,
        status: Status::Alive,
    };
    // Native Alive may precede its presence publication. The old C must be
    // withdrawn, but one incomplete sample must not end future donation.
    *claims.joiner.lock().unwrap() = Some((joiner.clone(), false));
    eventually_within("maintenance withdraws obsolete donor C", SETTLE, || {
        handle.status().may_serve
            && claims.local.lock().unwrap().is_none()
            && donor.ingress.lock().unwrap().is_none()
    })
    .await;
    assert_eq!(donor.recaptures.load(Ordering::SeqCst), 1);

    *claims.joiner.lock().unwrap() = Some((joiner, true));
    eventually_within(
        "complete native cut recaptures without a new LIST",
        SETTLE,
        || {
            handle.status().may_serve
                && donor.recaptures.load(Ordering::SeqCst) == 2
                && claims
                    .local
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|claim| claim.phase == ClaimPhase::Ready && claim.identity != old)
                && donor
                    .ingress
                    .lock()
                    .unwrap()
                    .as_ref()
                    .is_some_and(|ingress| {
                        ingress.with_journal(|journal| journal.image_members().len() == 2)
                    })
        },
    )
    .await;
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
}

#[tokio::test]
async fn invalidated_ready_capture_wakes_worker_and_failed_replacement_stays_withdrawn() {
    let (claims, donor, reads, handle) = open_local_only_donor();
    wait_ready(&claims, &donor, &handle).await;
    donor.allow_recapture.store(false, Ordering::SeqCst);
    let old = donor.ingress.lock().unwrap().clone().unwrap();
    old.with_journal(|journal| {
        journal.invalidate(groupnet_core::volatile_bootstrap::journal::Invalidation::DonorLost);
    });
    eventually_within("invalidated capture withdraws Ready", SETTLE, || {
        donor.ingress.lock().unwrap().is_none()
            && claims.local.lock().unwrap().is_none()
            && claims.presence.lock().unwrap().is_some()
    })
    .await;
    assert!(handle.status().may_serve);
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(donor.recaptures.load(Ordering::SeqCst), 1);
    assert_eq!(reads.old_origin_builds.load(Ordering::SeqCst), 0);
    handle.cancel().unwrap();
}
