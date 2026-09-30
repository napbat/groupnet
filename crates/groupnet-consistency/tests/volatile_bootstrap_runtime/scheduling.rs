//! A waiting acquisition must yield to the runtime that also drives its peers.

use super::{
    Arc, BootId, BootstrapCapabilities, BootstrapScope, BootstrapSession, Claims, Duration,
    Instant, NodeId, Ordering, OriginDonor, ReadAdapter, RecoveryConfig, RecoveryHandle,
    RecoveryMode, RecoveryRearm, SETTLE, admission, bootstrap_config, eventually_within,
};
use groupnet_core::volatile_bootstrap::ClaimPhase;

/// Consumers run the worker beside their gossip and peer tasks, often on a
/// current-thread runtime. While the child waits out its settle window it has
/// only timer work queued; it must sleep rather than re-tick forever, or no
/// other task on that thread (a donor's recapture, the claim source) runs
/// until the deadline passes.
#[tokio::test]
async fn settling_acquisition_yields_until_its_deadline() {
    const CHILD_SETTLE_MS: u64 = 600;
    let mut config = bootstrap_config();
    config.claim.settle_ms = CHILD_SETTLE_MS;
    config.claim.renew_ms = 1_000;
    config.claim.claim_ttl_ms = 2_000;
    config.claim.donor_wait_ms = 2_000;
    config.claim.total_ms = 3_000;
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
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
    let started = Instant::now();
    let handle = RecoveryHandle::open_with_bootstrap_and_rearm(
        Arc::new(ReadAdapter::default()),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 5_000,
            attempt_ms: 4_000,
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
    // A worker that never yields while Settling advances straight to the
    // origin build before this task is polled again, so the Willing claim is
    // never observable.
    eventually_within(
        "the settling child's Willing claim is observable",
        SETTLE,
        || {
            claims
                .local
                .lock()
                .unwrap()
                .as_ref()
                .is_some_and(|claim| claim.phase == ClaimPhase::Willing)
        },
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(CHILD_SETTLE_MS / 2),
        "another task ran only after {:?}",
        started.elapsed()
    );
    assert_eq!(donor.builds.load(Ordering::SeqCst), 0);
    eventually_within(
        "the child still builds once its settle window ends",
        SETTLE,
        || handle.status().may_serve && donor.builds.load(Ordering::SeqCst) == 1,
    )
    .await;
    handle.cancel().unwrap();
}
