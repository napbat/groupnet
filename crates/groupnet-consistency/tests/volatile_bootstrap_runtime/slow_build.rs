//! A local origin build slower than every fixed bound must neither be killed
//! nor let its Building claim lapse while it keeps committing pages.

use super::{
    Arc, BootId, BootstrapCapabilities, BootstrapScope, BootstrapSession, Claims, Duration,
    Instant, NodeId, Ordering, OriginDonor, ReadAdapter, RecoveryConfig, RecoveryHandle,
    RecoveryMode, RecoveryRearm, SETTLE, admission, bootstrap_config, eventually_within,
};
use groupnet_core::volatile_bootstrap::ClaimPhase;

/// Thirty 50 ms pages take 1.5 s: three donor waits (500 ms), about twice the
/// claim episode (800 ms) and 2.5 recovery episodes (600 ms). Each committed
/// page renews all of them, the claim renews on its cadence throughout, and
/// followers see its progress advance. Before, the build was dropped at the
/// donor wait and the recovery fell back to a second, origin-only scan.
#[tokio::test]
async fn progressing_local_build_outlives_fixed_bounds_and_keeps_its_claim_live() {
    const PAGES: usize = 30;
    const PAGE_MS: u64 = 50;
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    donor.build_pages.store(PAGES, Ordering::SeqCst);
    donor.page_ms.store(PAGE_MS, Ordering::SeqCst);
    donor.local_only.store(true, Ordering::SeqCst);
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: admission(),
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
    let adapter = Arc::new(ReadAdapter::default());
    let started = Instant::now();
    let handle = RecoveryHandle::open_with_bootstrap_and_rearm(
        Arc::clone(&adapter),
        RecoveryConfig {
            max_members: 2,
            max_member_bytes: 8,
            max_barrier_rounds: 2,
            total_ms: 600,
            attempt_ms: 300,
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
    let building = |min_progress: u64| {
        claims
            .local
            .lock()
            .unwrap()
            .as_ref()
            .filter(|claim| claim.phase == ClaimPhase::Building && claim.progress >= min_progress)
            .map(|claim| (claim.renewal, claim.progress))
    };
    eventually_within("the Building claim advertises progress", SETTLE, || {
        building(1).is_some()
    })
    .await;
    let (renewal, progress) = building(1).expect("sampled above");
    // Past one claim TTL the same claim is still live, renewed, and further on.
    let ttl = Duration::from_millis(bootstrap_config().claim.claim_ttl_ms);
    eventually_within(
        "the claim keeps renewing with more progress past its TTL",
        SETTLE,
        || {
            started.elapsed() > ttl
                && building(progress + 3).is_some_and(|(later, _)| later > renewal)
        },
    )
    .await;
    eventually_within("the one slow build becomes Ready", SETTLE * 2, || {
        handle.status().may_serve
    })
    .await;
    assert!(
        started.elapsed() >= Duration::from_millis(PAGE_MS * PAGES as u64),
        "the build ran all its pages"
    );
    assert_eq!(donor.build_starts.load(Ordering::SeqCst), 1, "one build");
    assert_eq!(donor.builds.load(Ordering::SeqCst), 1);
    assert_eq!(
        adapter.old_origin_builds.load(Ordering::SeqCst),
        0,
        "no fallback origin rescan"
    );
    handle.cancel().unwrap();
}
