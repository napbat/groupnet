//! A node's own origin scan must not take its presence down with it.

use super::{
    AdapterError, AdmissionLimits, Arc, AtomicBool, AtomicUsize, BootId, BootstrapCapabilities,
    BootstrapScope, BootstrapSession, BoxRecoveryFuture, ByteAdmission, Claims, Duration, Instant,
    Mark, NodeId, Ordering, OriginDonor, PeerObservation, PublicationPermit, RecoveryAdapter,
    RecoveryConfig, RecoveryHandle, RecoveryMode, RecoveryOperation, SETTLE, bootstrap_config,
    eventually_within,
};

const PAGES: usize = 25;
const PAGE_MS: u64 = 100;

/// A paged origin rebuild that commits a page and reports progress every
/// `PAGE_MS`, as a real scan does, so the parent never times it out.
#[derive(Debug, Default)]
struct SlowOrigin {
    scanning: AtomicBool,
    scans: AtomicUsize,
}

impl RecoveryAdapter for SlowOrigin {
    fn revoke_serving(&self) {}

    fn invalidate(
        &self,
        _op: RecoveryOperation,
        _distrust_bodies: bool,
        _permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn rebuild_origin(
        &self,
        _op: RecoveryOperation,
        permit: PublicationPermit,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            self.scanning.store(true, Ordering::SeqCst);
            for _ in 0..PAGES {
                tokio::time::sleep(Duration::from_millis(PAGE_MS)).await;
                permit.publish(|| ()).ok_or(AdapterError)?;
                permit.progress();
            }
            self.scans.fetch_add(1, Ordering::SeqCst);
            self.scanning.store(false, Ordering::SeqCst);
            Ok(())
        })
    }

    fn observe_peers(
        &self,
        _op: RecoveryOperation,
        _limits: RecoveryConfig,
    ) -> BoxRecoveryFuture<'_, PeerObservation> {
        Box::pin(async { Ok((Vec::new(), None::<Mark>)) })
    }

    fn wait_frontiers(
        &self,
        _op: RecoveryOperation,
        _heads: Vec<(NodeId, Mark)>,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async { Ok(()) })
    }

    fn affirm(&self, _op: RecoveryOperation) -> bool {
        true
    }
}

/// The bootstrap child declines (its donor cannot admit a local capture), so
/// the recovery worker falls back to its own 2.5 s origin rebuild: five claim
/// TTLs (500 ms). Throughout, the child's presence keeps renewing within one
/// TTL, so a peer's complete participation cut, which that peer's Ready
/// recapture needs, still finds this live member. Before, the worker did not
/// maintain the child while it awaited the rebuild: the presence lapsed after
/// one TTL and, refused at the scan's end, was withdrawn for good.
#[tokio::test]
async fn own_origin_rebuild_keeps_presence_renewing_past_claim_ttls() {
    let claims = Arc::new(Claims::default());
    let donor = Arc::new(OriginDonor::default());
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    let ttl = Duration::from_millis(config.claim.claim_ttl_ms);
    let (driver, _sender) = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::clone(&claims),
            donor: Arc::clone(&donor),
            admission: ByteAdmission::new(AdmissionLimits {
                max_total_bytes: 100_000,
                max_encoded_bytes: 0,
                max_decoded_bytes: 1_024,
                max_suffix_bytes: 50_000,
                max_native_overlap_bytes: 1_024,
                max_inflight_bytes: 8_192,
                max_reservations: 32,
            })
            .unwrap(),
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
    let origin = Arc::new(SlowOrigin::default());
    let handle = RecoveryHandle::open_with_bootstrap(
        Arc::clone(&origin),
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
    eventually_within("the declined child falls back to origin", SETTLE, || {
        origin.scanning.load(Ordering::SeqCst)
    })
    .await;
    let renewal = |claims: &Claims| {
        claims
            .presence
            .lock()
            .unwrap()
            .as_ref()
            .map(|presence| presence.renewal)
    };
    let started = Instant::now();
    let mut last = (renewal(&claims), Instant::now());
    let mut longest = Duration::ZERO;
    while origin.scanning.load(Ordering::SeqCst) {
        let now = renewal(&claims);
        assert!(now.is_some(), "presence withdrawn during the scan");
        if now != last.0 {
            last = (now, Instant::now());
        }
        longest = longest.max(last.1.elapsed());
        assert!(
            longest < ttl,
            "presence unrenewed for {longest:?}, past its {ttl:?} TTL, during the scan"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(
        started.elapsed() > ttl * 4,
        "the rebuild outlasted several claim TTLs"
    );
    eventually_within("the rebuilt node serves", SETTLE, || {
        handle.status().may_serve
    })
    .await;
    assert_eq!(origin.scans.load(Ordering::SeqCst), 1);
    assert_eq!(donor.builds.load(Ordering::SeqCst), 0);
    // After the scan the presence is still accepted and renewing.
    let after = renewal(&claims).expect("presence survives the scan");
    eventually_within("presence keeps renewing after the scan", SETTLE, || {
        renewal(&claims).is_some_and(|now| now > after + 2)
    })
    .await;
    handle.cancel().unwrap();
}
