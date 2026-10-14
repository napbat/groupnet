//! A follower waits for a remote build, and for the transfer of its image,
//! for as long as either keeps advancing, past every fixed bound of its own
//! recovery, then installs the image without an origin scan of its own.

use super::scenarios::{follower_identity, open_follower, peer_identity};
use super::{
    Arc, BootstrapClaim, Claims, Duration, Mutex, Ordering, PeerDonor, RecoveryConfig,
    RecoveryMode, SETTLE, bootstrap_config, eventually_within,
};
use groupnet_core::volatile_bootstrap::ClaimPhase;

/// Advance the peer's claim every `page_ms` for `pages` pages, then publish
/// it Ready and keep renewing it, with its presence, as a live donor does.
async fn advertise_build(claims: Arc<Claims>, pages: u64, page_ms: u64) {
    for _ in 0..pages {
        tokio::time::sleep(Duration::from_millis(page_ms)).await;
        let mut peer = claims.peer.lock().unwrap();
        let claim = peer.as_mut().expect("peer claim");
        claim.progress += 1;
        claim.renewal += 1;
    }
    loop {
        {
            let mut peer = claims.peer.lock().unwrap();
            let claim = peer.as_mut().expect("peer claim");
            claim.phase = ClaimPhase::Ready;
            claim.renewal += 1;
        }
        tokio::time::sleep(Duration::from_millis(page_ms)).await;
    }
}

fn claims(phase: ClaimPhase) -> Arc<Claims> {
    Arc::new(Claims {
        peer: Mutex::new(Some(BootstrapClaim {
            identity: peer_identity(),
            renewal: 1,
            phase,
            progress: 0,
            remaining_ms: 500,
        })),
        ..Claims::default()
    })
}

/// A recovery episode of 300 ms and a 150 ms attempt bound: far shorter
/// than the build and the transfer below.
fn short_recovery() -> RecoveryConfig {
    RecoveryConfig {
        max_members: 2,
        max_member_bytes: 8,
        max_barrier_rounds: 2,
        total_ms: 300,
        attempt_ms: 150,
        settle_ms: 5,
        poll_ms: 5,
    }
}

/// The remote build takes 2.4 s: eight recovery episodes (300 ms), three
/// claim episodes (800 ms) and twice the follower's stall grace (1.1 s).
/// Each advance the follower observes renews its recovery episode, so it
/// waits out the whole build and installs the image with no origin scan.
#[tokio::test]
async fn follower_outwaits_a_progressing_build_longer_than_its_recovery_budget() {
    let claims = claims(ClaimPhase::Building);
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    let (donor, admission, handle, reads) = open_follower(
        &claims,
        PeerDonor::new(&peer_identity(), &follower_identity()),
        config,
        RecoveryMode::Unleased,
        short_recovery(),
    );
    let builder = tokio::spawn(advertise_build(Arc::clone(&claims), 48, 50));
    eventually_within(
        "the follower installs the builder's image",
        SETTLE * 3,
        || handle.status().may_serve || reads.old_origin_builds.load(Ordering::SeqCst) > 0,
    )
    .await;
    assert_eq!(
        reads.old_origin_builds.load(Ordering::SeqCst),
        0,
        "no origin scan while the followed build progressed: {:?}",
        reads.fallbacks.lock().unwrap()
    );
    assert!(handle.status().may_serve);
    assert_eq!(donor.installed.load(Ordering::SeqCst), 1);
    builder.abort();
    handle.cancel().unwrap();
    eventually_within("peer transfer releases admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}

/// The production failure: the Ready donor's image arrives in 40 chunks of
/// 50 ms, 2 s in all, four times the donor wait (500 ms) that bounded the
/// whole transfer, and past the claim episode (800 ms) and the recovery
/// episode (300 ms). Each stored chunk renews all three, so the transfer
/// completes and the follower never scans the origin.
#[tokio::test]
async fn progressing_transfer_outlives_the_donor_wait_and_every_episode_bound() {
    const CHUNKS: usize = 40;
    let claims = claims(ClaimPhase::Ready);
    let mut config = bootstrap_config();
    config.require_participation = true;
    config.max_claim_metadata_bytes = 512;
    config.transfer.max_chunks = CHUNKS;
    config.transfer.max_encoded_bytes = CHUNKS;
    config.transfer.max_decoded_bytes = CHUNKS;
    let (donor, admission, handle, reads) = open_follower(
        &claims,
        PeerDonor::paced(&peer_identity(), &follower_identity(), CHUNKS, 50),
        config,
        RecoveryMode::Unleased,
        short_recovery(),
    );
    let renewals = tokio::spawn(advertise_build(Arc::clone(&claims), 0, 100));
    eventually_within("the paced transfer installs", SETTLE * 3, || {
        handle.status().may_serve
            || reads.old_origin_builds.load(Ordering::SeqCst) > 0
            || donor.local_builds.load(Ordering::SeqCst) > 0
    })
    .await;
    assert_eq!(
        (
            reads.old_origin_builds.load(Ordering::SeqCst),
            donor.local_builds.load(Ordering::SeqCst)
        ),
        (0, 0),
        "no origin scan, recovery's or its own build's, while the transfer progressed: {:?}",
        reads.fallbacks.lock().unwrap()
    );
    assert!(handle.status().may_serve);
    assert_eq!(donor.installed.load(Ordering::SeqCst), 1);
    assert_eq!(*donor.live.lock().unwrap(), Some(vec![42; CHUNKS]));
    renewals.abort();
    handle.cancel().unwrap();
    eventually_within("peer transfer releases admission", SETTLE, || {
        admission.usage().0 == 0
    })
    .await;
}
