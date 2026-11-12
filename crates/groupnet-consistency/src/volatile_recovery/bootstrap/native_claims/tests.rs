use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use groupnet_core::volatile_bootstrap::{BootId, ClaimPhase};
use groupnet_core::{NodeId, Status};
use groupnet_runtime::InspectedEntry;
use groupnet_testkit::cluster::MemCluster;

use super::*;
use crate::volatile_recovery::bootstrap::admission::AdmissionLimits;

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("nonzero")
}

fn options() -> NativeClaimOptions {
    NativeClaimOptions {
        scope: BootstrapScope {
            domain: "origin-account".into(),
            partition: "whole-index".into(),
        },
        policy: BootstrapConfig {
            max_members: nz(2),
            max_member_bytes: nz(16),
            max_scope_bytes: nz(64),
            settle_ms: 20,
            renew_ms: 100,
            claim_ttl_ms: 3_000,
            observe_ms: 200,
            donor_wait_ms: 1_000,
            total_ms: 2_000,
        },
        max_key_bytes: nz(128),
        max_value_bytes: nz(256),
    }
}

fn limits() -> ClaimObservationLimits {
    ClaimObservationLimits {
        max_members: nz(2),
        max_member_bytes: nz(16),
        max_metadata_bytes: nz(2_048),
    }
}

fn donor(attempt: u64) -> ClaimIdentity {
    ClaimIdentity {
        node: NodeId::from("donor"),
        incarnation: BootId(7),
        session: 8,
        attempt,
    }
}

/// A cut of the follower and a donor whose entry holds a Ready claim of
/// `attempt`, or no entry, at `status`.
fn cut(status: Status, attempt: Option<u64>) -> InspectedEntries {
    let options = options();
    let value = attempt.map(|attempt| {
        encode_claim_value(
            &options.scope,
            options.policy,
            &BootstrapClaim {
                identity: donor(attempt),
                renewal: 3,
                phase: ClaimPhase::Ready,
                progress: 0,
                remaining_ms: 0,
            },
            options.max_value_bytes.get(),
        )
        .expect("encodable claim")
    });
    InspectedEntries {
        sampled_at: Instant::now(),
        entries: vec![
            InspectedEntry {
                node: NodeId::from("donor"),
                status,
                remaining_ttl: value.as_ref().map(|_| Duration::from_secs(2)),
                value,
            },
            InspectedEntry {
                node: NodeId::from("follower"),
                status: Status::Alive,
                value: None,
                remaining_ttl: None,
            },
        ],
    }
}

/// What the follower's refresh read makes of `cut` for the donor's attempt 1.
async fn read(cut: InspectedEntries) -> Result<Option<ClaimPhase>, AdapterErrorKind> {
    let cluster = MemCluster::builder(&["follower"]).spawn().await;
    let admission = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: nz(16_384),
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 16_384,
        max_reservations: nz(8),
    })
    .expect("admission limits");
    let source = NativeClaimSource::new(cluster.groups[0].clone(), admission.clone(), options())
        .expect("valid options");
    let raw = admission
        .reserve(AdmissionClass::Inflight, nz(2_048))
        .expect("raw charge")
        .hold(cut);
    let read = source
        .selected_from_cut(raw, &donor(1), limits(), &admission)
        .map(|claim| claim.map(|claim| claim.get().claim.phase))
        .map_err(|error| error.kind());
    assert_eq!(admission.usage().0, 0, "the read retires every charge");
    read
}

/// A loaded donor that SWIM suspects still holds its claim: the follower's
/// refresh read is a missed sample, which keeps the transfer while the claim
/// it last saw is unexpired, not the donor's departure.
#[tokio::test]
async fn a_suspected_donors_claim_reads_as_a_missed_sample() {
    assert_eq!(
        read(cut(Status::Suspect, Some(1))).await,
        Err(AdapterErrorKind::Unavailable)
    );
    assert_eq!(
        read(cut(Status::Alive, Some(1))).await,
        Ok(Some(ClaimPhase::Ready))
    );
}

/// A donor that is dead, whose claim expired, or that replaced the selected
/// attempt has left that transfer: the read is absent, and the follower
/// abandons it at once.
#[tokio::test]
async fn a_departed_donor_still_reads_as_absent() {
    assert_eq!(read(cut(Status::Dead, Some(1))).await, Ok(None));
    assert_eq!(read(cut(Status::Suspect, None)).await, Ok(None));
    assert_eq!(read(cut(Status::Suspect, Some(2))).await, Ok(None));
    assert_eq!(read(cut(Status::Alive, Some(2))).await, Ok(None));
}
