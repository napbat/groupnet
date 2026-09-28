//! Native Groupnet claims are bounded TTL hints, never read authority.
#![cfg(feature = "volatile-recovery")]

use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::native_claims::NativeClaimSource;
use groupnet_consistency::volatile_recovery::bootstrap::ports::{
    ClaimObservationLimits, ClaimSource,
};
use groupnet_core::Status;
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapOperation, BootstrapScope, ClaimIdentity,
    ClaimPhase, claim_entry_key,
};
use groupnet_testkit::cluster::{MemCluster, eventually};

fn policy() -> BootstrapConfig {
    BootstrapConfig {
        max_members: 2,
        max_member_bytes: 16,
        max_scope_bytes: 64,
        settle_ms: 20,
        renew_ms: 100,
        claim_ttl_ms: 3_000,
        observe_ms: 200,
        donor_wait_ms: 1_000,
        total_ms: 2_000,
    }
}

fn admission() -> ByteAdmission {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 16_384,
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 16_384,
        max_reservations: 8,
    })
    .unwrap()
}

fn scope() -> BootstrapScope {
    BootstrapScope {
        domain: "origin-account".into(),
        partition: "whole-index".into(),
    }
}

fn claim(node: groupnet_core::NodeId) -> BootstrapClaim {
    BootstrapClaim {
        identity: ClaimIdentity {
            node,
            incarnation: BootId(7),
            session: 8,
            attempt: 1,
        },
        renewal: 1,
        phase: ClaimPhase::Willing,
        remaining_ms: policy().claim_ttl_ms,
    }
}

fn limits() -> ClaimObservationLimits {
    ClaimObservationLimits {
        max_members: 2,
        max_member_bytes: 16,
        max_metadata_bytes: 2_048,
    }
}

#[tokio::test]
async fn connected_actor_cut_decodes_claim_and_rejects_malformed_present_peer() {
    let cluster = MemCluster::builder(&["node-a", "node-b"])
        .group("g")
        .gossip_interval_ms(20)
        .spawn();
    let budget = admission();
    let source_a = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let source_b = NativeClaimSource::new(
        cluster.groups[1].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    source_a
        .publish_claim(claim(cluster.ids[0].clone()))
        .await
        .unwrap();
    let key = claim_entry_key(&scope(), 128).unwrap();
    eventually("peer receives native TTL claim and complete roster", || {
        cluster.groups[1]
            .node_entry(&cluster.ids[0], &key)
            .is_some()
            && cluster.groups[1]
                .statuses_held_bounded(2, 16)
                .is_ok_and(|members| {
                    members.len() == 2
                        && members
                            .iter()
                            .all(|(_, status, _)| *status == Status::Alive)
                })
    })
    .await;
    let op = BootstrapOperation {
        session: 8,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    let observed = source_b
        .observe_claims(op, limits(), &budget)
        .await
        .unwrap();
    assert_eq!(observed.get().members.len(), 2);
    assert_eq!(observed.get().claims.len(), 1);
    assert_eq!(observed.get().claims[0].identity.node, cluster.ids[0]);
    assert!(observed.get().claims[0].remaining_ms > 0);
    drop(observed);
    assert!(
        source_b
            .observe_claims(op, limits(), &admission())
            .await
            .is_err()
    );

    let queued = budget.reserve(AdmissionClass::Inflight, 128).unwrap();
    cluster.groups[1]
        .set_entry_confirmed(&key, b"malformed", Some(3_000), 128, 256, queued)
        .await
        .unwrap();
    eventually("malformed peer claim propagates", || {
        cluster.groups[0]
            .node_entry(&cluster.ids[1], &key)
            .is_some()
    })
    .await;
    assert!(
        source_a
            .observe_claims(op, limits(), &budget)
            .await
            .is_err()
    );
    drop(source_a);
    drop(source_b);
    assert_eq!(budget.usage().0, 0);
}

#[tokio::test]
async fn late_old_session_withdrawal_cannot_delete_new_local_claim() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let budget = admission();
    let old = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let new = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let old_claim = claim(cluster.ids[0].clone());
    old.publish_claim(old_claim.clone()).await.unwrap();
    let mut new_claim = old_claim.clone();
    new_claim.identity.incarnation = BootId(9);
    new_claim.identity.session = 10;
    new.publish_claim(new_claim.clone()).await.unwrap();
    old.withdraw_claim(old_claim.identity).await.unwrap();

    let op = BootstrapOperation {
        session: 10,
        incarnation: BootId(9),
        generation: 1,
        token: 1,
    };
    let snapshot = new.observe_claims(op, limits(), &budget).await.unwrap();
    assert_eq!(snapshot.get().claims.len(), 1);
    assert_eq!(snapshot.get().claims[0].identity, new_claim.identity);
    drop(snapshot);
    let key = claim_entry_key(&scope(), 128).unwrap();
    let queued = budget.reserve(AdmissionClass::Inflight, 128).unwrap();
    cluster.groups[0]
        .set_entry_confirmed(&key, b"present-without-ttl", None, 128, 256, queued)
        .await
        .unwrap();
    assert!(new.observe_claims(op, limits(), &budget).await.is_err());
    drop(old);
    drop(new);
    assert_eq!(budget.usage().0, 0);
}

#[tokio::test]
async fn failed_converted_admission_retires_raw_actor_response_first() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let budget = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 2_048,
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 2_048,
        max_reservations: 2,
    })
    .unwrap();
    let source = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let op = BootstrapOperation {
        session: 8,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    assert!(source.observe_claims(op, limits(), &budget).await.is_err());
    assert_eq!(budget.usage().0, 0);
}
