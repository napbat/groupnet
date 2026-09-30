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
    BootId, BootstrapClaim, BootstrapConfig, BootstrapOperation, BootstrapPresence, BootstrapScope,
    ClaimIdentity, ClaimPhase, PresenceIdentity, claim_entry_key, presence_entry_key,
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
        progress: 0,
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

fn presence(node: groupnet_core::NodeId, boot: u128, session: u64) -> BootstrapPresence {
    BootstrapPresence {
        identity: PresenceIdentity {
            node,
            boot: BootId(boot),
            session,
        },
        renewal: 1,
        remaining_ms: policy().claim_ttl_ms,
    }
}

#[tokio::test]
async fn presence_renews_under_unrelated_writes_and_old_withdrawal_cannot_erase_replacement() {
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
    let mut old_presence = presence(cluster.ids[0].clone(), 7, 8);
    old.publish_presence(old_presence.clone()).await.unwrap();
    for _ in 0..24 {
        let queued = budget.reserve(AdmissionClass::Inflight, 32).unwrap();
        cluster.groups[0]
            .set_entry_confirmed("hot", b"x".to_vec(), None, 32, 32, queued)
            .await
            .unwrap();
    }
    old_presence.renewal = 2;
    old.publish_presence(old_presence.clone()).await.unwrap();
    old.withdraw_presence(old_presence.identity.clone())
        .await
        .unwrap();
    let replacement = presence(cluster.ids[0].clone(), 9, 10);
    new.publish_presence(replacement.clone()).await.unwrap();
    old.withdraw_presence(old_presence.identity).await.unwrap();
    let key = presence_entry_key(&scope(), 128).unwrap();
    let bytes = cluster.groups[0]
        .node_entry(&cluster.ids[0], &key)
        .expect("new presence remains");
    assert_eq!(
        groupnet_core::volatile_bootstrap::decode_presence_value(
            &scope(),
            policy(),
            &cluster.ids[0],
            &bytes,
            256,
        )
        .unwrap()
        .identity,
        replacement.identity,
    );
    drop(old);
    drop(new);
    assert_eq!(budget.usage().0, 0);
}

/// A first presence create binds the whole local member revision, so any
/// unrelated local write landing between its inspection cut and the actor's
/// conditional apply rejects it without mutation. That benign race must not
/// fail publication (and with it the process's whole bootstrap episode).
#[tokio::test]
async fn first_presence_create_survives_unrelated_local_writes_between_cut_and_apply() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let budget = admission();
    let source = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    // Fewer racing writes than the bounded re-inspection allows. Each one
    // queues behind the publication's cut and ahead of its conditional apply.
    let writer = {
        let group = cluster.groups[0].clone();
        let budget = budget.clone();
        tokio::spawn(async move {
            for _ in 0..2 {
                let queued = budget.reserve(AdmissionClass::Inflight, 32).unwrap();
                group
                    .set_entry_confirmed("hot", b"x".to_vec(), None, 32, 32, queued)
                    .await
                    .unwrap();
            }
        })
    };
    let published = presence(cluster.ids[0].clone(), 7, 8);
    source.publish_presence(published.clone()).await.unwrap();
    writer.await.unwrap();
    let key = presence_entry_key(&scope(), 128).unwrap();
    let bytes = cluster.groups[0]
        .node_entry(&cluster.ids[0], &key)
        .expect("the created presence is visible");
    let visible = groupnet_core::volatile_bootstrap::decode_presence_value(
        &scope(),
        policy(),
        &cluster.ids[0],
        &bytes,
        256,
    )
    .unwrap();
    assert_eq!(
        (visible.identity, visible.renewal),
        (published.identity, published.renewal)
    );
    drop(source);
    assert_eq!(budget.usage().0, 0);
}

#[tokio::test]
#[expect(
    clippy::too_many_lines,
    reason = "one connected source schedule checks the same actor cut before and after claim withdrawal and malformed presence"
)]
async fn paired_cut_retains_transferred_participant_without_a_builder_claim() {
    let cluster = MemCluster::builder(&["node-a", "node-b"])
        .group("g")
        .gossip_interval_ms(20)
        .spawn();
    let budget = admission();
    let first = NativeClaimSource::new(
        cluster.groups[0].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let second = NativeClaimSource::new(
        cluster.groups[1].clone(),
        scope(),
        policy(),
        128,
        256,
        budget.clone(),
    )
    .unwrap();
    let first_presence = presence(cluster.ids[0].clone(), 7, 8);
    let second_presence = presence(cluster.ids[1].clone(), 9, 10);
    first.publish_presence(first_presence).await.unwrap();
    second.publish_presence(second_presence).await.unwrap();
    let first_claim = claim(cluster.ids[0].clone());
    first.publish_claim(first_claim.clone()).await.unwrap();
    let presence_key = presence_entry_key(&scope(), 128).unwrap();
    let claim_key = claim_entry_key(&scope(), 128).unwrap();
    eventually("paired source metadata reaches the second member", || {
        cluster.groups[1]
            .node_entry(&cluster.ids[0], &presence_key)
            .is_some()
            && cluster.groups[1]
                .node_entry(&cluster.ids[0], &claim_key)
                .is_some()
    })
    .await;
    let op = BootstrapOperation {
        session: 10,
        incarnation: BootId(9),
        generation: 1,
        token: 1,
    };
    let cut = second
        .observe_participation(op, limits(), &budget)
        .await
        .unwrap();
    assert_eq!(cut.get().members.len(), 2);
    assert_eq!(cut.get().roster.len(), 2);
    assert!(
        cut.get()
            .roster
            .iter()
            .all(|member| member.presence.is_some())
    );
    assert_eq!(cut.get().participants.len(), 2);
    assert_eq!(cut.get().claims.len(), 1);
    drop(cut);
    first.withdraw_claim(first_claim.identity).await.unwrap();
    eventually("transferred member claim withdrawal arrives", || {
        cluster.groups[1]
            .node_entry(&cluster.ids[0], &claim_key)
            .is_none()
    })
    .await;
    let cut = second
        .observe_participation(op, limits(), &budget)
        .await
        .unwrap();
    assert_eq!(cut.get().participants.len(), 2);
    assert!(cut.get().claims.is_empty());
    drop(cut);
    let queued = budget.reserve(AdmissionClass::Inflight, 128).unwrap();
    cluster.groups[0]
        .set_entry_confirmed(
            &presence_key,
            b"malformed",
            Some(policy().claim_ttl_ms),
            128,
            256,
            queued,
        )
        .await
        .unwrap();
    eventually("malformed scoped participation propagates", || {
        cluster.groups[1]
            .node_entry(&cluster.ids[0], &presence_key)
            .is_some_and(|value| value == b"malformed")
    })
    .await;
    assert!(
        second
            .observe_participation(op, limits(), &budget)
            .await
            .is_err()
    );
    drop(first);
    drop(second);
    assert_eq!(budget.usage().0, 0);
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
    let raw_charge = 2_048 + claim_entry_key(&scope(), 128).unwrap().len();
    let budget = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: raw_charge + 1,
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: raw_charge + 1,
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
