//! Native Groupnet claims are bounded TTL hints, never read authority.
#![cfg(feature = "volatile-recovery")]

use std::num::NonZeroUsize;
use std::time::Duration;

use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::native_claims::{
    NativeClaimOptions, NativeClaimSource, NativeClaimSourceError,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::{
    ClaimObservationLimits, ClaimSource,
};
use groupnet_consistency::volatile_recovery::bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapConfigError, BootstrapOperation,
    BootstrapPresence, BootstrapScope, ClaimIdentity, ClaimPhase, PresenceIdentity,
};
use groupnet_core::Status;
use groupnet_core::volatile_bootstrap::{ObservedEntry, claim_entry_key, presence_entry_key};
use groupnet_runtime::{EntryMutationLimits, EntryWrite};
use groupnet_testkit::cluster::{MemCluster, eventually};

const VALUE_BYTES: NonZeroUsize = NonZeroUsize::new(256).unwrap();
const KEY_BYTES: NonZeroUsize = NonZeroUsize::new(128).unwrap();

fn nz(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("nonzero")
}

fn limits_of(key: usize, value: usize) -> EntryMutationLimits {
    EntryMutationLimits {
        max_key_bytes: nz(key),
        max_value_bytes: nz(value),
    }
}

fn policy() -> BootstrapConfig {
    BootstrapConfig {
        max_members: std::num::NonZeroUsize::new(2).unwrap(),
        max_member_bytes: std::num::NonZeroUsize::new(16).unwrap(),
        max_scope_bytes: std::num::NonZeroUsize::new(64).unwrap(),
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
        max_total_bytes: std::num::NonZeroUsize::new(16_384).unwrap(),
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 16_384,
        max_reservations: std::num::NonZeroUsize::new(8).unwrap(),
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
        max_members: nz(2),
        max_member_bytes: nz(16),
        max_metadata_bytes: nz(2_048),
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

/// Each violated construction rule is named, never collapsed into one error.
#[tokio::test]
async fn construction_names_each_rejected_argument() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let group = &cluster.groups[0];
    let open = |scope, policy, max_key_bytes, max_value_bytes| {
        NativeClaimSource::new(
            group.clone(),
            admission(),
            NativeClaimOptions {
                scope,
                policy,
                max_key_bytes,
                max_value_bytes,
            },
        )
        .unwrap_err()
    };
    let long = BootstrapScope {
        domain: "d".repeat(64),
        partition: "p".into(),
    };
    assert_eq!(
        open(long, policy(), KEY_BYTES, VALUE_BYTES),
        NativeClaimSourceError::ScopeTooLong
    );
    let invalid = BootstrapConfig {
        max_members: nz(usize::MAX),
        ..policy()
    };
    let error = open(scope(), invalid, KEY_BYTES, VALUE_BYTES);
    assert_eq!(
        error,
        NativeClaimSourceError::Policy(BootstrapConfigError::UnboundedMaxMembers)
    );
    assert_eq!(
        std::error::Error::source(&error)
            .and_then(|source| source.downcast_ref::<BootstrapConfigError>()),
        Some(&BootstrapConfigError::UnboundedMaxMembers)
    );
    let error = open(scope(), policy(), nz(4), VALUE_BYTES);
    assert!(matches!(error, NativeClaimSourceError::ClaimKey(_)));
    assert!(std::error::Error::source(&error).is_some());
}

/// A process restarted under the same member name publishes its new life's
/// presence while the survivor still holds the previous life's unexpired one.
#[tokio::test]
async fn a_restarted_member_publishes_its_new_life_over_the_previous_lifes_presence() {
    let opts = groupnet_testkit::cluster::NodeOpts::new("g").gossip_interval_ms(25);
    let net = groupnet_transport_mem::Network::new();
    let (_a_id, _a_node, a_group) =
        groupnet_testkit::cluster::spawn_mem_node(&net, "re-a", &["re-b"], &opts).await;
    let (b_id, b_node, b_group) =
        groupnet_testkit::cluster::spawn_mem_node(&net, "re-b", &["re-a"], &opts).await;
    groupnet_testkit::cluster::converged(&[&a_group, &b_group]).await;
    let budget = admission();
    let source = |group| {
        NativeClaimSource::new(
            group,
            budget.clone(),
            NativeClaimOptions {
                scope: scope(),
                policy: policy(),
                max_key_bytes: KEY_BYTES,
                max_value_bytes: VALUE_BYTES,
            },
        )
        .unwrap()
    };
    let old = source(b_group.clone());
    old.publish_presence(presence(b_id.clone(), 7, 8))
        .await
        .unwrap();
    let key = presence_entry_key(&scope(), KEY_BYTES).unwrap();
    eventually("the survivor holds the old life's presence", || {
        a_group.node_entry(&b_id, &key).is_some()
    })
    .await;
    drop(old);
    drop(b_group);
    b_node.close().await;
    let (_b_id, _reborn_node, reborn) =
        groupnet_testkit::cluster::spawn_mem_node(&net, "re-b", &["re-a"], &opts).await;
    groupnet_testkit::cluster::converged(&[&a_group, &reborn]).await;
    source(reborn)
        .publish_presence(presence(b_id, 9, 10))
        .await
        .expect("the new life publishes its presence");
}

#[tokio::test]
async fn presence_renews_under_unrelated_writes_and_old_withdrawal_cannot_erase_replacement() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let budget = admission();
    let old = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let new = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let mut old_presence = presence(cluster.ids[0].clone(), 7, 8);
    old.publish_presence(old_presence.clone()).await.unwrap();
    for _ in 0..24 {
        let queued = budget.reserve(AdmissionClass::Inflight, nz(32)).unwrap();
        cluster.groups[0]
            .set_entry_confirmed(
                EntryWrite::new("hot", b"x").with_limits(limits_of(32, 32)),
                queued,
            )
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
    let key = presence_entry_key(&scope(), KEY_BYTES).unwrap();
    let bytes = cluster.groups[0]
        .node_entry(&cluster.ids[0], &key)
        .expect("new presence remains");
    assert_eq!(
        groupnet_core::volatile_bootstrap::decode_presence_value(
            &scope(),
            policy(),
            ObservedEntry {
                author: &cluster.ids[0],
                value: &bytes
            },
            256
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
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let budget = admission();
    let source = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    // Fewer racing writes than the bounded re-inspection allows. Each one
    // queues behind the publication's cut and ahead of its conditional apply.
    let writer = {
        let group = cluster.groups[0].clone();
        let budget = budget.clone();
        tokio::spawn(async move {
            for _ in 0..2 {
                let queued = budget.reserve(AdmissionClass::Inflight, nz(32)).unwrap();
                group
                    .set_entry_confirmed(
                        EntryWrite::new("hot", b"x").with_limits(limits_of(32, 32)),
                        queued,
                    )
                    .await
                    .unwrap();
            }
        })
    };
    let published = presence(cluster.ids[0].clone(), 7, 8);
    source.publish_presence(published.clone()).await.unwrap();
    writer.await.unwrap();
    let key = presence_entry_key(&scope(), KEY_BYTES).unwrap();
    let bytes = cluster.groups[0]
        .node_entry(&cluster.ids[0], &key)
        .expect("the created presence is visible");
    let visible = groupnet_core::volatile_bootstrap::decode_presence_value(
        &scope(),
        policy(),
        ObservedEntry {
            author: &cluster.ids[0],
            value: &bytes,
        },
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
        .spawn()
        .await;
    let budget = admission();
    let first = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let second = NativeClaimSource::new(
        cluster.groups[1].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let first_presence = presence(cluster.ids[0].clone(), 7, 8);
    let second_presence = presence(cluster.ids[1].clone(), 9, 10);
    first.publish_presence(first_presence).await.unwrap();
    second.publish_presence(second_presence).await.unwrap();
    let first_claim = claim(cluster.ids[0].clone());
    first.publish_claim(first_claim.clone()).await.unwrap();
    let presence_key = presence_entry_key(&scope(), KEY_BYTES).unwrap();
    let claim_key = claim_entry_key(&scope(), KEY_BYTES).unwrap();
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
    assert_eq!(
        cut.get().claims,
        [] as [groupnet_core::volatile_bootstrap::BootstrapClaim; 0]
    );
    drop(cut);
    let queued = budget.reserve(AdmissionClass::Inflight, nz(128)).unwrap();
    cluster.groups[0]
        .set_entry_confirmed(
            EntryWrite::new(&presence_key, b"malformed")
                .with_ttl(Duration::from_millis(policy().claim_ttl_ms))
                .with_limits(limits_of(128, 256)),
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
        .spawn()
        .await;
    let budget = admission();
    let source_a = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let source_b = NativeClaimSource::new(
        cluster.groups[1].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    source_a
        .publish_claim(claim(cluster.ids[0].clone()))
        .await
        .unwrap();
    let key = claim_entry_key(&scope(), KEY_BYTES).unwrap();
    eventually("peer receives native TTL claim and complete roster", || {
        cluster.groups[1]
            .node_entry(&cluster.ids[0], &key)
            .is_some()
            && cluster.groups[1]
                .statuses_held_bounded(nz(2), nz(16))
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

    let queued = budget.reserve(AdmissionClass::Inflight, nz(128)).unwrap();
    cluster.groups[1]
        .set_entry_confirmed(
            EntryWrite::new(&key, b"malformed")
                .with_ttl(Duration::from_secs(3))
                .with_limits(limits_of(128, 256)),
            queued,
        )
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
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let budget = admission();
    let old = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let new = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
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
    let key = claim_entry_key(&scope(), KEY_BYTES).unwrap();
    let queued = budget.reserve(AdmissionClass::Inflight, nz(128)).unwrap();
    cluster.groups[0]
        .set_entry_confirmed(
            EntryWrite::new(&key, b"present-without-ttl").with_limits(limits_of(128, 256)),
            queued,
        )
        .await
        .unwrap();
    assert!(new.observe_claims(op, limits(), &budget).await.is_err());
    drop(old);
    drop(new);
    assert_eq!(budget.usage().0, 0);
}

#[tokio::test]
async fn failed_converted_admission_retires_raw_actor_response_first() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let raw_charge = 2_048 + claim_entry_key(&scope(), KEY_BYTES).unwrap().len();
    let budget = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: std::num::NonZeroUsize::new(raw_charge + 1).unwrap(),
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: raw_charge + 1,
        max_reservations: std::num::NonZeroUsize::new(2).unwrap(),
    })
    .unwrap();
    let source = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
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

/// Observation bounds are nonzero by type; one above the source's own policy
/// is refused as invalid input before any inspection is charged.
#[tokio::test]
async fn observation_limits_above_the_policy_are_refused_before_any_charge() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn().await;
    let budget = admission();
    let source = NativeClaimSource::new(
        cluster.groups[0].clone(),
        budget.clone(),
        NativeClaimOptions {
            scope: scope(),
            policy: policy(),
            max_key_bytes: KEY_BYTES,
            max_value_bytes: VALUE_BYTES,
        },
    )
    .unwrap();
    let op = BootstrapOperation {
        session: 8,
        incarnation: BootId(7),
        generation: 1,
        token: 1,
    };
    for wide in [
        ClaimObservationLimits {
            max_members: policy().max_members.checked_add(1).unwrap(),
            ..limits()
        },
        ClaimObservationLimits {
            max_member_bytes: policy().max_member_bytes.checked_add(1).unwrap(),
            ..limits()
        },
    ] {
        let refused = source.observe_claims(op, wide, &budget).await;
        assert_eq!(
            refused.unwrap_err().kind(),
            groupnet_consistency::volatile_recovery::AdapterErrorKind::InvalidInput
        );
        let refused = source.observe_participation(op, wide, &budget).await;
        assert_eq!(
            refused.unwrap_err().kind(),
            groupnet_consistency::volatile_recovery::AdapterErrorKind::InvalidInput
        );
    }
    assert_eq!(budget.usage().0, 0);
}
