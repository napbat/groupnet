//! Bounded, cancellation-safe actor cuts of membership and one scoped entry.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::Poll;
use std::time::{Duration, Instant};

use groupnet_runtime::{
    EntryBudget, EntryInspectionError, EntryInspectionLimits, EntryMutationLimits, EntryRevision,
    Group,
};
use groupnet_testkit::cluster::{MemCluster, eventually, eventually_within};

#[derive(Debug)]
struct Budget {
    bytes: usize,
    dropped: Arc<AtomicUsize>,
}

impl EntryBudget for Budget {
    fn bytes(&self) -> usize {
        self.bytes
    }
}

impl Drop for Budget {
    fn drop(&mut self) {
        self.dropped.fetch_add(1, Ordering::SeqCst);
    }
}

fn limits() -> EntryInspectionLimits {
    EntryInspectionLimits {
        max_key_bytes: 32,
        max_members: 2,
        max_member_bytes: 16,
        max_value_bytes: 32,
        max_response_bytes: 900,
    }
}

fn budget(bytes: usize) -> (Budget, Arc<AtomicUsize>) {
    let dropped = Arc::new(AtomicUsize::new(0));
    (
        Budget {
            bytes,
            dropped: Arc::clone(&dropped),
        },
        dropped,
    )
}

#[tokio::test]
async fn actor_cut_has_bounded_complete_roster_and_aging_native_ttl() {
    let cluster = MemCluster::builder(&["node-a", "node-b"])
        .group("g")
        .gossip_interval_ms(20)
        .spawn();
    let a = &cluster.groups[0];
    let b = &cluster.groups[1];
    a.set_entry("~claim:test", b"owner-1", Some(3_000)).unwrap();
    a.set_entry("~claim:permanent", b"not-ttl", None).unwrap();
    eventually("peer receives scoped entry", || {
        b.node_entry(&cluster.ids[0], "~claim:test").is_some()
    })
    .await;
    eventually("peer receives permanent scoped entry", || {
        b.node_entry(&cluster.ids[0], "~claim:permanent").is_some()
    })
    .await;

    let (first_budget, dropped) = budget(1_024);
    let (first, first_budget) = b
        .inspect_scoped_entry("~claim:test", limits(), first_budget)
        .await
        .unwrap();
    assert_eq!(first.entries.len(), 2);
    let owner = first
        .entries
        .iter()
        .find(|entry| entry.node == cluster.ids[0])
        .unwrap();
    assert_eq!(owner.value.as_deref(), Some(&b"owner-1"[..]));
    let remaining = owner.remaining_ttl_ms.unwrap();
    assert!(remaining > 0 && remaining <= 3_000);
    assert!(first.sampled_at <= Instant::now());
    drop(first_budget);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let start = Instant::now();
    eventually_within("observer TTL ages", Duration::from_secs(2), || {
        start.elapsed() >= Duration::from_millis(30)
    })
    .await;
    let (second_budget, _) = budget(1_024);
    let (second, _) = b
        .inspect_scoped_entry("~claim:test", limits(), second_budget)
        .await
        .unwrap();
    let second_remaining = second
        .entries
        .iter()
        .find(|entry| entry.node == cluster.ids[0])
        .unwrap()
        .remaining_ttl_ms
        .unwrap();
    assert!(second_remaining < remaining);

    let (permanent_budget, _) = budget(1_024);
    let (permanent, _) = b
        .inspect_scoped_entry("~claim:permanent", limits(), permanent_budget)
        .await
        .unwrap();
    let permanent_owner = permanent
        .entries
        .iter()
        .find(|entry| entry.node == cluster.ids[0])
        .unwrap();
    assert_eq!(permanent_owner.value.as_deref(), Some(&b"not-ttl"[..]));
    assert_eq!(permanent_owner.remaining_ttl_ms, None);
}

#[tokio::test]
async fn pair_cut_keeps_membership_and_both_native_ttls_together() {
    let cluster = MemCluster::builder(&["node-a", "node-b"])
        .group("g")
        .gossip_interval_ms(20)
        .spawn();
    let a = &cluster.groups[0];
    let b = &cluster.groups[1];
    a.set_entry("~presence:test", b"boot-a", Some(3_000))
        .unwrap();
    a.set_entry("~claim:test", b"build-a", Some(2_000)).unwrap();
    eventually("peer sees both scoped values", || {
        b.node_entry(&cluster.ids[0], "~presence:test").is_some()
            && b.node_entry(&cluster.ids[0], "~claim:test").is_some()
    })
    .await;
    let (owner, dropped) = budget(2_048);
    let (cut, owner) = b
        .inspect_scoped_pair("~presence:test", "~claim:test", limits(), owner)
        .await
        .unwrap();
    assert_eq!(cut.entries.len(), 2);
    let member = cut
        .entries
        .iter()
        .find(|entry| entry.node == cluster.ids[0])
        .unwrap();
    assert_eq!(member.first.as_deref(), Some(&b"boot-a"[..]));
    assert_eq!(member.second.as_deref(), Some(&b"build-a"[..]));
    assert!(
        member
            .first_remaining_ttl_ms
            .is_some_and(|ttl| ttl > 0 && ttl <= 3_000)
    );
    assert!(
        member
            .second_remaining_ttl_ms
            .is_some_and(|ttl| ttl > 0 && ttl <= 2_000)
    );
    assert!(cut.sampled_at <= Instant::now());
    drop(cut);
    drop(owner);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let (small, dropped) = budget(1_024);
    assert_eq!(
        b.inspect_scoped_pair(
            "~presence:test",
            "~claim:test",
            EntryInspectionLimits {
                max_response_bytes: 1,
                ..limits()
            },
            small,
        )
        .await
        .unwrap_err(),
        EntryInspectionError::Capacity
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_pair_reply_keeps_owned_budget_until_actor_retirement() {
    use std::future::Future;

    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let (owner, dropped) = budget(2_048);
    let mut query =
        Box::pin(group.inspect_scoped_pair("~presence:test", "~claim:test", limits(), owner));
    std::future::poll_fn(|cx| {
        assert!(matches!(query.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(query);
    eventually("cancelled pair actor response retires budget", || {
        dropped.load(Ordering::SeqCst) == 1
    })
    .await;
}

async fn revision_write(group: &Group, key: &str, value: &[u8], expected: EntryRevision) -> bool {
    let (owner, _) = budget(1_024);
    group
        .set_entry_if_revision(
            key,
            value.to_vec(),
            Some(3_000),
            expected,
            EntryMutationLimits {
                max_key_bytes: 32,
                max_value_bytes: 32,
            },
            owner,
        )
        .await
        .unwrap()
        .0
}

#[tokio::test]
async fn scoped_revision_fences_old_create_after_new_withdraw_and_ignores_unrelated_writes() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let key = "~presence:test";
    let inspect = || async {
        let (owner, _) = budget(2_048);
        let (cut, _) = group
            .inspect_scoped_pair(key, "~claim:test", limits(), owner)
            .await
            .unwrap();
        cut.entries
            .into_iter()
            .find(|entry| entry.node == cluster.ids[0])
            .unwrap()
    };
    let initially = inspect().await;
    assert!(initially.first.is_none());
    assert!(
        revision_write(
            group,
            key,
            b"boot-new:1",
            EntryRevision {
                key: initially.first_version,
                member: initially.member_state_version,
            },
        )
        .await
    );
    let current = inspect().await;
    for update in 0..32_u8 {
        let (unrelated, _) = budget(1_024);
        group
            .set_entry_confirmed("unrelated", vec![update], None, 32, 32, unrelated)
            .await
            .unwrap();
    }
    let current_revision = EntryRevision {
        key: current.first_version,
        member: current.member_state_version,
    };
    assert!(revision_write(group, key, b"boot-new:2", current_revision).await);
    assert!(!revision_write(group, key, b"boot-old:renew", current_revision).await);
    let (withdraw, _) = budget(1_024);
    assert!(
        group
            .delete_entry_if_value(key, b"boot-new:2", 32, 32, withdraw)
            .await
            .unwrap()
            .0
    );
    assert!(
        !revision_write(
            group,
            key,
            b"boot-old:1",
            EntryRevision {
                key: initially.first_version,
                member: initially.member_state_version,
            },
        )
        .await
    );
    assert!(group.node_entry(&cluster.ids[0], key).is_none());
}

#[tokio::test]
async fn overflow_refuses_whole_actor_cut_and_releases_its_budget() {
    let cluster = MemCluster::builder(&["node-a", "node-b"])
        .group("g")
        .gossip_interval_ms(20)
        .spawn();
    let group = &cluster.groups[0];
    group
        .set_entry("~claim:test", b"claim-value", Some(800))
        .unwrap();
    eventually("local entry applied", || {
        group.node_entry(&cluster.ids[0], "~claim:test").is_some()
    })
    .await;
    eventually("membership roster converges", || {
        group
            .statuses_held_bounded(2, 32)
            .is_ok_and(|members| members.len() == 2)
    })
    .await;

    let (small_budget, dropped) = budget(1_024);
    let error = group
        .inspect_scoped_entry(
            "~claim:test",
            EntryInspectionLimits {
                max_response_bytes: 1,
                ..limits()
            },
            small_budget,
        )
        .await
        .unwrap_err();
    assert_eq!(error, EntryInspectionError::Capacity);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let (member_budget, _) = budget(1_024);
    let error = group
        .inspect_scoped_entry(
            "~claim:test",
            EntryInspectionLimits {
                max_members: 1,
                ..limits()
            },
            member_budget,
        )
        .await
        .unwrap_err();
    assert_eq!(error, EntryInspectionError::TooManyMembers);

    let (value_budget, _) = budget(1_024);
    let error = group
        .inspect_scoped_entry(
            "~claim:test",
            EntryInspectionLimits {
                max_value_bytes: 1,
                ..limits()
            },
            value_budget,
        )
        .await
        .unwrap_err();
    assert_eq!(error, EntryInspectionError::ValueTooLong);
}

#[tokio::test]
async fn confirmed_local_claim_and_exact_withdrawal_fence_a_newer_renewal() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let key = "~claim:test";
    let (write_budget, _) = budget(1_024);
    group
        .set_entry_confirmed(key, b"boot1:renew1", Some(3_000), 32, 32, write_budget)
        .await
        .unwrap();
    assert_eq!(
        group.node_entry(&cluster.ids[0], key).as_deref(),
        Some(&b"boot1:renew1"[..])
    );
    let (write_budget, _) = budget(1_024);
    group
        .set_entry_confirmed(key, b"boot1:renew2", Some(3_000), 32, 32, write_budget)
        .await
        .unwrap();
    let (withdraw_budget, _) = budget(1_024);
    assert!(
        !group
            .delete_entry_if_value(key, b"boot1:renew1", 32, 32, withdraw_budget)
            .await
            .unwrap()
            .0
    );
    assert_eq!(
        group.node_entry(&cluster.ids[0], key).as_deref(),
        Some(&b"boot1:renew2"[..])
    );
    let (withdraw_budget, _) = budget(1_024);
    assert!(
        group
            .delete_entry_if_value(key, b"boot1:renew2", 32, 32, withdraw_budget)
            .await
            .unwrap()
            .0
    );
    assert!(group.node_entry(&cluster.ids[0], key).is_none());
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_after_actor_enqueue_keeps_budget_until_queued_work_retires() {
    use std::future::Future;

    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let (owner, dropped) = budget(1_024);
    let mut query = Box::pin(group.inspect_scoped_entry("~claim:test", limits(), owner));
    // This first poll enqueues into the actor but cannot run that actor on a
    // current-thread executor before this task yields to it.
    std::future::poll_fn(|cx| {
        assert!(matches!(query.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(query);
    eventually("cancelled actor response releases budget", || {
        dropped.load(Ordering::SeqCst) == 1
    })
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn cancelled_confirmed_write_keeps_queue_charge_until_actor_publication() {
    use std::future::Future;

    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let (owner, dropped) = budget(1_024);
    let mut write = Box::pin(group.set_entry_confirmed(
        "~claim:test",
        b"boot1:renew1",
        Some(3_000),
        32,
        32,
        owner,
    ));
    std::future::poll_fn(|cx| {
        assert!(matches!(write.as_mut().poll(cx), Poll::Pending));
        Poll::Ready(())
    })
    .await;
    assert_eq!(dropped.load(Ordering::SeqCst), 0);
    drop(write);
    eventually("cancelled publication releases queue charge", || {
        dropped.load(Ordering::SeqCst) == 1
    })
    .await;
    assert_eq!(
        group.node_entry(&cluster.ids[0], "~claim:test").as_deref(),
        Some(&b"boot1:renew1"[..])
    );
}

#[tokio::test]
async fn mutation_rejects_unreserved_bytes_before_enqueue() {
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let (small, dropped) = budget(1);
    let error = group
        .set_entry_confirmed("~claim:test", b"value", Some(3_000), 32, 32, small)
        .await
        .unwrap_err();
    assert_eq!(error, groupnet_runtime::EntryMutationError::InvalidLimit);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    assert!(group.node_entry(&cluster.ids[0], "~claim:test").is_none());
}

#[tokio::test]
async fn short_owned_values_with_large_capacity_never_enter_the_actor_queue() {
    fn oversized_value() -> Vec<u8> {
        let mut value = Vec::with_capacity(512);
        value.push(b'x');
        value
    }
    let cluster = MemCluster::builder(&["node-a"]).group("g").spawn();
    let group = &cluster.groups[0];
    let (first_budget, dropped) = budget(1_024);
    assert_eq!(
        group
            .set_entry_confirmed(
                "~presence:test",
                oversized_value(),
                None,
                32,
                32,
                first_budget
            )
            .await
            .unwrap_err(),
        groupnet_runtime::EntryMutationError::InvalidLimit
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);
    let (second_budget, _) = budget(1_024);
    assert_eq!(
        group
            .set_entry_if_revision(
                "~presence:test",
                oversized_value(),
                None,
                EntryRevision {
                    key: None,
                    member: 0,
                },
                EntryMutationLimits {
                    max_key_bytes: 32,
                    max_value_bytes: 32,
                },
                second_budget,
            )
            .await
            .unwrap_err(),
        groupnet_runtime::EntryMutationError::InvalidLimit
    );
    let (budget, _) = budget(1_024);
    assert_eq!(
        group
            .delete_entry_if_value("~presence:test", oversized_value(), 32, 32, budget)
            .await
            .unwrap_err(),
        groupnet_runtime::EntryMutationError::InvalidLimit
    );
    assert!(
        group
            .node_entry(&cluster.ids[0], "~presence:test")
            .is_none()
    );
}
