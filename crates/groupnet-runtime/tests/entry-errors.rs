//! Every typed entry error names its rule, displays it, and keeps its cause as
//! `source()`; each is raised before anything is enqueued.

use std::error::Error;
use std::num::NonZeroUsize;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use groupnet_runtime::{
    CommandRejected, EntryBudget, EntryInspectionError, EntryInspectionLimits,
    EntryInspectionLimitsError, EntryMutationError, EntryMutationLimits, EntryMutationLimitsError,
    EntryRemoval, EntryRevision, EntryWrite,
};
use groupnet_testkit::cluster::MemCluster;

const fn nz(n: usize) -> NonZeroUsize {
    NonZeroUsize::new(n).unwrap()
}

/// A budget that counts its drops, so a refusal is seen to release it.
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

#[test]
fn default_limits_are_nonzero_and_validate() {
    assert_eq!(EntryInspectionLimits::default().validate(), Ok(()));
    assert_eq!(EntryMutationLimits::default().validate(), Ok(()));
}

#[test]
fn inverted_inspection_limits_are_named_by_field() {
    let limits = EntryInspectionLimits {
        max_member_bytes: nz(64),
        max_response_bytes: nz(32),
        max_value_bytes: nz(16),
        ..EntryInspectionLimits::default()
    };
    let error = limits.validate().unwrap_err();
    assert_eq!(
        error,
        EntryInspectionLimitsError::MemberAboveResponse {
            max_member_bytes: 64,
            max_response_bytes: 32,
        }
    );
    assert_eq!(
        error.to_string(),
        "entry inspection max_member_bytes 64 exceeds max_response_bytes 32"
    );
    let value = EntryInspectionLimits {
        max_member_bytes: nz(16),
        max_value_bytes: nz(64),
        ..limits
    };
    assert_eq!(
        value.validate(),
        Err(EntryInspectionLimitsError::ValueAboveResponse {
            max_value_bytes: 64,
            max_response_bytes: 32,
        })
    );
}

#[test]
fn an_unrepresentable_mutation_charge_is_rejected() {
    let limits = EntryMutationLimits {
        max_key_bytes: nz(usize::MAX),
        max_value_bytes: nz(1),
    };
    assert_eq!(
        limits.validate(),
        Err(EntryMutationLimitsError::ChargeOverflow {
            max_key_bytes: usize::MAX,
            max_value_bytes: 1,
        })
    );
    let error = EntryWrite::new("k", b"v")
        .with_limits(limits)
        .validate()
        .unwrap_err();
    assert!(matches!(error, EntryMutationError::Limits(_)));
    assert_eq!(error.to_string(), "invalid entry mutation limits");
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<EntryMutationLimitsError>())
            .is_some(),
        "the limits error is the source"
    );
}

#[test]
fn requests_name_their_violated_bound() {
    let limits = EntryMutationLimits {
        max_key_bytes: nz(4),
        max_value_bytes: nz(4),
    };
    assert_eq!(
        EntryWrite::new("", b"v").with_limits(limits).validate(),
        Err(EntryMutationError::EmptyKey)
    );
    assert_eq!(
        EntryWrite::new("long-key", b"v")
            .with_limits(limits)
            .validate(),
        Err(EntryMutationError::KeyTooLong { bytes: 8, max: 4 })
    );
    assert_eq!(
        EntryRemoval::new("k", b"too-long")
            .with_limits(limits)
            .validate(),
        Err(EntryMutationError::ValueTooLong { bytes: 8, max: 4 })
    );
    assert_eq!(
        EntryMutationError::KeyTooLong { bytes: 8, max: 4 }.to_string(),
        "entry key owns 8 bytes, above 4"
    );
}

#[tokio::test]
async fn mutations_refuse_before_enqueueing_and_release_their_budget() {
    let cluster = MemCluster::builder(&["errors-a"]).group("g").spawn().await;
    let group = &cluster.groups[0];

    let (owner, dropped) = budget(1_024);
    let error = group
        .set_entry_if_revision(
            EntryWrite::new("k", b"v"),
            EntryRevision {
                key: Some(u64::MAX),
                member: 0,
            },
            owner,
        )
        .await
        .unwrap_err();
    assert_eq!(error, EntryMutationError::RevisionExhausted);
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let (owner, _) = budget(1);
    let error = group
        .delete_entry_if_value(EntryRemoval::new("k", b"v"), owner)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        EntryMutationError::BudgetTooSmall {
            required: Some(2),
            available: 1,
        }
    );
    assert_eq!(
        error.to_string(),
        "entry mutation charges 2 bytes, above the 1-byte budget"
    );
    assert!(group.node_entry(&cluster.ids[0], "k").is_none());
}

#[tokio::test]
async fn inspections_refuse_before_enqueueing() {
    let cluster = MemCluster::builder(&["errors-b"]).group("g").spawn().await;
    let group = &cluster.groups[0];
    let limits = EntryInspectionLimits {
        max_key_bytes: nz(8),
        max_response_bytes: nz(4_096),
        ..EntryInspectionLimits::default()
    };

    let (owner, _) = budget(8_192);
    assert_eq!(
        group
            .inspect_scoped_entry("", limits, owner)
            .await
            .unwrap_err(),
        EntryInspectionError::EmptyKey
    );
    let (owner, _) = budget(8_192);
    assert_eq!(
        group
            .inspect_scoped_entry("a-key-too-long", limits, owner)
            .await
            .unwrap_err(),
        EntryInspectionError::KeyTooLong { bytes: 14, max: 8 }
    );
    let (owner, _) = budget(8_192);
    assert_eq!(
        group
            .inspect_scoped_pair("same", "same", limits, owner)
            .await
            .unwrap_err(),
        EntryInspectionError::DuplicateKeys
    );
    let (owner, dropped) = budget(4_096);
    let error = group
        .inspect_scoped_pair("first", "second", limits, owner)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        EntryInspectionError::BudgetTooSmall {
            required: Some(4_096 + 5 + 6),
            available: 4_096,
        }
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let inverted = EntryInspectionLimits {
        max_value_bytes: nz(8_192),
        ..limits
    };
    let (owner, _) = budget(1 << 20);
    let error = group
        .inspect_scoped_entry("k", inverted, owner)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        EntryInspectionError::Limits(EntryInspectionLimitsError::ValueAboveResponse {
            max_value_bytes: 8_192,
            max_response_bytes: 4_096,
        })
    );
    assert_eq!(error.to_string(), "invalid entry inspection limits");
    assert!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<EntryInspectionLimitsError>())
            .is_some(),
        "the limits error is the source"
    );
}

/// The actor cannot drain its inbox while this current-thread test never
/// yields, so enqueueing past its capacity is refused as `InboxFull`, and a
/// confirmed mutation reports that typed cause as its source.
#[tokio::test(flavor = "current_thread")]
async fn a_full_actor_inbox_is_reported_as_inbox_full() {
    let cluster = MemCluster::builder(&["errors-c"]).group("g").spawn().await;
    let group = &cluster.groups[0];
    let mut refused = None;
    for n in 0..4_096_u32 {
        if let Err(error) = group.set_entry(format!("k{n}"), Vec::new(), None) {
            refused = Some(error);
            break;
        }
    }
    let refused = refused.expect("a bounded inbox refuses before 4096 commands");
    assert_eq!(refused, CommandRejected::InboxFull);
    assert_eq!(refused.to_string(), "group actor inbox full");
    assert_eq!(
        group.delete_entry("k0"),
        Err(CommandRejected::InboxFull),
        "nothing drained the inbox in between"
    );

    let (owner, dropped) = budget(1_024);
    let error = group
        .set_entry_confirmed(EntryWrite::new("confirmed", b"v"), owner)
        .await
        .unwrap_err();
    assert_eq!(
        error,
        EntryMutationError::Rejected(CommandRejected::InboxFull)
    );
    assert_eq!(
        error
            .source()
            .and_then(|source| source.downcast_ref::<CommandRejected>()),
        Some(&CommandRejected::InboxFull)
    );
    assert_eq!(dropped.load(Ordering::SeqCst), 1);

    let (owner, _) = budget(8_192);
    let error = group
        .inspect_scoped_entry("k0", EntryInspectionLimits::default(), owner)
        .await;
    assert!(
        matches!(error, Err(EntryInspectionError::BudgetTooSmall { .. })),
        "the budget check precedes the enqueue"
    );
    let (owner, _) = budget(2 << 20);
    assert_eq!(
        group
            .inspect_scoped_entry("k0", EntryInspectionLimits::default(), owner)
            .await
            .unwrap_err(),
        EntryInspectionError::Rejected(CommandRejected::InboxFull)
    );
}
