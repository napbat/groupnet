//! Integration test: **`Node::close` with a grant write in flight**
//! (napbat/groupnet#11).
//!
//! `Node::close` waits until every group actor has stopped. An actor inside a
//! grant write that never returns used to hold the close for ever. The wait
//! for a running write is now bounded by the voter storage's close wait, and
//! the fencing rule holds either way:
//!
//! * a write that ends within the close wait ends before the close returns,
//!   and the report is complete — no grant can become durable afterwards;
//! * a write still running when the close wait runs out is named in the
//!   report, because the store can still make it durable after the close
//!   returned.
//!
//! The store's write blocks until the test releases it, so what is in flight
//! when is decided by explicit signals, not by timing. Every wait is a bounded
//! poll on a predicate (`eventually_within`) or a channel.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::{GroupId, NodeId, RecoveredGrant, VoterRoster};
use groupnet_runtime::{
    CommandRejected, GrantStore, Group, GroupProfile, Node, UnfinishedGrant, VoterStorage,
};
use groupnet_testkit::cluster::{NodeOpts, eventually_within, spawn_mem_node};
use groupnet_transport_mem::Network;

/// The poll budget of every wait: the first claim waits out the boot guard.
const SETTLE: Duration = Duration::from_secs(8);
/// A brisk gossip cadence, so the claim opens in wall-clock milliseconds.
const GOSSIP_MS: u64 = 15;
/// The lease, and under Quorum also the claim window and the boot guard.
const LEASE_MS: u64 = 600;

/// A [`GrantStore`] whose every write blocks until the test releases it.
struct BlockingStore {
    /// How many writes have started.
    started: AtomicUsize,
    /// One message releases one write.
    release: Mutex<mpsc::Receiver<()>>,
    /// The writes that ended, in order.
    written: Mutex<Vec<(u64, NodeId)>>,
}

impl BlockingStore {
    fn new() -> (Arc<Self>, mpsc::Sender<()>) {
        let (release, gate) = mpsc::channel();
        let store = Arc::new(Self {
            started: AtomicUsize::new(0),
            release: Mutex::new(gate),
            written: Mutex::new(Vec::new()),
        });
        (store, release)
    }

    fn started(&self) -> usize {
        self.started.load(Ordering::SeqCst)
    }

    fn written(&self) -> Vec<(u64, NodeId)> {
        self.written.lock().expect("written mutex poisoned").clone()
    }
}

impl GrantStore for BlockingStore {
    fn persist(&self, epoch: u64, claimant: &NodeId) -> std::io::Result<()> {
        self.started.fetch_add(1, Ordering::SeqCst);
        self.release
            .lock()
            .expect("release mutex poisoned")
            .recv()
            .map_err(|_| std::io::Error::other("the test dropped the release"))?;
        self.written
            .lock()
            .expect("written mutex poisoned")
            .push((epoch, claimant.clone()));
        Ok(())
    }
}

/// A voter roster of one, `id`, whose store is `store` and whose close wait
/// is `close_wait`. Its first claim self-grants epoch 1 after the boot guard,
/// and that write blocks in the store. Returns once the write has started.
async fn voter_in_a_write(
    net: &Network,
    group: &str,
    id: &str,
    store: &Arc<BlockingStore>,
    close_wait: Duration,
) -> (Node, Group) {
    let profile = GroupProfile::Quorum {
        lease_ms: LEASE_MS,
        voters: VoterRoster::new([NodeId::new(id)]),
        storage: Some(
            VoterStorage::new(RecoveredGrant::none(), store.clone()).with_close_wait(close_wait),
        ),
    };
    let opts = NodeOpts::new(group)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(profile);
    let (_, node, joined) = spawn_mem_node(net, id, &[], &opts).await;
    eventually_within("the self-grant write to start", SETTLE, || {
        store.started() == 1
    })
    .await;
    (node, joined)
}

/// A write that outlives the close wait is named in the report, and the
/// close returns although the store never answered. Before the fix the close
/// waited for the write for ever. After the release the write does land,
/// which is exactly why the report must name it.
#[tokio::test]
async fn a_close_names_a_grant_write_that_outlives_its_close_wait() {
    let net = Network::new();
    let (store, release) = BlockingStore::new();
    let (node, group) = voter_in_a_write(
        &net,
        "close-hung",
        "ch-a",
        &store,
        Duration::from_millis(50),
    )
    .await;

    let report = tokio::time::timeout(SETTLE, node.close())
        .await
        .expect("the close is bounded by the close wait, not by the store");
    assert_eq!(
        report.unfinished,
        vec![UnfinishedGrant {
            group: GroupId::new("close-hung"),
            epoch: 1,
            claimant: NodeId::new("ch-a"),
        }]
    );
    assert!(!report.is_complete());
    assert_eq!(
        group.set_entry("after", b"close".to_vec(), None),
        Err(CommandRejected::GroupClosed),
        "the actor stopped although its write did not end"
    );
    assert!(store.written().is_empty(), "the write was still blocked");

    release.send(()).expect("the store is waiting");
    eventually_within("the abandoned write to land", SETTLE, || {
        !store.written().is_empty()
    })
    .await;
    assert_eq!(store.written(), vec![(1, NodeId::new("ch-a"))]);
    assert_eq!(store.started(), 1, "no write started after the close");
}

/// A write that ends within the close wait ends before the close returns,
/// and the report is complete: no grant can become durable afterwards.
#[tokio::test]
async fn a_close_waits_for_a_grant_write_that_ends_within_its_close_wait() {
    let net = Network::new();
    let (store, release) = BlockingStore::new();
    let (node, _group) = voter_in_a_write(
        &net,
        "close-quick",
        "cq-a",
        &store,
        Duration::from_secs(3600),
    )
    .await;

    let closing = tokio::spawn(async move { node.close().await });
    release.send(()).expect("the store is waiting");
    let report = tokio::time::timeout(SETTLE, closing)
        .await
        .expect("the close returns once the write ended")
        .expect("the close task does not panic");
    assert!(report.is_complete(), "{report:?}");
    assert_eq!(
        store.written(),
        vec![(1, NodeId::new("cq-a"))],
        "the write ended before the close returned"
    );
    assert_eq!(store.started(), 1, "no write started after the close");
}
