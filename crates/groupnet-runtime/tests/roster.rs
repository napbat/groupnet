//! The shared roster snapshot and the subscribe-before-snapshot observation model.

use std::{sync::Arc, time::Duration};

use groupnet_runtime::{Group, GroupEvent};
use groupnet_testkit::cluster::{MemCluster, eventually_within};
use tokio::sync::broadcast::{Receiver, error::RecvError};

const WAIT: Duration = Duration::from_secs(10);

/// Re-reads `group`'s roster after every edge (or `Lagged` resync) until it
/// has `size` members, starting with a read after the subscription.
async fn roster_of(
    group: &Group,
    events: &mut Receiver<GroupEvent>,
    size: usize,
) -> Arc<[groupnet_core::NodeId]> {
    tokio::time::timeout(WAIT, async {
        loop {
            let roster = group.roster();
            if roster.len() == size {
                return roster;
            }
            match events.recv().await {
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => panic!("group actor stopped"),
            }
        }
    })
    .await
    .expect("roster converges")
}

#[tokio::test]
async fn roster_shares_the_published_snapshot_and_follows_membership_edges() {
    let cluster = MemCluster::builder(&["roster-a", "roster-b", "roster-c"])
        .spawn()
        .await;
    let observer = &cluster.groups[0];
    // Subscribe first: any change after this point produces an edge that the
    // loop below observes, so the first read cannot miss it.
    let mut events = observer.events();
    let full = roster_of(observer, &mut events, 3).await;
    assert_eq!(full.as_ref(), observer.members().as_slice());
    // Unchanged membership hands out the same allocation, not a copy.
    eventually_within("consecutive reads share one snapshot", WAIT, || {
        Arc::ptr_eq(&observer.roster(), &observer.roster())
    })
    .await;

    cluster.groups[2].leave();
    let remaining = roster_of(observer, &mut events, 2).await;
    assert!(!remaining.contains(&cluster.ids[2]));
    // A held snapshot is immutable: the departure published a new one.
    assert_eq!(full.len(), 3);
    assert!(full.contains(&cluster.ids[2]));
}
