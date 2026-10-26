//! Integration test: **a closed node stops its group actors**, whatever
//! `Group` handles its application still holds (napbat/groupnet#9).
//!
//! A group actor used to stop only when its inbox closed, which needs every
//! `Group` handle to be dropped. An application that kept one after it closed
//! the node (a cache, a task that was not aborted, a reference cycle) kept a
//! live election engine. Alone on a muted network, a Quorum voter's engine
//! claims a new epoch each lease and writes each self-grant to its store —
//! the store that the node's next life loads.
//!
//! * [`Node::close`] returns only after every group actor of the node has
//!   stopped: a command on a held handle is refused as
//!   [`CommandRejected::GroupClosed`] at once, and the voter's store takes no
//!   write after the close returns.
//! * A router shutdown alone ([`Router::shutdown`](groupnet_runtime::Router::shutdown))
//!   stops the actors too, without a wait.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::{NodeId, RecoveredGrant, VoterRoster};
use groupnet_runtime::{CommandRejected, GrantStore, Group, GroupProfile, Node, VoterStorage};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport_mem::Network;

/// The poll budget of every wait here: an election cannot open before the
/// engine's boot guard, one lease.
const SETTLE: Duration = Duration::from_secs(8);
/// A brisk gossip cadence, so elections happen in wall-clock milliseconds.
const GOSSIP_MS: u64 = 15;
/// The lease, and under Quorum also the claim window and the boot guard.
const LEASE_MS: u64 = 600;

/// A [`GrantStore`] that counts the persists the driver asks of it.
#[derive(Debug, Default)]
struct CountingStore {
    log: Mutex<Vec<(u64, NodeId)>>,
}

impl CountingStore {
    fn writes(&self) -> usize {
        self.log.lock().expect("store mutex poisoned").len()
    }
}

impl GrantStore for CountingStore {
    fn persist(&self, epoch: u64, claimant: &NodeId) -> std::io::Result<()> {
        self.log
            .lock()
            .expect("store mutex poisoned")
            .push((epoch, claimant.clone()));
        Ok(())
    }
}

/// Brings up `ids` as one Quorum group over the roster `ids`, each node with
/// its own store, and waits until the group has a host.
async fn elected(
    net: &Network,
    group: &str,
    ids: &[&str],
) -> Vec<(Node, Group, Arc<CountingStore>)> {
    let mut spawned = Vec::new();
    for id in ids {
        let store = Arc::new(CountingStore::default());
        let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
        let profile = GroupProfile::Quorum {
            lease_ms: LEASE_MS,
            voters: VoterRoster::new(ids.iter().map(|v| NodeId::new(*v))),
            storage: Some(VoterStorage::new(RecoveredGrant::none(), store.clone())),
        };
        let opts = NodeOpts::new(group)
            .gossip_interval_ms(GOSSIP_MS)
            .group_profile(profile);
        let (_, node, joined) = spawn_mem_node(net, id, &seeds, &opts).await;
        spawned.push((node, joined, store));
    }
    let groups: Vec<&Group> = spawned.iter().map(|(_, group, _)| group).collect();
    converged_within(&groups, SETTLE).await;
    eventually_within("the group to elect a host", SETTLE, || {
        groups.iter().all(|group| group.leadership().host.is_some())
    })
    .await;
    spawned
}

/// [`Node::close`] stops the node's group actors before it returns, while the
/// application still holds the node and its `Group` handle. Before the fix the
/// actor ran on: the command below was accepted, and the engine went on
/// claiming alone and writing its self-grants. The actor is the only writer of
/// the store, so a refused command after the close proves that no grant write
/// can follow it.
#[tokio::test]
async fn a_closed_node_stops_its_group_actors_while_a_handle_is_held() {
    let net = Network::new();
    let nodes = elected(&net, "close-held", &["ch-a", "ch-b"]).await;
    assert!(
        nodes.iter().any(|(_, _, store)| store.writes() > 0),
        "the election wrote no grant, so the stores prove nothing"
    );

    for (node, group, _) in &nodes {
        node.close().await;
        assert_eq!(
            group.set_entry("after", b"close".to_vec(), None),
            Err(CommandRejected::GroupClosed),
            "the group actor of {:?} runs on after its node closed",
            node.id()
        );
    }
}

/// A router shutdown alone — the fast kill lever, which does not wait for the
/// network to drain — stops the group actors too.
#[tokio::test]
async fn a_router_shutdown_stops_the_group_actors() {
    let net = Network::new();
    let nodes = elected(&net, "close-router", &["cr-a", "cr-b"]).await;

    for (node, group, _) in &nodes {
        node.router().shutdown();
        eventually_within("the group actor to stop", SETTLE, || {
            group.set_entry("after", b"shutdown".to_vec(), None)
                == Err(CommandRejected::GroupClosed)
        })
        .await;
    }
    for (node, _, _) in &nodes {
        node.close().await;
    }
}
