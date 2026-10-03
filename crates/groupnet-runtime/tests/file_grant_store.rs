//! Integration test: **[`FileGrantStore`] as a Quorum voter's ledger** over the
//! async runtime.
//!
//! `quorum.rs` proves the driver's write-ahead contract against an in-memory
//! fixture. What only a real file can prove is the restart half: a voter
//! killed after granting, rebuilt from nothing but its path, reads the grant
//! back with [`FileGrantStore::load`] and re-grants the sitting claimant at
//! once — well inside the `lease_ms` boot blackout an amnesiac voter would
//! have imposed.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::{Activation, HostedConfig, NodeId, RecoveredGrant, VoterRoster};
use groupnet_runtime::{FileGrantStore, Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{NodeOpts, converged_within, eventually_within, spawn_mem_node};
use groupnet_transport_mem::{MemTransport, Network};

/// Poll budget: an election cannot open before the boot guard, and the
/// restart additionally waits out the starved host's lease.
const SETTLE: Duration = Duration::from_secs(8);

/// A brisk gossip cadence, so grant rounds happen in wall-clock milliseconds.
const GOSSIP_MS: u64 = 15;

/// The lease, a claim's window, and the blackout an amnesiac voter sits out.
const LEASE_MS: u64 = 600;

/// A fresh, empty directory for this test's ledgers, removed on drop.
struct Scratch(PathBuf);

impl Scratch {
    fn new(tag: &str) -> Self {
        let dir = std::env::temp_dir().join(format!("groupnet-{tag}-{}", std::process::id()));
        let _ = fs::remove_dir_all(&dir);
        fs::create_dir_all(&dir).expect("create scratch dir");
        Self(dir)
    }

    /// The ledger path of voter `id`.
    fn ledger(&self, id: &str) -> PathBuf {
        self.0.join(format!("{id}.grant"))
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

/// Boots one voter of `voters` from the ledger at `path`: open, load, then
/// join — the order the store's docs require.
fn boot_voter(
    net: &Network,
    path: &Path,
    id: &str,
    seeds: &[&str],
    voters: &[&str],
    group: &str,
) -> (RecoveredGrant, Node<MemTransport>, Group) {
    let store = FileGrantStore::open(path).expect("open ledger");
    let recovered = store.load().expect("an intact ledger loads");
    let profile = GroupProfile::hosted(HostedConfig {
        activation: Activation::Quorum {
            voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
        },
        lease_ms: LEASE_MS,
    })
    .with_voter_storage(recovered.clone(), Arc::new(store));
    let opts = NodeOpts::new(group)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(profile);
    let (_, node, joined) = spawn_mem_node(net, id, seeds, &opts);
    (recovered, node, joined)
}

/// The one leadership every group agrees on, with exactly one host.
fn agreed(groups: &[&Group]) -> Option<Leadership> {
    let first = groups.first()?.leadership();
    first.host.as_ref()?;
    let all: Vec<Leadership> = groups.iter().map(|g| g.leadership()).collect();
    if all
        .iter()
        .any(|l| l.epoch != first.epoch || l.host != first.host)
    {
        return None;
    }
    (all.iter().filter(|l| l.role == Role::Host).count() == 1).then_some(first)
}

/// A voter restarted from its file re-grants the incumbent without a blackout,
/// and the file it reads back is the grant it actually made.
#[tokio::test]
async fn a_voter_restarted_from_its_file_recovers_its_grant() {
    const GROUP: &str = "file-ledger-restart";
    const IDS: [&str; 3] = ["fl-a", "fl-b", "fl-c"];

    let scratch = Scratch::new("file-ledger-restart");
    let net = Network::new();
    let mut nodes = Vec::new();
    let mut groups = Vec::new();
    for id in IDS {
        let seeds: Vec<&str> = IDS.iter().copied().filter(|other| *other != id).collect();
        let (recovered, node, group) =
            boot_voter(&net, &scratch.ledger(id), id, &seeds, &IDS, GROUP);
        assert_eq!(
            recovered,
            RecoveredGrant::none(),
            "a fresh disk never granted"
        );
        nodes.push(Some(node));
        groups.push(Some(group));
    }

    let first = {
        let refs: Vec<&Group> = groups.iter().flatten().collect();
        converged_within(&refs, SETTLE).await;
        eventually_within("the roster to close an epoch", SETTLE, || {
            agreed(&refs).is_some()
        })
        .await;
        agreed(&refs).expect("agreed just above")
    };
    let host = first.host.clone().expect("agreement names a host");
    let host_index = IDS
        .iter()
        .position(|id| NodeId::new(*id) == host)
        .expect("the host is one of ours");
    let others: Vec<usize> = (0..IDS.len()).filter(|i| *i != host_index).collect();

    // A majority closed the epoch, so a non-host's file holds the grant —
    // find one whose disk says so (the other may have granted nothing).
    let restart = *others
        .iter()
        .find(|i| {
            FileGrantStore::open(scratch.ledger(IDS[**i]))
                .and_then(|s| s.load())
                .is_ok_and(|g| g == RecoveredGrant::granted(first.epoch, host.clone()))
        })
        .expect("some non-host voter wrote the winning grant to its file");
    let gone = others
        .into_iter()
        .find(|i| *i != restart)
        .expect("two non-hosts");

    // Kill both non-hosts: drop the handles and evict their endpoints so the
    // receive loops stop.
    for index in [restart, gone] {
        groups[index] = None;
        nodes[index] = None;
    }
    let _evicted_restart = net.endpoint(NodeId::new(IDS[restart]));
    let _evicted_gone = net.endpoint(NodeId::new(IDS[gone]));
    let host_group = groups[host_index].as_ref().expect("the host survives");

    eventually_within("the starved host to lose its lease", SETTLE, || {
        host_group.leadership().host.is_none()
    })
    .await;

    // Restart the voter from nothing but its path.
    let restart_at = Instant::now();
    let (recovered, _restarted_node, restarted_group) = boot_voter(
        &net,
        &scratch.ledger(IDS[restart]),
        IDS[restart],
        &[host.as_str()],
        &IDS,
        GROUP,
    );
    assert_eq!(
        recovered,
        RecoveredGrant::granted(first.epoch, host.clone()),
        "the restarted voter recovers the grant it made before the kill"
    );

    eventually_within("the incumbent to regain the group", SETTLE, || {
        let now = host_group.leadership();
        now.role == Role::Host && now.epoch > first.epoch
    })
    .await;
    let regained = restart_at.elapsed();
    assert!(
        regained < Duration::from_millis(LEASE_MS),
        "re-election took {regained:?}, at or past the {LEASE_MS}ms blackout a \
         voter without a recovered ledger would have imposed"
    );

    let after = host_group.leadership();
    let on_disk = FileGrantStore::open(scratch.ledger(IDS[restart]))
        .and_then(|s| s.load())
        .expect("the ledger stays intact");
    assert_eq!(
        on_disk,
        RecoveredGrant::granted(after.epoch, host.clone()),
        "the re-grant is write-ahead: the file holds the new pair"
    );

    let live: Vec<&Group> = vec![host_group, &restarted_group];
    eventually_within("both survivors to agree on the new pair", SETTLE, || {
        agreed(&live).is_some_and(|l| l.host == Some(host.clone()) && l.epoch > first.epoch)
    })
    .await;
}
