//! Integration test: **leadership surfacing** — M1's runtime face of the
//! Hosted-mode election, over the real async runtime and a real `Transport`.
//!
//! The engine's election is proved deterministically in `groupnet-sim`; what
//! only this layer can prove is that the election *reaches a consumer*, and
//! that opting one group in leaves every other group exactly as it was:
//!
//! * three hosted nodes converge on one epoch-fenced host, every observer
//!   agrees on the `(epoch, host)` pair, exactly one of them reports
//!   [`Role::Host`], and that host is the [`coordinator`](Group::coordinator)
//!   the same nodes derive — plus the matching
//!   [`GroupEvent::LeadershipChanged`] lands on every node's event stream;
//! * killing the host promotes a successor at a **strictly higher** epoch, and
//!   the dead node's id stops appearing in anyone's belief;
//! * an `Eventual` group on the *same nodes*, joined the plain way, never
//!   elects anything — `Leadership { epoch: 0, host: None, role: Follower }`
//!   for its whole life, with an empty event log to match;
//! * a repeat `join_group_with` returns a working handle to the existing group
//!   and the **first** join's profile governs, whatever the second one asks
//!   for — including when the two joins *race*, where the whole point is that
//!   one engine exists and both callers are handed it;
//! * the reserved routing group is pinned `Eventual` even when the node's own
//!   config says `Hosted`, so fabric plumbing never grows an authority.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::{Activation, Config, GroupMode, HostedConfig, NodeId, wire};
use groupnet_runtime::{Group, GroupEvent, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{MemCluster, converged_within, eventually_within};
use groupnet_transport::{Inbound, Transport};
use groupnet_transport_mem::{MemTransport, Network};
use tokio::sync::broadcast::error::RecvError;

/// The poll budget for every assertion here.
///
/// Deliberately looser than the harness default: the failover path has to pay
/// a whole detection window (~900ms at the default detector timings, which the
/// fixtures keep) before the survivors' ranking even changes, and then a
/// settle window before a successor may activate. A genuine regression still
/// reports in seconds.
const SETTLE: Duration = Duration::from_secs(8);

/// A brisk gossip cadence, so convergence and detection happen in wall-clock
/// milliseconds. It also sets the driver's tick period: the driver ticks at
/// half the tightest configured deadline, i.e. `15 / 2 = 7ms` here, which both
/// election durations below are more than an order of magnitude above — the
/// sizing rule [`HostedConfig`] states.
const GOSSIP_MS: u64 = 15;

/// How long a claim must stand before its claimant activates — ~20 driver
/// ticks and ~10 gossip rounds, so peers get a real chance to answer.
const CLAIM_SETTLE_MS: u64 = 150;

/// A host's authority after its last renewal. Must stay under
/// `detection_window_ms(3) + CLAIM_SETTLE_MS` = `900 + 150` at the default
/// detector timings, so a deposed host has stepped down before anyone else can
/// step up.
const LEASE_MS: u64 = 600;

/// The [`HostedConfig`] every hosted fixture here runs.
const fn hosted_config() -> HostedConfig {
    HostedConfig {
        activation: Activation::Settle {
            claim_settle_ms: CLAIM_SETTLE_MS,
        },
        lease_ms: LEASE_MS,
    }
}

fn hosted() -> GroupProfile {
    GroupProfile::hosted(hosted_config())
}

/// Every [`GroupEvent::LeadershipChanged`] one group handle has published, in
/// order — see [`watch_leadership`].
type LeadershipLog = Arc<Mutex<Vec<(u64, Option<NodeId>)>>>;

/// Consumes a group's change-event stream in the background, keeping every
/// leadership edge it carries.
///
/// [`Group::events`] hands back a `tokio::sync::broadcast::Receiver`, which is
/// a bounded *edge trigger*, not a log: a subscriber that falls behind gets
/// `Lagged` and is expected to resync from the (always current) watch
/// snapshots. A background task draining it continuously is the shape a real
/// consumer uses, and it is the only way to observe an edge that has already
/// been superseded by the time the test looks.
fn watch_leadership(group: &Group) -> LeadershipLog {
    let log: LeadershipLog = Arc::new(Mutex::new(Vec::new()));
    let sink = log.clone();
    let mut events = group.events();
    tokio::spawn(async move {
        loop {
            match events.recv().await {
                Ok(GroupEvent::LeadershipChanged { epoch, host }) => sink
                    .lock()
                    .expect("event log mutex poisoned")
                    .push((epoch, host)),
                // Some other edge, or a lagged subscriber that has missed
                // edges outright — the snapshot reads stay current either way,
                // which is exactly what the stream promises.
                Ok(_) | Err(RecvError::Lagged(_)) => {}
                Err(RecvError::Closed) => return,
            }
        }
    });
    log
}

fn logged(log: &LeadershipLog) -> Vec<(u64, Option<NodeId>)> {
    log.lock().expect("event log mutex poisoned").clone()
}

/// The leadership the whole cluster agrees on, or `None` while it is still
/// settling.
///
/// Agreement means all of: every observer reports the same `(epoch, host)`,
/// that host is actually named, **exactly one** node claims [`Role::Host`],
/// and every node's derived coordinator is that same host — the steady state
/// the election is supposed to reach, asserted as one indivisible predicate so
/// a poll can never catch half of it.
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
    if all.iter().filter(|l| l.role == Role::Host).count() != 1 {
        return None;
    }
    if groups.iter().any(|g| g.coordinator() != first.host) {
        return None;
    }
    Some(first)
}

/// Three hosted nodes elect one host, everyone agrees on it, it is the node
/// the rendezvous ranking already picked as coordinator, and the change
/// arrives on every node's event stream as well as its snapshot read.
#[tokio::test]
async fn three_hosted_nodes_converge_on_one_epoch_fenced_host() {
    let cluster = MemCluster::builder(&["lead-a", "lead-b", "lead-c"])
        .group("hosted-shard")
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(hosted())
        .spawn();

    // Subscribed before anything can have been elected: the engine's boot
    // guard withholds every claim for one settle window, which is orders of
    // magnitude longer than getting here takes.
    let logs: Vec<LeadershipLog> = cluster.groups.iter().map(watch_leadership).collect();
    for group in &cluster.groups {
        assert_eq!(
            group.leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "a hosted group starts believing nothing, like every other observer"
        );
    }

    let groups: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&groups, SETTLE).await;
    eventually_within("the cluster to agree on one host", SETTLE, || {
        agreed(&groups).is_some()
    })
    .await;

    let lead = agreed(&groups).expect("agreed just above, and nothing perturbs it");
    let host = lead.host.clone().expect("agreement requires a named host");
    assert!(
        lead.epoch >= 1,
        "an activation takes the epoch it claimed, which is at least 1: {lead:?}"
    );
    assert!(
        cluster.ids.contains(&host),
        "the host must be one of the cluster's own nodes, not {host}"
    );

    // Per-node roles, spelled out rather than inferred from the count above:
    // the host reports Host, everyone else Follower, and nobody reports the
    // engine-internal Claimant.
    for (id, group) in cluster.ids.iter().zip(&cluster.groups) {
        let seen = group.leadership();
        let want = if *id == host {
            Role::Host
        } else {
            Role::Follower
        };
        assert_eq!(seen.role, want, "{id} reported the wrong role: {seen:?}");
        assert_eq!(
            seen.host.as_ref(),
            Some(&host),
            "{id} disagrees about the host"
        );
        assert_eq!(seen.epoch, lead.epoch, "{id} disagrees about the epoch");
        // The host is the rendezvous top — the derived coordinator and the
        // elected host are the same node, by construction of the claim guard.
        assert_eq!(
            group.coordinator(),
            Some(host.clone()),
            "{id} derives a coordinator other than the elected host"
        );
    }

    // ...and the same edge reached the event stream, on every node.
    eventually_within("every node to see the leadership edge", SETTLE, || {
        logs.iter()
            .all(|log| logged(log).contains(&(lead.epoch, Some(host.clone()))))
    })
    .await;
}

/// Kill the host and the group heals: the survivors elect a successor at a
/// strictly higher epoch, and the dead node stops appearing in anyone's
/// belief. The epoch is the fence — a successor at the *same* epoch would be a
/// second serializer for the same writes.
#[tokio::test]
async fn killing_the_host_promotes_a_successor_at_a_higher_epoch() {
    let mut cluster = MemCluster::builder(&["fail-a", "fail-b", "fail-c"])
        .group("hosted-failover")
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(hosted())
        .spawn();

    let first = {
        let groups: Vec<&Group> = cluster.groups.iter().collect();
        converged_within(&groups, SETTLE).await;
        eventually_within("the cluster to agree on one host", SETTLE, || {
            agreed(&groups).is_some()
        })
        .await;
        agreed(&groups).expect("agreed just above")
    };
    let dead_id = first.host.clone().expect("agreement requires a named host");
    let index = cluster
        .ids
        .iter()
        .position(|id| *id == dead_id)
        .expect("the host is one of ours");

    // --- Kill the host. ---
    //
    // Dropping the `Node` and `Group` handles is *not* on its own enough to
    // stop a node: the node's receive loop owns an `Arc` of the same inner
    // state that owns the transport, so the group actors keep ticking (and
    // gossiping) even with no handle left. Evicting the endpoint from the
    // `Network` — registering the id again, which replaces the sender — closes
    // the old inbox, ends that receive loop, breaks the cycle and tears the
    // actors down. Together they are a faithful process death: the node stops
    // sending, and nothing is delivered to it again.
    let dead_group = cluster.groups.remove(index);
    let dead_node = cluster.nodes.remove(index);
    cluster.ids.remove(index);
    drop(dead_group);
    drop(dead_node);
    let _evicted = cluster.net.endpoint(dead_id.clone());

    let survivors: Vec<&Group> = cluster.groups.iter().collect();
    eventually_within("the survivors to elect a successor", SETTLE, || {
        agreed(&survivors).is_some_and(|l| l.epoch > first.epoch)
    })
    .await;

    let next = agreed(&survivors).expect("agreed just above");
    let successor = next.host.clone().expect("agreement requires a named host");
    assert!(
        next.epoch > first.epoch,
        "the successor must fence the dead host's epoch: {} is not above {}",
        next.epoch,
        first.epoch
    );
    assert_ne!(
        successor, dead_id,
        "the killed node must not be re-elected host"
    );
    assert!(
        cluster.ids.contains(&successor),
        "the successor must be a survivor, not {successor}"
    );
    for (id, group) in cluster.ids.iter().zip(&cluster.groups) {
        let seen = group.leadership();
        assert_ne!(
            seen.host.as_ref(),
            Some(&dead_id),
            "{id} still believes the dead node hosts the group"
        );
        let want = if *id == successor {
            Role::Host
        } else {
            Role::Follower
        };
        assert_eq!(seen.role, want, "{id} reported the wrong role: {seen:?}");
    }
}

/// Per-group isolation: an `Eventual` group joined the plain way, on the very
/// same nodes that are hosting another group, never elects anything. Not a
/// host, not an epoch, not an event — for its whole life.
#[tokio::test]
async fn an_eventual_group_on_hosting_nodes_never_elects() {
    const QUIET: &str = "eventual-sibling";

    let cluster = MemCluster::builder(&["mixed-a", "mixed-b", "mixed-c"])
        .group("hosted-sibling")
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(hosted())
        .spawn();

    // The same nodes, a second group, joined with no profile at all.
    let quiet: Vec<Group> = cluster.nodes.iter().map(|n| n.join_group(QUIET)).collect();
    let quiet_logs: Vec<LeadershipLog> = quiet.iter().map(watch_leadership).collect();

    let hosted_groups: Vec<&Group> = cluster.groups.iter().collect();
    let quiet_groups: Vec<&Group> = quiet.iter().collect();
    converged_within(&hosted_groups, SETTLE).await;
    converged_within(&quiet_groups, SETTLE).await;
    eventually_within("the hosted group to elect", SETTLE, || {
        agreed(&hosted_groups).is_some()
    })
    .await;

    // The hosted sibling has a host; the eventual one has not moved a
    // millimetre — and is demonstrably live, not merely unconverged: it has a
    // full membership and a derived coordinator of its own.
    for (id, group) in cluster.ids.iter().zip(&quiet) {
        assert_eq!(
            group.leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "{id}'s eventual group must never leave the initial belief"
        );
        assert_eq!(group.members().len(), 3, "{id}'s eventual group is live");
        assert!(
            group.coordinator().is_some(),
            "{id}'s eventual group still derives a coordinator — that is the \
             leaderless surface, and it is unaffected"
        );
        assert_eq!(
            group.config().mode,
            GroupMode::Eventual,
            "{id} joined without a profile, so the group is Eventual"
        );
    }
    for (id, log) in cluster.ids.iter().zip(&quiet_logs) {
        assert!(
            logged(log).is_empty(),
            "{id}'s eventual group emitted a leadership event: {:?}",
            logged(log)
        );
    }
}

/// A repeat join returns a working handle to the group already joined, and the
/// **first** join's profile governs — the second call's profile is ignored,
/// exactly as `join_group_with` documents.
#[tokio::test]
async fn a_repeat_join_keeps_the_first_profile() {
    const GROUP: &str = "hosted-rejoin";

    let cluster = MemCluster::builder(&["again-a", "again-b", "again-c"])
        .group(GROUP)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(hosted())
        .spawn();

    let groups: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&groups, SETTLE).await;
    eventually_within("the cluster to agree on one host", SETTLE, || {
        agreed(&groups).is_some()
    })
    .await;
    let lead = agreed(&groups).expect("agreed just above");

    // Join again, asking for the opposite posture — and, on one node, through
    // the profile-less `join_group` as well.
    let rejoined: Vec<Group> = cluster
        .nodes
        .iter()
        .map(|node| node.join_group_with(GROUP, GroupProfile::eventual()))
        .collect();
    let plain = cluster.nodes[0].join_group(GROUP);

    for (id, group) in cluster.ids.iter().zip(&rejoined) {
        assert_eq!(group.id().as_str(), GROUP);
        assert_eq!(
            group.config().mode,
            GroupMode::Hosted(hosted_config()),
            "{id}: the first join's profile governs, not the repeat's"
        );
        assert_eq!(
            group.leadership(),
            cluster.groups[cluster
                .ids
                .iter()
                .position(|other| other == id)
                .expect("index-aligned")]
            .leadership(),
            "{id}: the repeat join is a handle to the same actor"
        );
    }
    assert_eq!(
        plain.config().mode,
        GroupMode::Hosted(hosted_config()),
        "a profile-less repeat join is the same dedupe path, and inherits the same group"
    );

    // Not a stale snapshot: the handle the repeat join returned is live in both
    // directions — it reads the elected leadership, and a write through it
    // reaches the other nodes.
    let rejoined_refs: Vec<&Group> = rejoined.iter().collect();
    assert_eq!(
        agreed(&rejoined_refs),
        Some(lead),
        "the repeat-join handles see the very leadership the first join elected"
    );
    rejoined[0]
        .set_entry("through-the-repeat-handle", b"ok".to_vec(), None)
        .expect("the repeat-join handle drives the live actor");
    eventually_within("the write to reach the other nodes", SETTLE, || {
        cluster.groups[1..].iter().all(|g| {
            g.node_entry(&cluster.ids[0], "through-the-repeat-handle")
                .is_some()
        })
    })
    .await;
}

/// Races two joins of `group` on `node` — one asking for Hosted, one for
/// Eventual — from two tasks released together, and hands back both handles.
async fn race_join(node: &Node<MemTransport>, group: &str) -> (Group, Group) {
    let gate = Arc::new(tokio::sync::Barrier::new(2));
    let racers: Vec<_> = [hosted(), GroupProfile::eventual()]
        .into_iter()
        .map(|profile| {
            let (node, group, gate) = (node.clone(), group.to_owned(), gate.clone());
            tokio::spawn(async move {
                gate.wait().await;
                node.join_group_with(group, profile)
            })
        })
        .collect();
    let mut joined = Vec::with_capacity(2);
    for racer in racers {
        joined.push(racer.await.expect("a join never panics"));
    }
    let second = joined.pop().expect("two racers");
    let first = joined.pop().expect("two racers");
    (first, second)
}

/// The first-join-governs contract under **concurrency**: two threads joining
/// one group at the same instant get one group, not two engines under one id.
///
/// The get-or-spawn holds the node's group table across the whole decision, so
/// the loser of the race finds the winner's handle instead of spawning a second
/// actor. Without that, both racers would spawn — and since the two ask for
/// *different* profiles, the node would be left running two engines for one
/// group id under two different modes, the second insert winning the table and
/// the first caller holding a live handle to the orphan (still ticking, still
/// gossiping, and in a Hosted group still electing).
///
/// A race is a race, so no schedule is asserted: the loop just gives the window
/// a hundred chances to open on a two-worker runtime. What is asserted is the
/// invariant that must hold whichever way it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_joins_of_one_group_never_spawn_two_engines() {
    const ROUNDS: usize = 100;

    let net = Network::new();
    let id = NodeId::new("race-a");
    let node = Node::builder(id.clone(), net.endpoint(id.clone())).spawn();

    for round in 0..ROUNDS {
        let group = format!("raced-{round}");
        let (first, second) = race_join(&node, &group).await;
        // The table, read after the dust settles. Two spawned actors would put
        // one of them here and orphan the other, so at most one racer could
        // still agree with it — a handle's mode is the mode of the engine it
        // actually drives.
        let settled = node.join_group(group.as_str());
        for (which, handle) in [("first", &first), ("second", &second)] {
            assert_eq!(
                handle.config().mode,
                settled.config().mode,
                "round {round}: the {which} racer holds a group the node no \
                 longer knows about — two engines were spawned for {group}"
            );
        }
    }

    // ...and one engine proved by behaviour rather than by comparing configs:
    // a write through one racer's handle must be readable through the other's.
    // This node has no peers, so nothing could carry it from one engine to
    // another — if the handles named different actors, the read never lands.
    let (first, second) = race_join(&node, "raced-liveness").await;
    first
        .set_entry("through-the-race", b"ok".to_vec(), None)
        .expect("the handle drives a live actor");
    eventually_within("both racers' handles to drive one actor", SETTLE, || {
        second.node_entry(&id, "through-the-race").is_some()
    })
    .await;
}

/// The reserved routing group is pinned `Eventual` in `spawn_group`, so it
/// never elects — *even when the node's own config asks every group to*.
///
/// It is fabric plumbing carrying the cluster's routing table on every node;
/// putting an epoch-fenced authority in front of the table every other group
/// publishes into would buy nothing and cost a whole failure mode. Proved on
/// the wire rather than through a read, because the routing group's handle is
/// deliberately internal: the only groups that ever emit an election frame are
/// the ones the application asked for.
#[tokio::test]
async fn the_reserved_routing_group_never_elects() {
    const PINNED: &str = "node-wide-hosted";

    let net = Network::new();
    let config = Config {
        gossip_interval_ms: GOSSIP_MS,
        anti_entropy_interval_ms: GOSSIP_MS,
        // Node-*wide* Hosted: the strongest thing a caller can ask for, and
        // the one that would drag the routing group in without the pin.
        mode: GroupMode::Hosted(hosted_config()),
        ..Config::default()
    };
    let ids = [NodeId::new("pin-a"), NodeId::new("pin-b")];
    let mut nodes = Vec::new();
    let mut sniffers = Vec::new();
    for id in &ids {
        let electing: ElectingGroups = Arc::new(Mutex::new(BTreeSet::new()));
        let transport = Sniffer {
            inner: net.endpoint(id.clone()),
            electing: electing.clone(),
        };
        let mut builder = Node::builder(id.clone(), transport).config(config.clone());
        for seed in ids.iter().filter(|other| *other != id) {
            builder = builder.seed(seed.clone());
        }
        nodes.push(builder.spawn());
        sniffers.push(electing);
    }

    // A profile-less join: the node config's mode governs, so this group *is*
    // Hosted — which is what makes the assertion below non-vacuous.
    let groups: Vec<Group> = nodes.iter().map(|node| node.join_group(PINNED)).collect();
    let refs: Vec<&Group> = groups.iter().collect();
    converged_within(&refs, SETTLE).await;
    eventually_within("the plainly-joined group to elect a host", SETTLE, || {
        agreed(&refs).is_some()
    })
    .await;
    for group in &groups {
        assert_eq!(group.config().mode, GroupMode::Hosted(hosted_config()));
    }

    let seen: BTreeSet<String> = sniffers
        .iter()
        .flat_map(|s| s.lock().expect("sniffer mutex poisoned").clone())
        .collect();
    assert_eq!(
        seen,
        BTreeSet::from([PINNED.to_owned()]),
        "only the application group may put election frames on the wire"
    );
}

/// The group ids observed putting an election frame on the wire.
type ElectingGroups = Arc<Mutex<BTreeSet<String>>>;

/// A [`MemTransport`] that records which groups a node sends *election* frames
/// for. The routing group is internal, so the wire is the only place its mode
/// is observable from a test.
#[derive(Debug)]
struct Sniffer {
    inner: MemTransport,
    electing: ElectingGroups,
}

impl Transport for Sniffer {
    type Error = <MemTransport as Transport>::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error> {
        if let Some(frame) = wire::decode(msg) {
            if matches!(
                frame.kind,
                wire::Kind::LeadClaim | wire::Kind::LeadGrant | wire::Kind::LeadState
            ) {
                self.electing
                    .lock()
                    .expect("sniffer mutex poisoned")
                    .insert(frame.group.to_string());
            }
        }
        self.inner.send(to, msg).await
    }

    async fn recv(&self) -> Result<Inbound, Self::Error> {
        self.inner.recv().await
    }
}
