//! Integration test: **Quorum activation over the async runtime** — M3's
//! runtime face, where a voter's grant is a durable act rather than a value in
//! a struct.
//!
//! The grant *rules* are proved deterministically in `groupnet-core` and
//! `groupnet-sim`. What only this layer can prove is the half that lives in the
//! driver: that a real store is written **before** the grant it belongs to
//! reaches a real transport, that a store which refuses leaves the grant
//! unsent, and that recovery hands a restarted voter back into a live election
//! rather than a blackout.
//!
//! * three voters close one epoch, everybody agrees, and a **majority of the
//!   stores hold the winning pair** — the write-ahead contract observed from
//!   outside, since the winner could not have activated without them;
//! * a voter whose store always fails **puts no grant on the wire at all**
//!   (counted on its own transport), attempts the persist every time, and the
//!   other two close the epoch without it — fail-closed costs availability,
//!   never safety;
//! * the same failing store on the **candidate** withholds the `LeadClaim`
//!   instead (a self-grant is counted, not sent), and the group stalls hostless
//!   for as long as it is observed — candidacy is rank-gated, so nobody else
//!   bids and no epoch is ever closed;
//! * a voter that restarts with its persisted ledger re-grants the **sitting
//!   claimant immediately**, so the incumbent is host again well inside the
//!   `lease_ms` a blackout would have cost;
//! * an `Eventual` group configured with voter storage never touches it.
//!
//! All waiting is a bounded poll on a predicate (`eventually_within`), never a
//! bare sleep.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use groupnet_core::{
    Activation, HostedConfig, NodeId, RecoveredGrant, VoterRoster, placement, wire,
};
use groupnet_runtime::{GrantStore, Group, GroupProfile, Leadership, Node, Role};
use groupnet_testkit::cluster::{
    MemCluster, NodeOpts, converged_within, eventually_within, spawn_mem_node,
};
use groupnet_transport::{Inbound, Transport};
use groupnet_transport_mem::{MemTransport, Network};

/// The poll budget for every assertion here. Deliberately looser than the
/// harness default: an election cannot even open before the engine's boot
/// guard, and the restart case additionally waits out a whole lease before the
/// incumbent lets go. A genuine regression still reports in seconds.
const SETTLE: Duration = Duration::from_secs(8);

/// A brisk gossip cadence, so grant rounds and renewals happen in wall-clock
/// milliseconds. It also sets the driver's tick period (half the tightest
/// deadline, i.e. ~7ms here), which [`LEASE_MS`] is two orders of magnitude
/// above — the sizing rule `HostedConfig` states.
const GOSSIP_MS: u64 = 15;

/// A host's authority after its last confirmed renewal round.
///
/// Under Quorum this one number does triple duty, which is why the tests below
/// are timed against it: it is the lease, it is a claim's window, and it is the
/// post-restart grant blackout a voter with no recovered ledger must sit out.
const LEASE_MS: u64 = 600;

/// [`TEST_GROUP`]-style rendezvous ranking of `ids` for `group`, best first —
/// the order the claim guard reads, so `ranked(..)[0]` is the node that will
/// bid and `ranked(..)[2]` is one whose grant is merely arithmetic.
///
/// [`TEST_GROUP`]: groupnet_testkit::frames::TEST_GROUP
fn ranked(group: &str, ids: &[&str]) -> Vec<NodeId> {
    let members: Vec<(NodeId, u32)> = ids.iter().map(|id| (NodeId::new(*id), 1)).collect();
    placement::owners(group, &members, ids.len())
}

/// A Quorum profile over `voters`, with `store` as this node's voter ledger and
/// `recovered` as what that ledger said on boot.
fn quorum_profile(
    voters: &[&str],
    store: Arc<RecordingStore>,
    recovered: RecoveredGrant,
) -> GroupProfile {
    GroupProfile::hosted(HostedConfig {
        activation: Activation::Quorum {
            voters: VoterRoster::new(voters.iter().map(|v| NodeId::new(*v))),
        },
        lease_ms: LEASE_MS,
    })
    .with_voter_storage(recovered, store)
}

/// A [`GrantStore`] that remembers every persist the driver asked of it and —
/// in its [`failing`](RecordingStore::failing) shape — refuses every one.
///
/// The log is of *attempts*, not of successes, so a failing store still proves
/// the driver asked. What a restart would read back is
/// [`recovered`](RecordingStore::recovered), which sees only what was accepted.
#[derive(Debug)]
struct RecordingStore {
    /// Every attempt, in the order the driver made it.
    log: Mutex<Vec<(u64, NodeId)>>,
    /// Whether every persist fails — the fail-closed fixture.
    failing: bool,
}

impl RecordingStore {
    /// A store that accepts everything.
    fn healthy() -> Arc<Self> {
        Arc::new(Self {
            log: Mutex::new(Vec::new()),
            failing: false,
        })
    }

    /// A store that refuses everything — the disk that is full, unmounted, or
    /// on fire.
    fn failing() -> Arc<Self> {
        Arc::new(Self {
            log: Mutex::new(Vec::new()),
            failing: true,
        })
    }

    /// Every persist the driver attempted, accepted or not.
    fn attempts(&self) -> Vec<(u64, NodeId)> {
        self.log.lock().expect("store mutex poisoned").clone()
    }

    /// The attempts that actually landed — none at all on a failing store.
    fn durable(&self) -> Vec<(u64, NodeId)> {
        if self.failing {
            Vec::new()
        } else {
            self.attempts()
        }
    }

    /// How many attempts were refused.
    fn errors(&self) -> usize {
        if self.failing {
            self.attempts().len()
        } else {
            0
        }
    }

    /// Whether this voter's disk holds a grant of `epoch` to `claimant`.
    fn holds(&self, epoch: u64, claimant: &NodeId) -> bool {
        self.durable()
            .iter()
            .any(|(e, c)| *e == epoch && c == claimant)
    }

    /// What a restart of this node would read back: the newest pair that landed,
    /// or the attested never-granted when nothing did.
    ///
    /// `RecoveredGrant::none()` is honest here because this fixture really did
    /// persist every grant it accepted and can see that none were — a real
    /// driver may only say `none()` when its *storage* attests it has never
    /// granted, never merely because it found nothing to read.
    fn recovered(&self) -> RecoveredGrant {
        match self.durable().last() {
            Some((epoch, claimant)) => RecoveredGrant::granted(*epoch, claimant.clone()),
            None => RecoveredGrant::none(),
        }
    }
}

impl GrantStore for RecordingStore {
    fn persist(&self, epoch: u64, claimant: &NodeId) -> std::io::Result<()> {
        self.log
            .lock()
            .expect("store mutex poisoned")
            .push((epoch, claimant.clone()));
        if self.failing {
            return Err(std::io::Error::other("the fixture's disk is on fire"));
        }
        Ok(())
    }
}

/// The leadership the whole cluster agrees on, or `None` while it is still
/// settling: same `(epoch, host)` everywhere, exactly one [`Role::Host`], and
/// every node's derived coordinator equal to that host — asserted as one
/// indivisible predicate so a poll can never catch half of it.
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

/// Brings `ids` up as an all-to-all Quorum cluster on `net`, each node with its
/// own store and its own store's recovered ledger.
fn spawn_roster(
    net: &Network,
    group: &str,
    ids: &[&str],
    stores: &[Arc<RecordingStore>],
) -> (Vec<NodeId>, Vec<Node<MemTransport>>, Vec<Group>) {
    let mut spawned = (Vec::new(), Vec::new(), Vec::new());
    for (id, store) in ids.iter().zip(stores) {
        let seeds: Vec<&str> = ids.iter().copied().filter(|other| other != id).collect();
        let recovered = store.recovered();
        let opts = NodeOpts::new(group)
            .gossip_interval_ms(GOSSIP_MS)
            .group_profile(quorum_profile(ids, store.clone(), recovered));
        let (node_id, node, joined) = spawn_mem_node(net, id, &seeds, &opts);
        spawned.0.push(node_id);
        spawned.1.push(node);
        spawned.2.push(joined);
    }
    spawned
}

/// Three voters close one epoch, every observer agrees on it, and the disks
/// back the agreement: a **majority** of the roster holds the winning pair,
/// because a majority is exactly what the winner had to collect — and every
/// grant that reached the wire was written down first.
#[tokio::test]
async fn three_voters_close_one_epoch_and_a_majority_wrote_the_grant_down() {
    const GROUP: &str = "quorum-elect";
    const IDS: [&str; 3] = ["qe-a", "qe-b", "qe-c"];

    let net = Network::new();
    let stores: Vec<Arc<RecordingStore>> = IDS.iter().map(|_| RecordingStore::healthy()).collect();
    let (ids, _nodes, groups) = spawn_roster(&net, GROUP, &IDS, &stores);

    let refs: Vec<&Group> = groups.iter().collect();
    converged_within(&refs, SETTLE).await;
    eventually_within("the roster to close an epoch", SETTLE, || {
        agreed(&refs).is_some()
    })
    .await;

    let lead = agreed(&refs).expect("agreed just above, and a renewed host stays put");
    let host = lead.host.clone().expect("agreement requires a named host");
    assert!(
        lead.epoch >= 1,
        "an activation takes the epoch it claimed: {lead:?}"
    );
    assert_eq!(
        host,
        ranked(GROUP, &IDS)[0],
        "under Quorum the claim guard is still the rendezvous top-ranked live member"
    );

    // The write-ahead contract, observed from outside the process: the winner
    // activated, so a majority of the roster granted it, so a majority of these
    // disks must already hold the pair. (A voter that answered a *re*-grant
    // wrote nothing new — but only because it had written that same pair down
    // the first time.)
    let backing = stores.iter().filter(|s| s.holds(lead.epoch, &host)).count();
    assert!(
        backing >= 2,
        "{backing} of 3 stores hold ({}, {host}) — a majority of the roster had to \
         grant it for the host to activate at all",
        lead.epoch
    );
    let winner = ids
        .iter()
        .position(|id| *id == host)
        .expect("the host is one of ours");
    assert!(
        stores[winner].holds(lead.epoch, &host),
        "a candidate counts its own grant into the round, so its own disk must hold it"
    );

    // And nothing was written that a grant rule forbids: one claimant per epoch
    // per voter, and never a claimant from outside the cluster.
    for (id, store) in ids.iter().zip(&stores) {
        for (epoch, claimant) in store.durable() {
            assert!(
                ids.contains(&claimant),
                "{id} wrote down a grant to a stranger: ({epoch}, {claimant})"
            );
            assert!(
                epoch <= lead.epoch,
                "{id} wrote down epoch {epoch}, above the one that closed ({})",
                lead.epoch
            );
        }
        let epochs: Vec<u64> = store.durable().iter().map(|(e, _)| *e).collect();
        let mut distinct = epochs.clone();
        distinct.dedup();
        assert_eq!(
            epochs, distinct,
            "{id} wrote two grants for one epoch — one grant per epoch per voter \
             is the rule the whole guarantee rests on"
        );
    }
}

/// A voter whose store always fails **grants nothing**: it attempts the persist
/// on every claim, and not one `LeadGrant` frame leaves it. The other two close
/// the epoch without it, which is the shape of the trade — fail-closed costs
/// availability (that voter stops counting), never safety.
///
/// The failing store is placed on the *worst*-ranked node deliberately. Put it
/// on the candidate and the group correctly stalls forever: a claimant that
/// cannot write its own grant down never broadcasts the claim that would count
/// it. That is the same rule, seen from the other side.
#[tokio::test]
async fn a_voter_whose_store_fails_puts_no_grant_on_the_wire() {
    const GROUP: &str = "quorum-failing-store";
    const IDS: [&str; 3] = ["qf-a", "qf-b", "qf-c"];

    let rank = ranked(GROUP, &IDS);
    let ordered: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    // Index 2 — the node whose grant is arithmetic rather than authority.
    let stores: Vec<Arc<RecordingStore>> = vec![
        RecordingStore::healthy(),
        RecordingStore::healthy(),
        RecordingStore::failing(),
    ];

    let net = Network::new();
    // Built first, joined second: no engine gossips toward a peer that has not
    // bound its endpoint yet.
    let wired: Vec<(Node<LeadWire>, Arc<WireCounts>)> = ordered
        .iter()
        .map(|id| build_wired(&net, id, &ordered))
        .collect();
    let groups: Vec<Group> = wired
        .iter()
        .zip(&stores)
        .map(|((node, _), store)| {
            let recovered = store.recovered();
            node.join_group_with(GROUP, quorum_profile(&IDS, store.clone(), recovered))
        })
        .collect();

    let refs: Vec<&Group> = groups.iter().collect();
    converged_within(&refs, SETTLE).await;
    eventually_within("the healthy two to close an epoch", SETTLE, || {
        agreed(&refs).is_some()
    })
    .await;

    let lead = agreed(&refs).expect("agreed just above");
    let host = lead.host.clone().expect("agreement requires a named host");
    assert_eq!(
        host, rank[0],
        "the top-ranked candidate is still the one that bids"
    );

    // The persist was attempted — the driver did not skip the store, it was
    // refused by it...
    let refused = &stores[2];
    assert!(
        refused.errors() >= 1,
        "the failing voter was never asked to persist anything: {:?}",
        refused.attempts()
    );
    assert!(
        refused.durable().is_empty(),
        "a failing store cannot hold anything"
    );
    // ...and not one grant left the node, on any round. The engine re-offers a
    // grant it has already recorded without re-persisting it, so this is the
    // assertion that catches a driver which only drops the *first* frame.
    assert_eq!(
        wired[2].1.grants(),
        0,
        "a grant the store refused reached the wire"
    );
    // Non-vacuous: the same wiring on a healthy store does send grants.
    assert!(
        wired[1].1.grants() >= 1,
        "the healthy voter sent no grants either — the counter proves nothing"
    );

    // The refused voter is not otherwise crippled: it converges on the pair the
    // others elected, it simply had no vote in it.
    assert_eq!(
        groups[2].leadership().host,
        Some(host),
        "a voter that cannot grant still learns who won"
    );
}

/// The other side of the same rule: the failing store on the **top-ranked**
/// voter — the one node that will ever bid — and the group stalls **hostless**
/// rather than electing anybody.
///
/// A claimant counts its own grant straight into its round, so what the driver
/// must withhold is not a `LeadGrant` but the `LeadClaim` the round was opened
/// for. Nothing else can rescue the group, either: candidacy is rank-gated (only
/// the rendezvous top-ranked live member opens a claim), so the second-ranked
/// node does not step in, and two healthy voters with nobody to grant cannot
/// elect anyone. Availability, all the way to zero — and still not safety.
///
/// The observation window is bounded by *progress*, not by a sleep: the poll
/// runs until the candidate has burnt [`STALLED_ROUNDS`] whole claim windows,
/// each one a round that opened, built a broadcast and had it dropped, and
/// every poll re-checks that the group is still hostless.
#[tokio::test]
async fn a_failing_store_on_the_candidate_stalls_the_group_hostless() {
    const GROUP: &str = "quorum-failing-candidate";
    const IDS: [&str; 3] = ["qc-a", "qc-b", "qc-c"];
    /// Claim windows the candidate must burn before the stall is called: enough
    /// that a driver dropping only the *first* claim would have been caught.
    const STALLED_ROUNDS: usize = 3;

    let rank = ranked(GROUP, &IDS);
    let ordered: Vec<&str> = rank.iter().map(NodeId::as_str).collect();
    // Index 0 — the candidate itself, and the only node that will ever bid.
    let stores: Vec<Arc<RecordingStore>> = vec![
        RecordingStore::failing(),
        RecordingStore::healthy(),
        RecordingStore::healthy(),
    ];

    let net = Network::new();
    let wired: Vec<(Node<LeadWire>, Arc<WireCounts>)> = ordered
        .iter()
        .map(|id| build_wired(&net, id, &ordered))
        .collect();
    let groups: Vec<Group> = wired
        .iter()
        .zip(&stores)
        .map(|((node, _), store)| {
            let recovered = store.recovered();
            node.join_group_with(GROUP, quorum_profile(&IDS, store.clone(), recovered))
        })
        .collect();

    let refs: Vec<&Group> = groups.iter().collect();
    converged_within(&refs, SETTLE).await;

    // Every epoch the candidate tried to write down. One per claim window: the
    // window shuts, the claim is abandoned, and the guard re-bids one higher.
    let candidate = &stores[0];
    let rounds = || {
        let mut epochs: Vec<u64> = candidate.attempts().iter().map(|(e, _)| *e).collect();
        epochs.dedup();
        epochs.len()
    };
    let hostless = |at: &str| {
        for (id, group) in ordered.iter().zip(&groups) {
            let lead = group.leadership();
            assert_eq!(
                lead,
                Leadership {
                    epoch: 0,
                    host: None,
                    role: Role::Follower,
                },
                "{id} left the initial belief {at}: an activation would have moved \
                 the adopted epoch, and a demotion keeps it"
            );
        }
    };

    hostless("before the first claim window");
    eventually_within(
        "the candidate to burn several claim windows",
        SETTLE,
        || {
            hostless("mid-stall");
            rounds() >= STALLED_ROUNDS
        },
    )
    .await;
    hostless("after the stall");

    // The claim never reached the wire — on any round. This is the arm of the
    // driver's guard that the worst-ranked case cannot reach: a self-grant is
    // never sent, so the frame the failed persist licenses is the claim.
    assert_eq!(
        wired[0].1.claims(),
        0,
        "a claim whose self-grant the store refused reached the wire"
    );
    assert_eq!(
        wired[0].1.grants(),
        0,
        "the candidate granted somebody on a disk that refuses everything"
    );
    // Non-vacuous twice over: the candidate is talking (gossip flows), and it
    // really did open a round per window that would have broadcast a claim.
    assert!(
        wired[0].1.frames() >= 1,
        "the candidate sent no frames at all — the zero above proves nothing"
    );
    assert!(
        candidate.errors() >= STALLED_ROUNDS,
        "only {} persists were refused across {} claim windows: {:?}",
        candidate.errors(),
        rounds(),
        candidate.attempts()
    );

    // Nothing unsafe was adopted anywhere, and nothing was granted: the healthy
    // voters never saw a claim, so their disks are as empty as the candidate's.
    for (id, store) in ordered.iter().zip(&stores) {
        assert!(
            store.durable().is_empty(),
            "{id}'s disk holds {:?} — no claim ever reached the wire to grant",
            store.durable()
        );
    }
    assert_eq!(
        wired[1].1.grants() + wired[2].1.grants(),
        0,
        "a healthy voter answered a claim that was never sent"
    );
}

/// A voter that restarts carrying its persisted ledger re-grants the claimant
/// named in it **at once**, so the incumbent regains the group well inside the
/// `lease_ms` a boot blackout would have cost.
///
/// The setup starves the host on purpose: with both other voters gone it cannot
/// renew, and its lease lapses. What comes back is therefore a real election,
/// and the restarted voter is the only node that can close it.
#[tokio::test]
async fn a_recovered_voter_re_grants_the_incumbent_without_a_blackout() {
    const GROUP: &str = "quorum-restart";
    const IDS: [&str; 3] = ["qr-a", "qr-b", "qr-c"];

    let net = Network::new();
    let stores: Vec<Arc<RecordingStore>> = IDS.iter().map(|_| RecordingStore::healthy()).collect();
    let (ids, nodes, groups) = spawn_roster(&net, GROUP, &IDS, &stores);

    let first = {
        let refs: Vec<&Group> = groups.iter().collect();
        converged_within(&refs, SETTLE).await;
        eventually_within("the roster to close an epoch", SETTLE, || {
            agreed(&refs).is_some()
        })
        .await;
        agreed(&refs).expect("agreed just above")
    };
    let host = first.host.clone().expect("agreement requires a named host");
    let host_index = ids
        .iter()
        .position(|id| *id == host)
        .expect("the host is one of ours");
    let others: Vec<usize> = (0..IDS.len()).filter(|i| *i != host_index).collect();
    let (restart, gone) = (others[0], others[1]);

    // --- Kill both non-hosts. ---
    //
    // Dropping the handles is not enough on its own: the node's receive loop
    // owns an `Arc` of the same inner state, so the actors keep ticking until
    // the endpoint is evicted from the `Network` (registering the id again
    // replaces the sender and closes the old inbox).
    let mut nodes: Vec<Option<Node<MemTransport>>> = nodes.into_iter().map(Some).collect();
    let mut groups: Vec<Option<Group>> = groups.into_iter().map(Some).collect();
    for index in [restart, gone] {
        groups[index] = None;
        nodes[index] = None;
    }
    let _evicted_restart = net.endpoint(ids[restart].clone());
    let _evicted_gone = net.endpoint(ids[gone].clone());
    let host_group = groups[host_index].as_ref().expect("the host survives");

    // Starved of a majority, the incumbent cannot renew: it lapses and steps
    // down, exactly as the CP posture requires of a minority side.
    eventually_within("the starved host to lose its lease", SETTLE, || {
        host_group.leadership().host.is_none()
    })
    .await;
    assert_eq!(
        host_group.leadership().role,
        Role::Follower,
        "a host that cannot reach a majority must stop being one"
    );

    // --- Restart one voter, on the very disk it was using before. ---
    let recovered = stores[restart].recovered();
    assert!(
        stores[restart].holds(first.epoch, &host),
        "the restarted voter's ledger is the grant it actually made: {:?}",
        stores[restart].attempts()
    );
    let seeds = [host.as_str()];
    let opts = NodeOpts::new(GROUP)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(quorum_profile(&IDS, stores[restart].clone(), recovered));
    let restart_at = Instant::now();
    let (_, _restarted_node, restarted_group) = spawn_mem_node(&net, IDS[restart], &seeds, &opts);

    eventually_within("the incumbent to regain the group", SETTLE, || {
        let now = host_group.leadership();
        now.role == Role::Host && now.epoch > first.epoch
    })
    .await;
    let regained = restart_at.elapsed();

    let after = host_group.leadership();
    assert!(
        stores[restart].holds(after.epoch, &host),
        "the re-grant is write-ahead too: the disk must hold ({}, {host}) — {:?}",
        after.epoch,
        stores[restart].attempts()
    );
    // The liveness claim, timed generously against the one number a blackout
    // would have charged: a voter with no recovered ledger refuses every new
    // claimant for a full `lease_ms` after *its own* boot, so a run that took
    // longer than that has lost the exemption recovery exists to buy. The
    // actual path is a claim re-broadcast (one anti-entropy round) plus a
    // round trip — an order of magnitude under the bound.
    assert!(
        regained < Duration::from_millis(LEASE_MS),
        "re-election took {regained:?}, at or past the {LEASE_MS}ms blackout a \
         voter without a recovered ledger would have imposed"
    );

    let live: Vec<&Group> = vec![host_group, &restarted_group];
    eventually_within("both survivors to agree on the new pair", SETTLE, || {
        agreed(&live).is_some_and(|l| l.host == Some(host.clone()) && l.epoch > first.epoch)
    })
    .await;
}

/// An `Eventual` group configured with voter storage never touches it: there is
/// no ledger to write, so the store stays empty for the life of a demonstrably
/// live group.
///
/// One store, shared by all three nodes, so a single stray persist anywhere
/// shows up here. (That it *can* show up is what the tests above establish.)
#[tokio::test]
async fn an_eventual_group_never_touches_its_voter_store() {
    const GROUP: &str = "eventual-with-storage";

    let store = RecordingStore::healthy();
    let cluster = MemCluster::builder(&["es-a", "es-b", "es-c"])
        .group(GROUP)
        .gossip_interval_ms(GOSSIP_MS)
        .group_profile(
            GroupProfile::eventual().with_voter_storage(RecoveredGrant::none(), store.clone()),
        )
        .spawn();

    let refs: Vec<&Group> = cluster.groups.iter().collect();
    converged_within(&refs, SETTLE).await;
    // Demonstrably live, not merely quiet: a write crosses the group.
    cluster.groups[0]
        .set_entry("crossed", b"ok".to_vec(), None)
        .expect("the group actor is live");
    eventually_within("the write to reach the other nodes", SETTLE, || {
        cluster.groups[1..]
            .iter()
            .all(|g| g.node_entry(&cluster.ids[0], "crossed").is_some())
    })
    .await;

    for (id, group) in cluster.ids.iter().zip(&cluster.groups) {
        assert_eq!(
            group.leadership(),
            Leadership {
                epoch: 0,
                host: None,
                role: Role::Follower,
            },
            "{id}'s eventual group must never leave the initial belief"
        );
    }
    assert!(
        store.attempts().is_empty(),
        "an Eventual group has no voter ledger, so nothing may be written to one: {:?}",
        store.attempts()
    );
}

/// What one node actually put on the wire, by frame kind.
///
/// A frame the driver dropped is invisible from every other vantage point — the
/// peers cannot tell it apart from one that was never emitted — so the sending
/// transport is the only place the fail-closed drop is observable. `frames`
/// exists to keep the zero assertions non-vacuous: a node that sent nothing at
/// all would satisfy them for the wrong reason.
#[derive(Debug, Default)]
struct WireCounts {
    claims: AtomicUsize,
    grants: AtomicUsize,
    frames: AtomicUsize,
}

impl WireCounts {
    /// `LeadClaim` frames sent — the bids a claimant made visible.
    fn claims(&self) -> usize {
        self.claims.load(Ordering::Relaxed)
    }

    /// `LeadGrant` frames sent — the endorsements this voter published.
    fn grants(&self) -> usize {
        self.grants.load(Ordering::Relaxed)
    }

    /// Every frame sent, of any kind: proof the transport is live and counting.
    fn frames(&self) -> usize {
        self.frames.load(Ordering::Relaxed)
    }
}

/// A [`MemTransport`] that tallies its node's outbound frames into a
/// [`WireCounts`] before passing them on, decoding each one rather than reading
/// bytes off it — the codec is the only authority on what a frame is.
#[derive(Debug)]
struct LeadWire {
    inner: MemTransport,
    counts: Arc<WireCounts>,
}

impl Transport for LeadWire {
    type Error = <MemTransport as Transport>::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error> {
        self.counts.frames.fetch_add(1, Ordering::Relaxed);
        match wire::decode(msg).map(|frame| frame.kind) {
            Some(wire::Kind::LeadClaim) => {
                self.counts.claims.fetch_add(1, Ordering::Relaxed);
            }
            Some(wire::Kind::LeadGrant) => {
                self.counts.grants.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
        self.inner.send(to, msg).await
    }

    async fn recv(&self) -> Result<Inbound, Self::Error> {
        self.inner.recv().await
    }
}

/// Builds (but does not join) a node on `net` whose outbound frames are
/// counted, seeded with every other id in `all`.
fn build_wired(net: &Network, id: &str, all: &[&str]) -> (Node<LeadWire>, Arc<WireCounts>) {
    let me = NodeId::new(id);
    let counts = Arc::new(WireCounts::default());
    let transport = LeadWire {
        inner: net.endpoint(me.clone()),
        counts: counts.clone(),
    };
    let mut builder = Node::builder(me, transport).gossip_interval_ms(GOSSIP_MS);
    for seed in all.iter().filter(|other| **other != id) {
        builder = builder.seed(NodeId::new(*seed));
    }
    (builder.spawn(), counts)
}
