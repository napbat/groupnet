//! Deterministic Simulation Testing for **`Activation::External` failover and
//! the anchor as the availability axis** — what the tier does when the fabric
//! is cut, when the store is unreachable, and when the host dies.
//!
//! Split out of `election_external.rs` the same way
//! `election_quorum_failover.rs` is split out of `election_quorum.rs`: the
//! chaos suite and the global properties live there, the *shaped* scenarios
//! live here. Each is a claim the tier makes in its own words, and each is
//! contrasted with what `Quorum` does with the identical schedule — because
//! "CP, but the partition is not the axis" only means something if the
//! inversion is exhibited rather than asserted.
//!
//! * **X-part — [`a_partitioned_host_that_can_still_reach_the_anchor_keeps_the_group`].**
//!   A full fabric partition around the host. It keeps hosting on the *minority*
//!   side; the majority side's top-ranked node — restarted mid-partition, so it
//!   has no memory and no gossip source for the pair — learns the true pair
//!   **through the anchor** and never activates. The contrast under `Quorum`
//!   (same shape, same timings) starves the incumbent instead.
//! * **X-closed — [`an_unreachable_anchor_lapses_the_host_and_elects_nobody`].**
//!   The store goes away for everyone. The incumbent lapses at *exactly* its
//!   lease instant, the group goes hostless, and a live, healthy, fully
//!   connected set of candidates elects nobody at all — the fail-closed
//!   posture, with the anchor log's silence as the reason.
//! * **X-budget — [`dst_external_failover_lands_inside_its_budget`].** What a
//!   host's death costs, in exact virtual time, with every millisecond of the
//!   budget itemized and no fudge term.
//!
//! The rank gate's own scenarios — X-rank, X-rank-compound and X-handback — are
//! next door in `election_external_rank.rs`.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    VoterRoster, placement,
};
use groupnet_sim::{AnchorEvent, Simulation, SplitMix64};

/// The engine lease, the anchor record's TTL and the boot guard — one number,
/// as `HostedConfig::lease_ms` documents under this activation.
const LEASE_MS: u64 = 400;
/// The anti-entropy cadence, which the anchor prompt rides.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// How far past a record's expiry a claimant must wait before it may steal.
const STEAL_MARGIN_MS: u64 = 150;
/// One store round trip.
const ANCHOR_LATENCY_MS: u64 = 15;

fn hosted(activation: Activation) -> Config {
    Config {
        gossip_interval_ms: GOSSIP_INTERVAL_MS,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: GOSSIP_INTERVAL_MS,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Hosted(HostedConfig {
            activation,
            lease_ms: LEASE_MS,
        }),
    }
}

fn cfg() -> Config {
    hosted(Activation::External {
        steal_margin_ms: STEAL_MARGIN_MS,
    })
}

/// The same cluster under `Quorum` over a roster of everyone — the contrast
/// every scenario here is read against.
fn quorum_cfg(voters: &BTreeSet<NodeId>) -> Config {
    hosted(Activation::Quorum {
        voters: VoterRoster::new(voters.iter().cloned()),
    })
}

fn nodes(ids: &[&str]) -> BTreeSet<NodeId> {
    ids.iter().map(|id| NodeId::new(*id)).collect()
}

/// A cluster of `members` bootstrapped all-to-all in `config`, with the anchor
/// armed at the External configuration. (A `Quorum` contrast run arms it too
/// and simply never prompts it — nothing in that tier emits `AnchorClaimDue`,
/// which is itself worth leaving observable.)
fn cluster(group: &str, members: &BTreeSet<NodeId>, config: &Config) -> Simulation {
    let mut sim = Simulation::new(10);
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(ANCHOR_LATENCY_MS);
    for id in members {
        sim.add(engine(group, id, members, config));
    }
    sim
}

fn engine(group: &str, id: &NodeId, peers: &BTreeSet<NodeId>, config: &Config) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(GroupId::new(group), id.clone(), seeds, config.clone())
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

fn sole_host(sim: &Simulation, label: &str) -> NodeId {
    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "{label}: expected exactly one host: {hosts:?}"
    );
    hosts.into_iter().next().expect("length asserted above")
}

/// The pair the register itself holds.
fn anchor_pair(sim: &Simulation, label: &str) -> (u64, NodeId) {
    let record = sim
        .anchor_record()
        .unwrap_or_else(|| panic!("{label}: the anchor is still empty"));
    (record.epoch, record.host)
}

/// **X-purity**, asserted at the end of every External run in this file. See
/// `election_external.rs` for why it is counted at issuance.
fn assert_pure(sim: &Simulation, all: &BTreeSet<NodeId>, label: &str) {
    assert_eq!(
        sim.claim_frames_seen(),
        0,
        "{label}: an External group put a claim on the wire"
    );
    assert_eq!(
        sim.grant_frames_seen(),
        0,
        "{label}: an External group put a grant on the wire"
    );
    for node in all {
        assert_eq!(
            sim.persisted_grant_of(node),
            None,
            "{label}: {node} persisted a grant under External"
        );
    }
}

/// Steps the simulation **one scheduled event at a time** up to `until`,
/// running `check` after each one.
///
/// No sampling cadence, and so no sampling gap: an engine only moves when it
/// takes a frame, a tick or an anchor round's command, so a property checked
/// after every event is checked at every instant it could have changed.
fn step_through(sim: &mut Simulation, until: u64, mut check: impl FnMut(&Simulation, u64)) {
    while let Some(at) = sim.step_until(Time(until)) {
        check(sim, at.0);
    }
}

/// How many rounds of each kind `node` has run — the floors the shaped
/// scenarios earn.
fn rounds_by(sim: &Simulation, node: &NodeId, what: AnchorEvent, since: u64) -> usize {
    sim.anchor_log
        .iter()
        .filter(|(at, who, event)| at.0 >= since && who == node && *event == what)
        .count()
}

// ---------------------------------------------------------------------------
// X-part — the partition is not the axis.
// ---------------------------------------------------------------------------

/// **X-part.** A host cut off from *every* peer keeps hosting, because the only
/// thing that can take the epoch from it is the anchor, and it can still reach
/// the anchor.
///
/// Both halves are asserted, and the second is the interesting one:
///
/// * **The minority side keeps serving.** The incumbent is alone against two
///   peers it can no longer see and that can no longer see it. It renews
///   through the whole hold — checked after every scheduled event, not sampled
///   — and its epoch never moves, because a renewal decides nothing.
/// * **The majority side learns the truth through the store.** Its top-ranked
///   node is *restarted* mid-partition, so it comes back at `(0, None)` with no
///   memory and — this is the part that makes it airtight — **no possible
///   gossip source**: the incumbent is partitioned away, and a follower never
///   beacons a pair (only a host does, row 7). The only place the pair can come
///   from is the anchor round its own claim guard prompts, which reads a live
///   record, yields, and reports it. It adopts `(epoch, incumbent)` and never
///   activates anything.
/// * **The third node stays where it was**, which is the honest shape of an
///   observer-local adoption: nobody re-taught it, and nothing needed to.
///
/// The contrast is [`the_same_partition_under_quorum_starves_the_incumbent`],
/// which runs the identical schedule with the identical timings.
#[test]
fn a_partitioned_host_that_can_still_reach_the_anchor_keeps_the_group() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-part";
    let label = "X-part";
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));

    let incumbent = sole_host(&sim, label);
    let (epoch, _) = anchor_pair(&sim, label);
    let others: BTreeSet<NodeId> = members
        .iter()
        .filter(|n| **n != incumbent)
        .cloned()
        .collect();
    let successor_side_top =
        placement::owner(group, &others).expect("two nodes on the majority side");

    // A full two-way partition: the incumbent against the world.
    let split_at = 2_000;
    for other in &others {
        sim.block(&incumbent, other);
        sim.block(other, &incumbent);
    }

    // The incumbent holds the group for the whole split, checked after every
    // single event rather than on a sampling cadence.
    step_through(&mut sim, split_at + 3_000, |sim, at| {
        assert_eq!(
            sim.role_of(&incumbent),
            Some(Role::Host),
            "the incumbent stopped hosting at {at} despite keeping the anchor"
        );
        assert_eq!(
            pair_of(sim, &incumbent).0,
            epoch,
            "a renewal moved the epoch at {at} — a renewal allocates nothing"
        );
    });
    assert!(
        rounds_by(&sim, &incumbent, AnchorEvent::Renew, split_at) > 10,
        "the incumbent kept hosting without renewing anything — the lease must be earned"
    );
    assert_eq!(
        anchor_pair(&sim, label),
        (epoch, incumbent.clone()),
        "the register changed hands during a partition it knows nothing about"
    );

    // The majority side's candidate is restarted: no memory, and no gossip
    // source for the pair. Whatever it ends up believing came from the store.
    // It is seeded with every member, incumbent included, so it has to bury the
    // one it cannot reach before it ranks — the partition outlives the restart,
    // as a real one would.
    let restart_at = split_at + 3_000;
    sim.crash(&successor_side_top);
    sim.add(engine(group, &successor_side_top, &members, &cfg()));
    assert_eq!(
        pair_of(&sim, &successor_side_top),
        (0, None),
        "a restart is amnesiac: no epoch, no hostship, no etag"
    );

    step_through(&mut sim, restart_at + 4_000, |sim, at| {
        assert_ne!(
            sim.role_of(&successor_side_top),
            Some(Role::Host),
            "the majority side activated at {at} while a live record named somebody else"
        );
    });
    assert_eq!(
        pair_of(&sim, &successor_side_top),
        (epoch, Some(incumbent.clone())),
        "the restarted candidate never learned the pair the store was holding for it"
    );
    assert!(
        rounds_by(&sim, &successor_side_top, AnchorEvent::Yield, restart_at) > 0,
        "it adopted a pair without ever asking the anchor — which is impossible here"
    );
    assert_eq!(
        rounds_by(&sim, &successor_side_top, AnchorEvent::Steal, restart_at),
        0,
        "a live record was stolen"
    );

    // And the heal changes nothing about who holds the group: L1, on the
    // register's own pair.
    sim.heal_all();
    sim.run_until(Time(restart_at + 12_000));
    assert_eq!(sole_host(&sim, label), incumbent);
    assert_eq!(anchor_pair(&sim, label), (epoch, incumbent.clone()));
    for node in &members {
        assert_eq!(
            pair_of(&sim, node),
            (epoch, Some(incumbent.clone())),
            "{node} did not converge on the register's pair after the heal"
        );
    }
    assert_pure(&sim, &members, label);
}

/// The contrast, on the identical schedule: under `Quorum` the same partition
/// **starves** the incumbent, because its authority comes from a roster it can
/// no longer reach.
///
/// This is the inversion in one file: same cluster, same timings, same cut —
/// and the opposite outcome, decided entirely by where the epoch comes from.
/// Under `External` the fabric is not the availability axis at all; under
/// `Quorum` it is the only one.
#[test]
fn the_same_partition_under_quorum_starves_the_incumbent() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-part";
    let label = "X-part (Quorum contrast)";
    let mut sim = cluster(group, &members, &quorum_cfg(&members));
    sim.run_until(Time(2_000));

    let incumbent = sole_host(&sim, label);
    let others: BTreeSet<NodeId> = members
        .iter()
        .filter(|n| **n != incumbent)
        .cloned()
        .collect();
    for other in &others {
        sim.block(&incumbent, other);
        sim.block(other, &incumbent);
    }

    // Row 6 via row Q7/Q8: no majority, no renewal, no lease.
    let demote_by = 2_000 + LEASE_MS + GOSSIP_INTERVAL_MS;
    sim.run_until(Time(demote_by));
    assert_ne!(
        sim.role_of(&incumbent),
        Some(Role::Host),
        "a Quorum incumbent kept hosting from a side with no majority"
    );

    sim.run_until(Time(8_000));
    let successor = sole_host(&sim, label);
    assert!(
        others.contains(&successor),
        "the majority side did not elect: {successor}"
    );
    // And the anchor sat untouched throughout — no tier but External ever
    // prompts it.
    assert!(
        sim.anchor_log.is_empty() && sim.anchor_record().is_none(),
        "a Quorum group ran an anchor round: {:?}",
        sim.anchor_log
    );
}

// ---------------------------------------------------------------------------
// X-closed — no anchor, no host.
// ---------------------------------------------------------------------------

/// **X-closed.** The store becomes unreachable for everybody.
///
/// * The incumbent demotes at **exactly** its lease instant — asserted event by
///   event on both sides of it, so a step-down one tick late would fail. Its
///   authority is the anchor's and nothing else extends it: it is still the
///   group's top-ranked live candidate the whole time, which under `Settle`
///   would have renewed it for ever (row 5 is gated to `Settle` precisely
///   here).
/// * Then the incumbent is crashed, so the survivors *become* candidates and
///   really do prompt. **They still elect nobody**, and the anchor log's total
///   silence through the outage is the reason: every round they asked for was
///   dropped at the store, and no engine will host on its own say-so.
/// * On heal, the top-ranked survivor takes the group within a stated budget —
///   the record it supersedes is long expired, so entitlement is immediate and
///   the only cost is the prompt cadence and the round trips.
#[test]
fn an_unreachable_anchor_lapses_the_host_and_elects_nobody() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-closed";
    let label = "X-closed";
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    let incumbent = sole_host(&sim, label);

    // Cut the store off. The lease standing at this instant is the last one any
    // round bought, because every round scheduled from here on is dropped.
    for node in &members {
        sim.block_anchor(node);
    }
    let lease = sim
        .lease_until_of(&incumbent)
        .expect("the incumbent holds a lease")
        .0;
    let outage_from = 2_000;

    step_through(&mut sim, lease - 1, |sim, at| {
        assert_eq!(
            sim.role_of(&incumbent),
            Some(Role::Host),
            "the incumbent gave up the group at {at}, before its lease ran out at {lease}"
        );
    });
    assert!(
        sim.is_member(&incumbent, &incumbent) && sim.role_of(&incumbent) == Some(Role::Host),
        "premise: the incumbent is healthy and connected right up to the lapse"
    );
    sim.run_until(Time(lease));
    assert_ne!(
        sim.role_of(&incumbent),
        Some(Role::Host),
        "the lease lapses at exactly {lease}, not a tick later"
    );
    assert_eq!(
        pair_of(&sim, &incumbent).1,
        None,
        "a lapse announces a hostless pair, keeping the epoch as the fence"
    );

    // Now make the rest of the cluster genuinely eligible: they bury the dead
    // incumbent, become top-ranked, and prompt for rounds that never happen.
    sim.crash(&incumbent);
    let survivors: BTreeSet<NodeId> = members
        .iter()
        .filter(|n| **n != incumbent)
        .cloned()
        .collect();
    step_through(&mut sim, lease + 4_000, |sim, at| {
        assert!(
            sim.hosts().is_empty(),
            "{:?} hosted at {at} with no anchor to award it anything",
            sim.hosts()
        );
    });
    // Exact: the block was applied after everything scheduled at `outage_from`
    // had already run, so a round *at* that instant is legitimate and anything
    // after it is a round that should have been dropped at the store.
    assert!(
        sim.anchor_log.iter().all(|(at, _, _)| at.0 <= outage_from),
        "a round reached the store during the outage: {:?}",
        sim.anchor_log
    );

    // Heal, and the group comes back inside the cost of asking.
    let healed_at = lease + 4_000;
    sim.heal_anchor_all();
    let budget = GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS;
    sim.run_until(Time(healed_at + budget));
    let successor = sole_host(&sim, label);
    assert!(
        survivors.contains(&successor),
        "{successor} is not one of the survivors"
    );
    assert_eq!(
        successor,
        placement::owner(group, &survivors).expect("survivors"),
        "the group came back on somebody other than the rendezvous owner"
    );
    let (epoch, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, successor);
    assert!(
        epoch > 1,
        "the successor resumed an epoch instead of winning one"
    );
    assert_pure(&sim, &members, label);
}

// ---------------------------------------------------------------------------
// X-budget — what a host's death costs.
// ---------------------------------------------------------------------------

/// The largest link latency this suite configures.
const MAX_LATENCY_MS: u64 = 6;
/// The largest per-message reorder jitter this suite configures.
const MAX_JITTER_MS: u64 = 5;

/// **X-budget.** After the host crashes, a successor is serving by an instant
/// this suite states in full — and states as an **absolute virtual instant**,
/// not as a delay, because the first term is one:
///
/// ```text
/// serving_by = max( expires_at_wall_ms + STEAL_MARGIN_MS,   // entitled to supersede
///                   crash_at + detection_window_ms(n) )     // entitled to candidate
///            + GOSSIP_INTERVAL_MS                           // the prompt's cadence
///            + 2 · ANCHOR_LATENCY_MS                        // one round wasted, one won
/// ```
///
/// * **`expires_at_wall_ms + STEAL_MARGIN_MS`** is the whole succession rule,
///   and it is an *instant* rather than a wait: `AnchorRecord::stealable`
///   compares the claimant's wall clock against exactly this number, and the
///   record's TTL was already ticking before the crash. Every seed here runs
///   with **zero skew**, which is what lets a wall-clock number be read as a
///   virtual instant; the suite that breaks that identity on purpose is
///   `election_external_skew.rs`. Note what is *absent*: no term for promises to
///   expire, because nothing was promised to anybody. There is no roster.
/// * **`crash_at + detection_window_ms(n)`** — a successor may not prompt until
///   it has buried the dead host, because row X1 carries row 1's rank guard.
/// * **The two are `max`ed, not added**, because they are concurrent waits, not
///   sequential ones: the record ages out while the detector works. Whichever
///   is later binds — and which one that is genuinely varies here, since the
///   detection window grows with the cluster while the record's TTL does not.
///   (Same argument as `Q-budget`'s `.max(LEASE_MS)`, with the promise term
///   replaced by the record's own expiry.)
/// * **`GOSSIP_INTERVAL_MS`** — the prompt is a level signal on the
///   anti-entropy cadence, so the first *entitled* round can be up to one
///   interval late. The same term covers tick rounding on the burial itself.
/// * **`2 · ANCHOR_LATENCY_MS`** — two store round trips: the round that
///   reached the store just before entitlement and yielded, and the one that
///   won. A prompt costs a round trip whether or not it wins, which is the
///   honest price of a store that is not on the cluster's network.
///
/// Also asserted per seed: the successor's epoch strictly fences the dead
/// host's, every survivor has adopted the new pair, the register names the
/// successor, and the run stayed pure.
#[test]
fn dst_external_failover_lands_inside_its_budget() {
    let mut steals = 0u64;
    let mut slack = u64::MAX;
    for seed in 0..32u64 {
        let (stole, spare) = failover_budget(seed);
        steals += u64::from(stole);
        slack = slack.min(spare);
    }
    assert!(steals > 0, "vacuous: no seed ever superseded a dead host");
    // Printed on success: a budget whose slack has drifted towards zero says so
    // here rather than the first time a seed drifts past it.
    println!("X-budget: {steals} steals, tightest slack {slack}ms");
}

/// Returns whether the successor took the record by stealing it, and how much
/// of the budget was left unspent.
fn failover_budget(seed: u64) -> (bool, u64) {
    let mut rng = SplitMix64::new(seed ^ 0xe2b0_9e37_79b9_7f4a);
    let n = 3 + usize::try_from(rng.below(4)).expect("a 0..4 draw"); // 3..=6 nodes
    let members: BTreeSet<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let group = format!("x-budget-{seed}");
    let label = format!("X-budget seed {seed}");

    let latency = 2 + u64::from(rng.below(u32::try_from(MAX_LATENCY_MS).expect("small") - 1));
    let mut sim = Simulation::new(latency);
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(ANCHOR_LATENCY_MS);
    sim.set_jitter(u64::from(
        rng.below(u32::try_from(MAX_JITTER_MS).expect("small") + 1),
    ));
    for id in &members {
        sim.add(engine(&group, id, &members, &cfg()));
    }

    // Converge first: the budget bounds *failover*, not bootstrap. The crash
    // instant is drawn so the schedule samples its phase against each
    // survivor's probe cursor and the incumbent's renewal round.
    let crash_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(crash_at));
    let incumbent = sole_host(&sim, &format!("{label} (before the crash)"));
    let record = sim.anchor_record().expect("an elected group has a record");
    assert_eq!(record.host, incumbent);

    let survivors: BTreeSet<NodeId> = members
        .iter()
        .filter(|x| **x != incumbent)
        .cloned()
        .collect();
    sim.crash(&incumbent);

    let entitled_at = record.expires_at_wall_ms + STEAL_MARGIN_MS;
    let ranked_by = crash_at + cfg().detection_window_ms(members.len());
    let serving_by = entitled_at.max(ranked_by) + GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS;
    assert!(
        serving_by > crash_at,
        "{label}: the budget expired before the crash"
    );

    // Walk the budget out event by event and record the first instant the group
    // was served again, so the slack is measured rather than assumed.
    let mut served_at = None;
    step_through(&mut sim, serving_by, |sim, at| {
        if served_at.is_none() && !sim.hosts().is_empty() {
            served_at = Some(at);
        }
    });
    let served_at =
        served_at.unwrap_or_else(|| panic!("{label}: nobody held the group by {serving_by}"));

    let successor = sole_host(&sim, &format!("{label} (at {serving_by})"));
    assert_eq!(
        successor,
        placement::owner(&group, &survivors).expect("at least one survivor"),
        "{label}: the group went to somebody other than the survivors' rendezvous owner"
    );
    let (epoch, holder) = anchor_pair(&sim, &label);
    assert_eq!(holder, successor);
    assert!(
        epoch > record.epoch,
        "{label}: {successor} took epoch {epoch}, which does not fence the dead host's {}",
        record.epoch
    );

    sim.run_until(Time(serving_by + 2_000));
    for id in &survivors {
        assert_eq!(
            pair_of(&sim, id),
            (epoch, Some(successor.clone())),
            "{label}: {id} had not adopted the new pair"
        );
    }
    assert_pure(&sim, &members, &label);

    let stole = rounds_by(&sim, &successor, AnchorEvent::Steal, crash_at) > 0;
    (stole, serving_by - served_at)
}

/// Floors, folded across the whole file: every shaped scenario is meant to
/// exercise a *different* round, and this fails loudly if one of them stops.
#[test]
fn the_shaped_scenarios_exercise_every_kind_of_round() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-floors";
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    let incumbent = sole_host(&sim, "X-floors");

    // A steal (the host dies), a supersede (it comes back and re-wins rather
    // than resuming), renewals and yields all in one schedule.
    sim.crash(&incumbent);
    sim.run_until(Time(5_000));
    sim.add(engine(group, &incumbent, &members, &cfg()));
    sim.run_until(Time(9_000));
    let successor = sole_host(&sim, "X-floors");
    sim.crash(&successor);
    sim.run_until(Time(14_000));

    let mut counts: BTreeMap<AnchorEvent, usize> = BTreeMap::new();
    for (_, _, event) in &sim.anchor_log {
        *counts.entry(*event).or_default() += 1;
    }
    for event in [
        AnchorEvent::Create,
        AnchorEvent::Steal,
        AnchorEvent::Renew,
        AnchorEvent::Yield,
    ] {
        assert!(
            counts.get(&event).copied().unwrap_or(0) > 0,
            "vacuous: no {event:?} round in the shaped corpus — {counts:?}"
        );
    }
    println!("X-shaped: {counts:?}");
    assert_pure(&sim, &members, "X-floors");
}
