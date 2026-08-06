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
//! * **X-rank — [`a_group_whose_top_ranked_node_lost_the_anchor_stays_hostless`].**
//!   The documented cost of a rank-gated candidate set: one node's store
//!   connectivity can pin the whole group hostless.
//! * **X-rank-compound —
//!   [`a_returning_top_ranked_node_without_the_anchor_unseats_a_working_host`].**
//!   The same cost compounded with the rank-gated *renewal*: an incumbent that
//!   is anchor-connected and serving perfectly well is outranked by a returning
//!   node that **cannot reach the store at all**, and the two rules together
//!   take the group away from the host that works and give it to nobody. The
//!   price of the CP posture, asserted rather than described.
//! * **X-handback —
//!   [`an_incumbent_that_loses_rank_lapses_and_the_rendezvous_top_takes_it_back`].**
//!   Renewal is rank-gated under *every* activation, `External` included: row X7
//!   reads `is_coordinator()` exactly as rows 5 and Q7 do. So an outranked
//!   incumbent stops being prompted, lets its record age out, and the returning
//!   rendezvous top supersedes it at a strictly higher epoch — the group lands
//!   back where the coordinator ranking points, and it costs a handful of
//!   yielded rounds rather than one per anti-entropy interval for ever.
//! * **X-budget — [`dst_external_failover_lands_inside_its_budget`].** What a
//!   host's death costs, in exact virtual time, with every millisecond of the
//!   budget itemized and no fudge term.
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
// X-rank — the documented cost of a rank-gated candidate set.
// ---------------------------------------------------------------------------

/// **X-rank.** Only the *top-ranked* node loses the anchor, and the whole group
/// stays hostless — even though two perfectly healthy peers could reach the
/// store and win a round in a millisecond.
///
/// This is deliberate and it is documented as such: candidacy is rank-gated
/// (row X1 is row 1's guard verbatim), which is what keeps the election free of
/// duelling timeouts, and the price is that one node's store connectivity can
/// pin the group. Asserting it here means a future change that quietly widens
/// the candidate set has to come and edit this test — which is exactly the
/// conversation such a change should start.
///
/// The operator's signal for this state is the anchor error at the driver, not
/// anything in the fabric: to every peer, and to the node itself, the cluster
/// looks perfectly healthy. That is what the two assertions on the anchor log
/// say — the second-ranked node never even *asked*.
#[test]
fn a_group_whose_top_ranked_node_lost_the_anchor_stays_hostless() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-rank";
    let label = "X-rank";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.block_anchor(&top);

    step_through(&mut sim, 6_000, |sim, at| {
        assert!(
            sim.hosts().is_empty(),
            "{:?} hosted at {at} — only the top-ranked node is a candidate",
            sim.hosts()
        );
    });
    assert!(
        sim.anchor_record().is_none(),
        "the register was written by a node that never prompts"
    );
    assert!(
        sim.anchor_log.is_empty(),
        "a non-candidate ran an anchor round: {:?}",
        sim.anchor_log
    );
    // The premise, asserted rather than assumed: the group is *healthy*. Every
    // node sees every other, and the pinned node is the rendezvous owner.
    for node in &members {
        assert_eq!(sim.members_of(node), members, "{node} lost sight of a peer");
    }

    // Heal it, and the group is elected on the next prompt.
    sim.heal_anchor(&top);
    sim.run_until(Time(6_000 + GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS));
    assert_eq!(sole_host(&sim, label), top);
    assert_eq!(anchor_pair(&sim, label), (1, top));
    assert_pure(&sim, &members, label);
}

/// **X-rank-compound.** The rank gate on *candidacy* (row X1) and the rank gate
/// on *renewal* (row X7) are each defensible alone. Composed, they have a price
/// this test names: a group with a **working, willing, anchor-connected host**
/// can be left hostless by the return of a node that ranks above it and cannot
/// reach the store at all.
///
/// The schedule is the hand-back's, with one change — the returning node's
/// anchor is blocked:
///
/// * the top-ranked node crashes and the second takes the group, serving it
///   perfectly well on an anchor it can reach;
/// * the top-ranked node comes back **without store access**. It outranks the
///   incumbent the moment gossip re-admits it, so row X7 stops prompting the
///   incumbent, its record ages out and its engine lease lapses on row 6;
/// * and row X1 will not let anybody else bid, because the top-ranked node is
///   the only candidate — and its rounds die at the store.
///
/// So the group is hostless *because* it has a healthy host and a higher-ranked
/// node that cannot host, which is the compound cost the design doc now states
/// beside both rules. Both halves are asserted: the incumbent **lapses**, and
/// the group stays hostless — event by event, right past the instant its record
/// became stealable — until the top-ranked node's anchor heals, at which point
/// it wins immediately.
///
/// Nothing here is a safety failure: the anchor still allocates every epoch and
/// the eventual hand-back is cross-epoch and store-fenced like any other. It is
/// availability, spent deliberately, and a future change that widens the
/// candidate set to buy it back has to come and edit this test.
#[test]
fn a_returning_top_ranked_node_without_the_anchor_unseats_a_working_host() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-rank-compound";
    let label = "X-rank-compound";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    assert_eq!(sole_host(&sim, label), top, "the premise: rank elected it");

    sim.crash(&top);
    sim.run_until(Time(5_000));
    let incumbent = sole_host(&sim, &format!("{label} (after the crash)"));
    assert_ne!(incumbent, top);
    let (epoch, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, incumbent, "premise: the incumbent holds the record");
    let stealable_at = sim
        .anchor_record()
        .expect("a record")
        .expires_at_wall_ms
        .saturating_add(STEAL_MARGIN_MS);

    // The top-ranked node returns — with no way to reach the store. It outranks
    // the incumbent regardless, which is all row X7 reads.
    let back_at = 5_000;
    sim.block_anchor(&top);
    sim.add(engine(group, &top, &members, &cfg()));

    let mut lapsed_at = None;
    step_through(&mut sim, back_at + 8_000, |sim, at| {
        if lapsed_at.is_none() {
            if sim.hosts().is_empty() {
                lapsed_at = Some(at);
            }
            return;
        }
        assert!(
            sim.hosts().is_empty(),
            "{:?} hosted at {at}: the only candidate is the returning node, and it \
             cannot reach the anchor",
            sim.hosts()
        );
    });
    let lapsed_at = lapsed_at.unwrap_or_else(|| {
        panic!("{label}: the outranked incumbent kept the group it was no longer prompted for")
    });
    assert!(
        back_at + 8_000 > stealable_at,
        "{label}: the window closed at {} before the record was even stealable at \
         {stealable_at} — the hostless stretch would prove nothing",
        back_at + 8_000
    );
    assert_eq!(
        anchor_pair(&sim, label),
        (epoch, incumbent.clone()),
        "{label}: the record changed hands while nobody could write to the store"
    );
    // The premise, asserted rather than assumed: the deposed incumbent is
    // healthy, connected, and could have reached the anchor the whole time.
    for node in &members {
        assert_eq!(sim.members_of(node), members, "{node} lost sight of a peer");
    }
    assert!(
        rounds_by(&sim, &incumbent, AnchorEvent::Renew, back_at - 1_000) > 0,
        "{label}: the host that lapsed was never reaching the store anyway — the \
         compound cost needs a *working* host to be a cost at all"
    );
    assert_eq!(
        rounds_by(&sim, &incumbent, AnchorEvent::Renew, lapsed_at),
        0,
        "{label}: an outranked host was still renewing"
    );

    // Heal the top-ranked node's anchor and the group is elected on the next
    // prompt — the availability was spent on the gate, not on anything broken.
    let healed_at = back_at + 8_000;
    sim.heal_anchor(&top);
    sim.run_until(Time(healed_at + GOSSIP_INTERVAL_MS + 2 * ANCHOR_LATENCY_MS));
    assert_eq!(sole_host(&sim, label), top);
    let (regained, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, top);
    assert!(
        regained > epoch,
        "{label}: {regained} does not fence the incumbent's {epoch}"
    );
    println!(
        "X-rank-compound: hostless from {lapsed_at} to {healed_at} \
         (stealable from {stealable_at}), epoch {epoch} -> {regained}"
    );
    assert_pure(&sim, &members, label);
}

// ---------------------------------------------------------------------------
// X-handback — renewal is rank-gated here too, so the group comes back.
// ---------------------------------------------------------------------------

/// How many yielded rounds the returning candidate is allowed to spend before
/// it is entitled to steal. The arithmetic it bounds: the incumbent stops being
/// prompted the moment gossip re-admits the returning node, its record then has
/// at most one `LEASE_MS` of TTL left, and a steal is entitled `STEAL_MARGIN_MS`
/// after that — so roughly `(LEASE_MS + STEAL_MARGIN_MS) / GOSSIP_INTERVAL_MS`
/// prompts, which is ~9, with the rest of this number as slack.
///
/// It is a *bound on cost*, and it is the whole point of the rank gate: without
/// one, this same window produces a yielded round every anti-entropy interval
/// for as long as the mismatch lasts, which is for ever.
const YIELD_BUDGET: usize = 20;

/// **X-handback.** An incumbent that is no longer the top-ranked live candidate
/// **lets its record lapse**, and the returning rendezvous top takes the group
/// back by superseding the expired record at a strictly higher epoch.
///
/// The shape is the ordinary one: the top-ranked node crashes, the second takes
/// the group, and then the first comes back. All three activations answer it the
/// same way, for one reason — **renewal is rank-gated under every activation**.
/// Row 5's tick-re-rank reads `is_coordinator()`; row Q7's renewal round opens
/// only for a coordinator ("a host that no longer ranks should be letting its
/// lease lapse, not asking the roster to extend it"); row X7's renewal prompt is
/// gated the same way. What differs between the three is only the *evidence*
/// that extends a lease — this node's own view, a fresh majority, a fresh anchor
/// round — never who is entitled to go looking for it.
///
/// The hand-back is walked event by event, and every step of it is asserted:
///
/// * the outranked incumbent **stops asking**: not one anchor round of its own
///   after it demotes, which is the rank gate in its falsifiable form;
/// * it demotes into a **hostless pair at its own epoch**, so the fence it held
///   still orders whatever comes next;
/// * the returning node takes the record by **stealing** it, never by being
///   handed it — there is no cooperative handoff in this milestone, and a record
///   is superseded on its expiry or not at all;
/// * and it costs a bounded handful of yielded rounds ([`YIELD_BUDGET`]) rather
///   than one per anti-entropy interval indefinitely.
///
/// Safety is not what is at stake either way: the anchor allocated both epochs,
/// so this handover is cross-epoch and store-fenced exactly like a crash-driven
/// one. What the gate buys is that the common case lands where the coordinator
/// ranking points, without pinning a candidate into a permanent store round trip
/// per gossip round.
#[test]
fn an_incumbent_that_loses_rank_lapses_and_the_rendezvous_top_takes_it_back() {
    let members = nodes(&["n1", "n2", "n3"]);
    let group = "x-handback";
    let label = "X-handback";
    let top = placement::owner(group, &members).expect("a non-empty cluster");
    let mut sim = cluster(group, &members, &cfg());
    sim.run_until(Time(2_000));
    assert_eq!(sole_host(&sim, label), top, "the premise: rank elected it");

    sim.crash(&top);
    sim.run_until(Time(5_000));
    let successor = sole_host(&sim, &format!("{label} (after the crash)"));
    assert_ne!(successor, top);
    let (epoch, _) = anchor_pair(&sim, label);

    // The top-ranked node returns. It outranks the incumbent again the moment
    // gossip re-admits it — and from that instant nothing prompts the incumbent.
    let back_at = 5_000;
    sim.add(engine(group, &top, &members, &cfg()));
    let (mut lapsed_at, mut retaken_at) = (None, None);
    step_through(&mut sim, back_at + 10_000, |sim, at| {
        if lapsed_at.is_none() && sim.role_of(&successor) != Some(Role::Host) {
            lapsed_at = Some(at);
        }
        if retaken_at.is_none() && sim.role_of(&top) == Some(Role::Host) {
            retaken_at = Some(at);
        }
    });
    let lapsed_at =
        lapsed_at.unwrap_or_else(|| panic!("{label}: the outranked incumbent never gave it up"));
    let retaken_at =
        retaken_at.unwrap_or_else(|| panic!("{label}: the rendezvous top never took it back"));
    assert!(
        lapsed_at <= retaken_at,
        "{label}: the successor was elected at {retaken_at} while the incumbent still \
         held the group until {lapsed_at} — a same-instant overlap, not a hand-back"
    );

    assert!(
        sim.is_member(&successor, &top) && sim.is_member(&top, &successor),
        "premise: the returning node is a live member again"
    );
    assert_eq!(
        placement::owner(group, &members),
        Some(top.clone()),
        "premise: it is the rendezvous owner of the live set once more"
    );

    assert_eq!(
        sole_host(&sim, label),
        top,
        "the group did not land back on the node the ranking points at"
    );
    let (regained, holder) = anchor_pair(&sim, label);
    assert_eq!(holder, top);
    assert!(
        regained > epoch,
        "a hand-back allocates like every other succession: {regained} does not fence {epoch}"
    );
    for node in &members {
        assert_eq!(
            pair_of(&sim, node),
            (regained, Some(top.clone())),
            "{node} did not converge on the register's pair after the hand-back"
        );
    }

    // The rank gate, in its falsifiable form: the demoted incumbent runs no
    // anchor round of any kind after it steps down — it is neither a host to
    // renew (row X7) nor a candidate to claim (row X1).
    let asked_after = sim
        .anchor_log
        .iter()
        .filter(|(at, who, _)| at.0 >= lapsed_at && who == &successor)
        .count();
    assert_eq!(
        asked_after, 0,
        "{label}: the demoted incumbent ran {asked_after} more anchor rounds: {:?}",
        sim.anchor_log
    );
    // And the returning node *stole* the record rather than being handed it.
    assert!(
        rounds_by(&sim, &top, AnchorEvent::Steal, back_at) > 0,
        "{label}: the record changed hands without being superseded on its expiry"
    );
    let yields = rounds_by(&sim, &top, AnchorEvent::Yield, back_at);
    assert!(
        yields <= YIELD_BUDGET,
        "{label}: the returning candidate spent {yields} rounds yielding, past the \
         {YIELD_BUDGET} the hand-back arithmetic allows — the pinned shape is back"
    );
    println!(
        "X-handback: lapsed at {lapsed_at}, retaken at {retaken_at} \
         (epoch {epoch} -> {regained}), {yields} yielded rounds"
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
