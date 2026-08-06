//! Deterministic Simulation Testing for **`Activation::External` under
//! wall-clock skew** — the one assumption the tier makes, held and then broken
//! on purpose.
//!
//! Everything else about `External` is clock-free: the anchor *allocates* the
//! epoch, so X-S1 (no two nodes ever hold the same epoch) survives any clock at
//! all, and `election_external.rs` runs its whole chaos corpus with arbitrary
//! skew to say so. Exactly one rule consults a clock —
//! [`AnchorRecord::stealable`], `now_wall_ms >= expires_at_wall_ms +
//! steal_margin_ms` — and this file is about that rule and nothing else.
//!
//! # The assumption, stated precisely, because the precision is load-bearing
//!
//! `Activation::External` says: *claimant wall-clock skew ≤
//! `steal_margin_ms`*. The quantity that actually enters the arithmetic is the
//! **pairwise disagreement between the claimant's clock and the holder's** —
//! the holder stamped `expires_at_wall_ms` from its clock and the claimant
//! judges it against its own. So the assumption to hold a family to is
//!
//! ```text
//! |skew(claimant) - skew(holder)| <= steal_margin_ms
//! ```
//!
//! and **not** "every node is within `steal_margin_ms` of true time", which
//! permits two nodes to disagree by *two* margins and is therefore a strictly
//! weaker premise than the rule needs. The within-margin family below draws
//! per-node offsets inside ±`margin/2` for exactly that reason, and the
//! boundary it pins is the pairwise one.
//!
//! # What each family is allowed to claim
//!
//! * **X-skew-a — [`dst_within_margin_never_overlaps`].** While the assumption
//!   holds, **at most one node believes it holds the group at any instant** —
//!   checked after *every scheduled event*, not on a sampling cadence, so the
//!   statement is absolute rather than "not observed at this resolution".
//! * **X-skew-b — [`dst_beyond_margin_overlaps_exactly_as_documented`].** When
//!   it is broken, the failure is the documented one and nothing worse: an
//!   overlap **occurs** (asserted per seed, so the family cannot pass by being
//!   lucky), is **bounded by the excess skew**, is **always cross-epoch**, and
//!   is **always resolved** — every observer ends on the higher pair. Where the
//!   deposed holder can still reach the store, what ends it is its own renewal
//!   coming back a mismatch, which is the store telling it it has been deposed.
//! * **X-ambiguity-a — [`dst_unknown_writes_never_double_activate`].** With a
//!   fifth of all conditional writes applying and reporting `Unknown`, the
//!   read-back rule never awards a hostship that was lost and never abdicates
//!   one that stood.
//! * **X-ambiguity-b —
//!   [`dst_unknown_writes_that_never_applied_lapse_the_lease_on_schedule`].**
//!   The other reading of the same timeout: the write **did not apply** and
//!   still said nothing — a write-throttled or read-only store, which is a
//!   *persistent* condition rather than a transient one. Every renewal then
//!   comes back ambiguous, and a read-back that judged the `(epoch, host)` pair
//!   alone would call each of them a win, because a renewal attempts exactly
//!   the pair that is already standing. The lease would extend for ever off a
//!   record ageing out beneath it, and a rival would steal it at
//!   `expires + margin` — two hosts, with **perfect clocks**, which is X-skew-a
//!   broken by a rule that never looked at a clock. Judged on the whole record
//!   the verdict is *lost*: the lease lapses at exactly the instant the last
//!   landed round bought it, and the group is hostless rather than doubly
//!   hosted.
//!
//! The bound in X-skew-b is `excess`, tighter than the envelope the design doc
//! states (excess + one renewal interval + one anchor latency). Both are
//! asserted; the tight one is what the arithmetic actually gives, and the
//! looser one is what a reader of the doc is entitled to rely on.
//!
//! [`AnchorRecord::stealable`]: groupnet_core::anchor::AnchorRecord::stealable

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    placement,
};
use groupnet_sim::{AnchorEvent, Simulation, SplitMix64};

/// A long lease, on purpose. The overlap this file is about lives *inside* the
/// deposed holder's remaining lease, so the lease has to be comfortably longer
/// than the detection window for the window to exist at all — and a deployment
/// that cares about clock skew is exactly one that has sized its lease well
/// above its detector.
const LEASE_MS: u64 = 2_000;
/// The anti-entropy cadence, which is also the renewal cadence and the prompt's.
const GOSSIP_INTERVAL_MS: u64 = 100;
/// The margin that absorbs pairwise clock disagreement.
const STEAL_MARGIN_MS: u64 = 300;
/// One store round trip.
const ANCHOR_LATENCY_MS: u64 = 20;
/// The per-node offset bound the within-margin family draws inside. Two nodes
/// at opposite ends of ±this disagree by **exactly** the steal margin, which is
/// the boundary the assumption is stated at — and the reason the bound is half
/// the margin rather than the margin.
fn half_margin() -> i64 {
    i64::try_from(STEAL_MARGIN_MS / 2).expect("a margin of a few hundred milliseconds")
}

/// A per-node clock offset drawn inside ±[`half_margin`], the extremes
/// included, so some pairs sit exactly on the boundary.
fn draw_skew(rng: &mut SplitMix64) -> i64 {
    let half = half_margin();
    let span = u32::try_from(2 * half + 1).expect("a small margin");
    i64::from(rng.below(span)) - half
}

fn cfg() -> Config {
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
            activation: Activation::External {
                steal_margin_ms: STEAL_MARGIN_MS,
            },
            lease_ms: LEASE_MS,
        }),
    }
}

fn engine(group: &str, id: &NodeId, peers: &BTreeSet<NodeId>) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(GroupId::new(group), id.clone(), seeds, cfg())
}

/// A cluster of `n` nodes with the anchor armed at the group's configuration.
fn cluster(group: &str, members: &BTreeSet<NodeId>) -> Simulation {
    let mut sim = Simulation::new(5);
    sim.enable_anchor(LEASE_MS, STEAL_MARGIN_MS);
    sim.set_anchor_latency(ANCHOR_LATENCY_MS);
    for id in members {
        sim.add(engine(group, id, members));
    }
    sim
}

fn ids(n: usize) -> BTreeSet<NodeId> {
    (0..n).map(|i| NodeId::new(format!("n{i}"))).collect()
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

/// **X-purity**, asserted at the end of every run here too. See
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

/// **X-S1**, folded over the whole run: no epoch was ever activated by two
/// nodes. Unconditional, and in this file it is the control — whatever a broken
/// clock costs, it is never this.
fn assert_sole_activator_per_epoch(log: &[(NodeId, u64, Option<NodeId>)], label: &str) {
    let mut by_epoch: BTreeMap<u64, NodeId> = BTreeMap::new();
    for (observer, epoch, host) in log {
        if host.as_ref() != Some(observer) {
            continue;
        }
        if let Some(first) = by_epoch.insert(*epoch, observer.clone()) {
            assert_eq!(
                &first, observer,
                "{label}: epoch {epoch} was activated by both {first} and {observer}"
            );
        }
    }
}

/// Every node that currently believes it hosts, with the epoch it believes it
/// holds — the split-brain probe, read at one instant.
fn hosting_pairs(sim: &Simulation) -> Vec<(NodeId, u64)> {
    sim.hosts()
        .into_iter()
        .map(|node| {
            let (epoch, _) = sim
                .leadership_of(&node)
                .expect("a host is in the simulation");
            (node, epoch)
        })
        .collect()
}

// ---------------------------------------------------------------------------
// X-skew-a — inside the assumption, the property is absolute.
// ---------------------------------------------------------------------------

/// **X-skew-a.** 96 seeds of adversarial handovers with every pair of clocks
/// disagreeing by at most the steal margin: **`hosts().len() <= 1` at every
/// scheduled event of every run**.
///
/// "Adversarial" means the schedule is built out of the only things that can
/// produce two hosts — a handover — and then produces as many as it can: the
/// anchor is cut off from the incumbent (so its record ages out), the fabric is
/// cut around it (so a successor becomes a candidate at all), both are healed
/// at drawn instants (so a deposed holder can come back and find its etag
/// stale), and hosts crash and restart underneath.
///
/// The check is per *event*, not per millisecond and not per round: an engine's
/// role can only change when it processes a frame, a tick or an anchor round's
/// command, so a claim checked after every event is checked at every instant it
/// could have become false. That is what earns the word "absolute" here, where
/// `election_quorum.rs` says "sampled every 5ms".
///
/// The arithmetic behind it, stated so a failing seed can be read against
/// something: a claimant `B` may supersede holder `A`'s record no earlier than
/// virtual `t_A + TTL + margin + skew(A) - skew(B)`, where `t_A` is when `A`'s
/// last winning round reached the store; and `A` keeps hosting only until
/// `t_A - anchor_latency + TTL`, because its engine lease was anchored at the
/// instant that round *began*. So an overlap needs
/// `skew(B) - skew(A) > margin + anchor_latency` — impossible while every pair
/// is within the margin, with a whole anchor latency to spare.
#[test]
fn dst_within_margin_never_overlaps() {
    let mut steals = 0u64;
    let mut handovers = 0u64;
    for seed in 0..96u64 {
        let (stole, changed) = within_margin(seed);
        steals += stole;
        handovers += changed;
    }
    assert!(
        steals > 0,
        "vacuous: no seed ever superseded a record, so nothing was ever at risk"
    );
    assert!(
        handovers > 0,
        "vacuous: the group never changed hands, which is the only way to get two hosts"
    );
    println!("X-skew-a: {steals} steals, {handovers} handovers, never two hosts");
}

/// Returns `(steals, handovers)` for one seed.
fn within_margin(seed: u64) -> (u64, u64) {
    let mut rng = SplitMix64::new(seed ^ 0x5c0f_9e37_79b9_7f4a);
    let n = 3 + usize::try_from(rng.below(3)).expect("a 0..3 draw"); // 3..=5
    let members = ids(n);
    let group = format!("x-skew-{seed}");
    let label = format!("X-skew-a seed {seed}");
    let mut sim = cluster(&group, &members);
    sim.set_jitter(u64::from(rng.below(6)));

    // Per-node offsets inside ±margin/2, so every *pair* is within the margin —
    // the assumption in the form the rule actually uses.
    for node in &members {
        sim.set_anchor_skew(node, draw_skew(&mut rng));
    }
    sim.run_until(Time(3_000));
    let first = sole_host(&sim, &format!("{label} (bootstrap)"));

    let mut hosts_seen: BTreeSet<NodeId> = [first].into_iter().collect();
    let mut now = 3_000u64;
    for _round in 0..8 {
        // One drawn fault per round, all of them handover pressure.
        match rng.below(6) {
            0 => {
                if let Some(host) = sim.hosts().first() {
                    sim.block_anchor(host);
                }
            }
            1 => sim.heal_anchor_all(),
            2 => {
                // Cut the current host off from everybody, so a successor side
                // exists at all.
                if let Some(host) = sim.hosts().first().cloned() {
                    for other in sim.nodes().iter().filter(|x| **x != host) {
                        sim.block(&host, other);
                        sim.block(other, &host);
                    }
                }
            }
            3 => sim.heal_all(),
            4 if sim.nodes().len() > 2 => {
                if let Some(host) = sim.hosts().first().cloned() {
                    sim.crash(&host);
                }
            }
            _ => {
                let down: Vec<NodeId> = members
                    .iter()
                    .filter(|x| !sim.nodes().contains(x))
                    .cloned()
                    .collect();
                if let Some(node) = down.first() {
                    sim.add(engine(&group, node, &members));
                }
            }
        }

        now += 1_000 + u64::from(rng.below(2_000));
        while let Some(Time(at)) = sim.step_until(Time(now)) {
            let hosting = hosting_pairs(&sim);
            assert!(
                hosting.len() <= 1,
                "{label}: two nodes held the group at {at} with every clock inside the \
                 margin: {hosting:?}"
            );
            if let Some((node, _)) = hosting.first() {
                hosts_seen.insert(node.clone());
            }
        }
    }

    sim.heal_all();
    sim.heal_anchor_all();
    now += 12_000;
    sim.run_until(Time(now));
    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
    assert_pure(&sim, &members, &label);

    let steals = sim
        .anchor_log
        .iter()
        .filter(|(_, _, event)| *event == AnchorEvent::Steal)
        .count();
    (
        u64::try_from(steals).expect("a bounded run"),
        u64::try_from(hosts_seen.len() - 1).expect("a bounded run"),
    )
}

// ---------------------------------------------------------------------------
// X-skew-b — outside the assumption, the documented failure and nothing worse.
// ---------------------------------------------------------------------------

/// The overlap one seed produced.
#[derive(Debug)]
struct Overlap {
    /// When two nodes first both believed they held the group.
    from: u64,
    /// When that stopped being true.
    until: u64,
    /// Whether the deposed holder ended it by *adopting* the successor's pair —
    /// which is the store telling it, through a failed renewal, that it has
    /// been deposed — rather than by simply running out of lease.
    ended_by_mismatch: bool,
}

/// **X-skew-b.** The assumption broken on purpose, by exactly a drawn excess,
/// and the resulting failure pinned to the millisecond.
///
/// The shape is the one the rule cares about: an incumbent loses the store (so
/// its record stops being renewed and ages out) and the fabric around it (so a
/// successor becomes a candidate), and the successor's clock is **fast by
/// `margin + anchor_latency + excess`** relative to the incumbent's. That is
/// the exact quantity the arithmetic in [`dst_within_margin_never_overlaps`]
/// says must be exceeded, so every seed here produces a real overlap rather
/// than hoping for one.
///
/// Four things are asserted per seed, and they are the four the design doc
/// claims:
///
/// * **It happens.** Asserted, not hoped for: a family that quietly stopped
///   producing overlaps would be pinning nothing at all.
/// * **It is bounded by the excess.** The deposed holder's lease outlives the
///   successor's entitlement by exactly the excess (less one anchor latency),
///   so that is the bound — tighter than the doc's stated envelope of
///   `excess + one renewal interval + one anchor latency`, and both are checked.
/// * **It is always cross-epoch.** At every instant of the overlap the two
///   believers hold *different* epochs, and the successor's is strictly the
///   higher. There is never a same-epoch duel to arbitrate, because the anchor
///   allocated both numbers — which is why a fence token settles every ordering
///   question the overlap raises.
/// * **It always resolves.** Every observer ends on the higher pair. Where the
///   deposed holder can reach the store again during the overlap (half the
///   seeds heal its anchor the moment the overlap starts), what ends it is its
///   own renewal returning a **mismatch**: the store refuses the write, the
///   driver reports the pair it read, and row X4 deposes it. That path is
///   asserted distinctly from the plain lease lapse.
#[test]
fn dst_beyond_margin_overlaps_exactly_as_documented() {
    let mut overlaps = 0u64;
    let mut by_mismatch = 0u64;
    let mut longest = 0u64;
    for seed in 0..64u64 {
        let overlap = beyond_margin(seed);
        overlaps += 1;
        by_mismatch += u64::from(overlap.ended_by_mismatch);
        longest = longest.max(overlap.until - overlap.from);
    }
    assert!(
        overlaps > 0,
        "vacuous: the family produced no overlap at all"
    );
    assert!(
        by_mismatch > 0,
        "vacuous: no seed ever ended an overlap through a failed renewal"
    );
    println!(
        "X-skew-b: {overlaps} overlaps ({by_mismatch} ended by a renewal mismatch), \
         longest {longest}ms"
    );
}

fn beyond_margin(seed: u64) -> Overlap {
    let mut rng = SplitMix64::new(seed ^ 0xb3ad_9e37_79b9_7f4a);
    let members = ids(3);
    let group = format!("x-excess-{seed}");
    let label = format!("X-skew-b seed {seed}");
    let mut sim = cluster(&group, &members);
    sim.run_until(Time(3_000));

    let incumbent = sole_host(&sim, &format!("{label} (bootstrap)"));
    let (epoch, _) = sim
        .leadership_of(&incumbent)
        .expect("the incumbent is in the simulation");
    let others: BTreeSet<NodeId> = members
        .iter()
        .filter(|n| **n != incumbent)
        .cloned()
        .collect();
    let successor = placement::owner(&group, &others).expect("two other nodes");

    // Break the assumption by a drawn excess. The floor keeps every seed's
    // overlap wide enough to survive the prompt cadence and the round trip that
    // the successor's steal costs — so the family cannot degenerate into "the
    // overlap was theoretically possible but never observed".
    let excess = u64::from(rng.below(800)) + GOSSIP_INTERVAL_MS + ANCHOR_LATENCY_MS + 50;
    let delta = STEAL_MARGIN_MS + ANCHOR_LATENCY_MS + excess;
    sim.set_anchor_skew(
        &successor,
        i64::try_from(delta).expect("a drawn excess is small"),
    );
    // The incumbent loses the store, and the fabric around it: the record ages
    // out, and the successor becomes a candidate.
    sim.block_anchor(&incumbent);
    for other in &others {
        sim.block(&incumbent, other);
        sim.block(other, &incumbent);
    }
    // Half the seeds give the incumbent its store back the moment it is deposed,
    // which is what lets its own renewal tell it so.
    let heal_on_overlap = rng.below(2) == 0;

    let deadline = 3_000 + 3 * LEASE_MS + 4_000;
    let (from, until) = watch_for_overlap(
        &mut sim,
        Shape {
            incumbent: &incumbent,
            successor: &successor,
            epoch,
            heal_on_overlap,
        },
        (deadline, &label),
    );

    let from = from.unwrap_or_else(|| {
        panic!(
            "{label}: no overlap at all with the successor's clock {delta}ms fast — \
             the family stopped exercising its own property"
        )
    });
    let until = until.unwrap_or_else(|| panic!("{label}: the overlap never ended"));
    let width = until - from;
    assert!(
        width <= excess,
        "{label}: the overlap ran {width}ms, past the {excess}ms of excess skew that bounds it"
    );
    assert!(
        width <= excess + GOSSIP_INTERVAL_MS + ANCHOR_LATENCY_MS,
        "{label}: the overlap ran past the envelope the design doc states"
    );

    // However it ended, it ended: the deposed holder is a follower on the
    // successor's pair, and so is everybody else.
    sim.heal_all();
    sim.heal_anchor_all();
    sim.run_until(Time(deadline + 8_000));
    let host = sole_host(&sim, &format!("{label} (resolved)"));
    let (settled, _) = sim.leadership_of(&host).expect("a live host");
    for node in &members {
        assert_eq!(
            sim.leadership_of(node),
            Some((settled, Some(host.clone()))),
            "{label}: {node} did not resolve onto the higher pair"
        );
    }
    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
    assert_pure(&sim, &members, &label);

    // Did the incumbent find out from the store, or just run out of lease? A
    // mismatch shows up as a Yield round by a node that was still hosting, and
    // as a *demotion into somebody else's pair* rather than into a hostless one.
    let ended_by_mismatch = heal_on_overlap
        && sim.anchor_log.iter().any(|(at, node, event)| {
            at.0 >= from && at.0 <= until && *node == incumbent && *event == AnchorEvent::Yield
        });
    if ended_by_mismatch {
        assert!(
            sim.leadership_log.iter().any(|(observer, e, host)| {
                *observer == incumbent && *e > epoch && host.as_ref() == Some(&successor)
            }),
            "{label}: the incumbent's renewal came back a mismatch and it did not adopt \
             the pair the store showed it"
        );
    }
    Overlap {
        from,
        until,
        ended_by_mismatch,
    }
}

/// The two nodes an overlap is between, and what to do when it starts.
#[derive(Clone, Copy, Debug)]
struct Shape<'a> {
    /// The holder whose record is ageing out.
    incumbent: &'a NodeId,
    /// The node whose fast clock entitles it to steal early.
    successor: &'a NodeId,
    /// The epoch the incumbent holds.
    epoch: u64,
    /// Whether to give the incumbent its store back the instant it is deposed,
    /// so its own renewal is what tells it.
    heal_on_overlap: bool,
}

/// Steps to `deadline` one event at a time, asserting the *shape* of any
/// overlap at every instant it exists — two believers, never three; two
/// distinct epochs, never one; and the successor's strictly above the
/// incumbent's — and returning when it started and when it ended.
fn watch_for_overlap(
    sim: &mut Simulation,
    shape: Shape<'_>,
    clock: (u64, &str),
) -> (Option<u64>, Option<u64>) {
    let (deadline, label) = clock;
    let (mut from, mut until) = (None, None);
    while let Some(Time(at)) = sim.step_until(Time(deadline)) {
        let hosting = hosting_pairs(sim);
        if hosting.len() <= 1 {
            if from.is_some() && until.is_none() {
                until = Some(at);
            }
            continue;
        }
        assert_eq!(
            hosting.len(),
            2,
            "{label}: three hosts at {at}: {hosting:?}"
        );
        let epochs: BTreeSet<u64> = hosting.iter().map(|(_, e)| *e).collect();
        assert_eq!(
            epochs.len(),
            2,
            "{label}: an overlap at {at} was *same-epoch*, which the anchor makes \
             impossible: {hosting:?}"
        );
        let Some((_, held)) = hosting.iter().find(|(node, _)| node == shape.successor) else {
            panic!("{label}: an overlap at {at} without the successor: {hosting:?}");
        };
        assert!(
            *held > shape.epoch,
            "{label}: the successor's epoch {held} does not fence the incumbent's {}",
            shape.epoch
        );
        if from.is_none() {
            from = Some(at);
            if shape.heal_on_overlap {
                sim.heal_anchor(shape.incumbent);
            }
        }
    }
    (from, until)
}

// ---------------------------------------------------------------------------
// X-ambiguity-a — a write that applied and said nothing.
// ---------------------------------------------------------------------------

/// **X-ambiguity-a.** A fifth to a half of every conditional write applies and
/// reports `Unknown`, and the driver resolves each one the only honest way: by
/// reading the record back, one store round trip later, and judging it with
/// `ambiguous_applied`.
///
/// Because the read-back is a *later instant*, the record really can have moved
/// on underneath it — that is the case the fail-closed rule exists for, and it
/// is reachable here rather than hypothetical. Asserted:
///
/// * **Never double-activates.** X-S1 over the whole run: an ambiguous write
///   that another node superseded before the read-back must not be read as won.
/// * **Never falsely abdicates.** Read as liveness, which is the only way it is
///   observable from outside: a run in which read-backs wrongly concluded "not
///   applied" would churn hostships and end somewhere other than on the
///   register's own pair. Every seed ends with exactly one host, and it is the
///   node the register names. (The converse half — never falsely *keeps* an
///   authority a lost write never earned — is
///   [`dst_unknown_writes_that_never_applied_lapse_the_lease_on_schedule`],
///   because no schedule in which every write applies can produce it.)
/// * **At most one host at any event point**, since these seeds keep every pair
///   of clocks inside the margin — an ambiguity is not a licence to overlap.
#[test]
fn dst_unknown_writes_never_double_activate() {
    let mut resolutions = 0u64;
    for seed in 0..64u64 {
        resolutions += unknown_writes(seed);
    }
    assert!(
        resolutions > 0,
        "vacuous: not one write in the corpus came back Unknown"
    );
    println!("X-ambiguity: {resolutions} ambiguous writes resolved by read-back");
}

fn unknown_writes(seed: u64) -> u64 {
    let mut rng = SplitMix64::new(seed ^ 0x0a3b_9e37_79b9_7f4a);
    let members = ids(3 + usize::try_from(rng.below(2)).expect("a 0..2 draw"));
    let group = format!("x-unknown-{seed}");
    let label = format!("X-ambiguity seed {seed}");
    let mut sim = cluster(&group, &members);
    sim.set_jitter(u64::from(rng.below(6)));
    sim.set_anchor_unknown_percent(u8::try_from(20 + rng.below(31)).expect("20..=50"));
    for node in &members {
        sim.set_anchor_skew(node, draw_skew(&mut rng));
    }

    let mut now = 0u64;
    for _round in 0..6 {
        now += 1_500 + u64::from(rng.below(1_500));
        while let Some(Time(at)) = sim.step_until(Time(now)) {
            let hosting = hosting_pairs(&sim);
            assert!(
                hosting.len() <= 1,
                "{label}: two hosts at {at} — an ambiguous write is not a licence to \
                 overlap: {hosting:?}"
            );
        }
        // Keep the group changing hands, so ambiguity lands on creates, steals
        // and renewals alike rather than only on a quiet renewal loop.
        match rng.below(3) {
            // Never below two live nodes: a cluster that crashed its way to
            // empty would end the run hostless for a reason that has nothing to
            // do with ambiguity.
            0 if sim.nodes().len() > 2 => {
                if let Some(host) = sim.hosts().first().cloned() {
                    sim.crash(&host);
                }
            }
            1 => {
                let down: Vec<NodeId> = members
                    .iter()
                    .filter(|x| !sim.nodes().contains(x))
                    .cloned()
                    .collect();
                if let Some(node) = down.first() {
                    sim.add(engine(&group, node, &members));
                }
            }
            _ => {
                if let Some(host) = sim.hosts().first().cloned() {
                    sim.block_anchor(&host);
                } else {
                    sim.heal_anchor_all();
                }
            }
        }
    }

    sim.heal_anchor_all();
    sim.heal_all();
    sim.set_anchor_unknown_percent(0);
    now += 15_000;
    sim.run_until(Time(now));

    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
    assert_pure(&sim, &members, &label);
    let host = sole_host(&sim, &format!("{label} (settled)"));
    let record = sim
        .anchor_record()
        .unwrap_or_else(|| panic!("{label}: the register is empty after a whole run"));
    assert_eq!(
        record.host, host,
        "{label}: the register names {} while the cluster hosts on {host} — a read-back \
         abdicated a record that was standing",
        record.host
    );
    for node in sim.nodes() {
        assert_eq!(
            sim.leadership_of(&node),
            Some((record.epoch, Some(host.clone()))),
            "{label}: {node} did not converge on the register's pair"
        );
    }
    assert_ne!(
        sim.role_of(&host),
        Some(Role::Claimant),
        "{label}: External entered Role::Claimant"
    );
    sim.anchor_unknown_rounds()
}

// ---------------------------------------------------------------------------
// X-ambiguity-b — a write that did *not* apply and said nothing.
// ---------------------------------------------------------------------------

/// **X-ambiguity-b.** 32 seeds of the store that swallows writes: every
/// conditional `PUT` reports `Unknown` and **applies nothing**, which is what a
/// write throttle, a read-only window or an expired write credential looks like
/// from a driver — and, unlike a timeout, it is a *standing* condition rather
/// than a coin flip.
///
/// The renewal is the case it is dangerous for. A renewal keeps the epoch and
/// the host and only moves the expiry, so an attempted renewal's
/// `(epoch, host)` is byte-identical to the record already standing: read back
/// on the pair alone, **every failed renewal resolves as won**. The lease would
/// then be extended by writes that never happened, indefinitely, off a record
/// nobody was refreshing — and the schedule below is the one that turns that
/// into two hosts: the incumbent is cut off from the fabric (so the other side
/// buries it and becomes entitled to the record the moment it ages out) while
/// **every clock in the run is perfect**. Two hosts here would be X-skew-a
/// broken by a rule that never consulted a clock at all.
///
/// Judged on the whole record — `ambiguous_applied` compares
/// `expires_at_wall_ms` too, which a renewal always moves strictly forward —
/// each of those rounds is *lost*, and every seed asserts:
///
/// * **The lease lapses on schedule**: the incumbent gives the group up at
///   exactly the instant the last round that really landed bought it, not one
///   tick later and not for ever.
/// * **No overlap, ever**, checked after every scheduled event of the whole
///   run — through the fault, through the heal, and through the succession that
///   follows.
/// * **Nothing reached the register.** The record is byte-identical at the end
///   of the fault to what it was at the start, which is what makes "the write
///   never applied" a premise rather than a hope.
/// * **The group is hostless, not doubly hosted**, for the whole stretch
///   between the lapse and the heal — a swallowed write elects nobody.
/// * **And it recovers**: once writes land again the group comes back on one
///   host at a **strictly higher** epoch, because hostship is re-won and never
///   resumed.
#[test]
fn dst_unknown_writes_that_never_applied_lapse_the_lease_on_schedule() {
    let (mut lost, mut widest) = (0u64, 0u64);
    for seed in 0..32u64 {
        let (rounds, hostless) = unknown_lost_writes(seed);
        lost += rounds;
        widest = widest.max(hostless);
    }
    assert!(
        lost > 0,
        "vacuous: not one write in the corpus was swallowed"
    );
    println!("X-ambiguity-b: {lost} writes lost to the store, longest hostless stretch {widest}ms");
}

/// Returns `(lost rounds, the hostless stretch this seed produced)`.
fn unknown_lost_writes(seed: u64) -> (u64, u64) {
    let mut rng = SplitMix64::new(seed ^ 0x7d41_9e37_79b9_7f4a);
    let members = ids(3 + usize::try_from(rng.below(2)).expect("a 0..2 draw"));
    let group = format!("x-lost-{seed}");
    let label = format!("X-ambiguity-b seed {seed}");
    let mut sim = cluster(&group, &members);
    sim.set_jitter(u64::from(rng.below(6)));
    // Every clock is exact, on purpose: nothing this family produces may be
    // attributable to skew. It is X-skew-a's premise, held exactly.
    let fault_at = 4_000 + u64::from(rng.below(400));
    sim.run_until(Time(fault_at));

    let incumbent = sole_host(&sim, &format!("{label} (bootstrap)"));
    let (epoch, _) = sim
        .leadership_of(&incumbent)
        .expect("the incumbent is in the simulation");
    let record = sim
        .anchor_record()
        .expect("an elected group has left a record");
    let lease = sim
        .lease_until_of(&incumbent)
        .expect("a host holds a lease")
        .0;

    // The fault. From here every write reports `Unknown` and applies nothing,
    // so the last round that landed is the last authority anybody earned...
    sim.set_anchor_unknown_lost_percent(100);
    // ...and the fabric is cut around the incumbent, so the other side buries
    // it and becomes entitled to steal the record it is no longer refreshing.
    for other in members.iter().filter(|n| **n != incumbent) {
        sim.block(&incumbent, other);
        sim.block(other, &incumbent);
    }

    let heal_at = fault_at + 3 * LEASE_MS;
    let lapsed_at = watch_the_lapse(&mut sim, &incumbent, (fault_at, heal_at), &label);
    assert_eq!(
        lapsed_at, lease,
        "{label}: the lease lapses at exactly the instant the last round that really \
         applied bought it"
    );
    assert_eq!(
        sim.anchor_record().as_ref(),
        Some(&record),
        "{label}: a write the store swallowed still reached the register"
    );
    let renewals = sim
        .anchor_log
        .iter()
        .filter(|(at, who, event)| {
            at.0 > fault_at && who == &incumbent && *event == AnchorEvent::Renew
        })
        .count();
    assert!(
        renewals > 0,
        "{label}: the incumbent never attempted a renewal during the fault, so nothing \
         was ambiguous and the family proved nothing"
    );

    // Heal both faults. Writes land again, and the group comes back — on one
    // host, at a strictly higher epoch, because hostship is re-won not resumed.
    sim.set_anchor_unknown_lost_percent(0);
    sim.heal_all();
    while let Some(Time(at)) = sim.step_until(Time(heal_at + 12_000)) {
        let hosting = hosting_pairs(&sim);
        assert!(
            hosting.len() <= 1,
            "{label}: two hosts at {at} during the recovery: {hosting:?}"
        );
    }
    let host = sole_host(&sim, &format!("{label} (recovered)"));
    let regained = sim
        .anchor_record()
        .expect("the register survived the outage");
    assert_eq!(
        regained.host, host,
        "{label}: the register names somebody else"
    );
    assert!(
        regained.epoch > epoch,
        "{label}: {host} resumed epoch {epoch} instead of winning {} above it",
        regained.epoch
    );
    for node in &members {
        assert_eq!(
            sim.leadership_of(node),
            Some((regained.epoch, Some(host.clone()))),
            "{label}: {node} did not converge on the register's pair"
        );
    }
    assert_sole_activator_per_epoch(&sim.leadership_log, &label);
    assert_pure(&sim, &members, &label);
    (sim.anchor_unknown_lost_rounds(), heal_at - lapsed_at)
}

/// Steps through the fault window one event at a time and returns the instant
/// the incumbent gave the group up.
///
/// Two claims are checked at every one of those instants, which is every
/// instant an engine could have changed its mind: **never two hosts** (the
/// failure a mis-resolved renewal produces, here with perfect clocks), and —
/// once the incumbent has lapsed — **nobody at all**, because a store that
/// applies nothing cannot award an epoch to the side that buried it either.
fn watch_the_lapse(
    sim: &mut Simulation,
    incumbent: &NodeId,
    window: (u64, u64),
    label: &str,
) -> u64 {
    let (fault_at, heal_at) = window;
    let mut lapsed_at = None;
    while let Some(Time(at)) = sim.step_until(Time(heal_at)) {
        let hosting = hosting_pairs(sim);
        assert!(
            hosting.len() <= 1,
            "{label}: two nodes held the group at {at} with every clock exact — a write \
             that never landed extended a lease: {hosting:?}"
        );
        if lapsed_at.is_none() && sim.role_of(incumbent) != Some(Role::Host) {
            lapsed_at = Some(at);
        }
        if let Some(from) = lapsed_at {
            assert!(
                hosting.is_empty(),
                "{label}: {hosting:?} hosted at {at} on a store that has applied nothing \
                 since {fault_at} (the incumbent lapsed at {from})"
            );
        }
    }
    lapsed_at.unwrap_or_else(|| {
        panic!(
            "{label}: the incumbent still held the group {heal_at}ms in, on a lease no \
             landed write has extended since {fault_at} — a failed renewal was read as won"
        )
    })
}
