//! Deterministic Simulation Testing for **Hosted-mode failover and fencing** —
//! what the `Activation::Settle` tier does when the host goes away or the
//! cluster is cut in two.
//!
//! * **E2 — [`dst_new_host_activates_inside_the_advertised_budget`].** Failover
//!   is honest about its own cost: after the host crashes a successor is
//!   hosting within `detection_window_ms(n) + claim_settle_ms` plus one hop and
//!   one tick — see [`NEW_HOST_SLACK_MS`], the *whole* of the slack granted.
//! * **E3 — [`dst_split_brain_heals_to_one_fenced_host`].** Both sides of a
//!   partition host, as the AP posture promises. At heal exactly one survives,
//!   the loser *announces* its demotion, and the survivor is the rendezvous top
//!   of the healed group.
//! * **E4 — [`dst_same_epoch_collision_is_fenced_deterministically`].** The
//!   case the pair order exists for: two sides settle the **same** epoch under
//!   different hosts. At heal the survivor is the one the equal-epoch tiebreak
//!   names — recomputed from [`placement`] alone, so the engine is not being
//!   checked against itself.
//!
//! The chaos suite is in `election.rs`; mode invariance is in `dst.rs`.
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Time, placement,
};
use groupnet_sim::{Simulation, SplitMix64};

/// How long a claim must stand unchallenged before its claimant activates.
const CLAIM_SETTLE_MS: u64 = 200;
/// How long a host's authority survives its last successful renewal.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence — the coarsest periodic deadline the
/// engine arms, and so the granularity at which it observes time.
const GOSSIP_INTERVAL_MS: u64 = 60;
/// The largest link latency any suite here configures.
const MAX_LATENCY_MS: u64 = 6;
/// The largest per-message reorder jitter any suite here configures.
const MAX_JITTER_MS: u64 = 5;

/// Seeds the shared deterministic PRNG so each schedule is reproducible. Each
/// suite salts the seed so the three of them explore independent streams.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

/// The Hosted timings these suites run on — the same ones `election.rs` uses,
/// and sized against [`HostedConfig`]'s rule there. `dead_timeout_ms` is 1s
/// (tombstones reaped at 2s): long enough that a partitioned side is still
/// gossiping about the other for the whole of a split.
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
            activation: Activation::Settle {
                claim_settle_ms: CLAIM_SETTLE_MS,
            },
            lease_ms: LEASE_MS,
        }),
    }
}

/// An engine for `id` bootstrapped against every other node in `peers`. The
/// seed set is what lets two sides of a hard partition find each other after a
/// heal: an engine keeps offering digests to its seeds even once it has buried
/// them.
fn engine(group: &GroupId, id: &NodeId, peers: &BTreeSet<NodeId>) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg())
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    v[rng.below(n) as usize].clone()
}

/// [`pick`], removing the drawn node — a draw without replacement.
fn take(set: &mut BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let node = pick(set, rng);
    set.remove(&node);
    node
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

/// The pair `node` has adopted. Every caller here reads a node it knows is in
/// the simulation.
fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

/// The single node that believes it holds the group, asserting there is exactly
/// one. `Simulation::hosts` yields ids in order, so failure text is stable.
fn sole_host(sim: &Simulation, suite: &str, seed: u64) -> NodeId {
    let hosts = sim.hosts();
    assert_eq!(
        hosts.len(),
        1,
        "{suite} seed {seed}: expected exactly one host, got {hosts:?}"
    );
    hosts.into_iter().next().expect("length asserted above")
}

// ---------------------------------------------------------------------------
// E2 — failover inside the advertised budget.
// ---------------------------------------------------------------------------

/// The **whole** slack E2 grants on top of the budget the crate advertises
/// (`detection_window_ms(n) + claim_settle_ms`), and where every millisecond of
/// it goes:
///
/// * **one hop** (`MAX_LATENCY_MS + MAX_JITTER_MS`) — the budget is stated in
///   *engine* time, and the successor's activation still has to cross the wire
///   as a `LeadState` before the other survivors agree on the pair.
/// * **one tick** ([`GOSSIP_INTERVAL_MS`], the coarsest periodic deadline the
///   engine arms) — the engine only observes time when it is ticked. Both
///   election deadlines *are* armed exactly (the settle window arms the driver
///   timer, and the claim guard is re-read on the very tick that buries the old
///   host), so one tick covers the rounding rather than two.
///
/// Nothing else: no fudge term, no "and a bit" — the conservatism has to live
/// in the formula, not in the test.
const NEW_HOST_SLACK_MS: u64 = MAX_LATENCY_MS + MAX_JITTER_MS + GOSSIP_INTERVAL_MS;

/// **E2.** When the host crashes, a new one is serving inside the advertised
/// failover budget — and it is the right one, at a strictly higher epoch, with
/// every survivor already naming it.
///
/// Also **S1-strict** for these calm seeds: fold the whole run's leadership log
/// and require at most one activator per epoch. Split-brain needs a partition;
/// without one, an epoch names exactly one serializer.
#[test]
fn dst_new_host_activates_inside_the_advertised_budget() {
    for seed in 0..64u64 {
        new_host_budget(seed);
    }
}

fn new_host_budget(seed: u64) {
    let mut rng = rng(seed ^ 0xe2b0);
    let group = GroupId::new(format!("budget-{seed}"));
    let n = 3 + rng.below(4); // 3..=6 nodes
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

    // A healthy fabric: no loss, but jittered, so links reorder.
    let mut sim = Simulation::new(2 + u64::from(rng.below(5)));
    sim.set_jitter(u64::from(rng.below(6)));
    for id in &ids {
        sim.add(engine(&group, id, &all));
    }

    // Converge first: the budget bounds *failover*, not bootstrap. The crash
    // instant is drawn too, so the schedule samples the phase of the crash
    // against each survivor's probe cursor and the incumbent's renewal tick.
    let crash_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(crash_at));

    let incumbent = sole_host(&sim, "E2", seed);
    let top = placement::owner(group.as_str(), &all).expect("a non-empty group");
    assert_eq!(
        incumbent, top,
        "E2 seed {seed}: the group settled on {incumbent}, not the rendezvous owner {top}"
    );
    let (epoch, _) = pair_of(&sim, &incumbent);
    for id in &ids {
        assert_eq!(
            pair_of(&sim, id),
            (epoch, Some(incumbent.clone())),
            "E2 seed {seed}: {id} had not adopted the incumbent's pair before the crash"
        );
    }

    sim.crash(&incumbent);
    let survivors: BTreeSet<NodeId> = all.iter().filter(|x| **x != incumbent).cloned().collect();
    let budget = cfg().detection_window_ms(ids.len()) + CLAIM_SETTLE_MS + NEW_HOST_SLACK_MS;
    sim.run_until(Time(crash_at + budget));

    let successor = sole_host(&sim, "E2", seed);
    let expected = placement::owner(group.as_str(), &survivors).expect("at least two survivors");
    assert_eq!(
        successor, expected,
        "E2 seed {seed} ({n} nodes): {successor} took the group {budget}ms after the crash, \
         not the survivors' rendezvous owner {expected}"
    );
    let (next, _) = pair_of(&sim, &successor);
    assert!(
        next > epoch,
        "E2 seed {seed}: {successor} activated epoch {next}, which does not fence epoch {epoch}"
    );
    for id in &survivors {
        assert_eq!(
            pair_of(&sim, id),
            (next, Some(successor.clone())),
            "E2 seed {seed}: {id} had not adopted the new pair within the {budget}ms budget"
        );
    }

    assert_sole_activator_per_epoch(&sim.leadership_log, seed);
}

/// **S1-strict**: across the whole run, no two nodes ever activated the same
/// epoch. Only a partition produces two serializers on one epoch number, and
/// these seeds run without one.
fn assert_sole_activator_per_epoch(log: &[(NodeId, u64, Option<NodeId>)], seed: u64) {
    let mut by_epoch: BTreeMap<u64, NodeId> = BTreeMap::new();
    for (observer, epoch, host) in log {
        if host.as_ref() != Some(observer) {
            continue; // not an activation — an adoption or a step down
        }
        if let Some(first) = by_epoch.insert(*epoch, observer.clone()) {
            assert_eq!(
                &first, observer,
                "E2 seed {seed}: epoch {epoch} was activated by both {first} and {observer}"
            );
        }
    }
}

// ---------------------------------------------------------------------------
// E3 — split brain, then one fenced host.
// ---------------------------------------------------------------------------

/// How long a split is held past the detect-and-settle budget before it heals:
/// enough for the side without the incumbent to bury it and settle a claim of
/// its own, and short of the 2s reap horizon.
const SPLIT_HOLD_SLACK_MS: u64 = 400;

/// How long the healed cluster is given to reconcile two beliefs into one.
const HEAL_SETTLE_MS: u64 = 15_000;

/// **E3.** Both sides of a two-way partition host — the AP posture, stated
/// plainly — and at heal exactly one survives.
///
/// The split is drawn per seed as a 1/4 or a 2/3, with the incumbent on either
/// side of the cut. During the split there are exactly two hosts at different
/// epochs; after the heal there is one, it is the rendezvous owner of the whole
/// group, and the loser **announced** its demotion rather than going quietly —
/// an application reconciling against `LeadershipChanged` has to be told.
#[test]
fn dst_split_brain_heals_to_one_fenced_host() {
    for seed in 0..32u64 {
        split_brain(seed);
    }
}

fn split_brain(seed: u64) {
    let mut rng = rng(seed ^ 0xe35b);
    let group = GroupId::new(format!("split-{seed}"));
    let ids: Vec<NodeId> = (0..5).map(|i| NodeId::new(format!("n{i}"))).collect();
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let mut sim = Simulation::new(2 + u64::from(rng.below(5)));
    sim.set_jitter(u64::from(rng.below(6)));
    for id in &ids {
        sim.add(engine(&group, id, &all));
    }

    let split_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(split_at));
    let incumbent = sole_host(&sim, "E3", seed);
    let top = placement::owner(group.as_str(), &all).expect("a non-empty group");
    assert_eq!(incumbent, top, "E3 seed {seed}: {incumbent} is not the top");
    let (epoch, _) = pair_of(&sim, &incumbent);

    let near = draw_side(&mut rng, &all, &incumbent);
    let far: BTreeSet<NodeId> = all.difference(&near).cloned().collect();
    for a in &near {
        for b in &far {
            sim.block(a, b);
            sim.block(b, a);
        }
    }

    let hold = cfg().detection_window_ms(ids.len()) + CLAIM_SETTLE_MS + SPLIT_HOLD_SLACK_MS;
    sim.run_until(Time(split_at + hold));

    // The incumbent is the rendezvous owner of the whole group, so it is still
    // the owner of whichever side it landed on: it keeps hosting its own epoch.
    // The orphaned side's owner is the only node there that can ever be
    // top-ranked, so it — and only it — settles a higher epoch.
    let orphans = if near.contains(&incumbent) {
        &far
    } else {
        &near
    };
    let usurper = placement::owner(group.as_str(), orphans).expect("a non-empty side");
    assert_eq!(
        sim.hosts(),
        ordered(&[incumbent.clone(), usurper.clone()]),
        "E3 seed {seed}: a {}/{} split should host on both sides",
        near.len(),
        far.len()
    );
    assert_eq!(
        pair_of(&sim, &incumbent),
        (epoch, Some(incumbent.clone())),
        "E3 seed {seed}: the incumbent did not keep its own side"
    );
    let (rival, _) = pair_of(&sim, &usurper);
    assert!(
        rival > epoch,
        "E3 seed {seed}: {usurper} settled epoch {rival}, which does not fence the incumbent's {epoch}"
    );

    sim.heal_all();
    sim.run_until(Time(split_at + hold + HEAL_SETTLE_MS));

    let survivor = sole_host(&sim, "E3", seed);
    assert_eq!(
        survivor, top,
        "E3 seed {seed}: the healed group settled on {survivor}, not the rendezvous owner {top}"
    );
    let settled = pair_of(&sim, &survivor);
    for id in &all {
        assert_eq!(
            pair_of(&sim, id),
            settled,
            "E3 seed {seed}: {id} disagrees on the pair the heal settled"
        );
    }
    assert_stood_down(&sim.leadership_log, &usurper, rival, &settled, ("E3", seed));
}

/// One side of a two-way split of five nodes: a 1/4 or a 2/3, with the
/// incumbent drawn onto one side or the other so both shapes — "the incumbent
/// keeps a majority" and "the incumbent is marooned" — appear across the seeds.
fn draw_side(rng: &mut SplitMix64, all: &BTreeSet<NodeId>, incumbent: &NodeId) -> BTreeSet<NodeId> {
    let wanted = 1 + usize::try_from(rng.below(2)).expect("a 0/1 draw");
    let mut pool: BTreeSet<NodeId> = all.clone();
    pool.remove(incumbent);
    let mut side = BTreeSet::new();
    if rng.below(2) == 0 {
        side.insert(incumbent.clone());
    }
    while side.len() < wanted {
        let node = take(&mut pool, rng);
        side.insert(node);
    }
    side
}

/// `nodes`, in the order [`Simulation::hosts`] reports them.
fn ordered(nodes: &[NodeId]) -> Vec<NodeId> {
    let set: BTreeSet<NodeId> = nodes.iter().cloned().collect();
    set.into_iter().collect()
}

/// The fenced loser's own tail of the leadership log: it activated `activated`
/// for itself, then **announced** a step down — another pair adopted, or a bare
/// `(epoch, None)` lapse — ending on the pair the healed group settled on.
fn assert_stood_down(
    log: &[(NodeId, u64, Option<NodeId>)],
    loser: &NodeId,
    activated: u64,
    settled: &Pair,
    suite: (&str, u64),
) {
    let (name, seed) = suite;
    let own: Vec<Pair> = log
        .iter()
        .filter(|(observer, _, _)| observer == loser)
        .map(|(_, epoch, host)| (*epoch, host.clone()))
        .collect();
    let at = own
        .iter()
        .position(|(epoch, host)| *epoch == activated && host.as_ref() == Some(loser))
        .unwrap_or_else(|| {
            panic!(
                "{name} seed {seed}: {loser} never announced activating epoch {activated}: {own:?}"
            )
        });
    assert!(
        own[at + 1..]
            .iter()
            .any(|(_, host)| host.as_ref() != Some(loser)),
        "{name} seed {seed}: {loser} never announced a step down after activating {activated}: {own:?}"
    );
    assert_eq!(
        own.last(),
        Some(settled),
        "{name} seed {seed}: {loser}'s last announced pair is not the one the heal settled: {own:?}"
    );
}

// ---------------------------------------------------------------------------
// E4 — the same-epoch collision the pair order exists for.
// ---------------------------------------------------------------------------

/// How long the crash-plus-split is held. Generous on purpose: a side may have
/// **three or more** peers fall silent at once, which
/// [`Config::detection_window_ms`] does not bound, so this is a settling time
/// rather than a budget — the budget claim is E2's job.
const COLLIDE_HOLD_MS: u64 = 2_500;

/// **E4.** Two sides settle the *same* epoch under different hosts — the case
/// the fencing order exists for, since each side counted only the members it
/// could see and so derived the same `highest_seen + 1`.
///
/// The host crashes and the survivors are cut in two at the same instant. Only
/// the rendezvous owner of a side can ever be its top-ranked live candidate
/// (the owner of any set is the best-scoring member of it), so each side has
/// exactly one possible claimant, neither hears the other, and both settle
/// `epoch + 1`. At heal the survivor must be the one the equal-epoch tiebreak
/// names — computed here straight from [`placement::owner`] over the two
/// activators, which reads nothing but the group id and the two node ids and is
/// therefore the same verdict on both sides of the cut.
#[test]
fn dst_same_epoch_collision_is_fenced_deterministically() {
    for seed in 0..32u64 {
        same_epoch_collision(seed);
    }
}

fn same_epoch_collision(seed: u64) {
    let mut rng = rng(seed ^ 0xe4c0);
    let group = GroupId::new(format!("collide-{seed}"));
    let n = 4 + rng.below(3); // 4..=6 nodes
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let mut sim = Simulation::new(2 + u64::from(rng.below(5)));
    sim.set_jitter(u64::from(rng.below(6)));
    for id in &ids {
        sim.add(engine(&group, id, &all));
    }

    let crash_at = 3_000 + u64::from(rng.below(500));
    sim.run_until(Time(crash_at));
    let incumbent = sole_host(&sim, "E4", seed);
    let (epoch, _) = pair_of(&sim, &incumbent);

    let survivors: BTreeSet<NodeId> = all.iter().filter(|x| **x != incumbent).cloned().collect();
    let left = draw_half(&mut rng, &survivors);
    let right: BTreeSet<NodeId> = survivors.difference(&left).cloned().collect();

    // The host goes silent and the survivors lose sight of each other at the
    // same instant: neither side can learn what the other settles.
    sim.crash(&incumbent);
    for a in &left {
        for b in &right {
            sim.block(a, b);
            sim.block(b, a);
        }
    }
    sim.run_until(Time(crash_at + COLLIDE_HOLD_MS));

    let a_host = placement::owner(group.as_str(), &left).expect("a non-empty side");
    let b_host = placement::owner(group.as_str(), &right).expect("a non-empty side");
    assert_eq!(
        sim.hosts(),
        ordered(&[a_host.clone(), b_host.clone()]),
        "E4 seed {seed}: a {}/{} split of the survivors should host on both sides",
        left.len(),
        right.len()
    );
    for host in [&a_host, &b_host] {
        assert_eq!(
            pair_of(&sim, host),
            (epoch + 1, Some(host.clone())),
            "E4 seed {seed}: {host} did not settle the collided epoch {}",
            epoch + 1
        );
    }

    // The tiebreak, computed independently of the engine.
    let both: BTreeSet<NodeId> = [a_host.clone(), b_host.clone()].into_iter().collect();
    let winner = placement::owner(group.as_str(), &both).expect("two candidates");
    let loser = if winner == a_host {
        b_host.clone()
    } else {
        a_host.clone()
    };

    sim.heal_all();
    sim.run_until(Time(crash_at + COLLIDE_HOLD_MS + HEAL_SETTLE_MS));

    let survivor = sole_host(&sim, "E4", seed);
    assert_eq!(
        survivor,
        winner,
        "E4 seed {seed}: the collision at epoch {} healed to {survivor}, but the pair rule names {winner}",
        epoch + 1
    );
    // The winner is also the rendezvous owner of the healed survivor set (the
    // owner of any set is its best-scoring member, so the owner of the two side
    // owners owns both sides together). Reconvergence may run further elections
    // — a node whose view has not caught up can be top-ranked for a moment —
    // but every one lands back on the same node, so what is pinned is the host
    // and the agreement, with the epoch only required to have moved *past* the
    // collision it fenced.
    let settled = pair_of(&sim, &survivor);
    assert_eq!(
        settled.1.as_ref(),
        Some(&winner),
        "E4 seed {seed}: {winner} hosts but names {settled:?}"
    );
    assert!(
        settled.0 > epoch,
        "E4 seed {seed}: the healed group fell back to epoch {} from the collided {}",
        settled.0,
        epoch + 1
    );
    for id in &survivors {
        assert_eq!(
            pair_of(&sim, id),
            settled,
            "E4 seed {seed}: {id} disagrees on the pair that survived the collision"
        );
    }
    assert_stood_down(
        &sim.leadership_log,
        &loser,
        epoch + 1,
        &settled,
        ("E4", seed),
    );
}

/// One side of a two-way cut of the survivors: at least one node on each side.
fn draw_half(rng: &mut SplitMix64, survivors: &BTreeSet<NodeId>) -> BTreeSet<NodeId> {
    let most = u32::try_from(survivors.len() - 1).expect("a handful of survivors");
    let wanted = 1 + usize::try_from(rng.below(most)).expect("bounded by the survivor count");
    let mut pool = survivors.clone();
    let mut side = BTreeSet::new();
    while side.len() < wanted {
        let node = take(&mut pool, rng);
        side.insert(node);
    }
    side
}
