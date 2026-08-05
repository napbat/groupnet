//! Deterministic Simulation Testing for the **Hosted-mode election** under
//! chaos — the `Activation::Settle` tier's safety properties, sampled at every
//! round of the same randomized fault schedule `dst.rs` runs.
//!
//! One suite here ([`dst_settle_chaos_holds_safety_and_converges`]); the
//! failover, split-brain and same-epoch-collision suites live in
//! `election_failover.rs`, and mode invariance in `dst.rs` — whose every
//! scenario asserts an `Eventual` group carries no election frame and logs no
//! leadership change.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::cmp::Ordering;
use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId, Role, Time,
    placement,
};
use groupnet_sim::{Simulation, SplitMix64};

/// How long a claim must stand unchallenged before its claimant activates.
const CLAIM_SETTLE_MS: u64 = 200;
/// How long a host's authority survives its last successful renewal.
const LEASE_MS: u64 = 400;
/// The anti-entropy / gossip cadence.
const GOSSIP_INTERVAL_MS: u64 = 60;

/// Seeds the shared deterministic PRNG so each schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

/// The Hosted timings both election suites run on: `detection.rs`'s detector,
/// plus a 200ms settle window and a 400ms lease.
///
/// [`HostedConfig`]'s sizing rule is `lease_ms < detection_window_ms(members) +
/// claim_settle_ms` — the deposed host has stepped down before anyone else can
/// step up. The smallest cluster built here is three nodes, where
/// `detection_window_ms(3)` is `2·(50 + 2·40) + 120` = 380ms, so the bound is
/// 580ms and the 400ms lease sits well inside it; larger clusters only widen
/// the margin. `dead_timeout_ms` is 1s (tombstones reaped at 2s), short enough
/// that a chaos run's tombstones are all reaped inside the final settle.
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
/// seed set is what lets a partitioned node be found again after a heal: an
/// engine keeps offering digests to its seeds even once it has buried them.
fn engine(group: &GroupId, id: &NodeId, peers: &BTreeSet<NodeId>) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg())
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    v[rng.below(n) as usize].clone()
}

/// An `(epoch, host)` pair: the unit that names a serializer.
type Pair = (u64, Option<NodeId>);

/// The pair `node` has adopted. Every caller here reads a node it knows is in
/// the simulation.
fn pair_of(sim: &Simulation, node: &NodeId) -> Pair {
    sim.leadership_of(node)
        .expect("a live node is in the simulation")
}

/// The fencing order over `(epoch, host)` pairs, **recomputed here** from
/// [`placement`] and nothing else: epoch-major; at equal epochs a `None` host
/// sorts below any `Some`, and two named hosts are separated by the placement
/// owner of the group among just those two. Deliberately not imported from the
/// engine — a suite that pins the order must not check the engine against its
/// own arithmetic.
fn cmp_pair(group: &str, a: &Pair, b: &Pair) -> Ordering {
    match a.0.cmp(&b.0) {
        Ordering::Equal => {}
        by_epoch => return by_epoch,
    }
    match (&a.1, &b.1) {
        (None, None) => Ordering::Equal,
        (None, Some(_)) => Ordering::Less,
        (Some(_), None) => Ordering::Greater,
        (Some(x), Some(y)) if x == y => Ordering::Equal,
        (Some(x), Some(y)) => {
            let two: BTreeSet<NodeId> = [x.clone(), y.clone()].into_iter().collect();
            if placement::owner(group, &two).as_ref() == Some(x) {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
    }
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
// E1 — chaos: safety every round, one fenced host at the end.
// ---------------------------------------------------------------------------

/// **E1.** A hosted group survives the full DST fault schedule with its safety
/// properties intact at *every* observation, and converges on exactly one
/// fenced host once the fabric is fair again.
///
/// Sampled every round, mid-chaos:
///
/// * **S4b — a host is always inside its lease.** Any node playing
///   [`Role::Host`] has a `lease_until` at or after the current instant — exact
///   in virtual time, since the lease deadline arms the driver timer.
/// * **S2 — beliefs do not run backwards.** For an observer that has not
///   restarted since the last sample, the highest epoch seen never decreases,
///   the epoch of its adopted pair never decreases, and the pair itself never
///   regresses in the fencing order — one carve-out, in [`check_monotone`].
/// * **Self-consistency.** `Role::Host` and "the adopted pair names me" are the
///   same statement, in both directions.
///
/// After heal, loss 0, jitter 0 and a long settle:
///
/// * **L1 —** exactly one node hosts, it is the rendezvous owner of the live
///   set, every live node names it *at the same epoch*, and (S4c) exactly one
///   unexpired lease exists anywhere in the cluster.
/// * **S4a —** folded over the whole run: no node ever re-enters a pair of its
///   own it has already left (see [`Activations`]).
///
/// # The restart wedge this suite found, and why L1 is unconditional again
///
/// L1 pins the epoch label as firmly as it pins the host — including on the
/// seeds where the settled host restarted mid-run, which is the interesting
/// case. Election state is in memory only, so a host that reboots comes back at
/// epoch 0; if it retakes the group before the survivors have buried it, it
/// serves a *lower* epoch than they remember for it. What used to happen next
/// was nothing at all: the survivors would not adopt the lower `(1, Some(h))`
/// it beacons, and `h` would not adopt — or even *learn the epoch of* — their
/// `(3, Some(h))`, because a pair naming this node was refused whole. The
/// cluster then agreed forever on the host and disagreed forever on the epoch
/// that fences it — 3 of these 128 seeds, seed 45 among them, and never once a
/// disagreement about *who* hosts.
///
/// Row 12b closes it: a better pair naming this node is taken with its hostship
/// stripped off, so `h` learns epoch 3, steps down to `(3, None)`, and — top
/// ranked, and no longer barred by an adopted hostship of its own — claims 4
/// and re-fences the group onto a pair everyone agrees on. The epoch assertion
/// below is therefore unconditional; a seed that trips it is a real regression,
/// not a documented hole.
#[test]
fn dst_settle_chaos_holds_safety_and_converges() {
    for seed in 0..128u64 {
        settle_chaos(seed);
    }
}

/// What one observer was last seen believing — the history S2 is read against.
#[derive(Debug, Default)]
struct Beliefs {
    /// Highest epoch observed from any source, per observer.
    epochs: BTreeMap<NodeId, u64>,
    /// Adopted `(epoch, host)` pair, per observer.
    pairs: BTreeMap<NodeId, Pair>,
}

impl Beliefs {
    /// A restarted node is a *new* observer on a fresh logical timeline: its
    /// election state is in memory only, so it legitimately comes back a
    /// Follower at epoch 0 and nothing recorded about it applies.
    fn forget(&mut self, node: &NodeId) {
        self.epochs.remove(node);
        self.pairs.remove(node);
    }
}

fn settle_chaos(seed: u64) {
    let mut rng = rng(seed ^ 0xe1c7);
    let group = GroupId::new(format!("hosted-{seed}"));
    let n = 3 + rng.below(4); // 3..=6 nodes
    let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();

    let mut sim = Simulation::new(u64::from(3 + rng.below(8))); // 3..=10ms links
    sim.set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    sim.set_jitter(u64::from(rng.below(9))); // up to 8ms reorder

    let mut alive: BTreeSet<NodeId> = all.iter().cloned().collect();
    for id in &all {
        sim.add(engine(&group, id, &alive));
    }

    let mut beliefs = Beliefs::default();
    let mut activations = Activations::default();
    let mut now = 0u64;
    for _round in 0..30 {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));
        sample(&sim, &group, &alive, &mut beliefs, (now, seed));
        activations.absorb(&sim.leadership_log, seed);

        if let Some(restarted) = inject_fault(&mut sim, &mut rng, &group, &all, &mut alive, now) {
            beliefs.forget(&restarted);
            activations.forget(&restarted);
        }
    }

    // Converge under fair conditions, then assert liveness.
    sim.heal_all();
    sim.set_loss(0);
    sim.set_jitter(0);
    now += 25_000;
    sim.run_until(Time(now));
    activations.absorb(&sim.leadership_log, seed);

    assert_one_fenced_host(&sim, &group, &alive, (now, seed));
}

/// The per-round mid-chaos sample: S4b, S2 and role/pair self-consistency, for
/// every node still running. `clock` is `(now, seed)`.
fn sample(
    sim: &Simulation,
    group: &GroupId,
    alive: &BTreeSet<NodeId>,
    beliefs: &mut Beliefs,
    clock: (u64, u64),
) {
    let (now, seed) = clock;
    for node in alive {
        let role = sim.role_of(node).expect("a live node is in the simulation");
        let pair = pair_of(sim, node);

        // S4b: hostship is never held past the lease that bounds it.
        if role == Role::Host {
            let lease = sim
                .lease_until_of(node)
                .expect("a host always has a lease to read");
            assert!(
                Time(now) <= lease,
                "E1 seed {seed}: {node} still plays Host at {now} on a lease that ran out at {lease:?}"
            );
        }

        // Role and adopted pair are two spellings of the same belief.
        if role == Role::Host {
            assert_eq!(
                pair.1.as_ref(),
                Some(node),
                "E1 seed {seed}: {node} plays Host at {now} but its adopted pair is {pair:?}"
            );
        } else {
            assert_ne!(
                pair.1.as_ref(),
                Some(node),
                "E1 seed {seed}: {node} plays {role:?} at {now} yet its adopted pair still names it"
            );
        }

        check_monotone(sim, group, node, beliefs, (now, seed));
    }
}

/// S2 for one observer: the highest epoch seen and the adopted pair, held
/// against what the same observer was last seen believing.
///
/// The pair may regress in the fencing order in exactly one situation, and the
/// assertion names it rather than tolerating it generally: a node giving up
/// **its own** hostship. Row 6 (the lease lapsed) and row 15 (a voluntary
/// leave) step `(e, Some(self))` down to `(e, None)`, which *is* lower in the
/// order — deliberately, since keeping the epoch is what leaves the stepped-down
/// node fenced against by any later pair. From `(e, None)` it may then adopt any
/// `(e, Some(x))`, so a sample straddling both moves can land on a pair that
/// loses the equal-epoch tiebreak to the one it held. Both are permitted only
/// when the *previous* pair named this observer; any other regression is a
/// fencing violation.
fn check_monotone(
    sim: &Simulation,
    group: &GroupId,
    node: &NodeId,
    beliefs: &mut Beliefs,
    clock: (u64, u64),
) {
    let (now, seed) = clock;
    let observed = sim
        .observed_epoch_of(node)
        .expect("a live node is in the simulation");
    if let Some(was) = beliefs.epochs.insert(node.clone(), observed) {
        assert!(
            observed >= was,
            "E1 seed {seed}: {node}'s highest observed epoch fell from {was} to {observed} by {now}"
        );
    }

    let pair = pair_of(sim, node);
    let Some(was) = beliefs.pairs.insert(node.clone(), pair.clone()) else {
        return; // first sighting — nothing to compare against
    };
    assert!(
        pair.0 >= was.0,
        "E1 seed {seed}: {node}'s adopted epoch fell from {} to {} by {now}",
        was.0,
        pair.0
    );
    if cmp_pair(group.as_str(), &pair, &was) == Ordering::Less {
        assert_eq!(
            was.1.as_ref(),
            Some(node),
            "E1 seed {seed}: {node} regressed from {was:?} to {pair:?} by {now} \
             without giving up hostship of its own"
        );
    }
}

/// L1 and S4c, asserted once the fabric is fair again and has had a long time
/// to settle.
fn assert_one_fenced_host(
    sim: &Simulation,
    group: &GroupId,
    alive: &BTreeSet<NodeId>,
    clock: (u64, u64),
) {
    let (now, seed) = clock;
    let expected = placement::owner(group.as_str(), alive).expect("a non-empty live set");

    // Membership first: what the pair assertions below mean depends on every
    // survivor agreeing who is in the group.
    for node in alive {
        assert_eq!(
            sim.members_of(node),
            *alive,
            "E1 seed {seed}: {node} did not converge on the live set"
        );
    }

    let host = sole_host(sim, "E1", seed);
    assert_eq!(
        host, expected,
        "E1 seed {seed}: the group settled on {host}, not the rendezvous owner {expected}"
    );

    let settled = pair_of(sim, &host);
    assert_eq!(
        settled.1.as_ref(),
        Some(&host),
        "E1 seed {seed}: {host} hosts but names {settled:?}"
    );
    // Who holds the group and under which epoch, both unconditionally — a
    // restarted host's stale epoch is reconciled by row 12b, not tolerated.
    for node in alive {
        assert_eq!(
            pair_of(sim, node),
            settled,
            "E1 seed {seed}: {node} disagrees on the pair the cluster settled"
        );
    }

    // S4c, in the calm: one authority means one live lease, cluster-wide.
    let leased: Vec<NodeId> = alive
        .iter()
        .filter(|n| sim.lease_until_of(n).is_some_and(|until| until > Time(now)))
        .cloned()
        .collect();
    assert_eq!(
        leased,
        vec![expected],
        "E1 seed {seed}: more than one unexpired lease after the settle"
    );
}

/// Applies one fault — crash, restart, two-way partition, heal, or one of two
/// write shapes that keep anti-entropy busy underneath the election. Returns
/// the node id if the fault was a **restart**, whose fresh engine invalidates
/// everything recorded about that observer.
fn inject_fault(
    sim: &mut Simulation,
    rng: &mut SplitMix64,
    group: &GroupId,
    all: &[NodeId],
    alive: &mut BTreeSet<NodeId>,
    now: u64,
) -> Option<NodeId> {
    match rng.below(8) {
        0 if alive.len() > 2 => {
            let victim = pick(alive, rng);
            sim.crash(&victim);
            alive.remove(&victim);
        }
        1 if alive.len() < all.len() => {
            // Restart a downed node with a fresh engine: incarnation 0, and —
            // election state being in memory only — epoch 0 and no hostship.
            let down: BTreeSet<NodeId> = all
                .iter()
                .filter(|x| !alive.contains(*x))
                .cloned()
                .collect();
            let node = pick(&down, rng);
            alive.insert(node.clone());
            sim.add(engine(group, &node, alive));
            return Some(node);
        }
        2 if alive.len() > 1 => {
            let a = pick(alive, rng);
            let b = pick(alive, rng);
            if a != b {
                sim.block(&a, &b);
                sim.block(&b, &a);
            }
        }
        3 => sim.heal_all(),
        4 | 5 => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::SetLocalEntry {
                    key: "kv".into(),
                    value: format!("v{now}").into_bytes(),
                    ttl_ms: None,
                },
            );
        }
        _ => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::UpdateMetadata {
                    key: "k".into(),
                    value: format!("v{now}"),
                },
            );
        }
    }
    None
}

/// **S4a**, folded over the whole run: the activation ledger.
///
/// A node enters hostship only by activating a claim of its own, which is the
/// one leadership transition it logs naming itself. Each such pair may open
/// **one contiguous interval**: once a node has left a pair of its own — by
/// lapsing, by standing down, or by adopting somebody else's — it must never
/// re-enter that same pair, which would be a deposed host resuming an authority
/// the cluster had already fenced. A pair here is `(epoch, Some(node))`, so per
/// node only the epoch varies. A **restart** clears a node's ledger: it comes
/// back at epoch 0 and may legitimately settle a stale epoch again.
#[derive(Debug, Default)]
struct Activations {
    /// How much of `leadership_log` has already been folded in.
    consumed: usize,
    /// The epoch each node currently holds open for itself.
    open: BTreeMap<NodeId, u64>,
    /// The epochs each node has activated and since left.
    closed: BTreeMap<NodeId, BTreeSet<u64>>,
}

impl Activations {
    fn absorb(&mut self, log: &[(NodeId, u64, Option<NodeId>)], seed: u64) {
        for (observer, epoch, host) in &log[self.consumed..] {
            if host.as_ref() != Some(observer) {
                // Anything else this node announces closes the interval it was
                // in: a demotion, a lapse, or the adoption of another pair.
                self.close(observer);
                continue;
            }
            if self.open.get(observer) == Some(epoch) {
                continue; // still inside the same activation interval
            }
            assert!(
                !self
                    .closed
                    .get(observer)
                    .is_some_and(|left| left.contains(epoch)),
                "E1 seed {seed}: {observer} re-activated ({epoch}, {observer}) after leaving it"
            );
            self.close(observer);
            self.open.insert(observer.clone(), *epoch);
        }
        self.consumed = log.len();
    }

    fn close(&mut self, node: &NodeId) {
        if let Some(epoch) = self.open.remove(node) {
            self.closed.entry(node.clone()).or_default().insert(epoch);
        }
    }

    fn forget(&mut self, node: &NodeId) {
        self.open.remove(node);
        self.closed.remove(node);
    }
}
