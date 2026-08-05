//! Deterministic Simulation Testing for the **detector-timing contract** and
//! the **continuous-status stamps** it is read against.
//!
//! Two properties, both asserted against real engines in exact virtual time:
//!
//! * **D1 — the advertised window is honest.** After a converged cluster loses
//!   a node — or *two at once* — every survivor holds each of them `Dead`
//!   within [`Config::detection_window_ms`] of the crash. The test adds **no
//!   slack of its own**: all the conservatism has to live in the formula,
//!   because a consumer sizing a trust window gets exactly that number and
//!   nothing more.
//! * **D2 — `status_since` is a continuous-status stamp.** Across a
//!   randomized crash / restart / partition / heal / write schedule, every
//!   observer's `(status, since)` pair for every node obeys: the stamp is
//!   never in the future, never moves backwards, moves *only* when the status
//!   value moves, and when it moves it lands strictly inside the window since
//!   the previous observation.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_sim::{Simulation, SplitMix64};

/// Seeds the shared deterministic PRNG so each schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

/// Detector timings for both suites.
///
/// `probe_timeout_ms` is deliberately well above the worst round-trip either
/// suite configures (latency ≤ 6ms + jitter ≤ 6ms per hop), because the
/// detection window's stated assumption is that a peer which is *up* answers
/// inside the probe window — a config that violates it is measuring false
/// suspicion, not detection. `dead_timeout_ms` is large enough that no
/// tombstone is reaped while D1 is still looking at it.
fn cfg() -> Config {
    Config {
        gossip_interval_ms: 60,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 5_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: 60,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
    }
}

/// The same detector, with a short reap horizon so the D2 schedule actually
/// reaps tombstones (and re-adopts members after them) inside its run.
fn chaos_cfg() -> Config {
    Config {
        dead_timeout_ms: 300,
        ..cfg()
    }
}

fn engine(group: &GroupId, id: &NodeId, peers: &BTreeSet<NodeId>, config: Config) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, config)
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    v[rng.below(n) as usize].clone()
}

/// Per-seed link characteristics, all in milliseconds: latency is drawn from
/// `base + 0..spread`, and each message takes an extra `0..=jitter` on top.
struct Link {
    base: u64,
    spread: u32,
    jitter: u32,
}

/// **D1, fast links.** The ordinary regime, `probe_timeout < probe_interval`:
/// a probe/ack round trip finishes well inside one probe slot, so the
/// round-robin advances one peer per `probe_interval`.
#[test]
fn dst_detection_completes_inside_the_advertised_window() {
    detection_within_window(
        &cfg(),
        &Link {
            base: 2,
            spread: 5,
            jitter: 5,
        },
        0xd37e,
        1,
    );
}

/// **D1, slow links.** The regime that actually squeezes the window's
/// per-step term: `probe_timeout > probe_interval` *and* a round trip that
/// straddles a probe slot, so every step of the round-robin costs two slots
/// instead of one. This is the configuration where a per-step budget of
/// `max(probe_interval, probe_timeout)` — rather than the sum this crate
/// settled on — would start promising sooner than the detector can deliver.
#[test]
fn dst_detection_window_holds_when_probes_straddle_a_slot() {
    let slow = Config {
        probe_interval_ms: 50,
        probe_timeout_ms: 70,
        ..cfg()
    };
    detection_within_window(
        &slow,
        &Link {
            // 56..60ms round trips: over one 50ms probe slot, under the 70ms
            // probe timeout — so healthy peers still answer in time.
            base: 28,
            spread: 1,
            jitter: 2,
        },
        0x5107,
        1,
    );
}

/// **D1, concurrent failures.** *Two* members crash at the same instant, so a
/// survivor's round-robin stalls behind the *other* silent peer on its way to
/// each victim: that peer's direct-miss deadline and the indirect (`ping-req`)
/// re-arm after it both hold the one outstanding-probe slot, and the whole
/// ring waits behind it.
///
/// This is what the window's per-step `2 · probe_timeout` term is for. The
/// timings are deliberately the unforgiving ones — `probe_timeout` over
/// `probe_interval`, so a miss costs more than a slot, and a suspicion window
/// long enough that the first victim is still in the probe ring (still
/// stalling it) while the second is being detected. A per-step budget of
/// `probe_interval + probe_timeout` with the two miss deadlines added once at
/// the end — the pre-fix formula — is exceeded here, which is what makes this
/// regime worth its runtime.
///
/// It stops at two on purpose: [`Config::detection_window_ms`] documents that
/// three or more simultaneous silences can still outrun the window, because a
/// `Suspect` member keeps its slot in the probe ring (that is how a false
/// suspicion gets a chance to be refuted) and can stall it more than once.
/// This suite asserts exactly as far as that claim goes.
#[test]
fn dst_detection_window_holds_when_two_members_fall_silent_together() {
    let crowded = Config {
        probe_interval_ms: 60,
        probe_timeout_ms: 90,
        suspect_timeout_ms: 200,
        ..cfg()
    };
    detection_within_window(
        &crowded,
        &Link {
            // 70..78ms round trips: over one 60ms probe slot, under the 90ms
            // probe timeout — so healthy peers still answer in time.
            base: 35,
            spread: 2,
            jitter: 3,
        },
        0xc0c1,
        2,
    );
}

/// The D1 body: across 64 seeds and cluster sizes `victims + 2 ..= 6` (always
/// at least two survivors, so there is someone to enlist as an indirect prober
/// and someone to be falsely doubted), converge, crash `victims` nodes *at the
/// same instant*, and run for exactly `detection_window_ms(cluster_size)` —
/// **no slack of the test's own**, no fudge term, no "and a bit". Every
/// survivor must hold **every** crashed node `Dead` by then.
///
/// If the formula under-counts any phase of the detector (the round-robin
/// pass, the direct miss, the indirect miss, the suspicion window) — or
/// under-counts what a *concurrently* silent peer costs the steps behind it —
/// a seed here fails, which is the whole point: the bug this pins is a window
/// that promises sooner than the detector delivers.
fn detection_within_window(config: &Config, link: &Link, salt: u64, victims: u32) {
    for seed in 0..64u64 {
        let mut rng = rng(seed ^ salt);
        let group = GroupId::new("detect");
        let smallest = victims + 2; // survivors >= 2
        let n = smallest + rng.below(7 - smallest); // smallest..=6 nodes
        let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

        // A healthy fabric: no loss, but jittered — so links reorder and a
        // probe/ack pair can arrive out of the order it was sent.
        let mut sim = Simulation::new(link.base + u64::from(rng.below(link.spread)));
        sim.set_jitter(u64::from(rng.below(link.jitter + 1)));
        for id in &ids {
            sim.add(engine(&group, id, &all, config.clone()));
        }

        // Converge first: the window bounds *detection*, not bootstrap. The
        // crash instant is drawn too, so the schedule samples the *phase* of
        // the crash against each observer's probe cursor and timers — the
        // difference between a victim that falls silent just before its slot
        // and one that falls silent just after being acked, which is the whole
        // spread the window has to cover.
        let crash_at = 3_000 + u64::from(rng.below(500));
        sim.run_until(Time(crash_at));
        for id in &ids {
            assert_eq!(
                sim.members_of(id).len(),
                ids.len(),
                "seed {seed}: {id} had not converged before the crash"
            );
        }

        // Every victim goes silent at the same instant — no stagger, so the
        // survivors' rings stall behind each other's misses.
        let mut doomed: BTreeSet<NodeId> = BTreeSet::new();
        let mut remaining = all.clone();
        for _ in 0..victims {
            let victim = pick(&remaining, &mut rng);
            remaining.remove(&victim);
            doomed.insert(victim);
        }
        for victim in &doomed {
            sim.crash(victim);
        }

        let window = config.detection_window_ms(ids.len());
        sim.run_until(Time(crash_at + window));

        for observer in ids.iter().filter(|id| !doomed.contains(*id)) {
            for victim in &doomed {
                let verdict = sim.status_since_of(observer, victim);
                assert_eq!(
                    verdict.map(|(status, _)| status),
                    Some(Status::Dead),
                    "seed {seed} ({n} nodes, {victims} crashed at once): {observer} \
                     still holds {victim} as {:?} after the advertised {window}ms \
                     detection window",
                    verdict.map(|(status, _)| status)
                );
                let (_, since) = verdict.expect("asserted Dead above");
                assert!(
                    since.0 > crash_at && since.0 <= crash_at + window,
                    "seed {seed}: {observer} dated {victim}'s death at {since:?}, \
                     outside the ({crash_at}, {}] window it happened in",
                    crash_at + window
                );
            }

            // The window must not be passing because the detector is trigger
            // happy: no survivor got caught in the sweep.
            for peer in ids
                .iter()
                .filter(|id| !doomed.contains(*id) && *id != observer)
            {
                assert_eq!(
                    sim.status_of(observer, peer),
                    Some(Status::Alive),
                    "seed {seed}: {observer} falsely doubted the healthy {peer}"
                );
            }
        }
    }
}

/// **D2.** `status_since` behaves like a stopwatch on the status *value*,
/// under a randomized fault schedule.
#[test]
fn dst_status_since_is_exact_across_fault_rounds() {
    for seed in 0..64u64 {
        run_since_scenario(seed);
    }
}

/// The last `(status, since)` pair observed for one `(observer, node)` pair.
type Observed = BTreeMap<(NodeId, NodeId), (Status, Time)>;

fn run_since_scenario(seed: u64) {
    let mut rng = rng(seed ^ 0x51ce);
    let group = GroupId::new("since");
    let n = 3 + rng.below(4);
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
    sim.set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    sim.set_jitter(u64::from(rng.below(9)));

    let mut alive: BTreeSet<NodeId> = all.clone();
    for id in &ids {
        sim.add(engine(&group, id, &alive, chaos_cfg()));
    }

    let mut history: Observed = BTreeMap::new();
    let mut prev_obs = 0u64;
    let mut now = 0u64;
    for _round in 0..30 {
        now += u64::from(20 + rng.below(120));
        sim.run_until(Time(now));
        check_stamps(&sim, &ids, &mut history, prev_obs, now, seed);
        prev_obs = now;

        if let Some(restarted) = inject_fault(&mut sim, &mut rng, &group, &ids, &mut alive, now) {
            // A restarted node is a *new* observer on a fresh logical
            // timeline — its stamps legitimately start over, so nothing we
            // recorded about what it used to believe still applies.
            history.retain(|(observer, _), _| *observer != restarted);
        }
    }
}

/// Asserts the four stamp invariants for every `(observer, node)` pair the
/// simulation can currently answer for, and records this round's readings.
fn check_stamps(
    sim: &Simulation,
    ids: &[NodeId],
    history: &mut Observed,
    prev_obs: u64,
    now: u64,
    seed: u64,
) {
    for observer in ids {
        for node in ids {
            // `None` = crashed observer, or a member reaped past the horizon.
            let Some((status, since)) = sim.status_since_of(observer, node) else {
                continue;
            };
            assert!(
                since.0 <= now,
                "seed {seed}: {observer} dates {node}'s {status:?} at {since:?}, in the future of {now}"
            );
            let Some((was, was_since)) =
                history.insert((observer.clone(), node.clone()), (status, since))
            else {
                continue; // first sighting — nothing to compare against
            };
            assert!(
                since >= was_since,
                "seed {seed}: {observer}'s stamp for {node} went backwards, {was_since:?} -> {since:?}"
            );
            if since == was_since {
                // The stamp stood still, so the status value must have too —
                // this is the "same-status re-merge must not reset it" rule
                // read from the outside, and its contrapositive: a value can
                // never change without taking a new stamp.
                assert_eq!(
                    status, was,
                    "seed {seed}: {observer} moved {node} {was:?} -> {status:?} without re-stamping {since:?}"
                );
            } else {
                // A fresh stamp must date from the window we were not looking:
                // after the previous reading, at or before this one.
                assert!(
                    since.0 > prev_obs && since.0 <= now,
                    "seed {seed}: {observer}'s new stamp {since:?} for {node} ({was:?} -> {status:?}) \
                     falls outside the ({prev_obs}, {now}] window it must have happened in"
                );
            }
        }
    }
}

/// Applies one fault from the schedule. Returns the node id if the fault was a
/// **restart**, whose fresh engine invalidates everything recorded about that
/// observer.
///
/// Deliberately excludes the command-path status writes (`Leave`, `AddPeer`):
/// those stamp from the engine's `now_hint` — the last time it was *told*,
/// which is legitimately a turn stale — so they cannot be held to the
/// "strictly inside the observation window" rule. Their stamping is pinned by
/// the engine unit tests instead.
fn inject_fault(
    sim: &mut Simulation,
    rng: &mut SplitMix64,
    group: &GroupId,
    ids: &[NodeId],
    alive: &mut BTreeSet<NodeId>,
    now: u64,
) -> Option<NodeId> {
    match rng.below(8) {
        0 if alive.len() > 2 => {
            let victim = pick(alive, rng);
            sim.crash(&victim);
            alive.remove(&victim);
        }
        1 if alive.len() < ids.len() => {
            let down: BTreeSet<NodeId> = ids
                .iter()
                .filter(|x| !alive.contains(*x))
                .cloned()
                .collect();
            let node = pick(&down, rng);
            alive.insert(node.clone());
            sim.add(engine(group, &node, alive, chaos_cfg()));
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
        4 => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::SetLocalState(format!("s{now}").into_bytes()),
            );
        }
        5 => {
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
