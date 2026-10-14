//! Deterministic Simulation Testing (DST).
//!
//! For each seed we generate a randomized-but-reproducible schedule of faults —
//! crashes, restarts, partitions, heals, message loss, reorder, membership
//! churn, and state/metadata/keyed-entry writes — drive the real sans-IO core
//! through it, and assert:
//!
//! * **Safety, every round (even mid-chaos):** a node never designates a
//!   coordinator it doesn't consider live, and always counts itself a member.
//! * **Liveness, after a fair settle (healed, loss-free, quiesced):** every live
//!   node converges on the same membership, the same (correct) coordinator, sees
//!   every peer alive (no false death survives), and holds identical metadata,
//!   `~blob` state, and per-node **keyed entries**.
//! * **Bounded frames:** every frame the engines emit across the whole run stays
//!   within the configured `max_delta_frame_bytes` cap.
//!
//! Since G3 the engines disseminate via digest/delta anti-entropy, so these seeds
//! also exercise **full-state convergence from arbitrary partial views** (each
//! node starts knowing only itself), the **reap-horizon invariant** (a digest
//! never resurrects a reaped entry — [`dst_no_resurrection_after_reap`]), and a
//! **50-node scale smoke** ([`dst_scale_smoke_converges`]) — the thing the v2
//! full-view push could not do.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::BTreeSet;
use std::num::NonZeroUsize;

use groupnet_core::Time;
use groupnet_core::{Command, Config, GroupEngine, GroupId, GroupMode, NodeId, Status, placement};
use groupnet_sim::{Percent, Simulation, SplitMix64};

/// The per-frame byte cap the DST holds the engines to. Small enough that the
/// large-state seeds genuinely split deltas across rounds, and every emitted
/// frame is asserted to stay within it.
const FRAME_CAP: usize = 4_096;

/// Seeds the shared deterministic PRNG so each fault schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn cfg() -> Config {
    // Tight timers so the final settle converges quickly in logical time; the
    // gossip round keeps the default `gossip_fanout` of 2.
    Config {
        gossip_interval_ms: 60,
        probe_interval_ms: 50,
        probe_timeout_ms: 25,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 300,
        max_delta_frame_bytes: NonZeroUsize::new(FRAME_CAP).expect("nonzero"),
        mode: GroupMode::Eventual,
        ..Config::default()
    }
}

fn engine(group: &GroupId, id: &NodeId, alive: &BTreeSet<NodeId>) -> GroupEngine {
    let seeds = alive.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg()).expect("valid config")
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are tens of nodes");
    v[rng.up_to(n - 1) as usize].clone()
}

#[test]
fn dst_safety_and_liveness_across_seeds() {
    for seed in 0..256u64 {
        run_scenario(seed);
    }
}

fn run_scenario(seed: u64) {
    let mut rng = rng(seed);
    let group = GroupId::new("shard");
    let n = 3 + rng.up_to(3); // 3..=6 nodes
    let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();

    let latency = u64::from(3 + rng.up_to(7));
    let mut sim = Simulation::new(latency);
    // up to 24% loss during chaos
    sim.set_loss(Percent::try_from(rng.up_to(24)).expect("at most 24%"));
    sim.set_jitter(u64::from(rng.up_to(8))); // up to 8ms reorder

    let mut alive: BTreeSet<NodeId> = all.iter().cloned().collect();
    for id in &all {
        sim.add(engine(&group, id, &alive)).expect("distinct node");
    }

    let mut now = 0u64;
    for _round in 0..30 {
        now += u64::from(30 + rng.up_to(119));
        sim.run_until(Time(now));

        // Safety: must hold at every step, even mid-chaos.
        for node in &alive {
            assert!(
                sim.members_of(node)
                    .expect("alive node is in the simulation")
                    .contains(node),
                "seed {seed}: {node} dropped itself from membership"
            );
            if let Some(c) = sim.coordinator_of(node) {
                assert!(
                    sim.members_of(node)
                        .expect("alive node is in the simulation")
                        .contains(&c),
                    "seed {seed}: {node} chose coordinator {c} it doesn't consider live"
                );
            }
        }

        inject_fault(&mut sim, &mut rng, &group, &all, &mut alive, now);

        // Bounded frames: no frame the engines emit ever exceeds the cap.
        assert!(
            sim.max_frame_bytes() <= FRAME_CAP,
            "seed {seed}: emitted a {}-byte frame over the {FRAME_CAP} cap",
            sim.max_frame_bytes()
        );
    }

    // Converge under fair conditions, then assert liveness.
    sim.heal_all();
    sim.set_loss(Percent::ZERO);
    sim.set_jitter(0);
    now += 20_000;
    sim.run_until(Time(now));

    // Mode invariance: every group here is `GroupMode::Eventual`, and an
    // Eventual group runs no election — not one election frame delivered, not
    // one leadership transition, across the whole chaos schedule and settle.
    assert_eq!(
        sim.election_frames_seen(),
        0,
        "seed {seed}: an Eventual group carried an election frame"
    );
    assert!(
        sim.leadership_log.is_empty(),
        "seed {seed}: an Eventual group changed leadership"
    );

    if alive.len() < 2 {
        return; // degenerate cluster, nothing to compare
    }
    assert_converged(&sim, &alive, seed);

    // Bounded frames held for the settle phase too.
    assert!(
        sim.max_frame_bytes() <= FRAME_CAP,
        "seed {seed}: settle emitted a {}-byte frame over the {FRAME_CAP} cap",
        sim.max_frame_bytes()
    );
}

/// Applies one randomly-chosen fault from the schedule: a crash, a restart, a
/// two-way partition, a heal, or one of the four write shapes. `alive` tracks
/// which nodes the caller should still hold to the safety invariants.
fn inject_fault(
    sim: &mut Simulation,
    rng: &mut SplitMix64,
    group: &GroupId,
    all: &[NodeId],
    alive: &mut BTreeSet<NodeId>,
    now: u64,
) {
    match rng.up_to(7) {
        0 if alive.len() > 2 => {
            let victim = pick(alive, rng);
            sim.crash(&victim).expect("live node");
            alive.remove(&victim);
        }
        1 if alive.len() < all.len() => {
            // Restart a downed node with a fresh engine (incarnation 0).
            let down: BTreeSet<NodeId> = all
                .iter()
                .filter(|x| !alive.contains(*x))
                .cloned()
                .collect();
            let node = pick(&down, rng);
            alive.insert(node.clone());
            sim.add(engine(group, &node, alive)).expect("distinct node");
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
            )
            .expect("live node");
        }
        5 => {
            // A permanent keyed entry authored by the node (no TTL, never
            // deleted here — delete+reap has its own no-resurrection seed set).
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::SetLocalEntry {
                    key: "kv".into(),
                    value: format!("v{now}").into_bytes(),
                    ttl_ms: None,
                },
            )
            .expect("live node");
        }
        6 => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::SetLocalEntry {
                    key: "ready".into(),
                    value: vec![1],
                    ttl_ms: None,
                },
            )
            .expect("live node");
        }
        _ => {
            let node = pick(alive, rng);
            sim.command(
                &node,
                Command::UpdateMetadata {
                    key: "k".into(),
                    value: format!("v{now}"),
                },
            )
            .expect("live node");
        }
    }
}

/// The liveness half of the scenario, asserted after a fair settle: every live
/// node agrees on the membership, the (correct) coordinator, everyone's
/// aliveness, and every replicated map.
fn assert_converged(sim: &Simulation, alive: &BTreeSet<NodeId>, seed: u64) {
    let expected_coord = placement::owner("shard", alive);
    let first = alive.iter().next().cloned().unwrap();
    let meta = sim
        .metadata_snapshot(&first)
        .expect("alive node is in the simulation");
    let state = sim
        .state_snapshot(&first)
        .expect("alive node is in the simulation");
    let entries = sim
        .entries_snapshot(&first)
        .expect("alive node is in the simulation");

    for node in alive {
        assert_eq!(
            sim.members_of(node)
                .expect("alive node is in the simulation"),
            *alive,
            "seed {seed}: {node} did not converge on the live set"
        );
        assert_eq!(
            sim.coordinator_of(node),
            expected_coord,
            "seed {seed}: {node} disagrees on the coordinator"
        );
        for peer in alive {
            assert_eq!(
                sim.status_of(node, peer),
                Some(Status::Alive),
                "seed {seed}: {node} still sees {peer} as not-alive after healing"
            );
        }
        assert_eq!(
            sim.metadata_snapshot(node)
                .expect("alive node is in the simulation"),
            meta,
            "seed {seed}: metadata diverged at {node}"
        );
        assert_eq!(
            sim.state_snapshot(node)
                .expect("alive node is in the simulation"),
            state,
            "seed {seed}: per-node blob state diverged at {node}"
        );
        assert_eq!(
            sim.entries_snapshot(node)
                .expect("alive node is in the simulation"),
            entries,
            "seed {seed}: per-node keyed entries diverged at {node}"
        );
    }
}

/// The reap-horizon invariant, pinned across seeds: a keyed entry that is written,
/// propagated, deleted, and reaped everywhere (under full connectivity, so the
/// tombstone reaches everyone before anyone reaps it) never comes back — no
/// digest resurrects it, even after long further gossip.
#[test]
fn dst_no_resurrection_after_reap() {
    for seed in 0..128u64 {
        let mut rng = rng(seed ^ 0x51e3);
        let group = GroupId::new("reap");
        let n = 3 + rng.up_to(3);
        let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

        let mut sim = Simulation::new(u64::from(2 + rng.up_to(5)));
        sim.set_jitter(u64::from(rng.up_to(5)));
        for id in &ids {
            sim.add(engine(&group, id, &all)).expect("distinct node");
        }

        // Author a key on a random node and let it fully propagate.
        let author = pick(&all, &mut rng);
        sim.command(
            &author,
            Command::SetLocalEntry {
                key: "doomed".into(),
                value: b"present".to_vec(),
                ttl_ms: None,
            },
        )
        .expect("live node");
        sim.run_until(Time(3_000));
        for obs in &ids {
            assert_eq!(
                sim.entries_snapshot(obs)
                    .expect("observer is in the simulation")
                    .get(&author)
                    .and_then(|m| m.get("doomed")),
                Some(&b"present".to_vec()),
                "seed {seed}: {obs} never learned the key before deletion"
            );
        }

        // Delete it. Under full connectivity the tombstone reaches everyone within
        // the dead_timeout window, then every node reaps it at 2×dead_timeout.
        sim.command(
            &author,
            Command::DeleteLocalEntry {
                key: "doomed".into(),
            },
        )
        .expect("live node");
        // Well past 2×dead_timeout (600ms) plus generous gossip time.
        sim.run_until(Time(30_000));

        for obs in &ids {
            assert!(
                sim.entries_snapshot(obs)
                    .expect("observer is in the simulation")
                    .get(&author)
                    .is_none_or(|m| !m.contains_key("doomed")),
                "seed {seed}: {obs} resurrected the reaped key"
            );
        }

        // Keep gossiping a long while: still gone (the high-water mark blocks any
        // digest from claiming to be behind on the reaped version).
        sim.run_until(Time(60_000));
        for obs in &ids {
            assert!(
                sim.entries_snapshot(obs)
                    .expect("observer is in the simulation")
                    .get(&author)
                    .is_none_or(|m| !m.contains_key("doomed")),
                "seed {seed}: {obs} resurrected the reaped key after further gossip"
            );
            assert!(
                sim.max_frame_bytes() <= FRAME_CAP,
                "seed {seed}: frame over cap"
            );
        }
    }
}

/// The 50-node scale smoke: fifty nodes, each authoring several keyed entries,
/// converge on one membership/coordinator and every node's full keyed map within
/// a bounded number of rounds — with every frame inside the cap. This is the case
/// the v2 full-view push (all nodes × all entries per frame) could not carry.
#[test]
fn dst_scale_smoke_converges() {
    const NODES: u32 = 50;
    const ENTRIES_PER_NODE: usize = 4;
    let group = GroupId::new("scale");
    let ids: Vec<NodeId> = (0..NODES)
        .map(|i| NodeId::new(format!("n{i:03}")))
        .collect();

    // Two well-known seeds bootstrap the mesh (not all-to-all).
    let seeds: Vec<NodeId> = ids[..2].to_vec();
    let mut sim = Simulation::new(5);
    for id in &ids {
        let seed_set: BTreeSet<NodeId> = seeds.iter().filter(|s| *s != id).cloned().collect();
        sim.add(
            GroupEngine::new(group.clone(), id.clone(), seed_set, cfg()).expect("valid config"),
        )
        .expect("distinct node");
    }

    // Every node authors a handful of permanent keyed entries.
    for (i, id) in ids.iter().enumerate() {
        for k in 0..ENTRIES_PER_NODE {
            sim.command(
                id,
                Command::SetLocalEntry {
                    key: format!("e{k}"),
                    value: format!("n{i}-e{k}").into_bytes(),
                    ttl_ms: None,
                },
            )
            .expect("live node");
        }
    }

    // Converge within a bounded number of anti-entropy rounds. 60ms cadence ×
    // ~130 rounds = the deadline below; convergence is empirically well inside it.
    let deadline = Time(8_000);
    sim.run_until(deadline);

    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();
    let expected_coord = placement::owner("scale", &all);
    let entries = sim
        .entries_snapshot(&ids[0])
        .expect("n000 is in the simulation");
    assert_eq!(
        entries.len(),
        ids.len(),
        "every node's entries should be present"
    );

    for id in &ids {
        assert_eq!(
            sim.members_of(id)
                .expect("observer is in the simulation")
                .len(),
            ids.len(),
            "{id} did not learn all {} members",
            ids.len()
        );
        assert_eq!(
            sim.coordinator_of(id),
            expected_coord,
            "{id} disagrees on the coordinator"
        );
        assert_eq!(
            sim.entries_snapshot(id)
                .expect("observer is in the simulation"),
            entries,
            "{id} did not converge on every node's keyed entries"
        );
    }
    assert!(
        sim.max_frame_bytes() <= FRAME_CAP,
        "scale run emitted a {}-byte frame over the {FRAME_CAP} cap",
        sim.max_frame_bytes()
    );
}

/// Bounded frames with continuation: a few nodes each holding more keyed state
/// than fits in one frame still converge, by splitting deltas across successive
/// anti-entropy rounds — and no frame ever exceeds a deliberately tiny cap.
#[test]
fn dst_bounded_frames_force_continuation() {
    const SMALL_CAP: usize = 512;
    let group = GroupId::new("bounded");
    let ids: Vec<NodeId> = (0..4).map(|i| NodeId::new(format!("n{i}"))).collect();
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();

    let config = Config {
        max_delta_frame_bytes: NonZeroUsize::new(SMALL_CAP).expect("nonzero"),
        ..cfg()
    };
    let mut sim = Simulation::new(4);
    sim.set_jitter(3);
    for id in &ids {
        let seeds = all.iter().filter(|x| *x != id).cloned();
        sim.add(
            GroupEngine::new(group.clone(), id.clone(), seeds, config.clone())
                .expect("valid config"),
        )
        .expect("distinct node");
    }

    // Each node authors far more entries than a single 512-byte frame can carry,
    // so a full transfer must span several deltas.
    for (i, id) in ids.iter().enumerate() {
        for k in 0..24 {
            sim.command(
                id,
                Command::SetLocalEntry {
                    key: format!("k{k:02}"),
                    value: format!("node{i}-key{k:02}-payload").into_bytes(),
                    ttl_ms: None,
                },
            )
            .expect("live node");
        }
    }

    sim.run_until(Time(12_000));

    let entries = sim
        .entries_snapshot(&ids[0])
        .expect("n0 is in the simulation");
    // 4 nodes × 24 keys each present on every node.
    assert_eq!(entries.len(), ids.len());
    for keys in entries.values() {
        assert_eq!(keys.len(), 24, "each node holds all 24 of its keys");
    }
    for id in &ids {
        assert_eq!(
            sim.entries_snapshot(id)
                .expect("observer is in the simulation"),
            entries,
            "{id} did not converge under the tiny frame cap"
        );
    }
    assert!(
        sim.max_frame_bytes() <= SMALL_CAP,
        "a frame reached {} bytes, over the {SMALL_CAP} cap",
        sim.max_frame_bytes()
    );
}
