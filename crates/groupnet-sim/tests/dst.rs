//! Deterministic Simulation Testing (DST).
//!
//! For each seed we generate a randomized-but-reproducible schedule of faults —
//! crashes, restarts, partitions, heals, message loss, membership churn, and
//! state/metadata writes — drive the real sans-IO core through it, and assert:
//!
//! * **Safety, every round (even mid-chaos):** a node never designates a
//!   coordinator it doesn't consider live, and always counts itself a member.
//! * **Liveness, after a fair settle (healed, loss-free, quiesced):** every live
//!   node converges on the same membership, the same (correct) coordinator, sees
//!   every peer alive (no false death survives), and holds identical metadata and
//!   per-node state.
//!
//! A failing seed is a reproducible counterexample, not a flake.

use std::collections::BTreeSet;

use groupnet_core::Time;
use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, placement};
use groupnet_sim::{Simulation, SplitMix64};

/// Seeds the shared deterministic PRNG so each fault schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn cfg() -> Config {
    // Tight timers so the final settle converges quickly in logical time.
    Config {
        gossip_interval_ms: 60,
        probe_interval_ms: 50,
        probe_timeout_ms: 25,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 300,
        indirect_probes: 2,
        fanout: 4,
    }
}

fn engine(group: &GroupId, id: &NodeId, alive: &BTreeSet<NodeId>) -> GroupEngine {
    let seeds = alive.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg())
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    v[rng.below(v.len() as u32) as usize].clone()
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
    let n = 3 + rng.below(4); // 3..=6 nodes
    let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();

    let latency = u64::from(3 + rng.below(8));
    let mut sim = Simulation::new(latency);
    sim.set_loss(rng.below(25) as u8); // up to 24% loss during chaos

    let mut alive: BTreeSet<NodeId> = all.iter().cloned().collect();
    for id in &all {
        sim.add(engine(&group, id, &alive));
    }

    let mut now = 0u64;
    for _round in 0..30 {
        now += u64::from(30 + rng.below(120));
        sim.run_until(Time(now));

        // ---- safety: must hold at every step, even mid-chaos ----
        for node in &alive {
            assert!(
                sim.members_of(node).contains(node),
                "seed {seed}: {node} dropped itself from membership"
            );
            if let Some(c) = sim.coordinator_of(node) {
                assert!(
                    sim.members_of(node).contains(&c),
                    "seed {seed}: {node} chose coordinator {c} it doesn't consider live"
                );
            }
        }

        // ---- inject one fault ----
        match rng.below(6) {
            0 if alive.len() > 2 => {
                let victim = pick(&alive, &mut rng);
                sim.crash(&victim);
                alive.remove(&victim);
            }
            1 if alive.len() < all.len() => {
                // Restart a downed node with a fresh engine (incarnation 0).
                let down: BTreeSet<NodeId> = all
                    .iter()
                    .filter(|x| !alive.contains(*x))
                    .cloned()
                    .collect();
                let node = pick(&down, &mut rng);
                alive.insert(node.clone());
                sim.add(engine(&group, &node, &alive));
            }
            2 if alive.len() > 1 => {
                let a = pick(&alive, &mut rng);
                let b = pick(&alive, &mut rng);
                if a != b {
                    sim.block(&a, &b);
                    sim.block(&b, &a);
                }
            }
            3 => sim.heal_all(),
            4 => {
                let node = pick(&alive, &mut rng);
                sim.command(
                    &node,
                    Command::SetLocalState(format!("s{now}").into_bytes()),
                );
            }
            _ => {
                let node = pick(&alive, &mut rng);
                sim.command(
                    &node,
                    Command::UpdateMetadata {
                        key: "k".into(),
                        value: format!("v{now}"),
                    },
                );
            }
        }
    }

    // ---- converge under fair conditions, then assert liveness ----
    sim.heal_all();
    sim.set_loss(0);
    now += 20_000;
    sim.run_until(Time(now));

    if alive.len() < 2 {
        return; // degenerate cluster, nothing to compare
    }

    let expected_coord = placement::owner("shard", &alive);
    let first = alive.iter().next().cloned().unwrap();
    let meta = sim.metadata_snapshot(&first);
    let state = sim.state_snapshot(&first);

    for node in &alive {
        assert_eq!(
            sim.members_of(node),
            alive,
            "seed {seed}: {node} did not converge on the live set"
        );
        assert_eq!(
            sim.coordinator_of(node),
            expected_coord,
            "seed {seed}: {node} disagrees on the coordinator"
        );
        for peer in &alive {
            assert_eq!(
                sim.status_of(node, peer),
                Some(Status::Alive),
                "seed {seed}: {node} still sees {peer} as not-alive after healing"
            );
        }
        assert_eq!(
            sim.metadata_snapshot(node),
            meta,
            "seed {seed}: metadata diverged at {node}"
        );
        assert_eq!(
            sim.state_snapshot(node),
            state,
            "seed {seed}: per-node state diverged at {node}"
        );
    }
}
