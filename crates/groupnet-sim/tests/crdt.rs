//! Proof that groupnet's per-node state is a sufficient foundation for
//! **arbitrary CRDTs** — the framework's generality claim.
//!
//! The mechanism: each node authors its *own* contribution as its per-node state
//! (gossiped, LWW by version so the latest always wins), and the application
//! combines every node's contribution on read with whatever merge it wants.
//! Specific CRDT types belong in the app (like TLS belongs in the transport),
//! not in groupnet — so here we build a real **PN-Counter** on top of the seam
//! and prove it converges to the exact value under partitions and loss.

use std::collections::BTreeSet;

use groupnet_core::Time;
use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId};
use groupnet_sim::Simulation;

struct Rng(u64);
impl Rng {
    fn new(seed: u64) -> Self {
        Self(seed ^ 0xda3e_39cb_94b9_5bdb)
    }
    fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % u64::from(n.max(1))) as u32
    }
}

/// A node's PN-Counter contribution: how much it has incremented and decremented.
/// Encoded as its per-node state; the counter's value is `Σ inc − Σ dec` over
/// all nodes.
fn encode(inc: u64, dec: u64) -> Vec<u8> {
    let mut v = inc.to_le_bytes().to_vec();
    v.extend_from_slice(&dec.to_le_bytes());
    v
}
fn decode(bytes: &[u8]) -> (u64, u64) {
    let inc = u64::from_le_bytes(bytes[0..8].try_into().unwrap());
    let dec = u64::from_le_bytes(bytes[8..16].try_into().unwrap());
    (inc, dec)
}

#[test]
fn pn_counter_on_per_node_state_converges_under_faults() {
    for seed in 0..96u64 {
        run(seed);
    }
}

fn run(seed: u64) {
    let mut rng = Rng::new(seed);
    let group = GroupId::new("counter");
    let n = 3 + rng.below(4);
    let ids: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();

    let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
    sim.set_loss(rng.below(30) as u8);
    let all: BTreeSet<NodeId> = ids.iter().cloned().collect();
    let cfg = Config {
        gossip_interval_ms: 60,
        ..Config::default()
    };
    for id in &ids {
        let seeds = all.iter().filter(|x| *x != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            cfg.clone(),
        ));
    }

    // Each node accumulates its own increments/decrements (its CRDT contribution).
    let mut inc = vec![0u64; n as usize];
    let mut dec = vec![0u64; n as usize];

    let mut now = 0u64;
    for _round in 0..50 {
        now += u64::from(20 + rng.below(80));
        sim.run_until(Time(now));
        match rng.below(5) {
            0 => {
                let a = &ids[rng.below(n) as usize];
                let b = &ids[rng.below(n) as usize];
                if a != b {
                    sim.block(a, b);
                    sim.block(b, a);
                }
            }
            1 => sim.heal_all(),
            _ => {
                // A random node applies an op and re-publishes its contribution.
                let i = rng.below(n) as usize;
                if rng.below(2) == 0 {
                    inc[i] += 1;
                } else {
                    dec[i] += 1;
                }
                sim.command(&ids[i], Command::SetLocalState(encode(inc[i], dec[i])));
            }
        }
    }

    // Heal, quiesce, and every node must compute the exact counter value.
    sim.heal_all();
    sim.set_loss(0);
    sim.run_until(Time(now + 10_000));

    let oracle: i128 = (0..n as usize)
        .map(|i| i128::from(inc[i]) - i128::from(dec[i]))
        .sum();

    for observer in &ids {
        // Combine every node's contribution — the app-side CRDT merge.
        let value: i128 = ids
            .iter()
            .map(|node| {
                sim.state_of(observer, node).map_or(0, |bytes| {
                    let (i, d) = decode(&bytes);
                    i128::from(i) - i128::from(d)
                })
            })
            .sum();
        assert_eq!(
            value, oracle,
            "seed {seed}: {observer} computed {value}, expected {oracle}"
        );
    }
}
