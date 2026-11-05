//! docstore's fabric shape (napbat/docstore#242) on a lossy network: 200
//! nodes that all seed each other, docstore's timings (gossip 200 ms, probe
//! 100 ms, probe timeout 50 ms, suspect 500 ms), 12 entries of 160 B per
//! node and one entry change per node per second, with 2 % of frames lost
//! and up to 10 ms of jitter. Every node stays up, so every `Dead` verdict is
//! false: lost acks keep raising suspicions, and each must clear — the
//! suspected node refutes, and every node that doubted it holds it `Alive`
//! again — before any node declares it `Dead`.

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_sim::{Percent, Simulation};

const N: usize = 200;
const ENTRIES: u64 = 12;
/// How often the run samples every node's view.
const SAMPLE_MS: u64 = 100;
/// The gap between two nodes' writes: 200 writes spread over each second.
const WRITE_GAP_MS: u64 = 1_000 / N as u64;
const RUN_MS: u64 = 12_000;

fn config() -> Config {
    Config {
        gossip_interval_ms: 200,
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 500,
        ..Config::default()
    }
}

/// What one run saw.
#[derive(Debug)]
struct Run {
    /// The first sample at which every node knew every member, none `Dead`.
    known_at: Option<u64>,
    /// The first sample at which every node held every member `Alive`.
    alive_at: Option<u64>,
    /// The most `(observer, member)` pairs `Dead` at any sample after
    /// `known_at`.
    worst_dead: usize,
    /// The most pairs not `Alive` at any sample after `known_at`.
    worst_doubt: usize,
    /// The longest any pair stayed not `Alive`, after `known_at`, in ms.
    longest_doubt: u64,
}

fn set_entry(sim: &mut Simulation, node: &NodeId, index: u64, generation: u64) {
    let mut value = vec![0; 160];
    value[..8].copy_from_slice(&generation.to_le_bytes());
    sim.command(
        node,
        Command::SetLocalEntry {
            key: format!("e{index:02}"),
            value,
            ttl_ms: None,
        },
    )
    .expect("live node");
}

/// Boots the group with node ids `{prefix}000`.. (the prefix is the seed:
/// it moves every hash — probe rings, fanout, probers, loss draws) and runs
/// it for `RUN_MS` with the entry churn.
fn run(prefix: &str) -> Run {
    let ids: Vec<NodeId> = (0..N)
        .map(|i| NodeId::new(format!("{prefix}{i:03}")))
        .collect();
    let mut sim = Simulation::new(10).with_loss(Percent::of(2));
    sim.set_jitter(10);
    for id in &ids {
        let seeds = ids.iter().filter(|s| *s != id).cloned();
        let engine = GroupEngine::new(GroupId::new("docstore-nodes"), id.clone(), seeds, config())
            .expect("valid config");
        sim.add(engine).expect("distinct node");
    }
    for id in &ids {
        for index in 0..ENTRIES {
            set_entry(&mut sim, id, index, 0);
        }
    }
    let mut seen = Run {
        known_at: None,
        alive_at: None,
        worst_dead: 0,
        worst_doubt: 0,
        longest_doubt: 0,
    };
    // When each doubted pair started being doubted, observer-major.
    let mut doubted_since: Vec<Option<u64>> = vec![None; N * N];
    let mut now = 0;
    while now < RUN_MS {
        now += WRITE_GAP_MS;
        sim.run_until(Time(now));
        let second = now / 1_000;
        let writer = usize::try_from((now % 1_000) / WRITE_GAP_MS).expect("below N");
        set_entry(&mut sim, &ids[writer], second % ENTRIES, second);
        if now % SAMPLE_MS != 0 {
            continue;
        }
        let (mut doubt, mut dead, mut unknown) = (0, 0, 0);
        for (o, observer) in ids.iter().enumerate() {
            for (m, member) in ids.iter().enumerate() {
                let status = sim.status_of(observer, member);
                let since = &mut doubted_since[o * N + m];
                match status {
                    Some(Status::Alive) => {
                        if let Some(start) = since.take()
                            && seen.known_at.is_some()
                        {
                            seen.longest_doubt = seen.longest_doubt.max(now - start);
                        }
                    }
                    Some(Status::Suspect) => doubt += 1,
                    Some(Status::Dead) => {
                        doubt += 1;
                        dead += 1;
                    }
                    None => unknown += 1,
                }
                if status != Some(Status::Alive) && since.is_none() {
                    *since = Some(now);
                }
            }
        }
        if doubt == 0 && unknown == 0 && seen.alive_at.is_none() {
            seen.alive_at = Some(now);
        }
        if seen.known_at.is_some() {
            seen.worst_dead = seen.worst_dead.max(dead);
            seen.worst_doubt = seen.worst_doubt.max(doubt);
        } else if dead == 0 && unknown == 0 {
            seen.known_at = Some(now);
        }
    }
    // A doubt still open at the end counts for as long as it has lasted.
    if let Some(known) = seen.known_at {
        for start in doubted_since.into_iter().flatten() {
            seen.longest_doubt = seen.longest_doubt.max(now - start.max(known));
        }
    }
    seen
}

/// Every live member stays live in every view. The group converges, and
/// every node holds every member `Alive`, within the bound the lossless
/// churn test derives: 2 × (⌈log2 N⌉ + ⌈N / ⌈window / 2⌉⌉ / fanout) rounds of
/// 200 ms = 4.8 s. From then on no node ever holds a member `Dead`, and every
/// doubt clears within the window the protocol grants a suspicion heard
/// through gossip — `suspect_timeout + (⌊log2 N⌋ + 2)` gossip rounds, the
/// rounds liveness news leads every full digest to cross the group:
/// 500 + 9 × 200 = 2,300 ms at N = 200.
///
/// Measured (seeds `dsn-` / `node-` / `x`): all `Alive` at 2.8 / 1.9 /
/// 2.9 s, no pair `Dead`, at most 47 / 33 / 49 of the 40,000 pairs doubted
/// at a sample, no doubt longer than 500 ms. Before, at f4058be: never all
/// `Alive`; up to 45 / 33 / 15 live pairs `Dead`, 668 / 706 / 749 doubted,
/// doubts of 800 / 800 / 2,100 ms. A suspecter told its suspect once, and a
/// lost accusation left the suspect unaware until gossip reached it — past
/// the suspecter's window, whose `Dead` verdict then spread; nodes that only
/// heard the suspicion timed it out on the suspecter's clock; and an
/// indirect probe got one probe timeout for two round trips, so most direct
/// misses became suspicions.
fn assert_stays_alive(prefix: &str) {
    let run = run(prefix);
    let config = config();
    let rounds = u64::from(N.bit_width()) + 1;
    let doubt_bound = config.suspect_timeout_ms + rounds * config.gossip_interval_ms;
    let known = run.known_at.expect("membership converged");
    assert!(
        known <= 4_800,
        "{prefix}: converged only at {known} ms: {run:?}"
    );
    let alive = run
        .alive_at
        .expect("every node held every member Alive at some point");
    assert!(
        alive <= 4_800,
        "{prefix}: all Alive only at {alive} ms: {run:?}"
    );
    assert_eq!(
        run.worst_dead, 0,
        "{prefix}: live members held Dead: {run:?}"
    );
    assert!(
        run.longest_doubt <= doubt_bound,
        "{prefix}: a live member stayed doubted {} ms, above {doubt_bound}: {run:?}",
        run.longest_doubt
    );
}

#[test]
fn live_members_stay_alive_under_loss_seed_dsn() {
    assert_stays_alive("dsn-");
}

#[test]
fn live_members_stay_alive_under_loss_seed_node() {
    assert_stays_alive("node-");
}

#[test]
fn live_members_stay_alive_under_loss_seed_x() {
    assert_stays_alive("x");
}
