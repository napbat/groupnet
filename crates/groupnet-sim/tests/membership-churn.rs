//! docstore's fabric shape (napbat/docstore#242) at 200 nodes: every node
//! seeds every other, the detector runs at groupnet's default timings
//! (gossip 200 ms, probe 100 ms, probe timeout 50 ms, suspect 500 ms), and
//! every node keeps re-setting one of its 12 entries each second — the busy
//! part of docstore's 33 s re-set cycle, all the time. Membership converges
//! fast, members stay `Alive`, and no digest outgrows the window, both on a
//! clean network and on a lossy one that keeps raising false suspicions.

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_sim::{Percent, Simulation};

const N: usize = 200;
/// Entries each node holds, as docstore's node state does. Only their count
/// and churn shape the gossip, so the values stay small.
const ENTRIES: u64 = 12;
/// How often the run samples every node's view.
const SAMPLE_MS: u64 = 500;

/// What one run saw.
struct Run {
    /// The first sample at which every node held every member `Alive`.
    alive_at: Option<u64>,
    /// The first sample at which every node knew every member and held none
    /// `Dead` (a member may still be `Suspect`, pending its refutation).
    known_at: Option<u64>,
    /// The most `(observer, member)` pairs not `Alive` at any sample after
    /// `known_at`.
    worst_doubt: usize,
    /// The most `(observer, member)` pairs `Dead` at any sample.
    worst_dead: usize,
    /// The most summaries any digest listed.
    largest_digest: u64,
}

fn status_pairs(sim: &Simulation, ids: &[NodeId], pick: impl Fn(Option<Status>) -> bool) -> usize {
    ids.iter()
        .flat_map(|observer| ids.iter().map(move |node| (observer, node)))
        .filter(|(observer, node)| pick(sim.status_of(observer, node)))
        .count()
}

fn set_entry(sim: &mut Simulation, node: &NodeId, index: u64, generation: u64) {
    sim.command(
        node,
        Command::SetLocalEntry {
            key: format!("e{index:02}"),
            value: generation.to_le_bytes().to_vec(),
            ttl_ms: None,
        },
    )
    .expect("live node");
}

/// Boots the 200-node all-seeds group over `loss` and runs it for `run_ms`
/// with every node re-setting one entry per second.
fn run(loss: Percent, run_ms: u64) -> Run {
    let ids: Vec<NodeId> = (0..N)
        .map(|i| NodeId::new(format!("node-{i:03}")))
        .collect();
    let group = GroupId::new("churn");
    let config = Config {
        gossip_interval_ms: 200,
        probe_interval_ms: 100,
        probe_timeout_ms: 50,
        suspect_timeout_ms: 500,
        ..Config::default()
    };
    let mut sim = Simulation::new(10).with_loss(loss);
    for id in &ids {
        let seeds = ids.iter().filter(|s| *s != id).cloned();
        let engine = GroupEngine::new(group.clone(), id.clone(), seeds, config.clone())
            .expect("valid config");
        sim.add(engine).expect("distinct node");
    }
    for id in &ids {
        for index in 0..ENTRIES {
            set_entry(&mut sim, id, index, 0);
        }
    }

    let mut seen = Run {
        alive_at: None,
        known_at: None,
        worst_doubt: 0,
        worst_dead: 0,
        largest_digest: 0,
    };
    let mut now = 0;
    while now < run_ms {
        now += SAMPLE_MS;
        sim.run_until(Time(now));
        // Each node re-sets one entry a second, nodes spread over the second.
        let second = now / 1_000;
        for (i, id) in (0u64..).zip(&ids) {
            if (i * 1_000 / N as u64) / SAMPLE_MS == (now % 1_000) / SAMPLE_MS {
                set_entry(&mut sim, id, second % ENTRIES, second);
            }
        }
        let not_alive = status_pairs(&sim, &ids, |s| s != Some(Status::Alive));
        let unknown_or_dead = status_pairs(&sim, &ids, |s| {
            !matches!(s, Some(Status::Alive | Status::Suspect))
        });
        let dead = status_pairs(&sim, &ids, |s| s == Some(Status::Dead));
        seen.worst_dead = seen.worst_dead.max(dead);
        if not_alive == 0 && seen.alive_at.is_none() {
            seen.alive_at = Some(now);
        }
        if seen.known_at.is_some() {
            seen.worst_doubt = seen.worst_doubt.max(not_alive);
        } else if unknown_or_dead == 0 {
            seen.known_at = Some(now);
        }
    }
    for id in &ids {
        let stats = sim.net_stats_of(id).expect("live node");
        seen.largest_digest = seen.largest_digest.max(stats.max_digest_summaries);
    }
    seen
}

/// On a clean network the group converges within 4 s and, from then on,
/// every node holds every member `Alive` at every sample. Measured: 2.5 s, no
/// doubt after; 273ad08 had not converged by the end of the 5 s run (its
/// digest and probe cursors all start at the same peers).
#[test]
fn a_healthy_group_converges_fast_and_every_member_stays_alive() {
    let run = run(Percent::ZERO, 5_000);
    let alive_at = run.alive_at.expect("membership converged");
    assert!(alive_at <= 4_000, "converged only at {alive_at} ms");
    assert_eq!(run.worst_doubt, 0, "a member left Alive in a healthy group");
    assert_eq!(run.worst_dead, 0, "a healthy member was declared Dead");
    assert!(
        run.largest_digest <= 64,
        "a digest listed {}",
        run.largest_digest
    );
}

/// 4% loss makes direct and indirect probes miss, so false suspicions keep
/// arising. A suspecter tells its subject at once, the subject answers with
/// its refutation, and liveness news leads every digest while it is fresh, so
/// suspicions clear before they turn into deaths.
///
/// Measured: every node knows every member, none `Dead`, at 2.5 s, and holds
/// every member `Alive` at 5.5 s; at most 83 of the 40 000
/// `(observer, member)` pairs doubted at a sample afterwards, at most 1
/// `Dead`. Before, at 8b94666 (liveness news buried under entry
/// churn in the windowed digest, no accusation): never converged in 8 s, up
/// to 170 `Dead` pairs; at 273ad08: never converged, up to 645.
#[test]
fn false_suspicions_clear_before_they_kill_under_loss() {
    let run = run(Percent::of(4), 8_000);
    let known_at = run.known_at.expect("membership converged despite the loss");
    assert!(known_at <= 6_000, "converged only at {known_at} ms");
    assert!(
        run.worst_doubt <= 200,
        "{} pairs doubted at once after convergence",
        run.worst_doubt
    );
    assert!(
        run.worst_dead <= 10,
        "{} pairs held a live member Dead",
        run.worst_dead
    );
    assert!(
        run.largest_digest <= 64,
        "a digest listed {}",
        run.largest_digest
    );
}
