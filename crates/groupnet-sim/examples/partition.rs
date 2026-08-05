//! A five-node cluster split down the middle, watched through the whole arc:
//! **converge → partition → detect → heal → rejoin**.
//!
//! Everything here runs on the simulator's virtual clock over its partitionable
//! in-memory network, so the narrative below is not a recording of one lucky run
//! — it is the *only* run. There is no wall clock to read, no socket to touch,
//! and no randomness anywhere, so the output is byte-identical every time.
//!
//! What to watch for:
//!
//! * Each side of the partition independently declares the other side `Dead` —
//!   failure detection needs no coordinator and no quorum.
//! * Each side then derives its *own* coordinator from the members it can still
//!   see. That is safe precisely because the coordinator is non-authoritative.
//! * A metadata write made on the majority side cannot cross the split, so the
//!   minority keeps serving the stale value — visibly, not silently.
//! * Healing the links is enough: the survivors refute their own tombstones, one
//!   membership re-forms, one coordinator is re-derived, and the write that was
//!   stranded lands everywhere.
//!
//! ```text
//! cargo run -p groupnet-sim --example partition
//! ```

use groupnet_core::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time};
use groupnet_sim::Simulation;

const GROUP: &str = "shard-42";
/// The side that keeps three of the five nodes.
const MAJORITY: [&str; 3] = ["node-a", "node-b", "node-c"];
/// The side that is cut down to two.
const MINORITY: [&str; 2] = ["node-d", "node-e"];

/// Virtual milliseconds of latency on every link.
const LATENCY_MS: u64 = 10;

fn main() {
    let group = GroupId::new(GROUP);
    let ids: Vec<NodeId> = MAJORITY
        .iter()
        .chain(MINORITY.iter())
        .map(|s| NodeId::new(*s))
        .collect();

    // One real `GroupEngine` per node, each seeded with the others. The sim owns
    // the clock and the network; the engines are the same sans-IO state machines
    // the async runtime drives in production.
    let mut sim = Simulation::new(LATENCY_MS);
    for id in &ids {
        let seeds = ids.iter().filter(|other| *other != id).cloned();
        sim.add(GroupEngine::new(
            group.clone(),
            id.clone(),
            seeds,
            Config::default(),
        ));
    }

    // ---- 1. formation ------------------------------------------------------
    sim.run_until(Time(3_000));
    phase(
        "1. formation",
        "gossip has carried every node into one membership, and every node has \
         derived the same coordinator from it",
    );
    report(&sim, &ids);

    // The coordinator publishes a metadata value everyone can read.
    let coordinator = sim.coordinator_of(&ids[0]).expect("a coordinator by now");
    sim.command(
        &coordinator,
        Command::UpdateMetadata {
            key: "routing".into(),
            value: "v1".into(),
        },
    );
    sim.run_until(Time(4_000));
    println!("\n  {coordinator} (coordinator) wrote routing=v1");
    metadata(&sim, &ids);

    // ---- 2. partition ------------------------------------------------------
    // Sever every link between the two sides, in both directions. Neither side
    // is told anything: they must work it out from missing acks.
    for near in &ids[..MAJORITY.len()] {
        for far in &ids[MAJORITY.len()..] {
            sim.block(near, far);
            sim.block(far, near);
        }
    }
    sim.run_until(Time(4_100));
    phase(
        "2. partition",
        "the majority/minority links are cut; the first probes across the split \
         are already unanswered, but nothing is buried yet — both sides still \
         count five members",
    );
    report(&sim, &ids);

    // ---- 3. detect ---------------------------------------------------------
    // Unanswered direct probes enlist indirect probers; with none reachable
    // across the split the suspicion window runs out and the peers are declared
    // Dead — independently, on both sides.
    sim.run_until(Time(8_000));
    phase(
        "3. detect",
        "each side buried the other and re-derived a coordinator from what it can \
         still see — two coordinators, which is harmless because neither can bind \
         anything",
    );
    report(&sim, &ids);

    // A write on the majority side during the split: the minority cannot see it.
    let majority_coordinator = sim.coordinator_of(&ids[0]).expect("majority coordinator");
    sim.command(
        &majority_coordinator,
        Command::UpdateMetadata {
            key: "routing".into(),
            value: "v2".into(),
        },
    );
    sim.run_until(Time(10_000));
    println!("\n  {majority_coordinator} wrote routing=v2 on the majority side");
    metadata(&sim, &ids);

    // ---- 4. heal -----------------------------------------------------------
    sim.heal_all();
    sim.run_until(Time(11_000));
    phase(
        "4. heal",
        "the links are back, and each side's tombstones crossed before the \
         refutations answering them did — this is the messiest moment of the run, \
         and no node needs telling to fix it",
    );
    report(&sim, &ids);

    // ---- 5. rejoin ---------------------------------------------------------
    // A node that hears itself called Dead bumps its incarnation and reasserts
    // Alive; that refutation outranks the tombstone everywhere it reaches.
    sim.run_until(Time(20_000));
    phase(
        "5. rejoin",
        "every node refuted its own tombstone, one membership re-formed, one \
         coordinator was re-derived, and the stranded write reached the minority",
    );
    report(&sim, &ids);
    metadata(&sim, &ids);

    println!("\nSame clock, same schedule, same output — every run. Re-run it and diff.");
}

/// Prints a phase banner: the heading plus a sentence of what just happened.
fn phase(title: &str, what: &str) {
    println!("\n== {title} ==");
    println!("  {what}.");
}

/// Prints, for every node, the coordinator it currently believes in and the
/// status it holds for each peer — the membership view, from five viewpoints.
fn report(sim: &Simulation, ids: &[NodeId]) {
    println!();
    for observer in ids {
        let coordinator = sim
            .coordinator_of(observer)
            .map_or_else(|| "none".to_owned(), |c| short(&c));
        let view: Vec<String> = ids
            .iter()
            .map(|node| format!("{}={}", short(node), status(sim, observer, node)))
            .collect();
        println!(
            "  {observer}  live={}  coordinator={coordinator}  view: {}",
            sim.member_count(observer),
            view.join(" ")
        );
    }
}

/// Prints the `routing` metadata value each node would serve right now.
fn metadata(sim: &Simulation, ids: &[NodeId]) {
    let values: Vec<String> = ids
        .iter()
        .map(|id| {
            format!(
                "{}={}",
                short(id),
                sim.metadata_of(id, "routing").unwrap_or_else(|| "-".into())
            )
        })
        .collect();
    println!("  routing: {}", values.join(" "));
}

/// How `observer` currently sees `node`: alive, suspect, dead, or reaped
/// entirely (`gone`).
fn status(sim: &Simulation, observer: &NodeId, node: &NodeId) -> &'static str {
    match sim.status_of(observer, node) {
        Some(Status::Alive) => "alive",
        Some(Status::Suspect) => "suspect",
        Some(Status::Dead) => "dead",
        None => "gone",
    }
}

/// `node-a` → `a`, so five viewpoints fit on one line each.
fn short(node: &NodeId) -> String {
    node.as_str().trim_start_matches("node-").to_owned()
}
