//! Deterministic Simulation Testing for the **Hosted write path** (M4), safety
//! half: the sans-IO cores ([`CommitCore`], [`CompletenessCore`]) driven from
//! real [`GroupEngine`] state, in virtual time, under the randomized chaos
//! schedule `groupnet-sim`'s Quorum election suites run. A failing seed is a
//! reproducible counterexample, not a flake.
//!
//! # What is real here, and what is modelled
//!
//! Real, and the whole point: **the engines**. Every group is a real
//! [`GroupMode::Hosted`] group under [`Activation::Quorum`], so every epoch is
//! closed by a real grant majority over a real (lossy, jittered, partitionable)
//! network, and every leadership reading is the engine's own adopted
//! `(epoch, host)` out of [`Simulation::leadership_of`]. Every ledger entry is
//! authored with the tier's **own exported codec** ([`encode_ledger`]) under its
//! **own key** ([`ledger_entry_key`]), gossiped, and read back with
//! [`decode_ledger`] out of *the reading node's* entry view — so a commit
//! majority is assembled from bytes that genuinely crossed the fabric. Restarts
//! are [`GroupEngine::with_recovered`] fed the sim's persisted grant: the
//! **`GrantStore` posture S5 presumes**.
//!
//! Modelled, deliberately, and named so nobody mistakes it for tested:
//!
//! * **The feed is harness-authored, in the feed's own layout** — `WriteFeed`'s
//!   frame codec is crate-private, so the host writes
//!   `(epoch)(first_seq)(count)(len, key)*` into `~writes:hosted` itself, under
//!   the name the tier's own [`hosted_feed_name`] gives it — and the follower
//!   half is `PeerWrites`' cursor rule over those bytes ([`scan`]): a ring
//!   overrun or an epoch change is the
//!   [`Gap`](groupnet_consistency::PeerWrite::Gap) whose `missed_through` the
//!   watermark records, which is the *remediation-defines-applied* contract.
//! * **"Apply" is the watermark advance** ([`Harness::apply`]): there is no data
//!   plane, so a node has applied a write exactly when its own engine can see it
//!   in the host's feed and its follower loop has folded the token in — a fold
//!   that mirrors [`CommitLedger`](groupnet_consistency::CommitLedger), stamp
//!   `max`-folded from the leadership epoch *at the publish instant*, a `record`
//!   that moves nothing publishing nothing, a leadership change forcing a
//!   `refresh`. Every applied key is checked against the sequence it was
//!   published at, so no watermark moves over bytes the fabric did not carry.
//! * **The leadership watch is prompt here**, re-read from each node's own
//!   engine every round. The *lagging* watch the tier's honesty box names ("a
//!   deposed host can admit one more write before it knows") — and the late-ack
//!   fence that rides on it — is the migration file's dimension, because a
//!   commit wait only survives into a hand-over when the watch is behind.
//! * **The follower is durable**: a restart reseeds its applied map from a store
//!   written at every apply ([`CommitLedger::with_recovered`]'s posture, stamp
//!   back to zero and all) and opens at each visible feed's current end, because
//!   history is not replayed. Its loop also **stalls** on a fault arm — the gate
//!   `tests/hosted_migration.rs` closes — which is how a node falls far enough
//!   behind that the recovery rule is the only thing between it and a lost
//!   write.
//!
//! # The properties
//!
//! Each is stated on the check that holds it, and every one is re-derived from
//! the **raw entry bytes** rather than read back off the core it checks: **S5**
//! — no acked write is ever missing from a serving host, the headline
//! ([`Harness::check_s5`]); **P1** — ack soundness ([`check_ack`]); **P2** —
//! recovery exactness ([`Harness::recover`]); **P3** — per-publisher-life
//! monotonicity ([`Harness::check_readings`]). Each passes vacuously on a
//! schedule that stopped producing the thing it is about, so the suite also
//! asserts what it *saw* — its **floors** — and prints the tally either way.
//!
//! Two siblings carry the rest, each on its own copy of this harness —
//! duplicated helpers across sibling test files is the house pattern
//! (`groupnet-sim`'s `election.rs` / `election_failover.rs`):
//! `hosted_dst_migrate.rs` runs the migration-heavy schedule with a *lagging*
//! watch, which is where the **late-ack fence** is asserted and where its floors
//! are met; `hosted_dst_liveness.rs` runs the liveness half. Every property the
//! corpus holds is asserted in at least one of the three, and S5/P1/P2/P3 in all
//! of them.

#![cfg(feature = "hosted")]

use std::collections::{BTreeMap, BTreeSet};

use groupnet_consistency::WriteToken;
use groupnet_consistency::hosted::{
    Commit, CommitCore, CompletenessCore, LedgerView, Watermarks, decode_ledger, encode_ledger,
    hosted_feed_name, ledger_entry_key,
};
use groupnet_core::{
    Activation, Command, Config, GroupEngine, GroupId, GroupMode, HostedConfig, NodeId,
    RecoveredGrant, Status, Time, VoterRoster,
};
use groupnet_sim::{Simulation, SplitMix64};

use Count::{
    Acked, GapRecovered, Ghost, Migration, Recovered, Restart, RingGap, S5Obligation, Stalled,
};

/// A host's authority after its last confirmed renewal round — and, on a boot
/// with no recovered grant, the blackout before it will grant a new claimant —
/// over a gossip cadence brisk enough that a republish lands well inside it.
const LEASE_MS: u64 = 400;
const GOSSIP_MS: u64 = 40;
/// The roomy feed ring — a `Gap` under it is a migration, never an overflow —
/// and the small one, which overruns anyone who missed a burst.
const RING: u64 = 32;
const SMALL_RING: u64 = 2;
/// The schedule: the whole fault menu, over this many rounds.
const SLOTS: u32 = 16;
const ROUNDS: u32 = 40;
/// In-flight committed writes per host, and the ledger fold's zero watermark.
const MAX_INFLIGHT: usize = 3;
const ZERO: WriteToken = WriteToken { epoch: 0, seq: 0 };

/// A node's gossiped entry view, and the pair its leadership watch reports.
type Entries = BTreeMap<NodeId, BTreeMap<String, Vec<u8>>>;
type Lead = (u64, Option<NodeId>);
/// One watermark advance a follower loop owes its ledger: the writer, the token
/// applied (or a `Gap`'s `missed_through`), and — when a **ring overrun** made
/// it — the first write it jumped over.
type Advance = (NodeId, WriteToken, Option<WriteToken>);

/// Seeds the shared deterministic PRNG so each schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn tok(epoch: u64, seq: u64) -> WriteToken {
    WriteToken { epoch, seq }
}

/// One entry authored on a node's own engine, TTL-free like this tier's own.
fn set(key: &str, value: Vec<u8>) -> Command {
    let (key, ttl_ms) = (key.to_owned(), None);
    Command::SetLocalEntry { key, value, ttl_ms }
}

/// The detector timings the Quorum election suites run on, over `voters`.
fn cfg(voters: &BTreeSet<NodeId>) -> Config {
    Config {
        gossip_interval_ms: GOSSIP_MS,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: GOSSIP_MS,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Hosted(HostedConfig {
            activation: Activation::Quorum {
                voters: VoterRoster::new(voters.iter().cloned()),
            },
            lease_ms: LEASE_MS,
        }),
    }
}

/// The entry key the hosted feed occupies: `WriteFeed`'s key derivation is
/// crate-private, so the prefix is spelled here, the *name* comes from the tier.
fn feed_entry_key() -> String {
    format!("~writes:{}", hosted_feed_name(""))
}

/// The datum one write carries: self-identifying, so a follower can prove the
/// bytes it applied are the write it thinks they are.
fn write_key(seq: u64) -> Vec<u8> {
    format!("w{seq}").into_bytes()
}

/// The host's feed life: `WriteFeed`'s ring, as the window it advertises.
#[derive(Debug, Clone, Copy)]
struct Ring {
    epoch: u64,
    first_seq: u64,
    len: u64,
}

fn ring(epoch: u64, first_seq: u64, len: u64) -> Ring {
    Ring {
        epoch,
        first_seq,
        len,
    }
}

impl Ring {
    /// One past the last sequence in the window.
    fn end(self) -> u64 {
        self.first_seq + self.len
    }

    /// The entry bytes, in `WriteFeed`'s ring-frame layout:
    /// `(epoch)(first_seq)(count)(len, key)*`, little-endian.
    fn encode(self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + usize::try_from(self.len * 8).expect("small"));
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.first_seq.to_le_bytes());
        out.extend_from_slice(&u32::try_from(self.len).unwrap_or(u32::MAX).to_le_bytes());
        for seq in self.first_seq..self.end() {
            let key = write_key(seq);
            out.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            out.extend_from_slice(&key);
        }
        out
    }

    /// The window and its keys as a subscriber decodes them, or `None` the
    /// moment the framing does not parse.
    fn decode(bytes: &[u8]) -> Option<(Ring, Vec<Vec<u8>>)> {
        let epoch = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let first_seq = u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?);
        let count = u32::from_le_bytes(bytes.get(16..20)?.try_into().ok()?);
        let mut offset = 20usize;
        let mut keys = Vec::with_capacity(usize::try_from(count).ok()?.min(4_096));
        for _ in 0..count {
            let len = usize::try_from(u32::from_le_bytes(
                bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
            ))
            .ok()?;
            let end = offset.checked_add(4)?.checked_add(len)?;
            keys.push(bytes.get(offset + 4..end)?.to_vec());
            offset = end;
        }
        let window = ring(epoch, first_seq, u64::from(count));
        (offset == bytes.len()).then_some((window, keys))
    }
}

/// Which suite and which seed a failure belongs to.
type Tag = (&'static str, u64);

/// Everything a run is counted by. Every floor the suite asserts, and every
/// number its tally prints, is one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Count {
    Acked,
    Stalled,
    Recovered,
    GapRecovered,
    RingGap,
    Migration,
    Restart,
    Ghost,
    S5Obligation,
}

/// What a run observed, summed across seeds: a suite that stops exercising its
/// own property fails loudly rather than passing vacuously.
#[derive(Debug, Default, Clone)]
struct Stats(BTreeMap<Count, u64>);

impl Stats {
    fn add(&mut self, what: Count, by: u64) {
        *self.0.entry(what).or_default() += by;
    }

    fn get(&self, what: Count) -> u64 {
        self.0.get(&what).copied().unwrap_or(0)
    }

    fn absorb(&mut self, other: &Stats) {
        for (what, by) in &other.0 {
            self.add(*what, *by);
        }
    }

    /// The whole tally as one line: a drifted schedule reports its own shape.
    fn tally(&self) -> String {
        let counts: Vec<String> = self.0.iter().map(|(k, n)| format!("{k:?} {n}")).collect();
        counts.join(", ")
    }
}

/// A subscriber's position in one writer's feed.
#[derive(Debug, Clone, Copy)]
struct Cursor {
    epoch: u64,
    next: u64,
}

fn cursor(epoch: u64, next: u64) -> Cursor {
    Cursor { epoch, next }
}

/// One node's whole harness state: the ledger's published half, its leadership
/// watch, the follower cursors, the feed life, and `HostedWrites`' latch.
#[derive(Debug)]
struct Node {
    id: NodeId,
    stamp: u64,
    applied: Watermarks,
    /// What this node's leadership watch reports. **Prompt** here — the
    /// *lagging* watch, and the late-ack race it opens, is the migration file's
    /// dimension, and so is the fence check that rides on it.
    watch: Lead,
    /// The epoch the last ledger publish was stamped for — a change forces the
    /// `refresh` the deployment contract asks for — and the **lowest** write a
    /// ring overrun ever jumped this node over, per writer: the first hole in
    /// its coverage, so a target at or above it was reached by remediation.
    stamped_lead: Option<u64>,
    cursors: BTreeMap<NodeId, Cursor>,
    skipped: BTreeMap<NodeId, WriteToken>,
    ring: Option<Ring>,
    recovered_at: Option<u64>,
    inflight: Vec<WriteToken>,
    /// Steps this node's **follower loop** is stopped for: a closed gate.
    stalled: u32,
}

impl Node {
    fn new(id: &NodeId, applied: Watermarks) -> Self {
        Self {
            id: id.clone(),
            stamp: 0,
            applied,
            watch: (0, None),
            stamped_lead: None,
            cursors: BTreeMap::new(),
            skipped: BTreeMap::new(),
            ring: None,
            recovered_at: None,
            inflight: Vec::new(),
            stalled: 0,
        }
    }

    /// What this node's leadership watch currently reports.
    fn lead(&self) -> Lead {
        self.watch.clone()
    }

    /// The epoch this node's watch names *it* the host of.
    fn hosting(&self) -> Option<u64> {
        let (epoch, host) = self.lead();
        (host.as_ref() == Some(&self.id)).then_some(epoch)
    }
}

/// The cluster, its engines, and one [`Node`] per *running* member.
#[derive(Debug)]
struct Harness {
    tag: Tag,
    group: GroupId,
    sim: Simulation,
    /// Every node that has ever run, in id order; the static voter roster the
    /// rules are denominated over.
    all: Vec<NodeId>,
    voters: BTreeSet<NodeId>,
    majority: usize,
    /// The running nodes only — a crashed node serves nothing — and the durable
    /// applied state a restart reseeds from, written at every apply.
    nodes: BTreeMap<NodeId, Node>,
    store: BTreeMap<NodeId, Watermarks>,
    /// The last reading each observer held for each publisher, and the nodes
    /// whose version clock a restart has reset — P3's inputs.
    seen: BTreeMap<(NodeId, NodeId), Vec<u8>>,
    reborn: BTreeSet<NodeId>,
    /// **The S5 obligation set**: every write acknowledged, and every hostship
    /// that has served.
    acked: Vec<(NodeId, WriteToken)>,
    served: BTreeSet<(u64, NodeId)>,
    ledger_key: String,
    feed_key: String,
    ring: u64,
    now: u64,
    stats: Stats,
}

impl Harness {
    /// A cluster of `n` nodes over a roster of three. The roster is rotated, so
    /// it is not always the low ids and the rendezvous top of a run is
    /// sometimes outside it.
    fn new(tag: Tag, n: u32, rng: &mut SplitMix64, cap: u64) -> Self {
        let group = GroupId::new(format!("hosted-{}-{}", tag.0, tag.1));
        let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let off = usize::try_from(rng.below(n)).expect("bounded by n");
        let voters: BTreeSet<NodeId> = (0..3).map(|k| all[(off + k) % all.len()].clone()).collect();
        let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
        let mut nodes = BTreeMap::new();
        for id in &all {
            let seeds: Vec<NodeId> = all.iter().filter(|x| *x != id).cloned().collect();
            let engine = GroupEngine::new(group.clone(), id.clone(), seeds, cfg(&voters));
            sim.add(engine);
            nodes.insert(id.clone(), Node::new(id, Watermarks::new()));
        }
        Self {
            tag,
            group,
            sim,
            all,
            majority: voters.len() / 2 + 1,
            voters,
            nodes,
            store: BTreeMap::new(),
            seen: BTreeMap::new(),
            reborn: BTreeSet::new(),
            acked: Vec::new(),
            served: BTreeSet::new(),
            ledger_key: ledger_entry_key(""),
            feed_key: feed_entry_key(),
            ring: cap,
            now: 0,
            stats: Stats::default(),
        }
    }

    fn live_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    /// One observation at `now`: follow (checking readings on the way), serve,
    /// then poll every write in flight.
    fn step_to(&mut self, now: u64) {
        self.now = now;
        self.sim.run_until(Time(now));
        self.follow();
        self.serve();
        self.poll_writes();
    }

    fn run_rounds(&mut self, rounds: u32, every_ms: u64) {
        for _ in 0..rounds {
            self.step_to(self.now + every_ms);
        }
    }

    /// The follower loop, per node: apply what this node's **own** engine can
    /// see of every writer's feed, then fold and publish. P3 rides the snapshot.
    fn follow(&mut self) {
        let tag = self.tag;
        for id in self.live_ids() {
            let view = self.sim.entries_snapshot(&id);
            let lead = self.advance_watch(&id);
            self.check_readings(&id, &view);
            if self.nodes[&id].stalled > 0 {
                self.nodes.get_mut(&id).expect("live").stalled -= 1;
                continue; // the gate is closed: no apply, no record, no refresh
            }
            let mut advances: Vec<Advance> = Vec::new();
            for (peer, entries) in &view {
                // The host records its own writes at publish, not here.
                let Some(bytes) = (*peer != id).then(|| entries.get(&self.feed_key)).flatten()
                else {
                    continue;
                };
                let (frame, keys) = Ring::decode(bytes)
                    .unwrap_or_else(|| panic!("{tag:?}: {peer}'s feed did not decode"));
                let node = self.nodes.get_mut(&id).expect("a live node");
                scan(node, peer, (frame, &keys), &mut advances, tag);
            }
            self.apply(&id, &lead, advances);
        }
    }

    /// Refreshes `id`'s leadership watch from its own engine, and returns what
    /// it now reports.
    fn advance_watch(&mut self, id: &NodeId) -> Lead {
        let engine = self.sim.leadership_of(id).unwrap_or((0, None));
        let node = self.nodes.get_mut(id).expect("a live node");
        node.watch = engine.clone();
        engine
    }

    /// The `record` / `refresh` pair: folds one node's advances into its ledger,
    /// republishes if anything moved, and writes the store beneath it.
    fn apply(&mut self, id: &NodeId, lead: &Lead, advances: Vec<Advance>) {
        let mut moved = false;
        {
            let node = self.nodes.get_mut(id).expect("a live node");
            for (writer, token, skipped) in advances {
                if let Some(first) = skipped {
                    self.stats.add(RingGap, 1);
                    let hole = node.skipped.entry(writer.clone()).or_insert(first);
                    *hole = (*hole).min(first);
                }
                let entry = node.applied.entry(writer).or_insert(ZERO);
                if token > *entry {
                    *entry = token;
                    moved = true;
                }
            }
            // The stamp half of the same fold: never below what it has stamped.
            // A leadership change republishes whether or not anything moved: a
            // voter whose apply loop is quiet still owes a recovering host a
            // *fresh* reading.
            node.stamp = node.stamp.max(lead.0);
            moved |= node.stamped_lead != Some(lead.0);
            node.stamped_lead = Some(lead.0);
        }
        if moved {
            self.publish_ledger(id);
        }
    }

    /// Stores `id`'s applied map and authors its ledger entry — real codec.
    fn publish_ledger(&mut self, id: &NodeId) {
        let node = &self.nodes[id];
        let encoded = encode_ledger(node.stamp, &node.applied);
        self.store.insert(id.clone(), node.applied.clone());
        self.sim.command(id, set(&self.ledger_key, encoded));
    }

    /// The view both rules are handed, from `observer`'s **own** engine: the
    /// whole roster, silent voters included as `reading: None`.
    fn roster_view(&self, observer: &NodeId) -> Vec<LedgerView> {
        let entries = self.sim.entries_snapshot(observer);
        let read = |m: &NodeId| decode_ledger(entries.get(m)?.get(&self.ledger_key)?);
        let alive = |m: &NodeId| self.sim.status_of(observer, m) == Some(Status::Alive);
        let view = |member: &NodeId| LedgerView {
            member: member.clone(),
            alive: alive(member),
            reading: read(member),
        };
        self.voters.iter().map(view).collect()
    }

    /// **P3.** No reading `observer` holds ever regresses, in stamp or in any
    /// watermark, for a publisher no restart has reset the version clock of. A
    /// **restart ghost** — an earlier life's publication still held beside the
    /// stamp of zero `with_recovered` starts at — is the one regression gossip
    /// can produce, so a restarted publisher is excused, and excuses counted.
    ///
    /// The excuse is **coarser than the property**, deliberately and visibly: it
    /// should end the moment the new life out-versions the ghost everywhere,
    /// but this harness reads entry *bytes* and never the version clock that
    /// would say when that happened, so `reborn` is permanent and the publisher
    /// stays excused for the rest of the run. Every use of it increments
    /// [`Ghost`], and the tally prints it — so the width of the excuse is
    /// reported rather than assumed.
    fn check_readings(&mut self, observer: &NodeId, entries: &Entries) {
        let tag = self.tag;
        for publisher in &self.all {
            let key = (observer.clone(), publisher.clone());
            let Some(bytes) = entries.get(publisher).and_then(|e| e.get(&self.ledger_key)) else {
                // Reaped: what this observer learns next is a fresh adoption,
                // not the next step of a history.
                self.seen.remove(&key);
                continue;
            };
            let now = decode_ledger(bytes)
                .unwrap_or_else(|| panic!("{tag:?}: {observer} holds undecodable ledger bytes"));
            if let Some(was_bytes) = self.seen.get(&key) {
                // Recorded from a decoded reading, so it decodes again.
                let before = decode_ledger(was_bytes).expect("decoded when recorded");
                let held =
                    |(w, t): (&NodeId, &WriteToken)| now.applied.get(w).is_none_or(|h| h < t);
                if now.lead_epoch < before.lead_epoch || before.applied.iter().any(held) {
                    assert!(
                        self.reborn.contains(publisher),
                        "{tag:?}: {observer}'s reading of {publisher} regressed from \
                         {before:?} to {now:?}, and {publisher} has never restarted"
                    );
                    self.stats.add(Ghost, 1);
                }
            }
            self.seen.insert(key, bytes.clone());
        }
    }

    /// The recovery gate, per node the watch names host — and **S5** at every
    /// instant a host is serving.
    fn serve(&mut self) {
        for id in self.live_ids() {
            let Some(epoch) = self.nodes[&id].hosting() else {
                continue;
            };
            if self.nodes[&id].recovered_at != Some(epoch) && !self.recover(&id, epoch) {
                continue;
            }
            self.check_s5(&id, epoch);
        }
    }

    /// One evaluation of the recovery rule for `id` at `epoch`, latched exactly
    /// as `HostedWrites` latches it. **P2** re-derives it from the raw bytes: a
    /// fresh majority, and this host past the fold for every writer it names.
    fn recover(&mut self, id: &NodeId, epoch: u64) -> bool {
        let view = self.roster_view(id);
        let own = self.nodes[id].applied.clone();
        if !CompletenessCore::step(epoch, &view, &own).is_complete() {
            return false;
        }
        let (fresh, target) = fresh_target(&view, epoch);
        let (tag, majority) = (self.tag, self.majority);
        assert!(
            fresh >= majority,
            "{tag:?}: {id} recovered at {epoch} on {fresh} fresh readings, short of \
             {majority} — view {view:?}"
        );
        for (writer, want) in &target {
            assert!(
                own.get(writer).is_some_and(|have| have >= want),
                "{tag:?}: {id} recovered at {epoch} holding {:?} of {writer}, short of the \
                 fresh majority's {want:?}",
                own.get(writer)
            );
        }
        // Gap-remediated: a target at or above the first write a ring overrun
        // jumped this host over, so its completeness rests on the coarse
        // remediation rather than on replaying the individual writes.
        let holes = &self.nodes[id].skipped;
        let gap = target
            .iter()
            .any(|(w, want)| holes.get(w).is_some_and(|first| want >= first));
        let migrated = !self.served.is_empty();
        self.nodes.get_mut(id).expect("a live node").recovered_at = Some(epoch);
        self.stats.add(Recovered, 1);
        self.stats.add(GapRecovered, u64::from(gap));
        self.stats.add(Migration, u64::from(migrated));
        self.served.insert((epoch, id.clone()));
        true
    }

    /// **S5.** Everything this run acknowledged below `epoch` is in this serving
    /// host's own applied state.
    ///
    /// Negative control, re-derived against this corpus: bypass the recovery
    /// gate (serve and write whether or not `recovered_at` is set) and this
    /// fires at **seed 27** — `n0` serving at epoch 9 holding `(1, 5)` of `n2`,
    /// below an acknowledged `(4, 1)`. The **stall arm** is what makes it bite,
    /// by leaving a node far enough behind to be elected short of the target;
    /// the same bypass on the migration file's schedule, which has no stall arm,
    /// passes all 64 seeds.
    fn check_s5(&mut self, host: &NodeId, epoch: u64) {
        let (tag, now) = (self.tag, self.now);
        let node = &self.nodes[host];
        let mut held = 0u64;
        for (writer, token) in self.acked.iter().filter(|(_, t)| t.epoch < epoch) {
            let own = node.applied.get(writer).copied();
            assert!(
                own.is_some_and(|wm| wm >= *token),
                "{tag:?}: S5 — {host} serves at {epoch} at {now} holding {own:?} of \
                 {writer}, below the acknowledged {token:?}"
            );
            held += 1;
        }
        self.stats.add(S5Obligation, held);
    }

    /// One poll of every committed write in flight against the writer's own
    /// engine: `HostedWrites::await_commit`'s loop, verdict before deposition.
    fn poll_writes(&mut self) {
        let (tag, majority) = (self.tag, self.majority);
        for id in self.live_ids() {
            if self.nodes[&id].inflight.is_empty() {
                continue;
            }
            let view = self.roster_view(&id);
            let lead = self.nodes[&id].lead();
            let waits = std::mem::take(&mut self.nodes.get_mut(&id).expect("live").inflight);
            let mut kept = Vec::new();
            for token in waits {
                if CommitCore::evaluate(&view, &id, token, Commit::QuorumApplied).is_committed() {
                    check_ack(&view, &id, token, majority, tag);
                    self.stats.add(Acked, 1);
                    self.acked.push((id.clone(), token));
                } else if lead.0 == token.epoch && lead.1.as_ref() == Some(&id) {
                    kept.push(token);
                }
            }
            self.nodes.get_mut(&id).expect("live").inflight = kept;
        }
    }

    /// Publishes one hosted write on `id` if the tier would admit it — the watch
    /// names it host, and recovery has opened for that epoch — into the feed,
    /// into its own ledger, and (if `wait`) onto the commit wait list.
    fn start_write(&mut self, id: &NodeId, wait: bool) {
        let Some(epoch) = self.nodes.get(id).and_then(Node::hosting) else {
            return;
        };
        let capacity = self.ring;
        let node = self.nodes.get_mut(id).expect("live");
        if node.recovered_at != Some(epoch) {
            return; // `HostedError::Recovering`: not serving at this epoch yet
        }
        // A fresh feed life the first time this node hosts at `epoch`.
        let mut life = node
            .ring
            .filter(|r| r.epoch == epoch)
            .unwrap_or(ring(epoch, 1, 0));
        let token = tok(epoch, life.end());
        life.len += 1;
        if life.len > capacity {
            life.first_seq += 1;
            life.len -= 1;
        }
        node.ring = Some(life);
        // The host counts itself: `publish` records the write into its own
        // ledger before returning, so its own reading satisfies its own
        // predicate.
        let entry = node.applied.entry(id.clone()).or_insert(ZERO);
        *entry = (*entry).max(token);
        if wait && node.inflight.len() < MAX_INFLIGHT {
            node.inflight.push(token);
        }
        self.sim.command(id, set(&self.feed_key, life.encode()));
        self.publish_ledger(id);
    }

    /// Cuts `victim` off every other live node — both ways, or one-way, where it
    /// keeps *hearing* the group that can no longer hear it.
    fn cut(&mut self, victim: &NodeId, live: &BTreeSet<NodeId>, one_way: bool) {
        for peer in live.iter().filter(|peer| **peer != *victim) {
            self.sim.block(victim, peer);
            if !one_way {
                self.sim.block(peer, victim);
            }
        }
    }

    fn kill(&mut self, victim: &NodeId) {
        self.sim.crash(victim);
        self.nodes.remove(victim);
    }

    /// Brings `id` back on an engine that **recovers its persisted grant** — the
    /// `GrantStore` posture — with a follower reseeded from durable storage. It
    /// is a *new observer* too, so nothing P3 recorded of it applies.
    fn restart(&mut self, id: &NodeId) {
        let recovered = match self.sim.persisted_grant_of(id) {
            None => RecoveredGrant::none(),
            Some((epoch, claimant)) => RecoveredGrant::granted(epoch, claimant),
        };
        let seeds: Vec<NodeId> = self.all.iter().filter(|x| *x != id).cloned().collect();
        let (group, cfg) = (self.group.clone(), cfg(&self.voters));
        let engine = GroupEngine::with_recovered(group, id.clone(), seeds, cfg, recovered);
        self.sim.add(engine);
        let applied = self.store.get(id).cloned().unwrap_or_default();
        self.reborn.insert(id.clone());
        self.seen.retain(|(observer, _), _| observer != id);
        let node = Node::new(id, applied);
        self.nodes.insert(id.clone(), node);
        self.stats.add(Restart, 1);
    }

    /// One fault, drawn uniformly from the first `slots` arms, ordered so a
    /// narrow draw is *migration-heavy*: `slots = 8` spends half the schedule
    /// killing or isolating the sitting host, the only way a hostship changes
    /// hands. Isolating it lapses its lease and lets the majority side elect a
    /// successor while its own watch catches up; the one-way flavour is the
    /// asymmetric partition the tier names, and the shape that fills a deposed
    /// host's view with readings stamped above the epoch it waits at. An arm
    /// whose guard fails falls through to churn, which keeps anti-entropy busy.
    fn inject_fault(&mut self, rng: &mut SplitMix64) {
        let live: BTreeSet<NodeId> = self.nodes.keys().cloned().collect();
        let is_down = |x: &&NodeId| !live.contains(*x);
        let down: BTreeSet<NodeId> = self.all.iter().filter(is_down).cloned().collect();
        // The group's writer, lowest id first, so the choice is deterministic.
        let serving = self.nodes.values().find(|node| node.hosting().is_some());
        let host = serving.map(|node| node.id.clone());
        match (rng.below(SLOTS), host) {
            (0, Some(host)) if live.len() > 2 => self.kill(&host),
            (1, Some(host)) if live.len() > 1 => self.cut(&host, &live, false),
            (5 | 11, Some(host)) if live.len() > 1 => self.cut(&host, &live, true),
            (2 | 6, host) => {
                let node = host.unwrap_or_else(|| pick(&live, rng));
                self.start_write(&node, true);
            }
            (3, _) if !down.is_empty() => {
                let node = pick(&down, rng);
                self.restart(&node);
            }
            (4 | 7, _) => self.sim.heal_all(),
            (8, _) if live.len() > 2 => {
                let victim = pick(&live, rng);
                self.kill(&victim);
            }
            // A burst: three writes in one round, which overruns any subscriber
            // that missed a beat on a small ring.
            (9 | 13, Some(host)) => {
                for i in 0..3 {
                    self.start_write(&host, i == 0);
                }
            }
            (10, _) if live.len() > 1 => {
                let (a, b) = (pick(&live, rng), pick(&live, rng));
                if a != b {
                    self.sim.block(&a, &b);
                    self.sim.block(&b, &a);
                }
            }
            (12, _) if live.len() > 1 => {
                let victim = pick(&live, rng);
                self.cut(&victim, &live, false);
            }
            (14, _) => {
                let victim = pick(&live, rng);
                self.nodes.get_mut(&victim).expect("live").stalled = 3 + rng.below(5);
                self.stats.add(Stalled, 1);
            }
            _ => {
                let node = pick(&live, rng);
                let churn = format!("v{}", self.now).into_bytes();
                self.sim.command(&node, set("kv", churn));
            }
        }
    }

    /// End of run: **S5** once more for every host still serving.
    fn finish(mut self) -> Stats {
        for id in self.live_ids() {
            let node = &self.nodes[&id];
            let serving = node.hosting().filter(|e| node.recovered_at == Some(*e));
            if let Some(epoch) = serving {
                self.check_s5(&id, epoch);
            }
        }
        self.stats
    }
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    let i = usize::try_from(rng.below(n)).expect("bounded by the set size");
    v[i].clone()
}

/// `PeerWrites`' cursor rule over one writer's feed, plus the lineage-opening
/// `Gap` the read half guarantees a fresh subscriber.
fn scan(
    node: &mut Node,
    peer: &NodeId,
    (frame, keys): (Ring, &[Vec<u8>]),
    advances: &mut Vec<Advance>,
    tag: Tag,
) {
    let at = |seq| tok(frame.epoch, seq);
    let Some(held) = node.cursors.get_mut(peer) else {
        // History is not replayed: open at the feed's current end, and record
        // the opening `Gap` — the coarse remediation the consumer owes.
        let opened = cursor(frame.epoch, frame.end());
        node.cursors.insert(peer.clone(), opened);
        if frame.len > 0 {
            advances.push((peer.clone(), at(frame.end() - 1), None));
        }
        return;
    };
    if frame.epoch < held.epoch {
        return; // a deposed hostship's late publish
    }
    if frame.epoch > held.epoch {
        // A new feed life. Epoch-major ordering makes this gap cover every write
        // of the previous one.
        advances.push((peer.clone(), at(frame.first_seq.saturating_sub(1)), None));
        *held = cursor(frame.epoch, frame.first_seq);
    } else if held.next < frame.first_seq {
        // The ring advanced past us: writes were provably missed, and the
        // remediation is what defines them applied.
        let missed = at(frame.first_seq.saturating_sub(1));
        advances.push((peer.clone(), missed, Some(at(held.next))));
        held.next = frame.first_seq;
    }
    while held.next < frame.end() {
        let index = usize::try_from(held.next - frame.first_seq).expect("inside the window");
        assert_eq!(
            keys[index],
            write_key(held.next),
            "{tag:?}: {peer}'s feed carried the wrong datum at {}",
            held.next
        );
        advances.push((peer.clone(), at(held.next), None));
        held.next += 1;
    }
}

/// The recovery rule's own arithmetic, re-derived from the raw view: how many
/// readings are fresh for `epoch`, and the per-writer maximum they name.
fn fresh_target(view: &[LedgerView], epoch: u64) -> (usize, Watermarks) {
    let mut fresh = 0usize;
    let mut target = Watermarks::new();
    for member in view {
        let Some(reading) = member.reading.as_ref().filter(|r| r.lead_epoch >= epoch) else {
            continue;
        };
        fresh += 1;
        for (writer, token) in &reading.applied {
            let entry = target.entry(writer.clone()).or_insert(ZERO);
            *entry = (*entry).max(*token);
        }
    }
    (fresh, target)
}

/// **P1.** A `Committed` verdict, re-derived from the raw bytes: a strict
/// majority, each stamped **exactly** the write's epoch and at or past it.
///
/// Negative control, re-derived against this corpus: poll the waits at
/// `Commit::Local` instead — the level that commits on the host's own apply —
/// and this fires at **seed 0**, on a write acknowledged by one reading where
/// the majority is two. The check is therefore re-deriving the majority rather
/// than echoing the core it is checking.
fn check_ack(view: &[LedgerView], host: &NodeId, token: WriteToken, majority: usize, tag: Tag) {
    let counts = |m: &&LedgerView| {
        m.reading.as_ref().is_some_and(|r| {
            r.lead_epoch == token.epoch && r.applied.get(host).is_some_and(|wm| *wm >= token)
        })
    };
    let counted: Vec<&NodeId> = view.iter().filter(counts).map(|m| &m.member).collect();
    let n = counted.len();
    assert!(
        n >= majority,
        "{tag:?}: {host}'s write {token:?} was acknowledged on {n} readings ({counted:?}), \
         short of {majority} — view {view:?}"
    );
}

/// **S5, P1, P2, P3 and the late-ack fence, over 128 seeds of the wide
/// schedule.** 3- and 5-node clusters over a roster of three, up to 22%
/// per-message loss, up to 8 ms of reordering jitter, host crashes, host
/// isolation (two-way and one-way), voter crashes and grant-recovering restarts,
/// stalled follower loops, write bursts, heals, and committed writes throughout
/// — half the corpus on a ring of two, where a burst overruns a subscriber and a
/// later recovery reaches its target by remediation rather than by replay.
///
/// The migration-heavy draw is next door in `hosted_dst_migrate.rs`: between the
/// two files every floor this corpus had before the split is still asserted, and
/// every assertion runs in both.
#[test]
fn dst_hosted_chaos_never_loses_an_acked_write() {
    let mut total = Stats::default();
    for seed in 0..128u64 {
        total.absorb(&chaos_scenario(seed));
    }
    let floors = [
        (Acked, "acknowledged write"),
        (Recovered, "completed recovery"),
        (Migration, "migration"),
        (S5Obligation, "S5 obligation on a serving host"),
        (RingGap, "ring overrun"),
        (GapRecovered, "recovery remediated through an overrun"),
        (Restart, "grant-recovering restart"),
        (Stalled, "stalled follower loop"),
    ];
    for (what, name) in floors {
        assert!(
            total.get(what) > 0,
            "vacuous: the corpus saw no {name} — {}",
            total.tally()
        );
    }
    // Printed on success too, so the floors are self-evidencing: a schedule that
    // has drifted towards the floor reports it here rather than the first time
    // it drifts past.
    println!("S5-chaos: {}", total.tally());
}

/// One chaotic run: elect under a fair fabric, then [`ROUNDS`] faults from all
/// [`SLOTS`] arms of [`Harness::inject_fault`]'s schedule, with every property
/// sampled at every round — then heal, quiesce, and hold S5 once more. The ring
/// is drawn per seed, so half this corpus runs the small one.
fn chaos_scenario(seed: u64) -> Stats {
    let mut rng = rng(seed ^ 0x5a17);
    let n = if rng.below(2) == 0 { 3 } else { 5 };
    let cap = if rng.below(2) == 0 { SMALL_RING } else { RING };
    let mut h = Harness::new(("S5-chaos", seed), n, &mut rng, cap);

    // Elect first, on a fair fabric: the properties below are about a running
    // write path, not about bootstrap.
    h.run_rounds(24, 60);
    h.sim
        .set_loss(u8::try_from(rng.below(23)).expect("below(23) is 0..23"));
    h.sim.set_jitter(u64::from(rng.below(9)));

    for _ in 0..ROUNDS {
        let step = u64::from(30 + rng.below(120));
        h.step_to(h.now + step);
        h.inject_fault(&mut rng);
    }

    // A fair fabric again, long enough for the survivors to elect, recover and
    // resolve every write still in flight.
    h.sim.heal_all();
    h.sim.set_loss(0);
    h.sim.set_jitter(0);
    h.run_rounds(40, 60);
    h.finish()
}
