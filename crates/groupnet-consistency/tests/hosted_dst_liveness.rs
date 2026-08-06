//! Deterministic Simulation Testing for the **Hosted write path** (M4),
//! liveness half: the sans-IO cores ([`CommitCore`], [`CompletenessCore`])
//! driven from real [`GroupEngine`] state, in virtual time, on a fabric that is
//! healthy — then cut, then healed. A failing seed is a reproducible
//! counterexample, not a flake.
//!
//! The safety half (**S5**, **P1**, **P2**, **P3**) is next door in
//! `hosted_dst.rs`, under a randomized fault schedule. The harness below is a
//! deliberate copy of that file's: duplicated helpers across sibling test files
//! is the house pattern (`groupnet-sim`'s `election.rs` /
//! `election_failover.rs`), and it keeps each suite's schedule and its
//! assertions readable in one place.
//!
//! # What is real here, and what is modelled
//!
//! Real: **the engines**. Every group is a real [`GroupMode::Hosted`] group
//! under [`Activation::Quorum`], so every epoch is closed by a real grant
//! majority over a real network; every ledger entry is authored with the tier's
//! own exported codec ([`encode_ledger`]) under its own key
//! ([`ledger_entry_key`]) and read back with [`decode_ledger`] out of *the
//! reading node's* entry view; a restart is [`GroupEngine::with_recovered`] fed
//! the sim's persisted grant, the `GrantStore` posture this tier presumes.
//!
//! Modelled, exactly as the safety file models it and for the same reasons: the
//! feed is harness-authored in `WriteFeed`'s own layout under the tier's own
//! feed name; "apply" is the watermark advance, folded and published the way
//! [`CommitLedger`](groupnet_consistency::CommitLedger) folds and publishes;
//! the follower is durable across restarts and does not replay history. The one
//! difference is the **leadership watch does not lag** here — the shell's watch
//! republish is prompt on a healthy fabric, and a lagging watch is a *safety*
//! question, which is the other file's subject.
//!
//! # The property
//!
//! * **H-L1 — liveness.** On a healthy run a host completes recovery and
//!   serves, and a [`Commit::QuorumApplied`] write resolves `Committed`. Cut the
//!   host off from the roster and the **minority side never serves**: it opens
//!   no new hostship at all, and once its lease has lapsed it is not serving
//!   even the epoch it held — while the majority side elects a successor that
//!   recovers and serves in its place. Heal, and service and commits come back.
//!
//! The safety properties travel with it rather than being switched off: every
//! observation still re-derives S5, P1, P2 and P3 from the raw entry bytes,
//! identically to the chaos file. What makes this suite *liveness* is the
//! schedule and the end-of-run assertions — that the run reached service and
//! resolved its writes at all, which no safety property can fail to hold
//! vacuously.

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

use Count::{Acked, Migration, MinorityQuiet, Recovered, Restart, S5Obligation, Served};

/// A host's authority after its last confirmed renewal round — and, on a boot
/// with no recovered grant, the blackout before it will grant a new claimant —
/// over a gossip cadence brisk enough that a republish lands well inside it.
const LEASE_MS: u64 = 400;
const GOSSIP_MS: u64 = 40;
/// The feed ring: larger than any write count here, so no `Gap` in this file is
/// an overflow. In-flight committed writes per host, and the fold's zero.
const RING: u64 = 32;
const MAX_INFLIGHT: usize = 3;
const ZERO: WriteToken = WriteToken { epoch: 0, seq: 0 };

/// A node's gossiped entry view, and the pair its leadership watch reports.
type Entries = BTreeMap<NodeId, BTreeMap<String, Vec<u8>>>;
type Lead = (u64, Option<NodeId>);
/// One watermark advance a follower loop owes its ledger: the writer, the token
/// applied (or the `missed_through` a `Gap` remediated to), and — when a ring
/// overrun produced it — the first write it jumped over.
type Advance = (NodeId, WriteToken, Option<WriteToken>);
/// Which suite and which seed a failure belongs to.
type Tag = (&'static str, u64);

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

/// The detector timings the Quorum election suites run on, with a static roster
/// over `voters`.
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

/// The datum one hosted write carries: small, and self-identifying, so a
/// follower can prove the bytes it applied are the write it thinks they are.
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

/// Everything a run is counted by: every floor the suite asserts, and every
/// number its tally prints, is one of these.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum Count {
    Acked,
    Recovered,
    Migration,
    Served,
    MinorityQuiet,
    Restart,
    S5Obligation,
}

/// What a whole run observed, summed across seeds: a suite that stops
/// exercising its own property fails loudly rather than passing vacuously.
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

/// A subscriber position in one writer feed — `PeerWrites` cursor.
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
    watch: Option<Lead>,
    /// The epoch the last ledger publish was stamped for — a change forces the
    /// `refresh` the deployment contract asks for.
    stamped_lead: Option<u64>,
    cursors: BTreeMap<NodeId, Cursor>,
    ring: Option<Ring>,
    recovered_at: Option<u64>,
    inflight: Vec<WriteToken>,
}

impl Node {
    fn new(id: &NodeId, applied: Watermarks) -> Self {
        Self {
            id: id.clone(),
            stamp: 0,
            applied,
            watch: None,
            stamped_lead: None,
            cursors: BTreeMap::new(),
            ring: None,
            recovered_at: None,
            inflight: Vec::new(),
        }
    }

    /// What this node's leadership watch reports.
    fn lead(&self) -> Lead {
        self.watch.clone().unwrap_or((0, None))
    }

    /// The epoch this node's watch names *it* the host of.
    fn hosting(&self) -> Option<u64> {
        let (epoch, host) = self.lead();
        (host.as_ref() == Some(&self.id)).then_some(epoch)
    }

    /// Whether this node is serving: host of an epoch it has recovered for.
    fn serving(&self) -> Option<u64> {
        self.hosting().filter(|e| self.recovered_at == Some(*e))
    }
}

/// The cluster, its engines, and one [`Node`] per *running* member.
#[derive(Debug)]
struct Harness {
    tag: Tag,
    group: GroupId,
    sim: Simulation,
    /// Every node that has ever run, in id order, and the static voter roster
    /// the rules are denominated over.
    all: Vec<NodeId>,
    voters: BTreeSet<NodeId>,
    majority: usize,
    nodes: BTreeMap<NodeId, Node>,
    /// The durable applied state a restart reseeds from, written at every apply.
    store: BTreeMap<NodeId, Watermarks>,
    /// The last reading each observer held for each publisher, and the nodes
    /// whose version clock a restart has reset — P3's inputs.
    seen: BTreeMap<(NodeId, NodeId), Vec<u8>>,
    reborn: BTreeSet<NodeId>,
    /// **The S5 obligation set**, and the hostships that have served.
    acked: Vec<(NodeId, WriteToken)>,
    served: BTreeSet<(u64, NodeId)>,
    /// The starved side, if the fabric is cut: each node mapped to the highest
    /// epoch it may still be serving. Serving *above* it is the CP violation.
    minority: BTreeMap<NodeId, u64>,
    ledger_key: String,
    feed_key: String,
    now: u64,
    stats: Stats,
}

impl Harness {
    /// A cluster of `n` nodes over a roster of three, all started at time zero.
    /// The roster is rotated, so it is not always the low ids and the rendezvous
    /// top of a run is sometimes outside it.
    fn new(tag: Tag, n: u32, rng: &mut SplitMix64) -> Self {
        let group = GroupId::new(format!("hosted-{}-{}", tag.0, tag.1));
        let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let off = usize::try_from(rng.below(n)).expect("bounded by n");
        let voters: BTreeSet<NodeId> = (0..3).map(|k| all[(off + k) % all.len()].clone()).collect();
        let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
        let mut nodes = BTreeMap::new();
        for id in &all {
            let seeds: Vec<NodeId> = all.iter().filter(|x| *x != id).cloned().collect();
            sim.add(GroupEngine::new(
                group.clone(),
                id.clone(),
                seeds,
                cfg(&voters),
            ));
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
            minority: BTreeMap::new(),
            ledger_key: ledger_entry_key(""),
            feed_key: feed_entry_key(),
            now: 0,
            stats: Stats::default(),
        }
    }

    fn live_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    /// One full observation at `now`: follow (checking every reading on the
    /// way), serve, then poll every write in flight.
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
            let lead = self.sim.leadership_of(&id).unwrap_or((0, None));
            self.nodes.get_mut(&id).expect("live").watch = Some(lead.clone());
            self.check_readings(&id, &view);
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

    /// The `record` / `refresh` pair: folds one node's advances into its ledger,
    /// republishes if anything moved, and writes the store beneath it.
    fn apply(&mut self, id: &NodeId, lead: &Lead, advances: Vec<Advance>) {
        let mut moved = false;
        {
            let node = self.nodes.get_mut(id).expect("a live node");
            for (writer, token, _) in advances {
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

    /// **P3.** No reading `observer` holds ever regresses for a publisher no
    /// restart has reset the version clock of. A restart ghost — an earlier
    /// life's publication still held beside the stamp of zero `with_recovered`
    /// starts at — is the one regression gossip can produce.
    ///
    /// The excuse is coarser than the property: it should end once the new life
    /// out-versions the ghost, but this harness reads entry *bytes* and never
    /// the version clock, so a restarted publisher stays excused for the rest of
    /// the run. `hosted_dst.rs` counts every use of the excuse; this file, whose
    /// schedule restarts exactly one node once, simply states the width.
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
                }
            }
            self.seen.insert(key, bytes.clone());
        }
    }

    /// The recovery gate, per node the watch names host — **S5** at every
    /// instant a host is serving, and the CP claim on a starved side.
    fn serve(&mut self) {
        let tag = self.tag;
        for id in self.live_ids() {
            let Some(epoch) = self.nodes[&id].hosting() else {
                continue;
            };
            if self.nodes[&id].recovered_at != Some(epoch) && !self.recover(&id, epoch) {
                continue;
            }
            // **H-L1, the CP half.** A node cut off from the roster may still be
            // serving the epoch it already held — until its lease lapses — but
            // it must never open a *new* hostship, because it can collect no
            // grant majority to close one.
            if let Some(held) = self.minority.get(&id) {
                assert!(
                    epoch <= *held,
                    "{tag:?}: {id} served at {epoch} from the minority side at {}, above the \
                     {held} it held when the fabric was cut",
                    self.now
                );
                self.stats.add(MinorityQuiet, 1);
            }
            self.stats.add(Served, 1);
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
        let migrated = !self.served.is_empty();
        self.nodes.get_mut(id).expect("a live node").recovered_at = Some(epoch);
        self.stats.add(Recovered, 1);
        self.stats.add(Migration, u64::from(migrated));
        self.served.insert((epoch, id.clone()));
        true
    }

    /// **S5.** Everything this run acknowledged below `epoch` is in this serving
    /// host's own applied state.
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
                let verdict = CommitCore::evaluate(&view, &id, token, Commit::QuorumApplied);
                if verdict.is_committed() {
                    check_ack(&view, &id, token, majority, tag);
                    self.stats.add(Acked, 1);
                    self.acked.push((id.clone(), token));
                } else if lead.0 != token.epoch || lead.1.as_ref() != Some(&id) {
                    // Deposed: no further reading can ever count this write.
                } else {
                    kept.push(token);
                }
            }
            self.nodes.get_mut(&id).expect("live").inflight = kept;
        }
    }

    /// Publishes one hosted write on `id` if the tier would admit it — the watch
    /// names it host, and recovery has opened for that epoch — into the feed,
    /// into its own ledger, and onto the commit wait list.
    fn start_write(&mut self, id: &NodeId) {
        let Some(epoch) = self.nodes.get(id).and_then(Node::serving) else {
            return; // `HostedError::NotHost` or `Recovering`
        };
        let node = self.nodes.get_mut(id).expect("live");
        // A fresh feed life the first time this node hosts at `epoch`.
        let mut life = node
            .ring
            .filter(|r| r.epoch == epoch)
            .unwrap_or(ring(epoch, 1, 0));
        let token = tok(epoch, life.end());
        life.len += 1;
        if life.len > RING {
            life.first_seq += 1;
            life.len -= 1;
        }
        node.ring = Some(life);
        // The host counts itself: `publish` records the write into its own
        // ledger before returning, so its own reading satisfies its predicate.
        let entry = node.applied.entry(id.clone()).or_insert(ZERO);
        *entry = (*entry).max(token);
        if node.inflight.len() < MAX_INFLIGHT {
            node.inflight.push(token);
        }
        self.sim.command(id, set(&self.feed_key, life.encode()));
        self.publish_ledger(id);
    }

    /// The node serving the group right now, if any — the one a write would be
    /// admitted on.
    fn serving_host(&self) -> Option<NodeId> {
        let serving = self.nodes.values().find(|node| node.serving().is_some());
        serving.map(|node| node.id.clone())
    }

    /// Whether every write `id` started has resolved, and how many are left.
    fn pending(&self, id: &NodeId) -> usize {
        self.nodes.get(id).map_or(0, |node| node.inflight.len())
    }

    /// Cuts `victim` off from every other live node, both ways, and records the
    /// epoch it may not serve above while it is starved.
    fn isolate(&mut self, victim: &NodeId) {
        let live = self.live_ids();
        for peer in live.iter().filter(|peer| *peer != victim) {
            self.sim.block(victim, peer);
            self.sim.block(peer, victim);
        }
        let held = self.nodes[victim].hosting().unwrap_or(0);
        self.minority.insert(victim.clone(), held);
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
        self.nodes.insert(id.clone(), Node::new(id, applied));
        self.stats.add(Restart, 1);
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

/// **H-L1.** 64 seeds, 3-node and 5-node clusters over a roster of three: a
/// healthy fabric elects, recovers and serves; a `QuorumApplied` write commits;
/// cutting the host off starves it — it opens no new hostship and stops serving
/// the one it held — while the majority side elects a successor that recovers
/// and serves; and after the heal, service and commits come back.
#[test]
fn dst_hosted_healthy_runs_serve_and_reestablish_after_a_heal() {
    let mut total = Stats::default();
    for seed in 0..64u64 {
        total.absorb(&healthy_scenario(seed));
    }
    let floors = [
        (Acked, "acknowledged write"),
        (Recovered, "completed recovery"),
        (Migration, "migration"),
        (Served, "serving observation"),
        (MinorityQuiet, "minority-side check"),
        (Restart, "grant-recovering restart"),
        (S5Obligation, "S5 obligation on a serving host"),
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
    println!("H-L1: {}", total.tally());
}

fn healthy_scenario(seed: u64) -> Stats {
    let mut rng = rng(seed ^ 0x11ee);
    let n = if rng.below(2) == 0 { 3 } else { 5 };
    let tag = ("H-L1", seed);
    let mut h = Harness::new(tag, n, &mut rng);

    // 1. A healthy fabric elects a host, and it recovers before it serves.
    h.run_rounds(30, 60);
    let host = h.serving_host().unwrap_or_else(|| {
        panic!(
            "{tag:?}: no host completed recovery on a fault-free fabric by {}",
            h.now
        )
    });

    // 2. A `QuorumApplied` write commits on the ack fast path.
    h.start_write(&host);
    h.run_rounds(10, 60);
    assert_eq!(
        h.pending(&host),
        0,
        "{tag:?}: the first committed write never resolved by {}",
        h.now
    );
    let after_first = h.stats.get(Acked);
    assert!(after_first > 0, "{tag:?}: the first write resolved unacked");

    // 3. Cut the host off: from here it may serve nothing above the epoch it
    //    held (asserted every round, inside `serve`), and the majority side must
    //    elect a successor. A voter restarts underneath, on its recovered grant.
    h.isolate(&host);
    let others: BTreeSet<NodeId> = h.all.iter().filter(|x| **x != host).cloned().collect();
    let victim = pick(&others, &mut rng);
    h.run_rounds(6, 60);
    h.nodes.remove(&victim);
    h.sim.crash(&victim);
    h.run_rounds(4, 60);
    h.restart(&victim);
    h.run_rounds(22, 60);
    assert!(
        h.nodes[&host].serving().is_none(),
        "{tag:?}: the stranded host was still serving at {} — a lease that never lapsed",
        h.now
    );

    // 4. Heal: a host serves again, and a write commits again.
    h.sim.heal_all();
    h.minority.clear();
    h.run_rounds(34, 60);
    let host = h.serving_host().unwrap_or_else(|| {
        panic!(
            "{tag:?}: no host completed recovery after the heal by {}",
            h.now
        )
    });
    h.start_write(&host);
    h.run_rounds(12, 60);
    assert_eq!(
        h.pending(&host),
        0,
        "{tag:?}: the post-heal committed write never resolved by {}",
        h.now
    );
    assert!(
        h.stats.get(Acked) > after_first,
        "{tag:?}: the post-heal write resolved unacked"
    );
    h.stats
}
