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
use groupnet_consistency::hosted::{Watermarks, hosted_feed_name};
use groupnet_core::{Activation, Command, Config, GroupMode, HostedConfig, NodeId, VoterRoster};
use groupnet_sim::SplitMix64;

use Count::{Acked, GapRecovered, Migration, Recovered, Restart, RingGap, S5Obligation, Stalled};

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

// The cluster harness that drives all of the above, and the chaos schedule it
// runs, live beside this file in `hosted_dst/harness.rs`.
#[path = "hosted_dst/harness.rs"]
mod harness;

use harness::chaos_scenario;

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
