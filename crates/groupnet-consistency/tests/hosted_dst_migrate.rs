//! Deterministic Simulation Testing for the **Hosted write path** (M4) across
//! **hand-overs**: the same sans-IO cores ([`CommitCore`], [`CompletenessCore`])
//! driven from real [`GroupEngine`] state, in virtual time, on a schedule half
//! of whose arms kill or isolate the sitting host. A failing seed is a
//! reproducible counterexample, not a flake.
//!
//! The harness is a deliberate copy of `hosted_dst.rs`'s, which carries the full
//! account of what is real (the engines, the gossiped ledger entries, the tier's
//! own codec and keys, `GrantStore`-posture restarts) and what is modelled (a
//! harness-authored feed in `WriteFeed`'s own layout, "apply" as the watermark
//! advance, a durable follower that does not replay history). Duplicated helpers
//! across sibling test files is the house pattern (`groupnet-sim`'s
//! `election.rs` / `election_failover.rs`).
//!
//! # The two dimensions this file owns
//!
//! * **A migration-heavy schedule.** Eight fault arms, four of them aimed at the
//!   host: crash it, cut it off two ways, cut it off one way. A hostship only
//!   changes hands when the incumbent goes, so this is the draw that produces
//!   migrations densely enough to sample what happens across one.
//! * **A lagging leadership watch**, up to [`LAG`] steps behind each node's own
//!   engine — the lag the tier's honesty box names ("a deposed host can admit
//!   one more write before it knows"). It is what leaves a commit wait *still in
//!   flight at the old epoch* while the roster has moved to the new one, which
//!   is the only shape in which the late-ack fence can be observed at all.
//!
//! # The properties
//!
//! Every one is re-derived from the **raw entry bytes** rather than read back
//! off the core it checks. **S5** — no acked write is ever missing from a
//! serving host ([`Harness::check_s5`]); **P1** — ack soundness
//! ([`check_ack`]); **P2** — recovery exactness ([`Harness::recover`]); **P3** —
//! per-publisher-life monotonicity ([`Harness::check_readings`]); and the
//! **late-ack fence** ([`Harness::check_fence`]), which is this file's own: a
//! voter stamped above the write's epoch never counts, so once the remainder
//! cannot reach a majority the verdict must be `Pending`. Each passes vacuously
//! on a schedule that stopped producing the thing it is about, so the suite also
//! asserts what it *saw* — its **floors** — and prints the tally either way.

#![cfg(feature = "hosted")]

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use groupnet_consistency::WriteToken;
use groupnet_consistency::hosted::{Watermarks, hosted_feed_name};
use groupnet_core::{Activation, Command, Config, GroupMode, HostedConfig, NodeId, VoterRoster};
use groupnet_sim::SplitMix64;

use Count::{Acked, FenceBlocked, FenceSeen, Migration, Recovered, Restart, S5Obligation};

/// A host's authority after its last confirmed renewal round — and, on a boot
/// with no recovered grant, the blackout before it will grant a new claimant —
/// over a gossip cadence brisk enough that a republish lands well inside it.
const LEASE_MS: u64 = 400;
const GOSSIP_MS: u64 = 40;
/// The feed ring, roomy enough that a `Gap` in this file is a migration and
/// never an overflow — the overrun corpus is `hosted_dst.rs`'s.
const RING: u64 = 32;
/// The schedule: eight fault arms, half of them aimed at the host, over this
/// many rounds; and the watch lag, drawn per node from `0..LAG`, which is what
/// leaves a commit wait in flight across a hand-over.
const SLOTS: u32 = 8;
const ROUNDS: u32 = 44;
const LAG: u32 = 4;
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
    Recovered,
    GapRecovered,
    RingGap,
    Migration,
    FenceSeen,
    FenceBlocked,
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

/// One committed write mid-flight (`fenced`: the late-ack race is counted once
/// per write), and a subscriber's position in one writer's feed.
#[derive(Debug, Clone, Copy)]
struct InFlight {
    token: WriteToken,
    fenced: bool,
}

#[derive(Debug, Clone, Copy)]
struct Cursor {
    epoch: u64,
    next: u64,
}

fn cursor(epoch: u64, next: u64) -> Cursor {
    Cursor { epoch, next }
}

/// One node's whole harness state: the ledger's published half, the lagging
/// watch, the follower cursors, the feed life, and `HostedWrites`' latch.
#[derive(Debug)]
struct Node {
    id: NodeId,
    stamp: u64,
    applied: Watermarks,
    lag: usize,
    watch: VecDeque<Lead>,
    /// The epoch the last ledger publish was stamped for — a change forces the
    /// `refresh` the deployment contract asks for — and the **lowest** write a
    /// ring overrun ever jumped this node over, per writer: the first hole in
    /// its coverage, so a target at or above it was reached by remediation.
    stamped_lead: Option<u64>,
    cursors: BTreeMap<NodeId, Cursor>,
    skipped: BTreeMap<NodeId, WriteToken>,
    ring: Option<Ring>,
    recovered_at: Option<u64>,
    inflight: Vec<InFlight>,
}

impl Node {
    fn new(id: &NodeId, lag: usize, applied: Watermarks) -> Self {
        Self {
            id: id.clone(),
            stamp: 0,
            applied,
            lag,
            watch: VecDeque::new(),
            stamped_lead: None,
            cursors: BTreeMap::new(),
            skipped: BTreeMap::new(),
            ring: None,
            recovered_at: None,
            inflight: Vec::new(),
        }
    }

    /// What this node's (lagging) leadership watch currently reports.
    fn lead(&self) -> Lead {
        self.watch.front().cloned().unwrap_or((0, None))
    }

    /// The epoch this node's watch names *it* the host of.
    fn hosting(&self) -> Option<u64> {
        let (epoch, host) = self.lead();
        (host.as_ref() == Some(&self.id)).then_some(epoch)
    }
}

// The cluster harness that drives all of the above, and the migration-heavy
// schedule it runs, live beside this file in `hosted_dst_migrate/harness.rs`.
#[path = "hosted_dst_migrate/harness.rs"]
mod harness;

use harness::migrate_scenario;

/// **S5, P1, P2, P3 and the late-ack fence across hand-overs, over 64 seeds.**
/// 3- and 5-node clusters over a roster of three, up to 22% per-message loss, up
/// to 8 ms of reordering jitter, and a schedule half of whose arms kill or
/// isolate the sitting host — with a watch lagging up to four steps, so a commit
/// wait really is still in flight at the old epoch while the roster has moved to
/// the new one.
///
/// The floors here are the ones a migration-heavy draw is *entitled* to see; the
/// ring-overrun and stalled-follower floors belong to the wider schedule in
/// `hosted_dst.rs`. Between the two files every floor the corpus had before the
/// split is still asserted, and every assertion runs in both.
#[test]
fn dst_hosted_migrations_never_lose_an_acked_write() {
    let mut total = Stats::default();
    for seed in 0..64u64 {
        total.absorb(&migrate_scenario(seed));
    }
    let floors = [
        (Acked, "acknowledged write"),
        (Recovered, "completed recovery"),
        (Migration, "migration"),
        (S5Obligation, "S5 obligation on a serving host"),
        (FenceSeen, "commit wait facing a higher-stamped voter"),
        (FenceBlocked, "commit wait a moved-on roster held open"),
        (Restart, "grant-recovering restart"),
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
    println!("S5-migrate: {}", total.tally());
}
