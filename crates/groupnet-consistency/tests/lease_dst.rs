//! Deterministic Simulation Testing for the **coherence-lease tier** (T3):
//! the sans-IO cores ([`LeaseCore`], [`CoherenceCore`]) driven from real
//! [`GroupEngine`] state, in virtual time, under the same randomized chaos
//! schedule `groupnet-sim`'s election suite runs. A failing seed is a
//! reproducible counterexample, not a flake.
//!
//! # What is real here, and what is modelled
//!
//! Real, and the whole point: **the engines**. Every renewal is an actual
//! `~lease` entry written into a real [`GroupEngine`] under a TTL of one lease
//! duration, gossiped over a lossy, jittered, partitionable network to real
//! peers, each of which arms its own expiry at its own adoption instant. A
//! granter's `~lease:g` map is folded from the renewals *its* engine can see; a
//! reader's roster and confirmations come out of *its* engine; a writer's wait
//! set is the members holding an unexpired `~lease` entry **in that writer's
//! engine**, read in exact virtual time through
//! [`Simulation::entry_expires_at_of`]. Nothing about the timing is faked: the
//! lapse a writer waits out is a TTL a different node armed.
//!
//! Modelled, deliberately and named so nobody mistakes it for tested:
//!
//! * **The roster is every not-reaped member.** The suite does not gossip
//!   `~caps`; it treats every node as advertising [`CAP_LEASE`]. That is the
//!   *maximal* min-set — the conservative direction, and the only one where the
//!   roster rule bites: a `Suspect` or `Dead`-but-not-reaped granter stays in it
//!   and freezes confirmation, and only the engine's own reap horizon removes
//!   one. Modelling capability entries could only ever shrink the set.
//! * **Applied-watermark acks are a two-entry stand-in for the T2 ledger.** A
//!   writer publishes its newest [`WriteToken`] under `~dst-write`; a peer that
//!   can *see* that entry in its own engine publishes the token back under
//!   `~dst-applied:<writer>`. Same shape as
//!   [`AckLedger`](groupnet_consistency::AckLedger), same round trip, same
//!   partition behaviour, none of the ledger's plumbing (`tests/acks.rs`).
//! * **The consumer's resynchronization is a scripted lag**: after a lapse a
//!   reader owes `resync_lag` observation steps of "flushing the cache" before
//!   it affirms catch-up (`0` = the cache that flushes instantly).
//! * **A booting node observes before it participates** ([`CONVERGE_MS`]) — the
//!   rule the tokio shell enforces for itself, modelled here because these cores
//!   are its sans-IO halves and know nothing of their own boot. Without it a
//!   *reader* serves on a roster it has not finished learning (the
//!   vacuous-confirmation footgun the shell's reader guard closes) and a
//!   *writer* completes a coherent write without waiting for lease-holders it
//!   has not heard of.
//!
//! # The properties
//!
//! * **L-P1 — the Gray–Cheriton core.** Whenever a writer's [`CoherenceCore`]
//!   excuses a silent reader because that reader's `~lease` entry expired *in
//!   the writer's engine*, the reader's own [`LeaseCore`] provably had no
//!   window left: `serve_until + rate_margin <= the writer's expiry instant`,
//!   and it is not `Serving` at that instant. It then stays out of service
//!   until its consumer affirms catch-up **after** the lapse.
//! * **L-P2 — the window is only ever bought by confirmation.** `serve_until`
//!   is exactly `s_i + D - rate_margin` for the confirmed renewal `i`, it never
//!   extends without `confirmed` advancing, and `i` is exactly the min over the
//!   roster of what each granter's map — re-decoded from raw engine bytes —
//!   really advertises.
//! * **L-P3 — a restart is a new life.** A restarted reader (fresh core, new
//!   boot epoch) is never `Serving` before its first successful
//!   `mark_caught_up` of that life, and grants naming its *previous* life
//!   confirm nothing. Ghost `~lease` entries left behind by a crash only ever
//!   *add* members to writers' wait sets — the safe direction.
//!
//! The liveness property (**L-L1**) is next door in `lease_dst_liveness.rs`, on
//! its own copy of this harness — duplicated helpers across sibling test files
//! is the house pattern (`groupnet-sim`'s `election.rs` / `election_failover.rs`),
//! and the alternative a shared `tests/` module would be is one this repository
//! rules out.
//!
//! **The floor under the excuse classes.** L-P1 is asserted at a *lapse event*,
//! and three classes of lapse event carry no assertion at all (`reader_gone`,
//! `diverged`, `vanished` — each for a reason the tier documents). A schedule
//! that drifted into producing only those would pass every assertion here while
//! proving nothing, so each suite asserts that the contract was **checked** more
//! often than it was excused, and prints the whole tally when it was not.

#![cfg(feature = "leases")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use groupnet_consistency::WriteToken;
use groupnet_consistency::lease::{
    ClockMs, CoherenceCore, CoherenceStep, GrantMap, LeaseConfig, LeaseCore, LeaseState, RenewalId,
    WaitMember, decode_grants, decode_renewal, encode_grants, encode_renewal, grant_entry_key,
    renewal_entry_key,
};
use groupnet_core::{Command, Config, GroupEngine, GroupId, GroupMode, NodeId, Time};
use groupnet_sim::{Simulation, SplitMix64};

/// The lease duration `D` every suite here runs on. Short enough that a
/// handful of seconds of virtual time covers many lease lifetimes, long enough
/// that a renewal survives two lost gossip rounds.
const LEASE_MS: u64 = 600;
/// The renewal cadence, exactly as [`LeaseConfig::for_duration`] derives it.
const RENEW_MS: u64 = LEASE_MS / 3;
/// The reader's rate margin, exactly as [`LeaseConfig::for_duration`] derives
/// it: `max(D/100, 5ms)`.
const MARGIN_MS: u64 = LEASE_MS / 100;
/// How long a coherent write may stay in flight before the harness abandons
/// it. Far past one lease duration, so a lapse always beats it to the answer.
const WRITE_DEADLINE_MS: u64 = 6 * LEASE_MS;

/// How long after joining a node observes without participating: one lease
/// duration plus a gossip round, exactly the rule the tier's honesty box
/// states for a booting node.
const CONVERGE_MS: u64 = LEASE_MS + 40;

/// The harness's stand-in for an invalidating write: the writer authors its
/// newest [`WriteToken`] here, and a peer has "applied" the write once this
/// entry is visible in that peer's own engine.
const WRITE_KEY: &str = "~dst-write";

/// The entry one node publishes to acknowledge `writer`'s newest applied
/// token — the shape of an [`AckLedger`](groupnet_consistency::AckLedger)
/// watermark, one key per writer.
fn ack_key(writer: &NodeId) -> String {
    format!("~dst-applied:{writer}")
}

/// Seeds the shared deterministic PRNG so each schedule is reproducible.
fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

/// The tier's tuning, pinned to the constants the assertions do arithmetic
/// with.
fn lease_cfg() -> LeaseConfig {
    let cfg = LeaseConfig::for_duration(Duration::from_millis(LEASE_MS));
    assert_eq!(cfg.duration_ms(), LEASE_MS);
    assert_eq!(cfg.renew_every_ms(), RENEW_MS);
    assert_eq!(cfg.rate_margin_ms(), MARGIN_MS);
    cfg
}

/// Membership timings: brisk gossip so a renewal lands inside a fraction of a
/// lease, and a `dead_timeout` whose reap horizon (`2×`) sits well past one
/// lease duration — so a granter that goes quiet freezes confirmation for at
/// least a whole lease before membership divergence can excuse it.
fn cfg() -> Config {
    Config {
        gossip_interval_ms: 40,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: 1_000,
        indirect_probes: 2,
        fanout: 4,
        anti_entropy_interval_ms: 40,
        anti_entropy_fanout: 2,
        eager_push: true,
        full_digest_every: 4,
        max_delta_frame_bytes: 4_096,
        mode: GroupMode::Eventual,
    }
}

/// An engine for `id` bootstrapped against every other node in `peers`.
fn engine(group: &GroupId, id: &NodeId, peers: &[NodeId]) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg())
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("these clusters are a handful of nodes");
    v[rng.below(n) as usize].clone()
}

/// `(u64 epoch, u64 seq)` little-endian — the same 16-byte shape the tier's own
/// renewals use, written out here rather than borrowed so the ack stand-in and
/// the protocol under test share no codec.
fn encode_token(token: WriteToken) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&token.epoch.to_le_bytes());
    out.extend_from_slice(&token.seq.to_le_bytes());
    out
}

/// The token in `bytes`, or `None` if they are not one.
fn decode_token(bytes: &[u8]) -> Option<WriteToken> {
    Some(WriteToken {
        epoch: u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?),
        seq: u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?),
    })
}

/// Which suite and which seed a failure belongs to — carried into every
/// assertion message so a counterexample is one `cargo test` away.
#[derive(Debug, Clone, Copy)]
struct Tag {
    suite: &'static str,
    seed: u64,
}

/// What a whole run observed. Summed across seeds and asserted on at the end,
/// so a suite that stops exercising its own property fails loudly instead of
/// passing vacuously. Three counters name the lapse-path resolutions L-P1 is
/// *not* held against, each for a reason the tier documents: `reader_gone` (the
/// straggler had crashed, so it serves nothing at all), `diverged` (the writer
/// had left the straggler's roster — membership divergence) and `vanished` (the
/// entry left the writer's view without expiring, its member record having been
/// reaped, so no TTL bound applies).
#[derive(Debug, Default, Clone)]
struct Stats {
    /// Lapse-path resolutions whose L-P1 contract was actually checked.
    contract_checked: u64,
    all_applied: u64,
    resolved_by_lapse: u64,
    timed_out: u64,
    reader_gone: u64,
    diverged: u64,
    vanished: u64,
    /// Grant maps advertising a renewal from a reader's *previous* lease life.
    ghost_grants: u64,
    /// Observations of a reader in [`LeaseState::Serving`], and of a return to
    /// service gated on a post-lapse catch-up affirmation.
    serving: u64,
    resync_after_lapse: u64,
}

impl Stats {
    fn absorb(&mut self, other: &Stats) {
        self.contract_checked += other.contract_checked;
        self.all_applied += other.all_applied;
        self.resolved_by_lapse += other.resolved_by_lapse;
        self.timed_out += other.timed_out;
        self.reader_gone += other.reader_gone;
        self.diverged += other.diverged;
        self.vanished += other.vanished;
        self.ghost_grants += other.ghost_grants;
        self.serving += other.serving;
        self.resync_after_lapse += other.resync_after_lapse;
    }

    /// The lapse-path tally as one line: what the suite checked against what it
    /// excused. Carried into every end-of-run assertion message, so a schedule
    /// that stops producing checkable lapses reports the *shape* of its drift
    /// rather than just a failed predicate.
    fn tally(&self) -> String {
        format!(
            "checked {}, excused: reader_gone {} + diverged {} + vanished {}; \
             writes: all_applied {}, resolved_by_lapse {}, timed_out {}",
            self.contract_checked,
            self.reader_gone,
            self.diverged,
            self.vanished,
            self.all_applied,
            self.resolved_by_lapse,
            self.timed_out,
        )
    }
}

/// One coherent write mid-flight.
#[derive(Debug, Clone, Copy)]
struct InFlight {
    token: WriteToken,
    started: u64,
}

/// One member the harness saw drop out of a writer's wait set — the lapse
/// event L-P1 is asserted at, recorded at the instant it happened rather than
/// at the instant the verdict fired (a straggler leaves the wait set possibly
/// several polls before the last waiter clears).
#[derive(Debug)]
struct Lapse {
    writer: NodeId,
    straggler: NodeId,
    at: u64,
    /// The writer's own expiry stamp for the straggler's `~lease`, if the
    /// entry expired rather than vanishing with its member.
    expiry: Option<u64>,
}

/// One node's whole harness state: both sans-IO cores, the ground truth the
/// assertions are read against, and the bookkeeping the two roles need.
#[derive(Debug)]
struct Node {
    id: NodeId,
    /// This lease life. Bumped on every restart, and used as both the
    /// [`LeaseCore`] epoch and the [`WriteToken`] epoch, so a restarted node's
    /// renewals and writes both out-rank its previous life's.
    boot: u64,
    lease: LeaseCore,
    coherence: CoherenceCore,
    /// The harness's own record of `seq -> s_i`, kept independently of the
    /// core so L-P2 checks the core's arithmetic against ground truth.
    published: BTreeMap<u64, u64>,
    /// The granters last handed to [`LeaseCore::set_roster`].
    roster: BTreeSet<NodeId>,
    next_renew: u64,
    /// The instant this node has been in the group long enough to affirm
    /// catch-up or start a coherent write — see [`CONVERGE_MS`].
    converged_at: u64,
    /// Observation steps of "flushing the cache" still owed before this
    /// consumer affirms catch-up, and the instant of the last
    /// [`LeaseCore::mark_caught_up`] that took.
    resync_owed: u32,
    last_resync: Option<u64>,
    /// The newest instant at which a writer excused this node by lapse, until
    /// it has legitimately returned to service.
    lapse_watch: Option<u64>,
    /// The previous observation's window and confirmation, for L-P2.
    last_serve_until: Option<u64>,
    last_confirmed: Option<RenewalId>,
    /// The watermarks this node has published, per writer.
    acked: BTreeMap<NodeId, WriteToken>,
    write_seq: u64,
    inflight: Option<InFlight>,
    /// The wait set as of the last poll — the harness's mirror of the core's,
    /// used to spot the exact step a member drops out — and the members already
    /// excused by lapse for the write in flight.
    waiting: BTreeSet<NodeId>,
    lapsed_out: BTreeSet<NodeId>,
    /// The last `~lease` expiry instant this node saw for each member, so a
    /// member that leaves the wait set can be classified even once the engine
    /// has reaped the expired entry.
    seen_expiry: BTreeMap<NodeId, u64>,
}

impl Node {
    /// A node joining at `joined`, renewing first at `next_renew`.
    fn new(id: &NodeId, boot: u64, joined: u64, next_renew: u64, resync_owed: u32) -> Self {
        Self {
            id: id.clone(),
            boot,
            lease: LeaseCore::new(id.clone(), &lease_cfg(), boot),
            coherence: CoherenceCore::new(id.clone()),
            published: BTreeMap::new(),
            roster: BTreeSet::new(),
            next_renew,
            converged_at: joined + CONVERGE_MS,
            resync_owed,
            last_resync: None,
            lapse_watch: None,
            last_serve_until: None,
            last_confirmed: None,
            acked: BTreeMap::new(),
            write_seq: 0,
            inflight: None,
            waiting: BTreeSet::new(),
            lapsed_out: BTreeSet::new(),
            seen_expiry: BTreeMap::new(),
        }
    }
}

/// The cluster, its engines, and one [`Node`] per *running* member.
#[derive(Debug)]
struct Harness {
    tag: Tag,
    group: GroupId,
    sim: Simulation,
    /// Every node that has ever run, in id order.
    all: Vec<NodeId>,
    /// The lease life each node is currently in (survives its crashes).
    boots: BTreeMap<NodeId, u64>,
    /// The running nodes only — a crashed node has no core and serves nothing.
    nodes: BTreeMap<NodeId, Node>,
    renewal_key: String,
    grant_key: String,
    now: u64,
    resync_lag: u32,
    stats: Stats,
}

impl Harness {
    /// A cluster of `n` nodes, all started at time zero with staggered renewal
    /// phases so renewals do not arrive as one herd.
    fn new(tag: Tag, n: u32, rng: &mut SplitMix64, resync_lag: u32) -> Self {
        let group = GroupId::new(format!("lease-{}", tag.seed));
        let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
        let mut nodes = BTreeMap::new();
        let mut boots = BTreeMap::new();
        for id in &all {
            sim.add(engine(&group, id, &all));
            boots.insert(id.clone(), 1);
            let phase = u64::from(rng.below(u32::try_from(RENEW_MS).unwrap_or(u32::MAX)));
            nodes.insert(id.clone(), Node::new(id, 1, 0, phase, resync_lag));
        }
        Self {
            tag,
            group,
            sim,
            all,
            boots,
            nodes,
            renewal_key: renewal_entry_key(""),
            grant_key: grant_entry_key(""),
            now: 0,
            resync_lag,
            stats: Stats::default(),
        }
    }

    fn live_ids(&self) -> Vec<NodeId> {
        self.nodes.keys().cloned().collect()
    }

    /// The instant `observer` will expire its copy of `holder`'s renewal, in
    /// exact virtual time. `None` once the entry is absent, tombstoned or
    /// reaped.
    fn lease_expiry(&self, observer: &NodeId, holder: &NodeId) -> Option<u64> {
        self.sim
            .entry_expires_at_of(observer, holder, &self.renewal_key)
            .map(|at| at.0)
    }

    /// Whether `observer` currently holds an unexpired renewal from `holder` —
    /// the writer-side membership test of the wait set, and the granter-side
    /// test of what may enter a grant map.
    fn lease_live(&self, observer: &NodeId, holder: &NodeId) -> bool {
        self.lease_expiry(observer, holder)
            .is_some_and(|at| self.now < at)
    }

    /// Advances virtual time to `now` and takes one full observation: publish,
    /// ingest, then step every writer and hold the lapse contract against every
    /// reader one of them named.
    fn step_to(&mut self, now: u64) {
        self.now = now;
        self.sim.run_until(Time(now));
        self.publish();
        self.ingest_and_serve();
        let lapses = self.step_writers();
        self.check_lapse_contract(&lapses);
    }

    fn run_rounds(&mut self, rounds: u32, every_ms: u64) {
        for _ in 0..rounds {
            self.step_to(self.now + every_ms);
        }
    }

    /// Everything a node writes each round, all off one snapshot of its own
    /// engine: its wholesale `~lease:g` grant map, its own renewal when one is
    /// due (`s_i` recorded *first*, which is the inequality the whole tier
    /// rests on), and an applied-watermark ack for every write it can now see.
    fn publish(&mut self) {
        let now = self.now;
        for id in self.live_ids() {
            let view = self.sim.entries_snapshot(&id);
            let mut grants = GrantMap::new();
            let mut acks: Vec<(NodeId, WriteToken)> = Vec::new();
            for (peer, entries) in &view {
                if *peer == id {
                    continue;
                }
                if let Some(renewal) = entries
                    .get(&self.renewal_key)
                    .and_then(|bytes| decode_renewal(bytes))
                    .filter(|_| self.lease_live(&id, peer))
                {
                    grants.insert(peer.clone(), renewal);
                }
                let seen = entries.get(WRITE_KEY).and_then(|bytes| decode_token(bytes));
                if let Some(token) = seen.filter(|seen| {
                    self.nodes[&id]
                        .acked
                        .get(peer)
                        .is_none_or(|held| held < seen)
                }) {
                    acks.push((peer.clone(), token));
                }
            }

            let encoded = encode_grants(&grants);
            if view.get(&id).and_then(|e| e.get(&self.grant_key)) != Some(&encoded) {
                let key = self.grant_key.clone();
                self.set(&id, key, encoded, None);
            }
            for (writer, token) in acks {
                let node = self.nodes.get_mut(&id).expect("a live node");
                node.acked.insert(writer.clone(), token);
                self.set(&id, ack_key(&writer), encode_token(token), None);
            }

            let renewal = {
                let node = self.nodes.get_mut(&id).expect("a live node");
                (now >= node.next_renew).then(|| {
                    node.next_renew = now + RENEW_MS;
                    let renewal = node.lease.on_renew(ClockMs(now));
                    node.published.insert(renewal.seq, now);
                    renewal
                })
            };
            if let Some(renewal) = renewal {
                let key = self.renewal_key.clone();
                self.set(&id, key, encode_renewal(renewal), Some(LEASE_MS));
            }
        }
    }

    /// Authors one entry on `node`'s engine.
    fn set(&mut self, node: &NodeId, key: String, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.sim
            .command(node, Command::SetLocalEntry { key, value, ttl_ms });
    }

    /// The reader's ingest — roster, then every granter's advertised map — the
    /// consumer's resync policy, and L-P2/L-P3's per-observation invariants.
    fn ingest_and_serve(&mut self) {
        let (now, lag, tag) = (self.now, self.resync_lag, self.tag);
        for id in self.live_ids() {
            let view = self.sim.entries_snapshot(&id);
            // Every member this node still knows about — `Suspect` and
            // `Dead`-but-not-reaped included; only a reap removes one.
            let roster: BTreeSet<NodeId> = self
                .all
                .iter()
                .filter(|peer| **peer != id && self.sim.status_of(&id, peer).is_some())
                .cloned()
                .collect();
            let maps: BTreeMap<NodeId, GrantMap> = roster
                .iter()
                .map(|granter| {
                    let map = view
                        .get(granter)
                        .and_then(|entries| entries.get(&self.grant_key))
                        .map(|bytes| decode_grants(bytes))
                        .unwrap_or_default();
                    (granter.clone(), map)
                })
                .collect();

            let node = self.nodes.get_mut(&id).expect("a live node");
            node.roster = roster;
            node.lease.set_roster(node.roster.iter().cloned());
            for (granter, map) in &maps {
                node.lease.observe_grant_map(granter, map);
            }

            let state = node.lease.poll(ClockMs(now));
            if state == LeaseState::Lapsed {
                node.resync_owed = lag; // a fresh lapse restarts the flush
            }
            if state != LeaseState::Serving && now >= node.converged_at {
                if node.resync_owed > 0 {
                    node.resync_owed -= 1;
                } else if node.lease.mark_caught_up(ClockMs(now)) {
                    node.last_resync = Some(now);
                }
            }
            check_reader(node, &maps, now, tag, &mut self.stats);
        }
    }

    /// One poll of every in-flight coherent write, against the live
    /// lease-holders in that writer's *own* engine.
    fn step_writers(&mut self) -> Vec<Lapse> {
        let now = self.now;
        let mut lapses = Vec::new();
        for id in self.live_ids() {
            let Some(inflight) = self.nodes[&id].inflight else {
                continue;
            };
            let view = self.sim.entries_snapshot(&id);
            let mut snapshot: Vec<WaitMember> = Vec::new();
            let mut expiries: Vec<(NodeId, u64)> = Vec::new();
            for (holder, entries) in &view {
                if *holder == id || !entries.contains_key(&self.renewal_key) {
                    continue;
                }
                let Some(expiry) = self.lease_expiry(&id, holder).filter(|at| now < *at) else {
                    continue;
                };
                expiries.push((holder.clone(), expiry));
                snapshot.push(WaitMember {
                    member: holder.clone(),
                    applied: entries.get(&ack_key(&id)).and_then(|b| decode_token(b)),
                });
            }
            let present: BTreeSet<NodeId> =
                snapshot.iter().map(|held| held.member.clone()).collect();

            let (verdict, dropped) = {
                let node = self.nodes.get_mut(&id).expect("a live node");
                for (holder, expiry) in expiries {
                    node.seen_expiry.insert(holder, expiry);
                }
                let dropped: Vec<NodeId> = node.waiting.difference(&present).cloned().collect();
                for straggler in &dropped {
                    node.lapsed_out.insert(straggler.clone());
                }
                (node.coherence.step(inflight.token, &snapshot), dropped)
            };
            for straggler in dropped {
                let expiry = self
                    .lease_expiry(&id, &straggler)
                    .or_else(|| self.nodes[&id].seen_expiry.get(&straggler).copied());
                lapses.push(Lapse {
                    writer: id.clone(),
                    straggler,
                    at: now,
                    expiry,
                });
            }
            self.settle_write(&id, inflight, verdict);
        }
        lapses
    }

    /// Records one write's verdict, cross-checking the core's straggler list
    /// against the drops the harness saw for itself.
    fn settle_write(&mut self, id: &NodeId, inflight: InFlight, verdict: CoherenceStep) {
        let (now, tag) = (self.now, self.tag);
        let node = self.nodes.get_mut(id).expect("a live node");
        match verdict {
            CoherenceStep::Waiting { on } => {
                node.waiting = on.into_iter().collect();
                if now.saturating_sub(inflight.started) <= WRITE_DEADLINE_MS {
                    return;
                }
                let _ = node.coherence.abandon(inflight.token);
                self.stats.timed_out += 1;
            }
            CoherenceStep::AllApplied => {
                assert!(
                    node.lapsed_out.is_empty(),
                    "{} seed {}: {id} reported AllApplied after excusing {:?}",
                    tag.suite,
                    tag.seed,
                    node.lapsed_out
                );
                self.stats.all_applied += 1;
            }
            CoherenceStep::LeaseLapsed { stragglers } => {
                let named: BTreeSet<NodeId> = stragglers.into_iter().collect();
                assert_eq!(
                    named, node.lapsed_out,
                    "{} seed {}: {id} named stragglers the harness did not see drop out of its wait set",
                    tag.suite, tag.seed
                );
                self.stats.resolved_by_lapse += 1;
            }
        }
        let node = self.nodes.get_mut(id).expect("a live node");
        node.inflight = None;
        node.waiting.clear();
        node.lapsed_out.clear();
    }

    /// **L-P1.** For every member a writer excused by lapse: the reader's own
    /// window had provably closed at least `rate_margin` before the writer's
    /// engine expired its copy, and the reader is not serving at that instant.
    fn check_lapse_contract(&mut self, lapses: &[Lapse]) {
        let tag = self.tag;
        for lapse in lapses {
            let Lapse {
                writer,
                straggler,
                at,
                expiry,
            } = lapse;
            let Some(reader) = self.nodes.get(straggler) else {
                self.stats.reader_gone += 1; // crashed: it serves nothing at all
                continue;
            };
            if !reader.roster.contains(writer) {
                self.stats.diverged += 1; // the writer is outside its min-set
                continue;
            }
            let Some(expiry) = expiry.filter(|at_expiry| at_expiry <= at) else {
                self.stats.vanished += 1; // the member record went, not the TTL
                continue;
            };
            let (suite, seed) = (tag.suite, tag.seed);
            let until = reader.lease.serve_until().map(|at_until| at_until.0);
            assert!(
                until.is_none_or(|at_until| at_until + MARGIN_MS <= expiry),
                "{suite} seed {seed}: {writer} expired {straggler}'s lease at {expiry} \
                 (seen at {at}) while {straggler} still claimed a window to {until:?} — a \
                 reader's window must close a {MARGIN_MS}ms margin before its granters' copies"
            );
            assert_ne!(
                reader.lease.peek(ClockMs(*at)),
                LeaseState::Serving,
                "{suite} seed {seed}: {writer} proceeded past {straggler} at {at} on the \
                 lapse path while {straggler} was still serving cached state"
            );
            self.stats.contract_checked += 1;
            let node = self.nodes.get_mut(straggler).expect("read above");
            node.lapse_watch = Some(node.lapse_watch.unwrap_or(0).max(*at));
        }
    }

    /// Starts a coherent write on `writer` (a no-op if one is already in
    /// flight): publish the token, then wait on the lease-holders.
    fn start_write(&mut self, writer: &NodeId) {
        let now = self.now;
        let Some(node) = self.nodes.get_mut(writer) else {
            return;
        };
        if node.inflight.is_some() || now < node.converged_at {
            return;
        }
        node.write_seq += 1;
        let token = WriteToken {
            epoch: node.boot,
            seq: node.write_seq,
        };
        node.inflight = Some(InFlight {
            token,
            started: now,
        });
        node.waiting.clear();
        node.lapsed_out.clear();
        self.set(writer, WRITE_KEY.to_owned(), encode_token(token), None);
    }

    /// Applies one fault, drawn uniformly from the first `slots` arms below.
    ///
    /// The arms are ordered so a narrow draw is *restart-heavy* rather than
    /// write-free: `slots = 8` puts a quarter of the schedule on crashes and a
    /// quarter on restarts; the full `slots = 16` dilutes both to an eighth and
    /// adds pair partitions and background churn.
    ///
    /// The isolation arms are the scenario the tier exists for: a node that is
    /// *up* and serving, whose renewals stop reaching anybody, so every peer's
    /// copy of its `~lease` expires one duration later while it keeps running.
    /// The one-way flavour is the asymmetric partition the honesty box names —
    /// the isolated node keeps *hearing* the group that can no longer hear it.
    fn inject_fault(&mut self, rng: &mut SplitMix64, slots: u32) {
        let live: BTreeSet<NodeId> = self.nodes.keys().cloned().collect();
        match rng.below(slots) {
            0 | 2 if live.len() > 2 => {
                let victim = pick(&live, rng);
                self.sim.crash(&victim);
                self.nodes.remove(&victim);
            }
            1 | 3 if live.len() < self.all.len() => {
                let down: BTreeSet<NodeId> = self
                    .all
                    .iter()
                    .filter(|x| !live.contains(*x))
                    .cloned()
                    .collect();
                let node = pick(&down, rng);
                let boot = self.boots.entry(node.clone()).or_insert(1);
                *boot += 1;
                let boot = *boot;
                let all = self.all.clone();
                self.sim.add(engine(&self.group, &node, &all));
                let fresh = Node::new(&node, boot, self.now, self.now, self.resync_lag);
                self.nodes.insert(node, fresh);
            }
            draw @ (4 | 5 | 9 | 14) if live.len() > 1 => {
                let victim = pick(&live, rng);
                for peer in live.iter().filter(|peer| **peer != victim) {
                    self.sim.block(&victim, peer);
                    if draw != 5 && draw != 14 {
                        self.sim.block(peer, &victim); // else: one-way
                    }
                }
            }
            11 if live.len() > 1 => {
                let a = pick(&live, rng);
                let b = pick(&live, rng);
                if a != b {
                    self.sim.block(&a, &b);
                    self.sim.block(&b, &a);
                }
            }
            7 | 10 => self.sim.heal_all(),
            6 | 8 | 12 => {
                let node = pick(&live, rng);
                self.start_write(&node);
            }
            // Arms 13 and 15, and any arm whose guard did not hold: background
            // churn, which keeps anti-entropy busy underneath the lease traffic.
            _ => {
                let node = pick(&live, rng);
                let churn = format!("v{}", self.now).into_bytes();
                self.set(&node, "kv".to_owned(), churn, None);
            }
        }
    }
}

/// **L-P2** and **L-P3**, per reader per observation.
///
/// The confirmation the window rests on is re-derived here from the raw grant
/// bytes each granter's entry carries — deliberately not from the core — so a
/// min-set that took a maximum, ignored a silent granter, or accepted a grant
/// from a previous lease life fails immediately.
fn check_reader(
    node: &mut Node,
    maps: &BTreeMap<NodeId, GrantMap>,
    now: u64,
    tag: Tag,
    stats: &mut Stats,
) {
    let Tag { suite, seed } = tag;
    let confirmed = node.lease.confirmed();

    // The min over the min-set of what each granter really advertises for this
    // reader *in this lease life*, capped at what the reader has published.
    let mut floor = node.published.keys().next_back().copied();
    for map in maps.values() {
        match map.get(&node.id) {
            Some(id) if id.epoch == node.boot => floor = floor.map(|seq| seq.min(id.seq)),
            Some(_) => {
                stats.ghost_grants += 1;
                floor = None;
            }
            None => floor = None,
        }
    }
    assert_eq!(
        confirmed.map(|id| id.seq),
        floor,
        "{suite} seed {seed}: {} confirmed {confirmed:?} at {now}, but the grant maps its \
         engine holds put the min-set floor at {floor:?}",
        node.id
    );

    let until = node.lease.serve_until().map(|at| at.0);
    if let Some(id) = confirmed {
        assert_eq!(
            until,
            node.published
                .get(&id.seq)
                .map(|s| s + LEASE_MS - MARGIN_MS),
            "{suite} seed {seed}: {}'s window for renewal {id:?} is not s_i + D - margin",
            node.id
        );
    }
    if let (Some(reached), Some(was)) = (until, node.last_serve_until) {
        assert!(
            reached <= was || confirmed > node.last_confirmed,
            "{suite} seed {seed}: {}'s window extended from {was} to {reached} at {now} \
             without a fresh confirmation ({:?} -> {confirmed:?})",
            node.id,
            node.last_confirmed
        );
    }
    node.last_serve_until = until;
    node.last_confirmed = confirmed;

    if node.lease.peek(ClockMs(now)) != LeaseState::Serving {
        return;
    }
    stats.serving += 1;
    // L-P3: a lease life begins in `NeedsResync` — boot is a lapse the node
    // slept through, and no grant from a previous life can end it.
    assert!(
        node.last_resync.is_some(),
        "{suite} seed {seed}: {} served at {now} in lease life {} without ever affirming \
         catch-up in it",
        node.id,
        node.boot
    );
    if let Some(lapsed_at) = node.lapse_watch.take() {
        assert!(
            node.last_resync.is_some_and(|at| at > lapsed_at),
            "{suite} seed {seed}: {} returned to service at {now} after being excused by \
             lapse at {lapsed_at}, on a catch-up affirmation from {:?} — before the lapse",
            node.id,
            node.last_resync
        );
        stats.resync_after_lapse += 1;
    }
}

/// **L-P1, L-P2 and L-P3 under the general fault schedule.** 128 seeds, 3..=7
/// nodes, up to 24% per-message loss, up to 8ms of reordering jitter,
/// crash/restart/isolate/heal, and coherent writes landing throughout.
#[test]
fn dst_lease_chaos_holds_the_lapse_contract() {
    let mut total = Stats::default();
    for seed in 0..128u64 {
        total.absorb(&chaos_scenario(seed, 0x1e45, "L-P1", 16, 1));
    }
    assert!(
        total.contract_checked > 0 && total.serving > 0 && total.resync_after_lapse > 0,
        "vacuous: the suite saw {total:?} ({}), and it must see lapse-path resolutions, \
         readers serving, and returns to service gated on a post-lapse affirmation",
        total.tally()
    );
    assert!(
        total.contract_checked > total.diverged + total.vanished,
        "coverage floor: L-P1 was excused more often than it was checked — {}",
        total.tally()
    );
}

/// **L-P3 under a restart-heavy schedule.** 96 seeds on the same body with a
/// quarter of the schedule on crashes and a quarter on restarts, so ghost
/// `~lease` entries and previous-life grant maps are the norm rather than the
/// exception — and a consumer that takes up to three observation steps to flush
/// its cache, so `NeedsResync` is a state the run actually sits in.
#[test]
fn dst_lease_restart_chaos_never_serves_a_previous_life() {
    let mut total = Stats::default();
    for seed in 0..96u64 {
        total.absorb(&chaos_scenario(seed, 0x9051, "L-P3", 8, 4));
    }
    assert!(
        total.ghost_grants > 0 && total.contract_checked > 0,
        "vacuous: the suite saw {total:?} ({}), and it must see previous-life grants \
         and lapse-path resolutions",
        total.tally()
    );
    // The restart-heavy schedule *earns* more excuses than the general one —
    // a crashed reader serves nothing, so `reader_gone` is expected here — but
    // the two classes that describe the writer's own view diverging must still
    // be the minority of what the suite saw.
    assert!(
        total.contract_checked > total.diverged + total.vanished,
        "coverage floor: L-P1 was excused more often than it was checked — {}",
        total.tally()
    );
}

/// The shared chaos body: converge under the lease protocol, then 40 rounds of
/// faults drawn from `slots` of [`Harness::inject_fault`]'s schedule, with
/// every property sampled at every round. `max_lag` bounds how many observation
/// steps a consumer spends flushing before it affirms catch-up (`1` = affirm as
/// soon as a lease is live again).
fn chaos_scenario(seed: u64, salt: u64, suite: &'static str, slots: u32, max_lag: u32) -> Stats {
    let mut rng = rng(seed ^ salt);
    let n = 3 + rng.below(5); // 3..=7 nodes
    let lag = rng.below(max_lag);
    let mut h = Harness::new(Tag { suite, seed }, n, &mut rng, lag);

    // Converge first, on a fair fabric: the properties below are about a
    // running protocol, not about bootstrap.
    h.run_rounds(16, 60);
    h.sim
        .set_loss(u8::try_from(rng.below(25)).expect("below(25) is 0..25"));
    h.sim.set_jitter(u64::from(rng.below(9)));

    for _ in 0..40 {
        let step = u64::from(30 + rng.below(120));
        h.step_to(h.now + step);
        h.inject_fault(&mut rng, slots);
    }

    // A fair fabric again, long enough for every in-flight write to end at an
    // ack or a lapse rather than at the harness's own deadline.
    h.sim.heal_all();
    h.sim.set_loss(0);
    h.sim.set_jitter(0);
    h.run_rounds(40, 60);
    h.stats
}
