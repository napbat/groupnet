//! Deterministic Simulation Testing for the coherence-lease tier under
//! **membership divergence**: partitions — one-way and full — that outlive the
//! reap horizon, departures, and clock-rate skew within the configured margin.
//!
//! `lease_dst.rs` holds the tier's per-event contracts (L-P1..L-P3) under a
//! general chaos schedule and *excuses* the lapse events membership divergence
//! produces (`diverged`, `vanished`). This suite is the end-to-end property
//! those excuses were hiding, on its own copy of the harness (the house pattern
//! for DST families):
//!
//! **L-D1 — no acknowledged write is invisible to a serving reader.** Whenever
//! a reader is [`LeaseState::Serving`] at an instant, every coherent write that
//! any other node completed (`AllApplied` or `LeaseLapsed`) before that
//! instant is known to it: either its own engine holds the writer's token at
//! or past the write (the feed, applied), or the re-synchronization its last
//! catch-up affirmation rests on began after the write did.
//!
//! # What is real, and what is modelled
//!
//! Real: the engines (membership, suspicion, reaping, TTL expiry, gossip over a
//! lossy, jittered, partitionable network) and all three sans-IO cores —
//! [`LeaseCore`], [`GrantLedger`] and [`CoherenceCore`] — fed exactly what the
//! tokio shell feeds them: a granter folds the renewals its engine holds, pins
//! what it grants, and reads every map its engine holds; a writer's wait set
//! is the live renewals in its engine plus its open grants.
//!
//! Modelled:
//!
//! * **Clocks.** Each node's cores run on its own clock, `t × (1 + ppm/10⁶)`,
//!   with `|ppm|` inside the bound the rate margin covers: a ratio of at most
//!   `D / (D − rate_margin)` between any two clocks. The engines run on true
//!   time, which is the conservative side for the TTL (no core reads it).
//! * **The roster is every member the node's engine still lists** — every node
//!   advertises [`CAP_LEASE`](groupnet_consistency::lease::CAP_LEASE) here.
//! * **Acks** are the two-entry stand-in `lease_dst.rs` uses: a writer
//!   publishes its newest token under `~dst-write`, a peer that holds that
//!   entry acknowledges it under `~dst-applied:<writer>`.
//! * **The origin.** A write is applied at the origin when it begins. A
//!   reader's re-synchronization snapshots the origin when it *begins* — at
//!   the lapse edge, or when a departed granter's drop restarts it — and its
//!   catch-up affirmation rests on that snapshot. That is the adversarial
//!   reading: a resync that started before a write and is affirmed after it
//!   knows nothing of it, so L-D1 holds only if the tier keeps such a reader
//!   out of service until something newer covers the write.

#![cfg(feature = "leases")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use groupnet_consistency::WriteToken;
use groupnet_consistency::lease::{
    ClockMs, CoherenceCore, CoherenceStep, GrantLedger, GrantMap, LeaseConfig, LeaseCore,
    LeaseState, RenewalId, WaitMember, decode_grants, decode_renewal, encode_grants,
    encode_renewal, grant_entry_key, renewal_entry_key,
};
use groupnet_core::{Command, Config, GroupEngine, GroupId, GroupMode, NodeId, Time};
use groupnet_sim::{Simulation, SplitMix64};

/// The lease duration `D`.
const LEASE_MS: u64 = 600;
/// The renewal cadence, as [`LeaseConfig::for_duration`] derives it.
const RENEW_MS: u64 = LEASE_MS / 3;
/// The rate margin, as [`LeaseConfig::for_duration`] derives it.
const MARGIN_MS: u64 = LEASE_MS / 100;
/// The widest clock-rate error any node runs with, in parts per million. Two
/// clocks at opposite ends differ in rate by `2 × SKEW_PPM`, which must stay
/// inside `MARGIN_MS / LEASE_MS` — see [`skew_fits_the_margin`].
const SKEW_PPM: i64 = 4_000;
/// How long a coherent write may wait before the harness abandons it — far
/// past one lease duration, so every write that can end on a proven lapse
/// does, and what is left over is the tier's honest `TimedOut`.
const WRITE_DEADLINE_MS: u64 = 6 * LEASE_MS;
/// How long a node observes before it participates — the shell's boot guard.
const CONVERGE_MS: u64 = LEASE_MS + 40;

const WRITE_KEY: &str = "~dst-write";

fn ack_key(writer: &NodeId) -> String {
    format!("~dst-applied:{writer}")
}

fn rng(seed: u64) -> SplitMix64 {
    SplitMix64::new(seed ^ 0x9e37_79b9_7f4a_7c15)
}

fn lease_cfg() -> LeaseConfig {
    let cfg = LeaseConfig::for_duration(Duration::from_millis(LEASE_MS));
    assert_eq!(cfg.duration_ms(), LEASE_MS);
    assert_eq!(cfg.renew_every_ms(), RENEW_MS);
    assert_eq!(cfg.rate_margin_ms(), MARGIN_MS);
    cfg
}

/// Membership timings with `dead_timeout_ms` at `D`, the tuning a lease
/// deployment runs (s3cache's): the reap horizon is a couple of lease
/// durations, so the partitions below outlive it.
fn cfg() -> Config {
    Config {
        gossip_interval_ms: 40,
        probe_interval_ms: 50,
        probe_timeout_ms: 40,
        suspect_timeout_ms: 120,
        dead_timeout_ms: LEASE_MS,
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

/// The longest stretch any partition here can outlast: suspicion, death and
/// the reap — `2 × dead_timeout_ms` past the `Dead` verdict.
fn reap_horizon_ms() -> u64 {
    let cfg = cfg();
    cfg.detection_window_ms(8) + cfg.suspect_timeout_ms + 2 * cfg.dead_timeout_ms
}

fn engine(group: &GroupId, id: &NodeId, peers: &[NodeId]) -> GroupEngine {
    let seeds = peers.iter().filter(|x| *x != id).cloned();
    GroupEngine::new(group.clone(), id.clone(), seeds, cfg())
}

fn pick(set: &BTreeSet<NodeId>, rng: &mut SplitMix64) -> NodeId {
    let v: Vec<&NodeId> = set.iter().collect();
    let n = u32::try_from(v.len()).expect("a handful of nodes");
    v[rng.below(n) as usize].clone()
}

fn encode_token(token: WriteToken) -> Vec<u8> {
    let mut out = Vec::with_capacity(16);
    out.extend_from_slice(&token.epoch.to_le_bytes());
    out.extend_from_slice(&token.seq.to_le_bytes());
    out
}

fn decode_token(bytes: &[u8]) -> Option<WriteToken> {
    Some(WriteToken {
        epoch: u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?),
        seq: u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?),
    })
}

/// What a whole run observed, summed across seeds so the suite fails loudly
/// if it stops exercising its own property.
#[derive(Debug, Default, Clone)]
struct Stats {
    /// Serving observations L-D1 was checked against at least one completed
    /// write of another node.
    checked: u64,
    all_applied: u64,
    resolved_by_lapse: u64,
    timed_out: u64,
    /// Observations of a reader still counting a granter its engine has
    /// reaped — proof the schedule outlived the reap horizon.
    pinned_past_reap: u64,
    /// Departed granters dropped from a roster.
    departures_dropped: u64,
}

impl Stats {
    fn absorb(&mut self, other: &Stats) {
        self.checked += other.checked;
        self.all_applied += other.all_applied;
        self.resolved_by_lapse += other.resolved_by_lapse;
        self.timed_out += other.timed_out;
        self.pinned_past_reap += other.pinned_past_reap;
        self.departures_dropped += other.departures_dropped;
    }
}

#[derive(Debug, Clone, Copy)]
struct InFlight {
    token: WriteToken,
    started: u64,
}

/// One running node: its three cores and the ground truth L-D1 reads.
#[derive(Debug)]
struct Node {
    id: NodeId,
    ppm: i64,
    lease: LeaseCore,
    ledger: GrantLedger,
    coherence: CoherenceCore,
    next_renew: u64,
    converged_at: u64,
    /// The origin as of the re-synchronization now in progress (taken when it
    /// began), observation steps of it still owed, and the snapshot the last
    /// catch-up affirmation that took rests on.
    resync: BTreeMap<NodeId, WriteToken>,
    resync_owed: u32,
    affirmed: Option<BTreeMap<NodeId, WriteToken>>,
    seen_lapses: u64,
    acked: BTreeMap<NodeId, WriteToken>,
    write_seq: u64,
    inflight: Option<InFlight>,
    /// Departed: crash once its last write ends.
    departing: bool,
}

impl Node {
    fn clock(&self, now: u64) -> ClockMs {
        let delta = i128::from(now) * i128::from(self.ppm) / 1_000_000;
        ClockMs(u64::try_from(i128::from(now) + delta).unwrap_or(0))
    }
}

#[derive(Debug)]
struct Harness {
    seed: u64,
    sim: Simulation,
    all: Vec<NodeId>,
    nodes: BTreeMap<NodeId, Node>,
    /// The newest token each writer has applied at the origin.
    origin: BTreeMap<NodeId, WriteToken>,
    /// Every completed write: writer, token, completion instant.
    completed: Vec<(NodeId, WriteToken, u64)>,
    renewal_key: String,
    grant_key: String,
    now: u64,
    resync_lag: u32,
    stats: Stats,
}

impl Harness {
    fn new(seed: u64, n: u32, rng: &mut SplitMix64, resync_lag: u32) -> Self {
        let group = GroupId::new(format!("lease-div-{seed}"));
        let all: Vec<NodeId> = (0..n).map(|i| NodeId::new(format!("n{i}"))).collect();
        let mut sim = Simulation::new(u64::from(3 + rng.below(8)));
        let mut nodes = BTreeMap::new();
        let span = u32::try_from(2 * SKEW_PPM + 1).expect("small");
        for id in &all {
            sim.add(engine(&group, id, &all));
            let ppm = i64::from(rng.below(span)) - SKEW_PPM;
            let phase = u64::from(rng.below(u32::try_from(RENEW_MS).expect("small")));
            let node = Node {
                id: id.clone(),
                ppm,
                lease: LeaseCore::new(id.clone(), &lease_cfg(), 1),
                ledger: GrantLedger::new(id.clone(), &lease_cfg(), 1, ClockMs::ZERO),
                coherence: CoherenceCore::new(id.clone()),
                next_renew: phase,
                converged_at: CONVERGE_MS,
                resync: BTreeMap::new(),
                resync_owed: resync_lag,
                affirmed: None,
                seen_lapses: 0,
                acked: BTreeMap::new(),
                write_seq: 0,
                inflight: None,
                departing: false,
            };
            nodes.insert(id.clone(), node);
        }
        Self {
            seed,
            sim,
            all,
            nodes,
            origin: BTreeMap::new(),
            completed: Vec::new(),
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

    fn lease_live(&self, observer: &NodeId, holder: &NodeId) -> bool {
        self.sim
            .entry_expires_at_of(observer, holder, &self.renewal_key)
            .is_some_and(|at| self.now < at.0)
    }

    fn set(&mut self, node: &NodeId, key: String, value: Vec<u8>, ttl_ms: Option<u64>) {
        self.sim
            .command(node, Command::SetLocalEntry { key, value, ttl_ms });
    }

    fn step_to(&mut self, now: u64) {
        self.now = now;
        self.sim.run_until(Time(now));
        self.publish();
        self.ingest_and_serve();
        self.step_writers();
    }

    fn run_for(&mut self, ms: u64, every_ms: u64, writes: &mut SplitMix64) {
        let until = self.now + ms;
        while self.now < until {
            self.step_to(self.now + every_ms);
            if writes.below(3) == 0 {
                let live: BTreeSet<NodeId> = self.nodes.keys().cloned().collect();
                let node = pick(&live, writes);
                self.start_write(&node);
            }
        }
    }

    /// The renewals `id`'s engine holds live, as the granter folds them.
    fn visible_renewals(&self, id: &NodeId) -> Vec<(NodeId, RenewalId)> {
        self.sim
            .entries_snapshot(id)
            .iter()
            .filter(|(peer, _)| *peer != id && self.lease_live(id, peer))
            .filter_map(|(peer, entries)| {
                let renewal = decode_renewal(entries.get(&self.renewal_key)?)?;
                Some((peer.clone(), renewal))
            })
            .collect()
    }

    /// Each node's grant map (folded through its ledger, pinned before it is
    /// published), its acks, and its renewal when due.
    fn publish(&mut self) {
        let now = self.now;
        for id in self.live_ids() {
            let view = self.sim.entries_snapshot(&id);
            let visible = self.visible_renewals(&id);
            let applied = |reader: &NodeId, writer: &NodeId| {
                view.get(reader)
                    .and_then(|entries| entries.get(&ack_key(writer)))
                    .and_then(|bytes| decode_token(bytes))
            };
            let node = self.nodes.get_mut(&id).expect("live");
            if node.departing {
                continue;
            }
            let clock = node.clock(now);
            let grants = node.ledger.fold(clock, visible, applied);
            for reader in grants.keys() {
                node.lease.pin(reader);
            }
            let encoded = encode_grants(&grants);
            let mut acks: Vec<(NodeId, WriteToken)> = Vec::new();
            for (peer, entries) in &view {
                if *peer == id {
                    continue;
                }
                let seen = entries.get(WRITE_KEY).and_then(|bytes| decode_token(bytes));
                if let Some(token) =
                    seen.filter(|seen| node.acked.get(peer).is_none_or(|held| held < seen))
                {
                    acks.push((peer.clone(), token));
                }
            }
            let renewal = (now >= node.next_renew).then(|| {
                node.next_renew = now + RENEW_MS;
                node.lease.on_renew(clock)
            });
            for (writer, token) in &acks {
                node.acked.insert(writer.clone(), *token);
            }
            if view.get(&id).and_then(|e| e.get(&self.grant_key)) != Some(&encoded) {
                let key = self.grant_key.clone();
                self.set(&id, key, encoded, None);
            }
            for (writer, token) in acks {
                self.set(&id, ack_key(&writer), encode_token(token), None);
            }
            if let Some(renewal) = renewal {
                let key = self.renewal_key.clone();
                self.set(&id, key, encode_renewal(renewal), Some(LEASE_MS));
            }
        }
    }

    /// Each reader's ingest, its consumer's resync policy, and L-D1.
    fn ingest_and_serve(&mut self) {
        let now = self.now;
        for id in self.live_ids() {
            let view = self.sim.entries_snapshot(&id);
            let known: BTreeSet<NodeId> = self
                .all
                .iter()
                .filter(|peer| **peer != id && self.sim.status_of(&id, peer).is_some())
                .cloned()
                .collect();
            let map_of = |node: &NodeId| -> GrantMap {
                view.get(node)
                    .and_then(|entries| entries.get(&self.grant_key))
                    .map(|bytes| decode_grants(bytes))
                    .unwrap_or_default()
            };
            let holders: Vec<NodeId> = self
                .visible_renewals(&id)
                .into_iter()
                .map(|(holder, _)| holder)
                .collect();
            let origin = self.origin.clone();
            let lag = self.resync_lag;
            let node = self.nodes.get_mut(&id).expect("live");
            let clock = node.clock(now);
            node.lease.set_roster(known.iter().cloned());
            let roster: Vec<NodeId> = node.lease.roster().cloned().collect();
            for granter in &roster {
                node.lease.observe_grant_map(granter, &map_of(granter));
            }
            for reader in roster.iter().chain(holders.iter()) {
                node.ledger.observe_reader_map(reader, &map_of(reader));
            }
            self.stats.pinned_past_reap += u64::try_from(
                roster
                    .iter()
                    .filter(|granter| !known.contains(*granter))
                    .count(),
            )
            .expect("small");

            let state = node.lease.poll(clock);
            if node.lease.lapses() > node.seen_lapses {
                if state != LeaseState::Lapsed {
                    self.stats.departures_dropped += 1;
                }
                node.seen_lapses = node.lease.lapses();
                node.resync = origin.clone();
                node.resync_owed = lag;
            }
            if state != LeaseState::Serving && now >= node.converged_at {
                if node.resync_owed > 0 {
                    node.resync_owed -= 1;
                } else if node.lease.mark_caught_up(clock) {
                    node.affirmed = Some(node.resync.clone());
                }
            }
            if node.lease.peek(clock) == LeaseState::Serving {
                let checked = check_serving(node, &self.completed, now, self.seed);
                self.stats.checked += u64::from(checked);
            }
        }
    }

    /// One poll of every in-flight write against its writer's own view: the
    /// live renewals there plus the ledger's open grants.
    fn step_writers(&mut self) {
        let now = self.now;
        for id in self.live_ids() {
            let Some(inflight) = self.nodes[&id].inflight else {
                continue;
            };
            let view = self.sim.entries_snapshot(&id);
            let mut members: BTreeSet<NodeId> = self
                .visible_renewals(&id)
                .into_iter()
                .map(|(holder, _)| holder)
                .collect();
            let node = self.nodes.get_mut(&id).expect("live");
            let clock = node.clock(now);
            members.extend(node.ledger.open_grants(clock).cloned());
            let snapshot: Vec<WaitMember> = members
                .into_iter()
                .filter(|member| *member != id)
                .map(|member| WaitMember {
                    applied: view
                        .get(&member)
                        .and_then(|entries| entries.get(&ack_key(&id)))
                        .and_then(|bytes| decode_token(bytes)),
                    excusable_at: node.ledger.excusable_at(&member),
                    member,
                })
                .collect();
            let verdict = node.coherence.step(inflight.token, clock, &snapshot);
            let done = match verdict {
                CoherenceStep::Waiting { .. } => {
                    if now.saturating_sub(inflight.started) <= WRITE_DEADLINE_MS {
                        continue;
                    }
                    let _ = node.coherence.abandon(inflight.token);
                    self.stats.timed_out += 1;
                    false
                }
                CoherenceStep::AllApplied => {
                    self.stats.all_applied += 1;
                    true
                }
                CoherenceStep::LeaseLapsed { .. } => {
                    self.stats.resolved_by_lapse += 1;
                    true
                }
            };
            node.ledger.end(&id, inflight.token);
            node.inflight = None;
            let departing = node.departing;
            if done {
                self.completed.push((id.clone(), inflight.token, now));
            }
            if departing {
                self.sim.crash(&id);
                self.nodes.remove(&id);
            }
        }
    }

    /// Starts a coherent write on `writer`: applied at the origin now,
    /// published, registered with its ledger.
    fn start_write(&mut self, writer: &NodeId) {
        let now = self.now;
        let Some(node) = self.nodes.get_mut(writer) else {
            return;
        };
        if node.inflight.is_some() || node.departing || now < node.converged_at {
            return;
        }
        node.write_seq += 1;
        let token = WriteToken {
            epoch: 1,
            seq: node.write_seq,
        };
        let clock = node.clock(now);
        node.ledger.begin(writer, token, clock);
        node.inflight = Some(InFlight {
            token,
            started: now,
        });
        self.origin.insert(writer.clone(), token);
        self.set(writer, WRITE_KEY.to_owned(), encode_token(token), None);
    }

    /// `victim` departs the lease set: its own serve window ends for good, its
    /// final map says so, its renewal is retracted, and it crashes once its
    /// last write has ended.
    fn depart(&mut self, victim: &NodeId) {
        let now = self.now;
        let visible = self.visible_renewals(victim);
        let Some(node) = self.nodes.get_mut(victim) else {
            return;
        };
        node.lease.depart();
        node.ledger.depart();
        node.departing = true;
        let clock = node.clock(now);
        let farewell = encode_grants(&node.ledger.fold(clock, visible, |_, _| None));
        let idle = node.inflight.is_none();
        let key = self.grant_key.clone();
        self.set(victim, key, farewell, None);
        self.sim.command(
            victim,
            Command::DeleteLocalEntry {
                key: self.renewal_key.clone(),
            },
        );
        if idle {
            self.sim.crash(victim);
            self.nodes.remove(victim);
        }
    }

    /// One partition, drawn from the divergence family: a victim cut off one
    /// way or both, or one pair cut one way or both.
    fn partition(&mut self, rng: &mut SplitMix64) {
        let live: BTreeSet<NodeId> = self.nodes.keys().cloned().collect();
        let victim = pick(&live, rng);
        match rng.below(4) {
            0 => {
                for peer in live.iter().filter(|peer| **peer != victim) {
                    self.sim.block(&victim, peer);
                    self.sim.block(peer, &victim);
                }
            }
            1 => {
                for peer in live.iter().filter(|peer| **peer != victim) {
                    self.sim.block(peer, &victim); // it hears nobody
                }
            }
            2 => {
                for peer in live.iter().filter(|peer| **peer != victim) {
                    self.sim.block(&victim, peer); // nobody hears it
                }
            }
            _ => {
                let other = pick(&live, rng);
                if other != victim {
                    self.sim.block(&victim, &other);
                    if rng.below(2) == 0 {
                        self.sim.block(&other, &victim);
                    }
                }
            }
        }
    }
}

/// **L-D1** for one serving reader. Returns whether any completed write of
/// another node was checked.
///
/// What the reader has applied is what it has acknowledged: its cache keeps
/// every write it applied, whatever membership later does to the writer's
/// entries.
fn check_serving(
    node: &Node,
    completed: &[(NodeId, WriteToken, u64)],
    now: u64,
    seed: u64,
) -> bool {
    let mut checked = false;
    for (writer, token, at) in completed {
        if *writer == node.id || *at >= now {
            continue;
        }
        checked = true;
        let applied = node.acked.get(writer).copied();
        let resynced = node
            .affirmed
            .as_ref()
            .and_then(|snapshot| snapshot.get(writer))
            .copied();
        let known = applied.max(resynced);
        assert!(
            known.is_some_and(|known| known >= *token),
            "seed {seed}: {} served at {now} without {writer}'s write {token:?}, acknowledged \
             at {at}: it has applied {applied:?} and its catch-up rests on a resync that saw \
             {resynced:?}",
            node.id
        );
    }
    checked
}

/// The rate-skew bound this suite runs inside: the fastest clock over the
/// slowest stays within what the margin buys over one lease duration.
#[test]
fn skew_fits_the_margin() {
    let fast = 1_000_000 + SKEW_PPM;
    let slow = 1_000_000 - SKEW_PPM;
    let lease = i64::try_from(LEASE_MS).expect("small");
    let margin = i64::try_from(MARGIN_MS).expect("small");
    assert!(fast * (lease - margin) <= slow * lease);
}

/// **L-D1 under partitions past the reap horizon.** 96 seeds, 2..=5 nodes, up
/// to 10% loss, a few milliseconds of jitter; each seed plays episodes of one
/// partition held for up to twice the reap horizon with writes landing
/// throughout, then a heal, and every few episodes a departure.
#[test]
fn dst_divergence_never_serves_an_acknowledged_write_stale() {
    let mut total = Stats::default();
    for seed in 0..96u64 {
        total.absorb(&divergence_scenario(seed));
    }
    assert!(
        total.checked > 0
            && total.resolved_by_lapse > 0
            && total.all_applied > 0
            && total.pinned_past_reap > 0
            && total.departures_dropped > 0,
        "vacuous: the suite saw {total:?}; it must check serving readers against \
         acknowledged writes, resolve writes on acks and on lapses, outlive the reap \
         horizon and drop departed granters"
    );
}

fn divergence_scenario(seed: u64) -> Stats {
    let mut rng = rng(seed ^ 0xd1fe);
    let n = 2 + rng.below(4); // 2..=5 nodes
    let lag = rng.below(3);
    let mut h = Harness::new(seed, n, &mut rng, lag);
    let mut writes = SplitMix64::new(seed ^ 0x0517);
    let horizon = reap_horizon_ms();

    h.run_for(2 * CONVERGE_MS, 40, &mut writes);
    h.sim
        .set_loss(u8::try_from(rng.below(11)).expect("below(11) is 0..11"));
    h.sim.set_jitter(u64::from(rng.below(6)));

    for episode in 0..6 {
        h.partition(&mut rng);
        let hold = u64::from(rng.below(u32::try_from(2 * horizon).expect("small")));
        h.run_for(hold, u64::from(20 + rng.below(60)), &mut writes);
        h.sim.heal_all();
        h.run_for(LEASE_MS, 40, &mut writes);
        if episode % 3 == 2 && h.nodes.len() > 2 {
            let live: BTreeSet<NodeId> = h.nodes.keys().cloned().collect();
            let victim = pick(&live, &mut rng);
            h.depart(&victim);
        }
    }

    h.sim.heal_all();
    h.sim.set_loss(0);
    h.sim.set_jitter(0);
    h.run_for(2 * horizon, 40, &mut writes);
    h.stats
}
