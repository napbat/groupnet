//! `hosted_dst.rs`'s own cluster harness: the engines, the follower loop, the
//! recovery gate, the fault schedule, and the chaos scenario drawn from it. The
//! model it drives — the feed ring's layout, the ledger fold, the tally — and
//! the properties it is measured against stay next door in `hosted_dst.rs`.

use std::collections::{BTreeMap, BTreeSet};

use groupnet_consistency::WriteToken;
use groupnet_consistency::hosted::{
    Commit, CommitCore, CompletenessCore, LedgerView, Watermarks, decode_ledger, encode_ledger,
    ledger_entry_key,
};
use groupnet_core::{GroupEngine, GroupId, NodeId, RecoveredGrant, Status, Time};
use groupnet_sim::{Simulation, SplitMix64};

use super::Count::{
    Acked, GapRecovered, Ghost, Migration, Recovered, Restart, RingGap, S5Obligation, Stalled,
};
use super::{
    Advance, Entries, Lead, MAX_INFLIGHT, Node, RING, ROUNDS, Ring, SLOTS, SMALL_RING, Stats, Tag,
    ZERO, cfg, cursor, feed_entry_key, ring, rng, set, tok, write_key,
};

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

/// One chaotic run: elect under a fair fabric, then [`ROUNDS`] faults from all
/// [`SLOTS`] arms of [`Harness::inject_fault`]'s schedule, with every property
/// sampled at every round — then heal, quiesce, and hold S5 once more. The ring
/// is drawn per seed, so half this corpus runs the small one.
pub(crate) fn chaos_scenario(seed: u64) -> Stats {
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
