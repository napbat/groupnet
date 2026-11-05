//! Digest/delta anti-entropy: windowed full digests, per-peer delta digests,
//! reconciliation, and eager push.

use std::cmp::{Ordering, Reverse};
use std::iter::Peekable;
use std::ops::Bound::{Excluded, Unbounded};

use crate::membership::{Member, StateEntry, Status};
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::spread;
use super::state::{GroupEngine, PeerDigest};

impl GroupEngine {
    /// Brings the next anti-entropy round forward for a change this node
    /// *learned* or derived (a merged digest or delta, its own probe verdict,
    /// new contacts) — to now, but no sooner than half a gossip interval
    /// after the previous round. The floor bounds the round rate at twice the
    /// configured one: without it, a burst of liveness changes (every learned
    /// suspicion, refutation and death nudges) runs a round on every tick,
    /// and on an overloaded host the extra rounds delay the probe acks that
    /// would have ended the burst.
    pub(super) fn nudge_anti_entropy(&mut self) {
        let earliest = self
            .last_round
            .saturating_add(self.config.gossip_interval_ms / 2);
        self.next_anti_entropy = self.next_anti_entropy.min(self.now_hint.max(earliest));
    }

    /// Brings the next anti-entropy round forward to now for a change the
    /// application made here (a write, a leave, a metadata update, a peer
    /// it added), so it disseminates on the next tick rather than waiting a
    /// whole interval.
    pub(super) fn nudge_anti_entropy_now(&mut self) {
        self.next_anti_entropy = self.next_anti_entropy.min(self.now_hint);
    }

    /// Pushes the just-authored entry straight to the current fanout targets
    /// as an unsolicited `Delta` frame — one hop, no digest round-trip — so a
    /// local write reaches live peers at network latency rather than tick
    /// cadence. Receivers adopt it through the ordinary versioned merge, so
    /// duplication with the following anti-entropy round is harmless; that
    /// round remains the repair path for peers outside this fanout and for
    /// any frame the transport drops.
    pub(super) fn eager_push(&mut self) -> Vec<Effect> {
        if !self.config.eager_push {
            return Vec::new();
        }
        let have = self
            .members
            .get(&self.local)
            .map_or(0, |m| m.max_state_version.saturating_sub(1));
        let now = self.now_hint;
        let Some(delta) = self.build_delta_frame(&[(self.local.clone(), have)], now) else {
            return Vec::new();
        };
        let targets = self.select_fanout_targets();
        self.stats.delta_frames_sent += targets.len() as u64;
        self.stats.anti_entropy_bytes_sent += (delta.len() * targets.len()) as u64;
        targets
            .into_iter()
            .map(|to| Effect::Send {
                to,
                wire: delta.clone(),
            })
            .collect()
    }

    /// Runs one anti-entropy round: send a digest (chunked to the frame budget)
    /// to a rotating fanout of peers.
    pub(super) fn disseminate_digest(&mut self, now: Time) -> Vec<Effect> {
        self.last_round = now;
        let targets = self.select_fanout_targets();
        if targets.is_empty() {
            return Vec::new();
        }
        self.refresh_content_hashes(now);
        let mut effects = Vec::new();
        for to in targets {
            let (summaries, full, peer) = self.digest_for(&to, now);
            self.digest_peers.insert(to.clone(), peer);
            let listed = summaries.len() as u64;
            let chunks = self.build_digest_chunks(summaries);
            self.stats.digests_built += 1;
            if full {
                self.stats.full_digests_built += 1;
            }
            self.stats.digest_summaries_listed += listed;
            self.stats.max_digest_summaries = self.stats.max_digest_summaries.max(listed);
            for chunk in chunks {
                self.stats.digest_frames_sent += 1;
                self.stats.digest_bytes_sent += chunk.len() as u64;
                self.stats.anti_entropy_bytes_sent += chunk.len() as u64;
                effects.push(Effect::Send {
                    to: to.clone(),
                    wire: chunk,
                });
            }
        }
        effects
    }

    /// Chooses the member summaries of the next digest to `to` — at most
    /// [`Config::digest_window`](crate::Config::digest_window) of them — and
    /// returns them with whether the digest is full and the peer's updated
    /// bookkeeping.
    ///
    /// Every digest starts from the members whose summary changed since the
    /// last digest built for this peer. A **delta** digest lists exactly
    /// those — safe because a digest only ever triggers per-listed-member
    /// reconciliation (absence is never interpreted).
    ///
    /// A peer's first digest, every `full_digest_every`-th after it, and any
    /// delta that would list more than the window are **full**. A full
    /// digest leads with up to half the window of those changed members:
    /// the local member first, then members whose liveness (incarnation or
    /// status) changed within the last [`liveness_hot_ms`](Self::liveness_hot_ms),
    /// most recent first, then the most recently changed. Liveness news so
    /// rides every digest for long enough to cross the group however busy
    /// the entries are — a suspicion reaches its subject, and the refutation
    /// its suspecters, before entry churn can bury them — and entry news
    /// rides the rest. The digest fills up from a rolling window over the
    /// gossipable members that resumes after the last member the previous
    /// full digest to this peer walked past. The first window starts at an
    /// offset hashed from the pair, so a node's peers learn different slices
    /// of its view first. Each full digest walks at least `⌈window / 2⌉`
    /// members, so a receiver that lacks a member learns it within
    /// `⌈N / ⌈window / 2⌉⌉` full digests from any peer that has it —
    /// `⌈N / window⌉` when nothing changed since the previous one.
    ///
    /// The cursor advances on build, not delivery: anything a dropped frame,
    /// or a change a full digest had no room to lead with, left out stays
    /// divergent only until the rolling window reaches it.
    fn digest_for(&self, to: &NodeId, now: Time) -> (Vec<wire::NodeDigest>, bool, PeerDigest) {
        let window = self.config.digest_window.get();
        let prev = self.digest_peers.get(to);
        let visits = prev.map_or(0, |p| p.visits);
        let cursor = prev.map_or(0, |p| p.cursor);
        let mut peer = PeerDigest {
            visits: visits + 1,
            cursor: self.change_clock,
            resume: prev.and_then(|p| p.resume.clone()),
        };
        let gossipable = || {
            self.members
                .iter()
                .filter(|(_, m)| self.should_gossip(m, now))
        };
        let mut listed: Vec<(&NodeId, &Member)> = gossipable()
            .filter(|(_, m)| m.changed_at > cursor)
            .collect();
        let scheduled_full = visits.is_multiple_of(self.config.full_digest_every.get());
        if !scheduled_full && listed.len() <= window {
            let summaries = listed
                .into_iter()
                .map(|(node, m)| self.summary(node, m, now))
                .collect();
            return (summaries, false, peer);
        }

        // Lead: the local member, then hot liveness news, then the freshest
        // changes, at most half the window — the rest is the rolling walk's,
        // so it always advances. Under churn nearly every member changed, so
        // the lead is selected (linear) before it is sorted; `changed_at` is
        // unique per member, so this is the full sort's first half-window.
        let hot_ms = self.liveness_hot_ms();
        let rank = |entry: &(&NodeId, &Member)| {
            let (node, m) = *entry;
            let hot = now.0.saturating_sub(m.liveness_since.0) < hot_ms;
            (
                *node != self.local,
                !hot,
                Reverse(if hot { m.liveness_since } else { Time::ZERO }),
                Reverse(m.changed_at),
            )
        };
        let lead_cap = window / 2;
        if lead_cap == 0 {
            listed.clear();
        } else if listed.len() > lead_cap {
            listed.select_nth_unstable_by_key(lead_cap - 1, rank);
            listed.truncate(lead_cap);
        }
        listed.sort_unstable_by_key(rank);
        let lead = listed.len();
        // The rolling walk resumes after the last member the previous full
        // digest to this peer walked past, wrapping once round the members —
        // read straight off the member map's ranges, without collecting them.
        let walk_gossipable = |(_, m): &(&NodeId, &Member)| self.should_gossip(m, now);
        if let Some(after) = peer.resume.clone() {
            let walk = self
                .members
                .range((Excluded(&after), Unbounded))
                .chain(self.members.range(..=&after))
                .filter(walk_gossipable);
            fill_window(&mut listed, lead, window, walk, &mut peer.resume);
        } else {
            let len = gossipable().count();
            if len > 0 {
                let start = spread::index(spread::pair_hash(&self.local, to), len);
                let walk = gossipable().skip(start).chain(gossipable().take(start));
                fill_window(&mut listed, lead, window, walk, &mut peer.resume);
            }
        }
        let summaries = listed
            .into_iter()
            .map(|(node, m)| self.summary(node, m, now))
            .collect();
        (summaries, true, peer)
    }

    /// How long a liveness change stays hot — leads every full digest — at
    /// this node: `⌊log2 N⌋ + 2` gossip rounds for `N` known members, the
    /// rounds push gossip needs to carry it across the group.
    pub(super) fn liveness_hot_ms(&self) -> u64 {
        let rounds = u64::from(self.members.len().bit_width()) + 1;
        self.config.gossip_interval_ms.saturating_mul(rounds)
    }

    /// One member's digest line.
    fn summary(&self, node: &NodeId, m: &Member, now: Time) -> wire::NodeDigest {
        wire::NodeDigest {
            node: node.clone(),
            incarnation: m.incarnation,
            status: m.status.to_wire(),
            max_version: m.max_state_version,
            content_hash: self.content_hash(m, now),
        }
    }

    /// A one-line digest telling `to` how this node holds `about` — the
    /// accusation a suspecter sends its subject, and the subject's answer
    /// carrying its current record back. One hop each way: unless a frame is
    /// lost, the suspecter stops doubting a live subject within a round trip
    /// instead of waiting for gossip to carry the refutation to it. `None`
    /// when `about` is unknown.
    pub(super) fn liveness_notice(
        &mut self,
        to: NodeId,
        about: &NodeId,
        now: Time,
    ) -> Option<Effect> {
        let m = self.members.get(about)?;
        let wire = wire::encode(&wire::Frame {
            kind: wire::Kind::Digest,
            group: self.group.clone(),
            target: None,
            digest: vec![self.summary(about, m, now)],
            wants: Vec::new(),
            members: Vec::new(),
            metadata: Vec::new(),
            lead: None,
        });
        self.stats.digest_frames_sent += 1;
        self.stats.digest_bytes_sent += wire.len() as u64;
        self.stats.anti_entropy_bytes_sent += wire.len() as u64;
        Some(Effect::Send { to, wire })
    }

    /// Picks `gossip_fanout` distinct peers, rotating a cursor so every
    /// peer is covered over successive rounds.
    pub(super) fn select_fanout_targets(&mut self) -> Vec<NodeId> {
        let out = self.rotated_targets(self.gossip_cursor, self.config.gossip_fanout.get());
        self.gossip_cursor = self.gossip_cursor.wrapping_add(out.len());
        out
    }

    /// Up to `k` distinct dissemination targets, from position `cursor`
    /// (modulo their count) onward, wrapping. Indexes the cached merged
    /// targets ([`GroupEngine::targets`]), rebuilt only after the live set or
    /// the contacts changed, so a round costs `O(k)`.
    pub(super) fn rotated_targets(&mut self, cursor: usize, k: usize) -> Vec<NodeId> {
        if self.targets.is_none() {
            self.targets = Some(self.dissemination_targets().cloned().collect());
        }
        let targets = self.targets.as_deref().unwrap_or_default();
        debug_assert!(
            targets.iter().eq(self.dissemination_targets()),
            "the cached dissemination targets went stale"
        );
        let n = targets.len();
        if n == 0 {
            return Vec::new();
        }
        let first = cursor % n;
        (0..k.min(n))
            .map(|i| targets[(first + i) % n].clone())
            .collect()
    }

    /// Encodes one digest's `summaries` as [`wire::Kind::Digest`] frames
    /// within the frame budget. The metadata register set rides the first
    /// chunk; a digest with no summaries still sends it when there is any.
    pub(super) fn build_digest_chunks(&self, summaries: Vec<wire::NodeDigest>) -> Vec<Vec<u8>> {
        let budget = self.config.max_delta_frame_bytes.get();
        let mut metadata: Vec<wire::MetaDelta> = self
            .metadata
            .iter()
            .map(|(key, v)| wire::MetaDelta {
                key: key.clone(),
                version: v.version,
                writer: v.writer.clone(),
                value: v.value.clone(),
            })
            .collect();

        let base = 1 + 1 + (4 + self.group.as_str().len()) + 1 + 4; // ..+ digest count
        let mut chunks: Vec<Vec<u8>> = Vec::new();
        let mut rest = summaries.into_iter().peekable();
        loop {
            let meta = std::mem::take(&mut metadata);
            let meta_len = 4 + meta.iter().map(wire::meta_len).sum::<usize>();
            let mut size = base + meta_len;
            let mut slice: Vec<wire::NodeDigest> = Vec::new();
            while let Some(d) = rest.peek() {
                let dlen = wire::digest_len(d);
                if size + dlen > budget && !slice.is_empty() {
                    break;
                }
                size += dlen;
                slice.extend(rest.next());
            }
            if slice.is_empty() && meta.is_empty() {
                break; // nothing left to emit
            }
            chunks.push(wire::encode(&wire::Frame {
                kind: wire::Kind::Digest,
                group: self.group.clone(),
                target: None,
                digest: slice,
                wants: Vec::new(),
                members: Vec::new(),
                metadata: meta,
                lead: None,
            }));
            if rest.peek().is_none() {
                break;
            }
        }
        chunks
    }

    /// Merges a peer's digest: reconcile liveness and metadata directly, then
    /// request whatever entries we're behind on and offer whatever we're ahead
    /// on. A digest that doubts us — names us `Suspect` or `Dead` at any
    /// incarnation, or made us refute — is answered at once with our current
    /// summary. That covers a suspecter behind on our incarnation, whose
    /// accusation no longer makes us refute: without the answer it would keep
    /// doubting us, and reap us to `Dead`, until gossip reached it. A node
    /// that left is `Dead` by its own word and answers nothing.
    pub(super) fn on_digest(
        &mut self,
        from: &NodeId,
        frame: &wire::Frame,
        now: Time,
    ) -> Vec<Effect> {
        let incarnation = self.incarnation;
        let doubted = frame
            .digest
            .iter()
            .any(|d| d.node == self.local && Status::from_wire(d.status) != Some(Status::Alive));
        let mut effects = self.merge_digest_liveness(&frame.digest, now);
        for d in &frame.digest {
            self.refresh_content_hash(&d.node, now);
        }
        if (doubted && !self.leaving) || self.incarnation > incarnation {
            let me = self.local.clone();
            effects.extend(self.liveness_notice(from.clone(), &me, now));
        }
        effects.extend(self.merge_metadata(frame.metadata.clone()));

        let mut wants: Vec<wire::NodeWant> = Vec::new();
        let mut offers: Vec<(NodeId, u64)> = Vec::new();
        for d in &frame.digest {
            let (ours_max, ours_hash) = match self.members.get(&d.node) {
                Some(m) => (m.max_state_version, self.content_hash(m, now)),
                None => (0, 0),
            };
            if d.max_version > ours_max {
                wants.push(wire::NodeWant {
                    node: d.node.clone(),
                    have_version: ours_max,
                });
            } else if d.max_version < ours_max {
                offers.push((d.node.clone(), d.max_version));
            } else if d.content_hash != ours_hash {
                // Equal high-water but divergent holdings — a restart reused a
                // version clock, so the same number now names different entries on
                // each side. Fall back to a full per-key exchange (request and
                // offer everything), which last-writer-wins reconciles where the
                // scalar comparison alone was blind.
                wants.push(wire::NodeWant {
                    node: d.node.clone(),
                    have_version: 0,
                });
                offers.push((d.node.clone(), 0));
            }
        }
        if !wants.is_empty() {
            effects.push(self.send_delta_request(from.clone(), wants));
        }
        if let Some(delta) = self.build_delta_frame(&offers, now) {
            self.stats.delta_frames_sent += 1;
            self.stats.anti_entropy_bytes_sent += delta.len() as u64;
            effects.push(Effect::Send {
                to: from.clone(),
                wire: delta,
            });
        }
        effects
    }

    /// Answers a peer's `DeltaRequest` with the entries it asked for, bounded to
    /// the frame budget.
    pub(super) fn on_delta_request(
        &mut self,
        from: &NodeId,
        frame: &wire::Frame,
        now: Time,
    ) -> Vec<Effect> {
        let offers: Vec<(NodeId, u64)> = frame
            .wants
            .iter()
            .map(|w| (w.node.clone(), w.have_version))
            .collect();
        match self.build_delta_frame(&offers, now) {
            Some(delta) => {
                self.stats.delta_frames_sent += 1;
                self.stats.anti_entropy_bytes_sent += delta.len() as u64;
                vec![Effect::Send {
                    to: from.clone(),
                    wire: delta,
                }]
            }
            None => Vec::new(),
        }
    }

    pub(super) fn send_delta_request(&mut self, to: NodeId, wants: Vec<wire::NodeWant>) -> Effect {
        let budget = self.config.max_delta_frame_bytes.get();
        let base = 1 + 1 + (4 + self.group.as_str().len()) + 1 + 4;
        let mut size = base;
        let mut kept: Vec<wire::NodeWant> = Vec::new();
        for w in wants {
            let wlen = wire::want_len(&w);
            if size + wlen > budget && !kept.is_empty() {
                break; // remainder re-requested next round
            }
            size += wlen;
            kept.push(w);
        }
        Effect::Send {
            to,
            wire: wire::encode(&wire::Frame {
                kind: wire::Kind::DeltaRequest,
                group: self.group.clone(),
                target: None,
                digest: Vec::new(),
                wants: kept,
                members: Vec::new(),
                metadata: Vec::new(),
                lead: None,
            }),
        }
    }

    /// Assembles the entries newer than each `(node, have_version)` into a single
    /// bounded `Delta` frame. Entries go out in ascending version order and are
    /// truncated at the budget (the recipient re-requests the tail next round);
    /// a member with no qualifying entries but a higher-water mark than the
    /// requester is still included so the recipient can advance past a reaped
    /// tail. Returns `None` when there is nothing to send.
    pub(super) fn build_delta_frame(&self, wants: &[(NodeId, u64)], now: Time) -> Option<Vec<u8>> {
        let budget = self.config.max_delta_frame_bytes.get();
        let mut size = wire::delta_frame_overhead(&self.group);
        let mut members: Vec<wire::MemberDelta> = Vec::new();

        'wants: for (node, have) in wants {
            let Some(m) = self.members.get(node) else {
                continue;
            };
            let mut qualifying: Vec<(&String, &StateEntry)> = m
                .entries
                .iter()
                .filter(|(_, e)| e.version > *have && self.should_gossip_entry(e, now))
                .collect();
            qualifying.sort_by_key(|(_, e)| e.version);

            let header = wire::member_header_len(node);
            if size + header > budget && !members.is_empty() {
                break 'wants; // no room even for the header
            }

            let mut md = wire::MemberDelta {
                node: node.clone(),
                incarnation: m.incarnation,
                status: m.status.to_wire(),
                max_version: 0,
                entries: Vec::new(),
            };
            let mut member_size = header;
            let mut top = *have;
            let mut truncated = false;

            for (k, e) in qualifying {
                let ed = wire::EntryDelta {
                    key: k.clone(),
                    version: e.version,
                    ttl_ms: e.ttl_ms,
                    tombstone: e.tombstone,
                    value: e.value.clone(),
                };
                let elen = wire::entry_len(&ed);
                // The one exception to the budget: never starve the very first
                // entry, even if its value alone exceeds the cap.
                let first_ever = members.is_empty() && md.entries.is_empty();
                if size + member_size + elen > budget && !first_ever {
                    truncated = true;
                    break;
                }
                top = ed.version;
                member_size += elen;
                md.entries.push(ed);
            }

            // If we sent everything qualifying, the recipient can jump straight
            // to our true high-water (which may sit above the last entry when
            // the top was reaped); if we truncated, only to the last we included.
            md.max_version = if truncated {
                top
            } else {
                m.max_state_version.max(top)
            };

            if !md.entries.is_empty() || md.max_version > *have {
                size += member_size;
                members.push(md);
            }
            if truncated {
                break 'wants;
            }
        }

        if members.is_empty() {
            return None;
        }
        Some(wire::encode(&wire::Frame {
            kind: wire::Kind::Delta,
            group: self.group.clone(),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members,
            metadata: Vec::new(),
            lead: None,
        }))
    }

    /// Every peer a round may target, in id order and each once: the live
    /// members other than us, the seeds and the discovery contacts. A merge of
    /// the three sorted sets, so a round neither copies nor sorts them.
    pub(super) fn dissemination_targets(&self) -> impl Iterator<Item = &NodeId> {
        Union::new(
            Union::new(self.probe_candidates(), self.seeds.iter()),
            self.bootstrap_contacts.iter(),
        )
    }

    /// A `Dead` member is summarized in digests only until `dead_timeout`
    /// elapses; after that peers are assumed to know, and dropping it lets
    /// everyone reap the tombstone without re-teaching each other.
    pub(super) fn should_gossip(&self, m: &Member, now: Time) -> bool {
        m.status != Status::Dead || now < m.dead_since.saturating_add(self.config.dead_timeout_ms)
    }

    /// An entry is offered in a delta while live and unexpired; a tombstone only
    /// until `dead_timeout` (after that peers are assumed to know — the same
    /// shape as a Dead member tombstone, and what upholds the reap horizon).
    pub(super) fn should_gossip_entry(&self, e: &StateEntry, now: Time) -> bool {
        if e.tombstone {
            return now
                < e.tombstone_since
                    .saturating_add(self.config.dead_timeout_ms);
        }
        !e.expired(now)
    }
}

/// Fills `listed` up to `window` from `walk`, skipping the first `lead`
/// members already listed, and records in `resume` the last member walked
/// past.
fn fill_window<'a>(
    listed: &mut Vec<(&'a NodeId, &'a Member)>,
    lead: usize,
    window: usize,
    walk: impl Iterator<Item = (&'a NodeId, &'a Member)>,
    resume: &mut Option<NodeId>,
) {
    for (node, m) in walk {
        if listed.len() >= window {
            break;
        }
        *resume = Some(node.clone());
        if listed[..lead].iter().any(|(led, _)| *led == node) {
            continue; // already leads the digest
        }
        listed.push((node, m));
    }
}

/// The sorted union of two sorted, duplicate-free sequences of ids.
struct Union<'a, A: Iterator<Item = &'a NodeId>, B: Iterator<Item = &'a NodeId>> {
    a: Peekable<A>,
    b: Peekable<B>,
}

impl<'a, A: Iterator<Item = &'a NodeId>, B: Iterator<Item = &'a NodeId>> Union<'a, A, B> {
    fn new(a: A, b: B) -> Self {
        Self {
            a: a.peekable(),
            b: b.peekable(),
        }
    }
}

impl<'a, A: Iterator<Item = &'a NodeId>, B: Iterator<Item = &'a NodeId>> Iterator
    for Union<'a, A, B>
{
    type Item = &'a NodeId;

    fn next(&mut self) -> Option<&'a NodeId> {
        match (self.a.peek(), self.b.peek()) {
            (Some(x), Some(y)) => match x.cmp(y) {
                Ordering::Less => self.a.next(),
                Ordering::Greater => self.b.next(),
                Ordering::Equal => {
                    self.b.next();
                    self.a.next()
                }
            },
            (Some(_), None) => self.a.next(),
            (None, _) => self.b.next(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use crate::{Command, Config, GroupEngine, GroupId, NodeId, Status, Time, wire};

    fn digest(lines: &[(&str, u64, Status)]) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind: wire::Kind::Digest,
            group: GroupId::new("g"),
            target: None,
            digest: lines
                .iter()
                .map(|(node, incarnation, status)| wire::NodeDigest {
                    node: NodeId::new(*node),
                    incarnation: *incarnation,
                    status: status.to_wire(),
                    max_version: 0,
                    content_hash: 0,
                })
                .collect(),
            wants: Vec::new(),
            members: Vec::new(),
            metadata: Vec::new(),
            lead: None,
        })
    }

    fn delta_with_ttl(node: &str, ttl_ms: u64) -> Vec<u8> {
        wire::encode(&wire::Frame {
            kind: wire::Kind::Delta,
            group: GroupId::new("g"),
            target: None,
            digest: Vec::new(),
            wants: Vec::new(),
            members: vec![wire::MemberDelta {
                node: NodeId::new(node),
                incarnation: 0,
                status: Status::Alive.to_wire(),
                max_version: 1,
                entries: vec![wire::EntryDelta {
                    key: "k".into(),
                    version: 1,
                    ttl_ms,
                    tombstone: false,
                    value: b"v".to_vec(),
                }],
            }],
            metadata: Vec::new(),
            lead: None,
        })
    }

    /// The cached deadline and dissemination targets follow every change
    /// that moves them, between ticks: an adopted TTL entry, a suspicion and
    /// a death learned by gossip, new discovery contacts. The cached
    /// deadline then equals a scan of every member; the targets, a fresh
    /// merge.
    #[test]
    fn the_cached_deadline_and_targets_follow_every_change() {
        let config = Config {
            probe_interval_ms: 1_000_000,
            gossip_interval_ms: 1_000_000,
            ..Config::default()
        };
        let suspect_ms = config.suspect_timeout_ms;
        let dead_ms = config.dead_timeout_ms;
        let mut e = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], config)
            .expect("valid config");
        e.start(Time(0));
        let targets = |e: &mut GroupEngine| e.rotated_targets(0, 10);
        let ids = |names: &[&str]| names.iter().map(|n| NodeId::new(*n)).collect::<Vec<_>>();

        e.on_message(
            NodeId::new("b"),
            &digest(&[("b", 0, Status::Alive), ("c", 0, Status::Alive)]),
            Time(5),
        );
        assert_eq!(targets(&mut e), ids(&["b", "c"]));

        e.on_message(NodeId::new("b"), &delta_with_ttl("b", 500), Time(10));
        assert_eq!(e.slow_deadline, e.scan_slow_deadline());
        assert_eq!(e.next_deadline(), Time(510), "the TTL expiry is next");

        e.on_message(
            NodeId::new("b"),
            &digest(&[("c", 0, Status::Suspect)]),
            Time(20),
        );
        assert_eq!(e.slow_deadline, e.scan_slow_deadline());
        assert_eq!(e.next_deadline(), Time(510).min(Time(20 + suspect_ms)));

        e.apply(Command::SetBootstrapContacts(ids(&["z"])));
        assert_eq!(targets(&mut e), ids(&["b", "c", "z"]));

        e.on_message(
            NodeId::new("b"),
            &digest(&[("c", 0, Status::Dead)]),
            Time(30),
        );
        assert_eq!(e.slow_deadline, e.scan_slow_deadline());
        assert_eq!(
            targets(&mut e),
            ids(&["b", "z"]),
            "a dead member is no target"
        );
        assert_eq!(e.next_deadline(), Time(510).min(Time(30 + 2 * dead_ms)));
    }

    /// The merged, rotated selection equals the one it replaced: a sorted set
    /// of members, seeds and contacts, indexed at `(cursor + i) % n`.
    #[test]
    fn rotated_targets_match_a_sorted_set_indexed_round_robin() {
        let id = |s: &str| NodeId::new(s);
        let seeds = ["b", "d", "x", "z"].map(id);
        let mut e = GroupEngine::new(GroupId::new("g"), id("m"), seeds.clone(), Config::default())
            .expect("valid config");
        for peer in ["a", "b", "c", "q", "m2"] {
            e.apply(Command::AddPeer(id(peer)));
        }
        e.apply(Command::SetBootstrapContacts(
            ["c", "y", "z", "aa"].map(id).to_vec(),
        ));
        let mut all: BTreeSet<NodeId> = e.probe_candidates().cloned().collect();
        all.extend(seeds);
        all.extend(e.bootstrap_contacts.iter().cloned());
        let all: Vec<NodeId> = all.into_iter().collect();
        assert_eq!(e.dissemination_targets().cloned().collect::<Vec<_>>(), all);
        for cursor in [0, 1, 5, 9, 10, 11, usize::MAX - 3, usize::MAX] {
            for k in 1..=all.len() + 2 {
                let expected: Vec<NodeId> = (0..k.min(all.len()))
                    .map(|i| all[(cursor % all.len() + i) % all.len()].clone())
                    .collect();
                assert_eq!(
                    e.rotated_targets(cursor, k),
                    expected,
                    "cursor {cursor} k {k}"
                );
            }
        }
    }
}
