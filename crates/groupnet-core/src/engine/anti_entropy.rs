//! Digest/delta anti-entropy: windowed full digests, per-peer delta digests,
//! reconciliation, and eager push.

use std::cmp::Reverse;
use std::collections::BTreeSet;

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
        // so it always advances.
        let hot_ms = self.liveness_hot_ms();
        listed.sort_unstable_by_key(|(node, m)| {
            let hot = now.0.saturating_sub(m.liveness_since.0) < hot_ms;
            (
                **node != self.local,
                !hot,
                Reverse(if hot { m.liveness_since } else { Time::ZERO }),
                Reverse(m.changed_at),
            )
        });
        listed.truncate(window / 2);
        let lead = listed.len();
        let ring: Vec<(&NodeId, &Member)> = gossipable().collect();
        if !ring.is_empty() {
            let start = match &peer.resume {
                Some(after) => ring.partition_point(|(node, _)| *node <= after),
                None => spread::index(spread::pair_hash(&self.local, to), ring.len()),
            };
            for step in 0..ring.len() {
                if listed.len() >= window {
                    break;
                }
                let (node, m) = ring[(start + step) % ring.len()];
                peer.resume = Some(node.clone());
                if listed[..lead].iter().any(|(led, _)| *led == node) {
                    continue; // already leads the digest
                }
                listed.push((node, m));
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
        let candidates = self.dissemination_targets();
        let n = candidates.len();
        if n == 0 {
            return Vec::new();
        }
        let k = self.config.gossip_fanout.get().min(n);
        let mut out = Vec::with_capacity(k);
        for i in 0..k {
            out.push(candidates[(self.gossip_cursor + i) % n].clone());
        }
        self.gossip_cursor = self.gossip_cursor.wrapping_add(k);
        out
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

    pub(super) fn dissemination_targets(&self) -> Vec<NodeId> {
        let mut set: BTreeSet<NodeId> = self.probe_candidates().cloned().collect();
        set.extend(self.seeds.iter().cloned());
        set.extend(self.bootstrap_contacts.iter().cloned());
        set.into_iter().collect()
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
