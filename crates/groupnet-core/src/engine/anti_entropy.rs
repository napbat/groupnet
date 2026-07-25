//! Digest/delta anti-entropy: per-peer delta digests, reconciliation, and
//! eager push.

use std::collections::BTreeSet;

use crate::membership::{Member, StateEntry, Status};
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::state::GroupEngine;

impl GroupEngine {
    pub(super) fn anti_entropy_interval(&self) -> u64 {
        self.config.anti_entropy_interval_ms.max(1)
    }

    /// Brings the next anti-entropy round forward to now, so a fresh membership
    /// change or local write disseminates on the next tick rather than waiting a
    /// whole interval — restoring the "state change rides the next frame"
    /// promptness the full-view piggyback used to give for free.
    pub(super) fn nudge_anti_entropy(&mut self) {
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
        let targets = self.select_fanout_targets();
        if targets.is_empty() {
            return Vec::new();
        }
        let mut effects = Vec::new();
        for to in targets {
            // A peer's first digest — and every Nth after — is full; the rest
            // are per-peer delta digests listing only members whose summary
            // changed since the last digest built for this peer. The cursor
            // advances on build, not delivery: anything a dropped frame loses
            // stays divergent only until this peer's next full digest.
            let every = self.config.full_digest_every.max(1);
            let visit = self.digest_visits.entry(to.clone()).or_insert(0);
            let full = *visit % every == 0;
            *visit += 1;
            let since = if full {
                None
            } else {
                Some(self.digest_cursors.get(&to).copied().unwrap_or(0))
            };
            let (chunks, listed) = self.build_digest_chunks(now, since);
            self.digest_cursors.insert(to.clone(), self.change_clock);
            self.stats.digests_built += 1;
            if full {
                self.stats.full_digests_built += 1;
            }
            self.stats.digest_summaries_listed += listed as u64;
            for chunk in chunks {
                self.stats.digest_frames_sent += 1;
                self.stats.anti_entropy_bytes_sent += chunk.len() as u64;
                effects.push(Effect::Send {
                    to: to.clone(),
                    wire: chunk,
                });
            }
        }
        effects
    }

    /// Picks `anti_entropy_fanout` distinct peers, rotating a cursor so every
    /// peer is covered over successive rounds.
    pub(super) fn select_fanout_targets(&mut self) -> Vec<NodeId> {
        let candidates = self.dissemination_targets();
        let n = candidates.len();
        if n == 0 {
            return Vec::new();
        }
        let k = self.config.anti_entropy_fanout.max(1).min(n);
        let mut out = Vec::with_capacity(k);
        for i in 0..k {
            out.push(candidates[(self.gossip_cursor + i) % n].clone());
        }
        self.gossip_cursor = self.gossip_cursor.wrapping_add(k);
        out
    }

    /// Builds a digest for one peer as encoded [`wire::Kind::Digest`] frames
    /// within the frame budget, returning the frames and how many member
    /// summaries they list. `since` of `None` builds a full digest; `Some(c)`
    /// lists only members stamped after change-clock `c` — a per-peer delta
    /// digest, safe because a digest only ever triggers per-listed-member
    /// reconciliation (absence is never interpreted). The metadata register
    /// set rides the first chunk either way.
    pub(super) fn build_digest_chunks(
        &self,
        now: Time,
        since: Option<u64>,
    ) -> (Vec<Vec<u8>>, usize) {
        let budget = self.config.max_delta_frame_bytes;
        let summaries: Vec<wire::NodeDigest> = self
            .members
            .iter()
            .filter(|(_, m)| self.should_gossip(m, now))
            .filter(|(_, m)| since.is_none_or(|cursor| m.changed_at > cursor))
            .map(|(node, m)| wire::NodeDigest {
                node: node.clone(),
                incarnation: m.incarnation,
                status: m.status.to_wire(),
                max_version: m.max_state_version,
                content_hash: self.content_hash(m, now),
            })
            .collect();
        let metadata: Vec<wire::MetaDelta> = self
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
        let mut i = 0usize;
        loop {
            let first = chunks.is_empty();
            let meta = if first { metadata.clone() } else { Vec::new() };
            let meta_len = 4 + meta.iter().map(wire::meta_len).sum::<usize>();
            let mut size = base + meta_len;
            let mut slice: Vec<wire::NodeDigest> = Vec::new();
            while i < summaries.len() {
                let d = &summaries[i];
                let dlen = wire::digest_len(d);
                if size + dlen > budget && !slice.is_empty() {
                    break;
                }
                size += dlen;
                slice.push(d.clone());
                i += 1;
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
            }));
            if i >= summaries.len() {
                break;
            }
        }
        let listed = summaries.len();
        (chunks, listed)
    }

    /// Merges a peer's digest: reconcile liveness and metadata directly, then
    /// request whatever entries we're behind on and offer whatever we're ahead
    /// on.
    pub(super) fn on_digest(
        &mut self,
        from: &NodeId,
        frame: &wire::Frame,
        now: Time,
    ) -> Vec<Effect> {
        let mut effects = self.merge_digest_liveness(&frame.digest, now);
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
        let budget = self.config.max_delta_frame_bytes;
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
        let budget = self.config.max_delta_frame_bytes;
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
        }))
    }

    pub(super) fn dissemination_targets(&self) -> Vec<NodeId> {
        let mut set: BTreeSet<NodeId> = self.probe_candidates().cloned().collect();
        set.extend(self.seeds.iter().cloned());
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
