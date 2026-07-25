//! SWIM-precedence and last-writer-wins merges, refutation, and the
//! content hash delta digests rely on.

use crate::membership::{Member, StateEntry, Status};
use crate::{NodeId, Time, wire};

use super::effect::Effect;
use super::state::{GroupEngine, VersionedValue};

/// One step of an FNV-1a fold (dep-free), used to hash a member's held entries
/// into the digest's content hash.
fn fnv1a(mut h: u64, bytes: &[u8]) -> u64 {
    for &b in bytes {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h
}

impl GroupEngine {
    /// Would an incoming `(status, incarnation)` about *ourselves* require us to
    /// refute (bump our incarnation and reassert Alive)? Returns the incarnation
    /// to jump to, if so. A voluntary leave is never refuted.
    pub(super) fn self_refute_target(&self, status: Status, incarnation: u64) -> Option<u64> {
        if self.leaving {
            return None;
        }
        let false_suspicion = status != Status::Alive && incarnation >= self.incarnation;
        let peer_ahead = incarnation > self.incarnation;
        (false_suspicion || peer_ahead).then_some(incarnation + 1)
    }

    pub(super) fn apply_refutation(&mut self, refute_to: Option<u64>) -> bool {
        let Some(ni) = refute_to else {
            return false;
        };
        self.incarnation = ni;
        self.stamp_self();
        if let Some(m) = self.members.get_mut(&self.local) {
            m.incarnation = ni;
            m.status = Status::Alive;
        }
        true
    }

    /// Applies a peer's liveness claim about a *remote* node by SWIM precedence.
    /// Adopts an unknown node (liveness only — no entries) or updates a known
    /// one. Returns whether membership changed.
    pub(super) fn merge_remote_liveness(
        &mut self,
        node: &NodeId,
        incarnation: u64,
        status: Status,
        now: Time,
    ) -> bool {
        match self.members.get(node) {
            None => {
                let mut member = Member::new(incarnation, status);
                match status {
                    Status::Suspect => member.suspect_since = now,
                    Status::Dead => member.dead_since = now,
                    Status::Alive => {}
                }
                self.members.insert(node.clone(), member);
                self.stamp(node);
                true
            }
            Some(cur) => {
                if !cur.superseded_by(incarnation, status) {
                    return false;
                }
                let member = self.members.get_mut(node).expect("present");
                member.incarnation = incarnation;
                member.status = status;
                match status {
                    Status::Suspect => member.suspect_since = now,
                    Status::Dead => member.dead_since = now,
                    Status::Alive => {}
                }
                self.stamp(node);
                true
            }
        }
    }

    /// Merges the liveness half of a digest (incarnation/status per node) and
    /// refutes any suspicion of ourselves it carries. State reconciliation is a
    /// separate delta round-trip.
    pub(super) fn merge_digest_liveness(
        &mut self,
        digest: &[wire::NodeDigest],
        now: Time,
    ) -> Vec<Effect> {
        let mut membership_changed = false;
        let mut refute_to: Option<u64> = None;
        for d in digest {
            let Some(status) = Status::from_wire(d.status) else {
                continue;
            };
            if d.node == self.local {
                if let Some(t) = self.self_refute_target(status, d.incarnation) {
                    refute_to = Some(refute_to.map_or(t, |x| x.max(t)));
                }
                continue;
            }
            membership_changed |= self.merge_remote_liveness(&d.node, d.incarnation, status, now);
        }
        membership_changed |= self.apply_refutation(refute_to);

        let mut effects = Vec::new();
        if membership_changed {
            effects.push(Effect::MembershipChanged);
            effects.extend(self.recompute_coordinator());
            self.nudge_anti_entropy();
        }
        effects
    }

    /// Merges the member deltas of a `Delta` frame. Liveness (`incarnation` /
    /// `status`, by SWIM precedence) and app state (per-key last-writer-wins) are
    /// merged *independently*, high-water marks advance, and our own echoed
    /// entries are adopted for restart recovery.
    pub(super) fn merge_members(
        &mut self,
        deltas: Vec<wire::MemberDelta>,
        now: Time,
    ) -> Vec<Effect> {
        let mut membership_changed = false;
        let mut state_changed: Vec<(NodeId, String)> = Vec::new();
        let mut refute_to: Option<u64> = None;

        for delta in deltas {
            let Some(status) = Status::from_wire(delta.status) else {
                continue; // unknown status code — ignore
            };

            if delta.node == self.local {
                // Refute a false suspicion / out-incarnate a peer ahead of us.
                if let Some(t) = self.self_refute_target(status, delta.incarnation) {
                    refute_to = Some(refute_to.map_or(t, |x| x.max(t)));
                }
                // Restart recovery (the wipe fix): our own entries echoed back at
                // versions above what we hold are OUR data from before a restart.
                // ADOPT them verbatim for keys we have NOT authored this boot; for
                // authored keys keep our value and out-version the echo.
                for entry in delta.entries {
                    let ours = self.members[&self.local].entries.get(&entry.key);
                    if ours.is_some_and(|e| entry.version <= e.version) {
                        continue; // echo of something we already hold — ignore
                    }
                    let m = self.members.get_mut(&self.local).expect("self present");
                    if self.authored.contains(&entry.key) {
                        // Sole-author rule: never let an echo (or forgery) replace
                        // a value we wrote this boot. Jump our version above it and
                        // keep re-advertising OUR value, which supersedes everywhere.
                        let bumped = m.entries.get_mut(&entry.key).map(|e| {
                            e.version = entry.version.saturating_add(1);
                            e.version
                        });
                        if let Some(v) = bumped {
                            m.observe_version(v);
                            state_changed.push((self.local.clone(), entry.key));
                            self.stamp_self();
                        }
                    } else {
                        // A key we have NOT authored this boot, echoed at a higher
                        // version, is our own pre-restart data — adopt it verbatim.
                        m.observe_version(entry.version);
                        m.entries.insert(
                            entry.key.clone(),
                            StateEntry::adopted(
                                entry.version,
                                entry.value,
                                entry.ttl_ms,
                                entry.tombstone,
                                now,
                            ),
                        );
                        state_changed.push((self.local.clone(), entry.key));
                        self.stamp_self();
                    }
                }
                continue;
            }

            match self.members.get(&delta.node) {
                None => {
                    // Unknown node: adopt its liveness and state wholesale.
                    let mut member = Member::new(delta.incarnation, status);
                    match status {
                        Status::Suspect => member.suspect_since = now,
                        Status::Dead => member.dead_since = now,
                        Status::Alive => {}
                    }
                    for entry in delta.entries {
                        member.observe_version(entry.version);
                        state_changed.push((delta.node.clone(), entry.key.clone()));
                        member.entries.insert(
                            entry.key,
                            StateEntry::adopted(
                                entry.version,
                                entry.value,
                                entry.ttl_ms,
                                entry.tombstone,
                                now,
                            ),
                        );
                    }
                    member.observe_version(delta.max_version);
                    self.members.insert(delta.node.clone(), member);
                    self.stamp(&delta.node);
                    membership_changed = true;
                }
                Some(cur) => {
                    let status_wins = cur.superseded_by(delta.incarnation, status);
                    let member = self.members.get_mut(&delta.node).expect("present");
                    let high_water_before = member.max_state_version;
                    let mut adopted = false;
                    if status_wins {
                        member.incarnation = delta.incarnation;
                        member.status = status;
                        match status {
                            Status::Suspect => member.suspect_since = now,
                            Status::Dead => member.dead_since = now,
                            Status::Alive => {}
                        }
                        membership_changed = true;
                    }
                    // Per-key LWW, independent of liveness: each entry is
                    // single-writer, so version order alone decides; a fresher
                    // version also re-arms the local TTL. Every seen version
                    // advances the high-water mark.
                    for entry in delta.entries {
                        member.observe_version(entry.version);
                        // Per-key LWW by version, with a deterministic tiebreak
                        // (tombstone, then value) so a version reused across a
                        // restart can never deadlock two divergent values at the
                        // same number — one side always wins and both converge.
                        let wins = member.entries.get(&entry.key).is_none_or(|e| {
                            (entry.version, entry.tombstone, &entry.value)
                                > (e.version, e.tombstone, &e.value)
                        });
                        if !wins {
                            continue;
                        }
                        member.entries.insert(
                            entry.key.clone(),
                            StateEntry::adopted(
                                entry.version,
                                entry.value,
                                entry.ttl_ms,
                                entry.tombstone,
                                now,
                            ),
                        );
                        adopted = true;
                        state_changed.push((delta.node.clone(), entry.key));
                    }
                    // The sender's high-water (>= every version it holds) lets us
                    // advance our summary past a reaped tail without re-requesting.
                    member.observe_version(delta.max_version);
                    // Anything digest-visible moved (liveness, content, or
                    // high-water): re-advertise via future delta digests.
                    if status_wins || adopted || member.max_state_version > high_water_before {
                        self.stamp(&delta.node);
                    }
                }
            }
        }

        membership_changed |= self.apply_refutation(refute_to);

        let mut effects = Vec::new();
        if membership_changed {
            effects.push(Effect::MembershipChanged);
            effects.extend(self.recompute_coordinator());
            self.nudge_anti_entropy();
        }
        for (node, key) in state_changed {
            effects.push(Effect::NodeStateChanged { node, key });
        }
        effects
    }

    /// Merges incoming metadata deltas by last-writer-wins: an entry is adopted
    /// iff its `(version, writer)` strictly exceeds what we hold. A per-key
    /// LWW-register (a CRDT), so all replicas converge on one value.
    pub(super) fn merge_metadata(&mut self, incoming: Vec<wire::MetaDelta>) -> Vec<Effect> {
        let mut effects = Vec::new();
        for wire::MetaDelta {
            key,
            version,
            writer,
            value,
        } in incoming
        {
            let wins = match self.metadata.get(&key) {
                Some(local) => (version, &writer) > (local.version, &local.writer),
                None => true,
            };
            if wins {
                self.metadata.insert(
                    key.clone(),
                    VersionedValue {
                        value: value.clone(),
                        version,
                        writer,
                    },
                );
                effects.push(Effect::MetadataChanged { key, value });
            }
        }
        effects
    }

    /// A hash of a member's currently-advertised entries (keys, versions,
    /// tombstones, values, in key order), carried in the digest so a receiver can
    /// tell two summaries apart when their high-water marks coincide but their
    /// holdings do not (a version reused across a restart). Empty holdings hash to
    /// zero. A tiny dependency-free FNV-1a fold — the core stays dep-free.
    pub(super) fn content_hash(&self, m: &Member, now: Time) -> u64 {
        let mut h: u64 = 0;
        for (key, e) in &m.entries {
            if !self.should_gossip_entry(e, now) {
                continue;
            }
            h = fnv1a(h, key.as_bytes());
            h = fnv1a(h, &e.version.to_le_bytes());
            h = fnv1a(h, &[u8::from(e.tombstone)]);
            h = fnv1a(h, &e.value);
        }
        h
    }
}
