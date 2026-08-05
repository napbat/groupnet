//! The SWIM membership model.
//!
//! This is what the engine knows about *who is in the group and whether they are
//! alive*: a member's liveness [`Status`], the local [`Member`] record, the
//! merge-precedence rule that reconciles two views of the same member
//! ([`Member::superseded_by`]), and the one-byte wire encoding of a status.
//!
//! The engine owns the `Status` ↔ byte mapping deliberately, so the
//! [`wire`](crate::wire) codec only ever moves opaque bytes and never has to
//! know what a status *means*.

use crate::Time;

/// A member's liveness status. The variants are ordered by precedence: when two
/// views of a member are merged at equal incarnation, the higher-precedence
/// status wins (`Dead` > `Suspect` > `Alive`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Believed healthy.
    Alive,
    /// A probe went unanswered; awaiting refutation or death.
    Suspect,
    /// Confirmed gone (failed or voluntarily left). Terminal.
    Dead,
}

/// Wire codes for [`Status`]. Named constants, not inline literals, so the
/// byte-level protocol has one auditable source of truth.
const STATUS_ALIVE: u8 = 0;
const STATUS_SUSPECT: u8 = 1;
const STATUS_DEAD: u8 = 2;

impl Status {
    /// Encodes this status as its one-byte wire code.
    ///
    /// Public because the mapping *is* public protocol: [`wire::NodeDigest`]
    /// and [`wire::MemberDelta`] carry their `status` as a bare `u8`, so anyone
    /// constructing or reading a frame needs this to interpret it.
    ///
    /// [`wire::NodeDigest`]: crate::wire::NodeDigest
    /// [`wire::MemberDelta`]: crate::wire::MemberDelta
    pub fn to_wire(self) -> u8 {
        match self {
            Status::Alive => STATUS_ALIVE,
            Status::Suspect => STATUS_SUSPECT,
            Status::Dead => STATUS_DEAD,
        }
    }

    /// Decodes a one-byte wire code, or `None` for an unrecognised one — a
    /// forward-compatible peer may advertise codes this version doesn't know.
    ///
    /// The inverse of [`Status::to_wire`], and public for the same reason.
    pub fn from_wire(code: u8) -> Option<Status> {
        match code {
            STATUS_ALIVE => Some(Status::Alive),
            STATUS_SUSPECT => Some(Status::Suspect),
            STATUS_DEAD => Some(Status::Dead),
            _ => None,
        }
    }
}

/// One key of a member's app-defined state.
///
/// Per-node state is a **keyed map** of independently-versioned entries, so an
/// application can update one fact (an address, a readiness flag, one page of
/// a progress map) without re-shipping or re-versioning the rest. Each entry
/// is single-writer (its owning node) and stamped from that node's one
/// monotonic version clock, so a bare version totally orders it — no writer
/// tiebreak needed — and a scalar per-node maximum summarizes the whole map.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct StateEntry {
    /// The owning node's version clock at authoring time. Monotonic across all of
    /// that node's keys.
    pub(crate) version: u64,
    /// The value. Meaningless when `tombstone` is set.
    pub(crate) value: Vec<u8>,
    /// TTL in ms as authored (0 = none). Carried on the wire so every receiver
    /// can arm its own expiry.
    pub(crate) ttl_ms: u64,
    /// Local expiry stamp, computed at write/merge time from `ttl_ms` against
    /// the *receiver's* clock ([`Time::MAX`] = never). Entries converge to
    /// absent once the author stops refreshing them; small cross-node skew in
    /// exactly when is inherent and documented.
    pub(crate) expires_at: Time,
    /// Deletion marker: gossiped for a while so peers drop the key too, then
    /// reaped (same lifecycle shape as a Dead member tombstone).
    pub(crate) tombstone: bool,
    /// When this node first held the entry as a tombstone (for reaping).
    pub(crate) tombstone_since: Time,
}

impl StateEntry {
    /// An entry as written or adopted at `now`: `ttl_ms == 0` never expires,
    /// and a tombstone's gossip age starts now.
    pub(crate) fn adopted(
        version: u64,
        value: Vec<u8>,
        ttl_ms: u64,
        tombstone: bool,
        now: Time,
    ) -> Self {
        Self {
            version,
            value,
            ttl_ms,
            expires_at: if ttl_ms == 0 {
                Time::MAX
            } else {
                now.saturating_add(ttl_ms)
            },
            tombstone,
            tombstone_since: if tombstone { now } else { Time::ZERO },
        }
    }

    /// Whether the entry has expired at `now` (tombstones don't expire — they
    /// reap on their own schedule).
    pub(crate) fn expired(&self, now: Time) -> bool {
        !self.tombstone && now >= self.expires_at
    }
}

/// One member's record, as this node currently sees it: SWIM liveness plus the
/// keyed per-node state the member authors about itself.
///
/// Fields are `pub(crate)` because the engine is their sole manager; nothing
/// outside the crate mutates a member directly.
#[derive(Clone, Debug)]
pub(crate) struct Member {
    /// The member's incarnation, bumped by the member itself to refute suspicion.
    pub(crate) incarnation: u64,
    /// The member's liveness as we last resolved it.
    pub(crate) status: Status,
    /// When *this* node first observed the member as `Suspect` (for the suspicion
    /// timeout). Only meaningful while `status == Suspect`.
    pub(crate) suspect_since: Time,
    /// When *this* node first observed the member as `Dead` (for gossip TTL and
    /// reaping). Only meaningful while `status == Dead`.
    pub(crate) dead_since: Time,
    /// Keyed app-defined per-node state; each entry merged by its own version,
    /// independently of liveness.
    pub(crate) entries: std::collections::BTreeMap<String, StateEntry>,
    /// **High-water mark** over every entry version this node has *ever* held for
    /// the member — the scalar digest summary the anti-entropy round advertises.
    ///
    /// It only ever rises: reaping a tombstone or expiring a TTL entry drops the
    /// `entries` map but never lowers this, so a digest can never claim to be
    /// behind on a version it has already reaped, and therefore can never
    /// resurrect it. Combined with the single-writer per-node version clock, the
    /// scalar is an exact summary: two observers reporting the same high-water for
    /// a member hold the identical set of that member's live entries.
    pub(crate) max_state_version: u64,
    /// The engine's change-clock value when this member's digest-visible
    /// summary (incarnation, status, or version high-water) last changed —
    /// what per-peer delta digests filter on.
    pub(crate) changed_at: u64,
}

impl Member {
    /// A fresh record at `incarnation`/`status`, with cleared timers and no state.
    pub(crate) fn new(incarnation: u64, status: Status) -> Self {
        Self {
            incarnation,
            status,
            suspect_since: Time::ZERO,
            dead_since: Time::ZERO,
            entries: std::collections::BTreeMap::new(),
            max_state_version: 0,
            changed_at: 0,
        }
    }

    /// Records that an entry at `version` was seen for this member, advancing the
    /// high-water mark (never regressing it).
    pub(crate) fn observe_version(&mut self, version: u64) {
        self.max_state_version = self.max_state_version.max(version);
    }

    /// SWIM merge precedence: would an incoming `(incarnation, status)` override
    /// this record?
    ///
    /// * `Alive` overrides only a strictly newer incarnation (you must
    ///   out-incarnate to refute a suspicion).
    /// * `Suspect` overrides an alive member at equal-or-newer incarnation, or a
    ///   suspect at strictly newer; never a dead one.
    /// * `Dead` overrides anything not already dead at equal-or-newer incarnation.
    pub(crate) fn superseded_by(&self, incarnation: u64, status: Status) -> bool {
        match status {
            Status::Alive => incarnation > self.incarnation,
            Status::Suspect => match self.status {
                Status::Alive => incarnation >= self.incarnation,
                Status::Suspect => incarnation > self.incarnation,
                Status::Dead => false,
            },
            Status::Dead => self.status != Status::Dead && incarnation >= self.incarnation,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every status survives a wire round-trip, and unknown codes decode to
    /// `None` rather than silently aliasing a known status.
    #[test]
    fn status_round_trips_through_its_wire_code() {
        for status in [Status::Alive, Status::Suspect, Status::Dead] {
            assert_eq!(Status::from_wire(status.to_wire()), Some(status));
        }
        assert_eq!(Status::from_wire(3), None);
        assert_eq!(Status::from_wire(u8::MAX), None);
    }
}
