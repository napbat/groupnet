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

use std::collections::{BTreeMap, BTreeSet};

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
    #[must_use]
    pub fn to_wire(self) -> u8 {
        match self {
            Status::Alive => STATUS_ALIVE,
            Status::Suspect => STATUS_SUSPECT,
            Status::Dead => STATUS_DEAD,
        }
    }

    /// Decodes a one-byte wire code, or `None` for an unrecognised one. Peers
    /// run the same build, so an unknown code is malformed input, and the
    /// engine drops any frame that carries one.
    ///
    /// The inverse of [`Status::to_wire`], and public for the same reason.
    #[must_use]
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

    /// When this entry next needs a tick: its TTL expiry, or for a tombstone
    /// its reap `reap_after` after it was tombstoned ([`Time::MAX`] for a
    /// live entry without a TTL).
    pub(crate) fn reap_due(&self, reap_after: u64) -> Time {
        if self.tombstone {
            self.tombstone_since.saturating_add(reap_after)
        } else {
            self.expires_at
        }
    }
}

/// Where the engine's index has a member filed, so a change can move it
/// without a search: under its earliest deadline, and — while it is live —
/// under its coordinator score.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Filed {
    /// The member's earliest deadline: its suspect window or `Dead` reap,
    /// or its first entry TTL or tombstone reap ([`Time::MAX`] = none, not
    /// filed).
    pub(crate) due: Time,
    /// The member's coordinator score while it is live (not `Dead`).
    pub(crate) score: Option<u64>,
}

impl Filed {
    /// Not filed anywhere.
    const NONE: Self = Self {
        due: Time::MAX,
        score: None,
    };
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
    /// When *this* observer last saw [`status`](Self::status) take a **new
    /// value** — the instant the member has continuously held its current
    /// status since. First adoption counts; re-merging the same status (at any
    /// incarnation) does not move it, so it measures uninterrupted duration,
    /// not last-touched.
    ///
    /// Distinct from [`suspect_since`](Self::suspect_since), which is the
    /// suspicion *timer* and may legitimately re-arm on a higher-incarnation
    /// suspicion while the status value stays `Suspect`.
    pub(crate) status_since: Time,
    /// When *this* node first observed the member as `Suspect` (for the suspicion
    /// timeout). Only meaningful while `status == Suspect`.
    pub(crate) suspect_since: Time,
    /// When *this* node first observed the member as `Dead` (for gossip TTL and
    /// reaping). Only meaningful while `status == Dead`.
    pub(crate) dead_since: Time,
    /// When *this* observer last adopted a liveness verdict for the member —
    /// a first sighting, or a new incarnation or status that superseded the
    /// old one. Full digests lead with members whose liveness changed
    /// recently, so suspicions, refutations and deaths spread at epidemic
    /// speed however busy the entries are.
    pub(crate) liveness_since: Time,
    /// Keyed app-defined per-node state; each entry merged by its own version,
    /// independently of liveness.
    pub(crate) entries: BTreeMap<String, StateEntry>,
    /// The keys of [`entries`](Self::entries) by when each next needs a tick
    /// ([`StateEntry::reap_due`]); an entry that never does is not listed.
    /// Kept by every entry write and removal here, so a tick reaps only the
    /// entries that are due.
    pub(crate) entry_dues: BTreeMap<Time, BTreeSet<String>>,
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
    /// The member's [content hash](crate::GroupEngine) as last computed, with
    /// the instant it stops being valid: the next moment an entry's TTL
    /// expires or a tombstone stops being gossiped. `None` until computed and
    /// after any change to [`entries`](Self::entries) — every write goes
    /// through [`put_entry`](Self::put_entry) or
    /// [`entries_changed`](Self::entries_changed). Computing the hash folds
    /// every entry's bytes, and a digest lists up to a window of members, so
    /// recomputing it per digest dominated a busy group's CPU.
    pub(crate) hash_cache: Option<(u64, Time)>,
    /// Where the engine's index has this member filed (engine-maintained).
    pub(crate) filed: Filed,
}

impl Member {
    /// A fresh record at `incarnation`/`status` adopted at `now`, with cleared
    /// timers and no state. First adoption of a status counts as a change, so
    /// `status_since` starts at `now`.
    pub(crate) fn new(incarnation: u64, status: Status, now: Time) -> Self {
        Self {
            incarnation,
            status,
            status_since: now,
            suspect_since: Time::ZERO,
            dead_since: Time::ZERO,
            liveness_since: now,
            entries: BTreeMap::new(),
            entry_dues: BTreeMap::new(),
            max_state_version: 0,
            changed_at: 0,
            hash_cache: None,
            filed: Filed::NONE,
        }
    }

    /// Records a resolved liveness verdict, stamping
    /// [`status_since`](Self::status_since) **only when the status value
    /// actually changes**. A same-status re-merge — a repeated gossip claim, a
    /// restatement at a higher incarnation — leaves the duration running,
    /// which is what makes `status_since` a continuous-status stamp rather
    /// than a last-touched one.
    ///
    /// Every caller has just resolved a verdict that superseded the old one
    /// (a new incarnation or status), so
    /// [`liveness_since`](Self::liveness_since) moves on every call.
    ///
    /// The suspicion/death *timers* are the caller's: they re-arm on their own
    /// rules (a higher-incarnation suspicion legitimately restarts
    /// `suspect_since` while the status value stands still), so this never
    /// touches them.
    pub(crate) fn adopt_status(&mut self, status: Status, now: Time) {
        self.liveness_since = now;
        if self.status != status {
            self.status = status;
            self.status_since = now;
        }
    }

    /// Records that an entry at `version` was seen for this member, advancing the
    /// high-water mark (never regressing it).
    pub(crate) fn observe_version(&mut self, version: u64) {
        self.max_state_version = self.max_state_version.max(version);
    }

    /// Stores `entry` under `key`, invalidating the cached content hash and
    /// re-listing the key under its new deadline.
    pub(crate) fn put_entry(&mut self, key: String, entry: StateEntry, reap_after: u64) {
        let due = entry.reap_due(reap_after);
        let old = self.entries.get(&key).map(|e| e.reap_due(reap_after));
        if old != Some(due) {
            let kept = old.and_then(|at| self.unlist_due(at, &key));
            if due != Time::MAX {
                self.entry_dues
                    .entry(due)
                    .or_default()
                    .insert(kept.unwrap_or_else(|| key.clone()));
            }
        }
        self.entries.insert(key, entry);
        self.entries_changed();
    }

    /// Removes every entry due by `now` from the map, in key order, with the
    /// key each was stored under.
    pub(crate) fn take_due_entries(&mut self, now: Time) -> Vec<(String, StateEntry)> {
        let mut keys: Vec<String> = Vec::new();
        while let Some(first) = self.entry_dues.first_entry()
            && *first.key() <= now
        {
            keys.extend(first.remove());
        }
        keys.sort_unstable();
        let due: Vec<(String, StateEntry)> = keys
            .into_iter()
            .filter_map(|key| self.entries.remove(&key).map(|e| (key, e)))
            .collect();
        if !due.is_empty() {
            self.entries_changed();
        }
        due
    }

    /// When the first of this member's entries next needs a tick
    /// ([`Time::MAX`] when none does).
    pub(crate) fn next_entry_due(&self) -> Time {
        self.entry_dues.keys().next().copied().unwrap_or(Time::MAX)
    }

    /// Takes `key` out of the deadline `at` it is listed under, returning
    /// the stored key for reuse.
    fn unlist_due(&mut self, at: Time, key: &str) -> Option<String> {
        let keys = self.entry_dues.get_mut(&at)?;
        let kept = keys.take(key);
        if keys.is_empty() {
            self.entry_dues.remove(&at);
        }
        kept
    }

    /// Invalidates the cached content hash after an in-place entry change
    /// that leaves every entry's deadline where it was.
    pub(crate) fn entries_changed(&mut self) {
        self.hash_cache = None;
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

    /// `status_since` measures how long the *value* has stood: adoption starts
    /// it, a real transition restarts it, and re-adopting the same status —
    /// however many times — leaves it exactly where it was.
    #[test]
    fn status_since_tracks_value_changes_only() {
        let mut m = Member::new(0, Status::Alive, Time(10));
        assert_eq!(m.status_since, Time(10));

        m.adopt_status(Status::Alive, Time(20));
        assert_eq!(m.status_since, Time(10), "same status must not re-stamp");

        m.adopt_status(Status::Suspect, Time(30));
        assert_eq!(m.status_since, Time(30));
        m.adopt_status(Status::Suspect, Time(40));
        assert_eq!(m.status_since, Time(30), "same status must not re-stamp");

        // The suspicion timer re-arms independently of the status stamp.
        m.suspect_since = Time(40);
        m.adopt_status(Status::Suspect, Time(50));
        assert_eq!(m.status_since, Time(30));
        assert_eq!(m.suspect_since, Time(40));

        m.adopt_status(Status::Dead, Time(60));
        assert_eq!(m.status_since, Time(60));
    }
}
