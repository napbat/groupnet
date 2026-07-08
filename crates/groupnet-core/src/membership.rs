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
    pub(crate) fn to_wire(self) -> u8 {
        match self {
            Status::Alive => STATUS_ALIVE,
            Status::Suspect => STATUS_SUSPECT,
            Status::Dead => STATUS_DEAD,
        }
    }

    /// Decodes a one-byte wire code, or `None` for an unrecognised one — a
    /// forward-compatible peer may advertise codes this version doesn't know.
    pub(crate) fn from_wire(code: u8) -> Option<Status> {
        match code {
            STATUS_ALIVE => Some(Status::Alive),
            STATUS_SUSPECT => Some(Status::Suspect),
            STATUS_DEAD => Some(Status::Dead),
            _ => None,
        }
    }
}

/// One member's record, as this node currently sees it: SWIM liveness plus the
/// opaque per-node state the member authors about itself.
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
    /// Monotonic version of `state`, authored by the member itself.
    pub(crate) state_version: u64,
    /// Opaque app-defined per-node state; merged by `state_version`,
    /// independently of liveness.
    pub(crate) state: Vec<u8>,
}

impl Member {
    /// A fresh record at `incarnation`/`status`, with cleared timers and no state.
    pub(crate) fn new(incarnation: u64, status: Status) -> Self {
        Self {
            incarnation,
            status,
            suspect_since: Time::ZERO,
            dead_since: Time::ZERO,
            state_version: 0,
            state: Vec::new(),
        }
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
