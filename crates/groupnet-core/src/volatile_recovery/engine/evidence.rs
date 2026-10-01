//! What one turn's head observations prove: a writer's advertised head may
//! only progress within its life, unless the observer delivered that life
//! through its seal. A delivered seal accounts for every write of the life, so
//! the head leaving it — or the writer leaving the roster — loses nothing.

use crate::NodeId;
use crate::volatile_recovery::{Mark, Peer, RecoveryError};

use super::RecoveryEngine;

impl RecoveryEngine {
    /// Remembers each peer's latest head and delivered seal for this turn,
    /// refusing a head that disappeared, left its life or moved backwards
    /// within it unless the observer [`crossed`] that life's seal.
    pub(super) fn record_known_heads(&mut self, peers: &[Peer]) -> Result<(), RecoveryError> {
        for peer in peers {
            match (self.known_heads.get(&peer.node).copied(), peer.head) {
                (Some(before), None) if crossed(before, peer) => {
                    self.known_heads.remove(&peer.node);
                }
                (Some(_), None) => return Err(RecoveryError::InvalidEvidence),
                (Some(before), Some(now)) if regressed(before, now) && !crossed(before, peer) => {
                    return Err(RecoveryError::InvalidEvidence);
                }
                (_, Some(now)) => {
                    self.known_heads.insert(peer.node.clone(), now);
                }
                (None, None) => {}
            }
            match peer.sealed {
                Some(sealed) => {
                    self.seals.insert(peer.node.clone(), sealed);
                }
                None => {
                    self.seals.remove(&peer.node);
                }
            }
        }
        if self.known_heads.len() > self.config.max_members
            || self.seals.len() > self.config.max_members
        {
            return Err(RecoveryError::Capacity);
        }
        Ok(())
    }

    /// Whether `node`, gone from the roster, left nothing this turn has to
    /// wait for: its last observation carried a delivered seal that ends the
    /// life of every head this turn knows of it.
    pub(super) fn departed_sealed(&self, node: &NodeId) -> bool {
        self.seals.get(node).is_some_and(|sealed| {
            self.known_heads
                .get(node)
                .is_none_or(|known| ends_after(*sealed, *known))
        })
    }
}

/// `sealed` ends the life `head` belongs to, after it.
fn ends_after(sealed: Mark, head: Mark) -> bool {
    sealed.epoch == head.epoch && sealed.sequence > head.sequence
}

/// A head that left its life or moved backwards within it: evidence the
/// engine cannot account for unless the observer [`crossed`] a delivered seal.
pub(super) fn regressed(before: Mark, now: Mark) -> bool {
    now.epoch != before.epoch || now.sequence < before.sequence
}

/// The observer delivered the life `before` belongs to through its seal —
/// which lies after `before`, so no write of that life is unaccounted for —
/// and the head `peer` advertises now is gone or in a later life. Either the
/// observer renewed into the life the head names, and the frontier barrier on
/// it covers the rest; or it still stands at the seal, and the head is gone
/// (the member reaped, relearned with no state, or its next life not written
/// yet) or names a later life the observer will cross by renewal or gap
/// before its frontier reaches that head.
pub(super) fn crossed(before: Mark, peer: &Peer) -> bool {
    peer.renewal.is_some_and(|renewal| {
        ends_after(renewal.sealed, before) && peer.head.is_none_or(|now| now.epoch == renewal.epoch)
    }) || peer.sealed.is_some_and(|sealed| {
        ends_after(sealed, before) && peer.head.is_none_or(|now| now.epoch > sealed.epoch)
    })
}
