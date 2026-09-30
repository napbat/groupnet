//! Exact peer-transfer handoff and incarnation-roster checks.

use std::collections::BTreeSet;

use crate::volatile_bootstrap::transfer::{NativeHandoffReceipt, native_cuts_cover};
use crate::{
    NodeId,
    volatile_bootstrap::{BootstrapMemberIdentity, same_membership},
};

use super::RecoveryEngine;
use crate::volatile_recovery::{Peer, RecoveryOperation};

impl RecoveryEngine {
    pub(super) fn valid_handoff(
        &self,
        op: RecoveryOperation,
        handoff: &NativeHandoffReceipt,
    ) -> bool {
        let coverage = &handoff.coverage;
        let reservation = &coverage.barrier.reservation;
        let capture = &reservation.capture;
        handoff.recovery == op
            && coverage.members.len() <= self.config.max_members.saturating_add(1)
            && coverage
                .members
                .iter()
                .all(|identity| identity.valid_bounded(self.config.max_member_bytes))
            && coverage
                .members
                .windows(2)
                .all(|pair| pair[0].node < pair[1].node)
            && coverage
                .members
                .iter()
                .any(|member| member.matches_claim(&reservation.follower))
            && coverage
                .members
                .iter()
                .any(|member| member.matches_claim(&capture.donor))
            && reservation.follower.node == self.me
            && reservation.follower.incarnation == coverage.parent.incarnation
            && reservation.follower.session == coverage.parent.session
            && reservation.follower.attempt == coverage.parent.generation
            && capture.serial != 0
            && capture.recovery_generation != 0
            && !capture.scope.domain.is_empty()
            && !capture.scope.partition.is_empty()
            && reservation.serial != 0
            && handoff.schema != 0
            && handoff.install.session == coverage.parent.session
            && handoff.install.incarnation == coverage.parent.incarnation
            && handoff.install.generation == coverage.parent.generation
            && handoff.install.token > coverage.parent.token
            && coverage.parent.session != 0
            && coverage.parent.generation != 0
            && handoff.attachment.reservation == coverage.barrier.reservation
            && handoff.attachment.operation == coverage.barrier.attach_operation
            && handoff.attachment.operation != 0
            && coverage.barrier.barrier_operation != 0
            && coverage.barrier.cursor.capture == *capture
            && coverage.staged_through == coverage.barrier.cursor
            && coverage.members == coverage.barrier.members
            && handoff.applier_generation != 0
            && native_cuts_cover(&coverage.proven_cuts, &coverage.barrier.covered_cuts)
            && native_cuts_cover(&handoff.continued_cuts, &coverage.proven_cuts)
    }

    /// A fresh cut after the handoff must bind the same membership as the
    /// donor's roster at C ([`same_membership`]), and name exactly the peers
    /// this node's lease and frontier checks covered.
    pub(super) fn valid_peer_roster(
        &self,
        peers: &[Peer],
        identities: &[BootstrapMemberIdentity],
    ) -> bool {
        if identities.len() > self.config.max_members.saturating_add(1)
            || !same_membership(identities, &self.peer_members)
            || identities
                .iter()
                .any(|identity| !identity.valid_bounded(self.config.max_member_bytes))
            || identities
                .windows(2)
                .any(|pair| pair[0].node >= pair[1].node)
        {
            return false;
        }
        let observed: BTreeSet<&NodeId> = peers.iter().map(|peer| &peer.node).collect();
        let expected: BTreeSet<&NodeId> = identities
            .iter()
            .filter(|identity| identity.node != self.me)
            .map(|identity| &identity.node)
            .collect();
        observed == expected
    }
}
