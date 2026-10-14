//! One admitted native actor cut for membership, presence, and builder hints.

use std::mem::size_of;

use groupnet_core::Status;
use groupnet_core::volatile_bootstrap::{
    BootstrapMemberIdentity, decode_claim_value, decode_presence_value,
};
use groupnet_runtime::InspectedPair;

use super::{
    AdapterError, AdapterErrorKind, AdmissionClass, Admitted, BootstrapClaim, BootstrapMember,
    ByteAdmission, CAPACITY, ClaimObservationLimits, MALFORMED, NativeClaimSource, UNAVAILABLE,
    allocation,
};
use crate::volatile_recovery::bootstrap::ports::{ParticipationSnapshot, TimedParticipant};

impl NativeClaimSource {
    pub(super) async fn inspect_participation(
        &self,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ParticipationSnapshot>, AdapterError> {
        if !self.admission.same_pool(admission) {
            return Err(AdapterErrorKind::InvalidInput.into());
        }
        let mut inspection = self.inspection_limits(limits)?;
        inspection.max_key_bytes = self.key.len().max(self.presence_key.len());
        let raw_charge = limits
            .max_metadata_bytes
            .checked_add(self.key.len())
            .and_then(|bytes| bytes.checked_add(self.presence_key.len()))
            .ok_or(CAPACITY)?;
        let raw_budget = admission.reserve(AdmissionClass::Inflight, raw_charge)?;
        let (raw, raw_budget) = self
            .group
            .inspect_scoped_pair(&self.presence_key, &self.key, inspection, raw_budget)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let raw = raw_budget.hold(raw);
        let decoded_bytes = decoded_charge(raw.get(), limits.max_metadata_bytes)?;
        let decoded_budget = admission.reserve(AdmissionClass::Inflight, decoded_bytes)?;
        let converted = raw.consume(|raw| self.convert_participation(raw))?;
        Ok(decoded_budget.hold(converted))
    }

    fn convert_participation(
        &self,
        raw: InspectedPair,
    ) -> Result<ParticipationSnapshot, AdapterError> {
        let mut members = Vec::new();
        let mut roster = Vec::new();
        let mut participants = Vec::new();
        let mut claims = Vec::new();
        members
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        roster
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        participants
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        claims
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        for entry in raw.entries {
            let eligible = entry.status == Status::Alive;
            let node = entry.node;
            let mut exact = BootstrapMemberIdentity {
                node: node.clone(),
                presence: None,
                member_incarnation: entry.member_incarnation,
                status: entry.status,
            };
            if let Some(value) = entry.first {
                let remaining_ms = entry.first_remaining_ttl_ms.ok_or(MALFORMED)?;
                if remaining_ms == 0 || remaining_ms > self.policy.claim_ttl_ms {
                    return Err(MALFORMED);
                }
                let presence = decode_presence_value(
                    &self.scope,
                    self.policy,
                    &node,
                    &value,
                    self.max_value_bytes,
                )
                .map_err(|_| MALFORMED)?;
                exact.presence = Some(presence.identity);
                participants.push(TimedParticipant {
                    member: exact.clone(),
                    renewal: presence.renewal,
                    remaining_ms,
                });
            } else if eligible {
                return Err(MALFORMED);
            }
            if let Some(value) = entry.second {
                let remaining_ms = entry.second_remaining_ttl_ms.ok_or(MALFORMED)?;
                if remaining_ms == 0 || remaining_ms > self.policy.claim_ttl_ms {
                    return Err(MALFORMED);
                }
                let mut claim = decode_claim_value(
                    &self.scope,
                    self.policy,
                    &node,
                    &value,
                    self.max_value_bytes,
                )
                .map_err(|_| MALFORMED)?;
                claim.remaining_ms = remaining_ms;
                if eligible {
                    claims.push(claim);
                }
            }
            members.push(BootstrapMember { node, eligible });
            roster.push(exact);
        }
        members.sort_by(|a, b| a.node.cmp(&b.node));
        roster.sort_by(|a, b| a.node.cmp(&b.node));
        Ok(ParticipationSnapshot {
            sampled_at: raw.sampled_at,
            members,
            roster,
            participants,
            claims,
        })
    }
}

fn decoded_charge(raw: &InspectedPair, max_bytes: usize) -> Result<usize, AdapterError> {
    let per_member = size_of::<BootstrapMember>()
        .checked_add(size_of::<BootstrapMemberIdentity>())
        .and_then(|bytes| bytes.checked_add(size_of::<TimedParticipant>()))
        .and_then(|bytes| bytes.checked_add(size_of::<BootstrapClaim>()))
        .ok_or(CAPACITY)?;
    let total = size_of::<ParticipationSnapshot>()
        .checked_add(raw.entries.len().checked_mul(per_member).ok_or(CAPACITY)?)
        .and_then(|bytes| {
            raw.entries.iter().try_fold(bytes, |total, entry| {
                total.checked_add(entry.node.as_str().len().checked_mul(4)?)
            })
        })
        .ok_or(CAPACITY)?;
    if total == 0 || total > max_bytes {
        return Err(CAPACITY);
    }
    Ok(total)
}
