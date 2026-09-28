//! Native Groupnet TTL entries as bounded, advisory bootstrap claims.

use groupnet_core::Status;
use groupnet_core::volatile_bootstrap::{
    BootstrapClaim, BootstrapConfig, BootstrapMember, BootstrapOperation, BootstrapScope,
    ClaimIdentity, PresenceIdentity, claim_entry_key, decode_claim_value, encode_claim_value,
    encoded_claim_len, presence_entry_key,
};
use groupnet_runtime::{EntryBudget, EntryInspectionLimits, Group, InspectedEntries};
use std::mem::size_of;
use tokio::sync::Mutex;

use super::admission::{AdmissionClass, Admitted, ByteAdmission, Reservation};
use super::ports::{
    ClaimObservationLimits, ClaimSnapshot, ClaimSource, ParticipationSnapshot, TimedClaim,
};
use crate::volatile_recovery::{AdapterError, BoxRecoveryFuture};

mod participation;
mod presence;

impl EntryBudget for Reservation {
    fn bytes(&self) -> usize {
        Reservation::bytes(self)
    }
}

/// Optional Groupnet-backed source of finite, non-authoritative claims.
///
/// The owned admission budget is shared across scopes. A source instance
/// serializes its own local publication and exact withdrawal, while all
/// roster/entry observations are one bounded Group actor cut. Queue copies,
/// responses, and the source-retained exact value are charged here. The
/// `GroupEngine`'s adopted entry and legacy watcher copies use separate
/// ownership; this admission does not bound their memory. Deployments must
/// also bound the number of configured bootstrap scopes and claims.
#[derive(Debug)]
pub struct NativeClaimSource {
    group: Group,
    scope: BootstrapScope,
    policy: BootstrapConfig,
    key: String,
    presence_key: String,
    max_value_bytes: usize,
    admission: ByteAdmission,
    published: Mutex<Option<(ClaimIdentity, Admitted<Vec<u8>>)>>,
    presence_published: Mutex<Option<(PresenceIdentity, Admitted<Vec<u8>>)>>,
}

impl NativeClaimSource {
    /// Binds one stable scope/policy to a Group and shared byte budget.
    ///
    /// # Errors
    /// Rejects an invalid policy or an unrepresentable scoped entry key.
    pub fn new(
        group: Group,
        scope: BootstrapScope,
        policy: BootstrapConfig,
        max_key_bytes: usize,
        max_value_bytes: usize,
        admission: ByteAdmission,
    ) -> Result<Self, AdapterError> {
        policy.validate().map_err(|_| AdapterError)?;
        if max_value_bytes == 0 {
            return Err(AdapterError);
        }
        if scope
            .domain
            .len()
            .checked_add(scope.partition.len())
            .ok_or(AdapterError)?
            > policy.max_scope_bytes
        {
            return Err(AdapterError);
        }
        let key = claim_entry_key(&scope, max_key_bytes).map_err(|_| AdapterError)?;
        let presence_key = presence_entry_key(&scope, max_key_bytes).map_err(|_| AdapterError)?;
        Ok(Self {
            group,
            scope,
            policy,
            key,
            presence_key,
            max_value_bytes,
            admission,
            published: Mutex::new(None),
            presence_published: Mutex::new(None),
        })
    }

    fn inspection_limits(
        &self,
        limits: ClaimObservationLimits,
    ) -> Result<EntryInspectionLimits, AdapterError> {
        if limits.max_members == 0
            || limits.max_members > self.policy.max_members
            || limits.max_member_bytes == 0
            || limits.max_member_bytes > self.policy.max_member_bytes
            || limits.max_metadata_bytes == 0
        {
            return Err(AdapterError);
        }
        Ok(EntryInspectionLimits {
            max_key_bytes: self.key.len(),
            max_members: limits.max_members,
            max_member_bytes: limits.max_member_bytes,
            max_value_bytes: self.max_value_bytes,
            max_response_bytes: limits.max_metadata_bytes,
        })
    }

    async fn inspect(
        &self,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ClaimSnapshot>, AdapterError> {
        if !self.admission.same_pool(admission) {
            return Err(AdapterError);
        }
        let inspection_limits = self.inspection_limits(limits)?;
        let raw_charge = limits
            .max_metadata_bytes
            .checked_add(self.key.len())
            .ok_or(AdapterError)?;
        let raw_budget = admission
            .reserve(AdmissionClass::Inflight, raw_charge)
            .map_err(|_| AdapterError)?;
        let (raw, raw_budget) = self
            .group
            .inspect_scoped_entry(self.key.clone(), inspection_limits, raw_budget)
            .await
            .map_err(|_| AdapterError)?;
        let raw = raw_budget.hold(raw);
        let converted_bytes = converted_charge(raw.get(), limits.max_metadata_bytes)?;
        let converted_budget = admission
            .reserve(AdmissionClass::Inflight, converted_bytes)
            .map_err(|_| AdapterError)?;
        let snapshot = raw.consume(|raw| self.convert(raw))?;
        Ok(converted_budget.hold(snapshot))
    }

    fn convert(&self, raw: InspectedEntries) -> Result<ClaimSnapshot, AdapterError> {
        let mut members = Vec::new();
        let mut claims = Vec::new();
        members
            .try_reserve_exact(raw.entries.len())
            .map_err(|_| AdapterError)?;
        claims
            .try_reserve_exact(raw.entries.len())
            .map_err(|_| AdapterError)?;
        for entry in raw.entries {
            let eligible = entry.status == Status::Alive;
            if let Some(value) = entry.value {
                let remaining_ms = entry.remaining_ttl_ms.ok_or(AdapterError)?;
                if remaining_ms == 0 || remaining_ms > self.policy.claim_ttl_ms {
                    return Err(AdapterError);
                }
                let mut claim = decode_claim_value(
                    &self.scope,
                    self.policy,
                    &entry.node,
                    &value,
                    self.max_value_bytes,
                )
                .map_err(|_| AdapterError)?;
                claim.remaining_ms = remaining_ms;
                if eligible {
                    claims.push(claim);
                }
            }
            members.push(BootstrapMember {
                node: entry.node,
                eligible,
            });
        }
        Ok(ClaimSnapshot {
            sampled_at: raw.sampled_at,
            members,
            claims,
        })
    }

    async fn exact_local_readback(&self, expected: &[u8]) -> Result<bool, AdapterError> {
        let per_member = size_of::<groupnet_runtime::InspectedEntry>()
            .checked_add(self.policy.max_member_bytes)
            .and_then(|bytes| bytes.checked_add(self.max_value_bytes))
            .ok_or(AdapterError)?;
        let limits = ClaimObservationLimits {
            max_members: self.policy.max_members,
            max_member_bytes: self.policy.max_member_bytes,
            max_metadata_bytes: size_of::<InspectedEntries>()
                .checked_add(
                    self.policy
                        .max_members
                        .checked_mul(per_member)
                        .ok_or(AdapterError)?,
                )
                .ok_or(AdapterError)?,
        };
        let inspection_limits = self.inspection_limits(limits)?;
        let raw_charge = limits
            .max_metadata_bytes
            .checked_add(self.key.len())
            .ok_or(AdapterError)?;
        let budget = self
            .admission
            .reserve(AdmissionClass::Inflight, raw_charge)
            .map_err(|_| AdapterError)?;
        let (observed, budget) = self
            .group
            .inspect_scoped_entry(self.key.clone(), inspection_limits, budget)
            .await
            .map_err(|_| AdapterError)?;
        let matching = observed.entries.iter().any(|entry| {
            &entry.node == self.group.local_node()
                && entry.value.as_deref() == Some(expected)
                && entry.remaining_ttl_ms.is_some_and(|ttl| ttl > 0)
        });
        drop(observed);
        drop(budget);
        Ok(matching)
    }
}

impl ClaimSource for NativeClaimSource {
    fn observe_participation<'a>(
        &'a self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<Admitted<ParticipationSnapshot>, AdapterError>> {
        Box::pin(async move { self.inspect_participation(limits, admission).await })
    }

    fn publish_presence(
        &self,
        presence: groupnet_core::volatile_bootstrap::BootstrapPresence,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move { self.publish_presence_inner(presence).await })
    }

    fn withdraw_presence(
        &self,
        identity: PresenceIdentity,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move { self.withdraw_presence_inner(identity).await })
    }

    fn publish_claim(
        &self,
        claim: BootstrapClaim,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            if claim.identity.node != *self.group.local_node() {
                return Err(AdapterError);
            }
            let mut published = self.published.lock().await;
            let value_len =
                encoded_claim_len(&self.scope, self.policy, &claim, self.max_value_bytes)
                    .map_err(|_| AdapterError)?;
            let queued_bytes = self.key.len().checked_add(value_len).ok_or(AdapterError)?;
            let queued = self
                .admission
                .reserve(AdmissionClass::Inflight, queued_bytes)
                .map_err(|_| AdapterError)?;
            let retained = self
                .admission
                .reserve(AdmissionClass::Inflight, value_len)
                .map_err(|_| AdapterError)?;
            let value = encode_claim_value(&self.scope, self.policy, &claim, self.max_value_bytes)
                .map_err(|_| AdapterError)?;
            let value = retained.hold(value);
            let confirmed = self
                .group
                .set_entry_confirmed(
                    self.key.clone(),
                    value.get().clone(),
                    Some(self.policy.claim_ttl_ms),
                    self.key.len(),
                    self.max_value_bytes,
                    queued,
                )
                .await;
            if confirmed.is_err() && !self.exact_local_readback(value.get()).await? {
                return Err(AdapterError);
            }
            *published = Some((claim.identity, value));
            Ok(())
        })
    }

    fn withdraw_claim(
        &self,
        selected: ClaimIdentity,
    ) -> BoxRecoveryFuture<'_, Result<(), AdapterError>> {
        Box::pin(async move {
            let mut published = self.published.lock().await;
            let Some((identity, value)) = published.as_ref() else {
                return Ok(());
            };
            if *identity != selected {
                return Ok(());
            }
            let queued_bytes = self
                .key
                .len()
                .checked_add(value.get().len())
                .ok_or(AdapterError)?;
            let queued = self
                .admission
                .reserve(AdmissionClass::Inflight, queued_bytes)
                .map_err(|_| AdapterError)?;
            let expected = value.get().clone();
            let result = self
                .group
                .delete_entry_if_value(
                    self.key.clone(),
                    expected,
                    self.key.len(),
                    self.max_value_bytes,
                    queued,
                )
                .await;
            match result {
                Ok(_) => {
                    *published = None;
                    Ok(())
                }
                Err(_) if !self.exact_local_readback(value.get()).await? => {
                    *published = None;
                    Ok(())
                }
                Err(_) => Err(AdapterError),
            }
        })
    }

    fn observe_claims<'a>(
        &'a self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<Admitted<ClaimSnapshot>, AdapterError>> {
        Box::pin(async move { self.inspect(limits, admission).await })
    }

    fn observe_selected_claim<'a>(
        &'a self,
        _op: BootstrapOperation,
        selected: ClaimIdentity,
        limits: ClaimObservationLimits,
        admission: &'a ByteAdmission,
    ) -> BoxRecoveryFuture<'a, Result<Option<Admitted<TimedClaim>>, AdapterError>> {
        Box::pin(async move {
            let snapshot = self.inspect(limits, admission).await?;
            let selected_claim = snapshot
                .get()
                .claims
                .iter()
                .find(|claim| claim.identity == selected);
            let Some(selected_claim) = selected_claim else {
                return Ok(None);
            };
            let charge_bytes = size_of::<TimedClaim>()
                .checked_add(selected.node.as_str().len())
                .ok_or(AdapterError)?;
            let charge = admission
                .reserve(AdmissionClass::Inflight, charge_bytes)
                .map_err(|_| AdapterError)?;
            let result = TimedClaim {
                sampled_at: snapshot.get().sampled_at,
                claim: selected_claim.clone(),
            };
            drop(snapshot);
            Ok(Some(charge.hold(result)))
        })
    }
}

fn converted_charge(raw: &InspectedEntries, max_bytes: usize) -> Result<usize, AdapterError> {
    let members = raw
        .entries
        .len()
        .checked_mul(size_of::<BootstrapMember>())
        .ok_or(AdapterError)?;
    let claims = raw
        .entries
        .len()
        .checked_mul(size_of::<BootstrapClaim>())
        .ok_or(AdapterError)?;
    let identities = raw.entries.iter().try_fold(0usize, |total, entry| {
        total.checked_add(entry.node.as_str().len())
    });
    let total = size_of::<ClaimSnapshot>()
        .checked_add(members)
        .and_then(|bytes| bytes.checked_add(claims))
        .and_then(|bytes| bytes.checked_add(identities?))
        .ok_or(AdapterError)?;
    if total == 0 || total > max_bytes {
        return Err(AdapterError);
    }
    Ok(total)
}
