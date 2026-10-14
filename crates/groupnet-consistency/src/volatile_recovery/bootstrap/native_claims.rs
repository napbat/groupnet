//! Native Groupnet TTL entries as bounded, advisory bootstrap claims.

use std::num::NonZeroUsize;

use groupnet_core::Status;
use groupnet_core::volatile_bootstrap::{
    BootstrapClaim, BootstrapConfig, BootstrapConfigError, BootstrapMember, BootstrapOperation,
    BootstrapScope, ClaimCodecError, ClaimIdentity, PresenceIdentity, claim_entry_key,
    decode_claim_value, encode_claim_value, encoded_claim_len, presence_entry_key,
};
use groupnet_runtime::{EntryBudget, EntryInspectionLimits, Group, InspectedEntries};
use std::mem::size_of;
use tokio::sync::Mutex;

use super::admission::{AdmissionClass, Admitted, ByteAdmission, Reservation};
use super::ports::{
    ClaimObservationLimits, ClaimSnapshot, ClaimSource, ParticipationSnapshot, TimedClaim,
};
use crate::volatile_recovery::{AdapterError, AdapterErrorKind};

/// A Group inspection or mutation failed; the source stays unreadable.
const UNAVAILABLE: AdapterError = AdapterError::new(AdapterErrorKind::Unavailable);

/// A native entry is malformed, outside policy, or contradicts membership.
const MALFORMED: AdapterError = AdapterError::new(AdapterErrorKind::Malformed);

/// A size computation overflowed or exceeded the caller's metadata bound.
const CAPACITY: AdapterError = AdapterError::new(AdapterErrorKind::Capacity);

/// A bounded snapshot vector could not be allocated.
fn allocation(error: std::collections::TryReserveError) -> AdapterError {
    AdapterError::with_source(AdapterErrorKind::Capacity, error)
}

/// What one [`NativeClaimSource`] publishes and observes: the stable scope,
/// its claim policy, and the native entry bounds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NativeClaimOptions {
    /// Complete application partition the claims are scoped to.
    pub scope: BootstrapScope,
    /// Claim limits; must pass [`BootstrapConfig::validate`].
    pub policy: BootstrapConfig,
    /// Maximum bytes in a scoped claim or presence entry key.
    pub max_key_bytes: usize,
    /// Maximum bytes in one native claim entry value.
    pub max_value_bytes: NonZeroUsize,
}

impl NativeClaimOptions {
    /// Checks the policy, the scope against it, and that both scoped entry
    /// keys fit `max_key_bytes`.
    ///
    /// # Errors
    /// The [`NativeClaimSourceError`] naming the first violated rule.
    pub fn validate(&self) -> Result<(), NativeClaimSourceError> {
        self.entry_keys().map(drop)
    }

    /// The scoped claim and presence entry keys, after every rule passed.
    fn entry_keys(&self) -> Result<(String, String), NativeClaimSourceError> {
        self.policy
            .validate()
            .map_err(NativeClaimSourceError::Policy)?;
        if self
            .scope
            .domain
            .len()
            .checked_add(self.scope.partition.len())
            .is_none_or(|bytes| bytes > self.policy.max_scope_bytes.get())
        {
            return Err(NativeClaimSourceError::ScopeTooLong);
        }
        let key = claim_entry_key(&self.scope, self.max_key_bytes)
            .map_err(NativeClaimSourceError::ClaimKey)?;
        let presence_key = presence_entry_key(&self.scope, self.max_key_bytes)
            .map_err(NativeClaimSourceError::PresenceKey)?;
        Ok((key, presence_key))
    }
}

/// A [`NativeClaimOptions`] rule that was violated.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NativeClaimSourceError {
    /// `policy` failed [`BootstrapConfig::validate`]; the
    /// [`BootstrapConfigError`] is the [`source`](std::error::Error::source).
    Policy(BootstrapConfigError),
    /// `scope` exceeds `policy.max_scope_bytes`.
    ScopeTooLong,
    /// The scoped claim entry key does not fit `max_key_bytes`; the
    /// [`ClaimCodecError`] is the [`source`](std::error::Error::source).
    ClaimKey(ClaimCodecError),
    /// The scoped presence entry key does not fit `max_key_bytes`; the
    /// [`ClaimCodecError`] is the [`source`](std::error::Error::source).
    PresenceKey(ClaimCodecError),
}

impl std::fmt::Display for NativeClaimSourceError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::Policy(_) => "invalid bootstrap policy",
            Self::ScopeTooLong => "scope exceeds policy.max_scope_bytes",
            Self::ClaimKey(_) => "claim entry key unrepresentable",
            Self::PresenceKey(_) => "presence entry key unrepresentable",
        })
    }
}

impl std::error::Error for NativeClaimSourceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Policy(error) => Some(error),
            Self::ClaimKey(error) | Self::PresenceKey(error) => Some(error),
            Self::ScopeTooLong => None,
        }
    }
}

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
/// `GroupEngine`'s adopted entry and the Group's published entry-watch
/// snapshots use separate ownership; this admission does not bound their
/// memory. Deployments must also bound the number of configured bootstrap
/// scopes and claims.
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
    /// Binds one stable scope/policy to a Group and shared byte budget, after
    /// [`NativeClaimOptions::validate`].
    ///
    /// # Errors
    /// Names the invalid policy, oversized scope, or unrepresentable scoped
    /// entry key.
    pub fn new(
        group: Group,
        admission: ByteAdmission,
        options: NativeClaimOptions,
    ) -> Result<Self, NativeClaimSourceError> {
        let (key, presence_key) = options.entry_keys()?;
        let NativeClaimOptions {
            scope,
            policy,
            max_value_bytes,
            ..
        } = options;
        Ok(Self {
            group,
            scope,
            policy,
            key,
            presence_key,
            max_value_bytes: max_value_bytes.get(),
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
            || limits.max_members > self.policy.max_members.get()
            || limits.max_member_bytes == 0
            || limits.max_member_bytes > self.policy.max_member_bytes.get()
            || limits.max_metadata_bytes == 0
        {
            return Err(AdapterErrorKind::InvalidInput.into());
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
            return Err(AdapterErrorKind::InvalidInput.into());
        }
        let inspection_limits = self.inspection_limits(limits)?;
        let raw_charge = limits
            .max_metadata_bytes
            .checked_add(self.key.len())
            .ok_or(CAPACITY)?;
        let raw_budget = admission.reserve(AdmissionClass::Inflight, raw_charge)?;
        let (raw, raw_budget) = self
            .group
            .inspect_scoped_entry(self.key.clone(), inspection_limits, raw_budget)
            .await
            .map_err(|_| UNAVAILABLE)?;
        let raw = raw_budget.hold(raw);
        let converted_bytes = converted_charge(raw.get(), limits.max_metadata_bytes)?;
        let converted_budget = admission.reserve(AdmissionClass::Inflight, converted_bytes)?;
        let snapshot = raw.consume(|raw| self.convert(raw))?;
        Ok(converted_budget.hold(snapshot))
    }

    fn convert(&self, raw: InspectedEntries) -> Result<ClaimSnapshot, AdapterError> {
        let mut members = Vec::new();
        let mut claims = Vec::new();
        members
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        claims
            .try_reserve_exact(raw.entries.len())
            .map_err(allocation)?;
        for entry in raw.entries {
            let eligible = entry.status == Status::Alive;
            if let Some(value) = entry.value {
                let remaining_ms = entry.remaining_ttl_ms.ok_or(MALFORMED)?;
                if remaining_ms == 0 || remaining_ms > self.policy.claim_ttl_ms {
                    return Err(MALFORMED);
                }
                let mut claim = decode_claim_value(
                    &self.scope,
                    self.policy,
                    &entry.node,
                    &value,
                    self.max_value_bytes,
                )
                .map_err(|_| MALFORMED)?;
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
            .checked_add(self.policy.max_member_bytes.get())
            .and_then(|bytes| bytes.checked_add(self.max_value_bytes))
            .ok_or(CAPACITY)?;
        let limits = ClaimObservationLimits {
            max_members: self.policy.max_members.get(),
            max_member_bytes: self.policy.max_member_bytes.get(),
            max_metadata_bytes: size_of::<InspectedEntries>()
                .checked_add(
                    self.policy
                        .max_members
                        .get()
                        .checked_mul(per_member)
                        .ok_or(CAPACITY)?,
                )
                .ok_or(CAPACITY)?,
        };
        let inspection_limits = self.inspection_limits(limits)?;
        let raw_charge = limits
            .max_metadata_bytes
            .checked_add(self.key.len())
            .ok_or(CAPACITY)?;
        let budget = self
            .admission
            .reserve(AdmissionClass::Inflight, raw_charge)?;
        let (observed, budget) = self
            .group
            .inspect_scoped_entry(self.key.clone(), inspection_limits, budget)
            .await
            .map_err(|_| UNAVAILABLE)?;
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
    async fn observe_participation(
        &self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ParticipationSnapshot>, AdapterError> {
        self.inspect_participation(limits, admission).await
    }

    async fn publish_presence(
        &self,
        presence: groupnet_core::volatile_bootstrap::BootstrapPresence,
    ) -> Result<(), AdapterError> {
        self.publish_presence_inner(presence).await
    }

    async fn withdraw_presence(&self, identity: PresenceIdentity) -> Result<(), AdapterError> {
        self.withdraw_presence_inner(identity).await
    }

    async fn publish_claim(&self, claim: BootstrapClaim) -> Result<(), AdapterError> {
        if claim.identity.node != *self.group.local_node() {
            return Err(AdapterErrorKind::InvalidInput.into());
        }
        let mut published = self.published.lock().await;
        let value_len = encoded_claim_len(&self.scope, self.policy, &claim, self.max_value_bytes)
            .map_err(|_| AdapterError::new(AdapterErrorKind::InvalidInput))?;
        let queued_bytes = self.key.len().checked_add(value_len).ok_or(CAPACITY)?;
        let queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)?;
        let retained = self
            .admission
            .reserve(AdmissionClass::Inflight, value_len)?;
        let value = encode_claim_value(&self.scope, self.policy, &claim, self.max_value_bytes)
            .map_err(|_| AdapterError::new(AdapterErrorKind::InvalidInput))?;
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
            return Err(UNAVAILABLE);
        }
        *published = Some((claim.identity, value));
        Ok(())
    }

    async fn withdraw_claim(&self, selected: ClaimIdentity) -> Result<(), AdapterError> {
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
            .ok_or(CAPACITY)?;
        let queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)?;
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
            Err(_) => Err(UNAVAILABLE),
        }
    }

    async fn observe_claims(
        &self,
        _op: BootstrapOperation,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Admitted<ClaimSnapshot>, AdapterError> {
        self.inspect(limits, admission).await
    }

    async fn observe_selected_claim(
        &self,
        _op: BootstrapOperation,
        selected: ClaimIdentity,
        limits: ClaimObservationLimits,
        admission: &ByteAdmission,
    ) -> Result<Option<Admitted<TimedClaim>>, AdapterError> {
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
            .ok_or(CAPACITY)?;
        let charge = admission.reserve(AdmissionClass::Inflight, charge_bytes)?;
        let result = TimedClaim {
            sampled_at: snapshot.get().sampled_at,
            claim: selected_claim.clone(),
        };
        drop(snapshot);
        Ok(Some(charge.hold(result)))
    }
}

fn converted_charge(raw: &InspectedEntries, max_bytes: usize) -> Result<usize, AdapterError> {
    let members = raw
        .entries
        .len()
        .checked_mul(size_of::<BootstrapMember>())
        .ok_or(CAPACITY)?;
    let claims = raw
        .entries
        .len()
        .checked_mul(size_of::<BootstrapClaim>())
        .ok_or(CAPACITY)?;
    let identities = raw.entries.iter().try_fold(0usize, |total, entry| {
        total.checked_add(entry.node.as_str().len())
    });
    let total = size_of::<ClaimSnapshot>()
        .checked_add(members)
        .and_then(|bytes| bytes.checked_add(claims))
        .and_then(|bytes| bytes.checked_add(identities?))
        .ok_or(CAPACITY)?;
    if total == 0 || total > max_bytes {
        return Err(CAPACITY);
    }
    Ok(total)
}
