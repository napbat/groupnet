//! Revision-fenced native participation publication for one bootstrap scope.

use std::mem::size_of;

use groupnet_core::volatile_bootstrap::{
    BootstrapPresence, PresenceIdentity, decode_presence_value, encode_presence_value,
    encoded_presence_len,
};
use groupnet_runtime::{
    EntryInspectionLimits, EntryMutationLimits, EntryRevision, InspectedPair, InspectedPairEntry,
};

use super::{AdapterError, AdmissionClass, Admitted, NativeClaimSource};

/// Confirmed CAS rejections re-inspected per publication. Each attempt is two
/// bounded actor round trips inside the caller's own operation deadline.
const MAX_PRESENCE_ATTEMPTS: usize = 4;

impl NativeClaimSource {
    fn pair_limits(&self) -> Result<EntryInspectionLimits, AdapterError> {
        let per_member = size_of::<InspectedPairEntry>()
            .checked_add(self.policy.max_member_bytes)
            .and_then(|bytes| bytes.checked_add(self.max_value_bytes.checked_mul(2)?))
            .ok_or(AdapterError)?;
        let max_response_bytes = size_of::<InspectedPair>()
            .checked_add(
                self.policy
                    .max_members
                    .checked_mul(per_member)
                    .ok_or(AdapterError)?,
            )
            .ok_or(AdapterError)?;
        Ok(EntryInspectionLimits {
            max_key_bytes: self.key.len().max(self.presence_key.len()),
            max_members: self.policy.max_members,
            max_member_bytes: self.policy.max_member_bytes,
            max_value_bytes: self.max_value_bytes,
            max_response_bytes,
        })
    }

    async fn inspect_local_presence(&self) -> Result<Admitted<InspectedPair>, AdapterError> {
        let limits = self.pair_limits()?;
        let charge = limits
            .max_response_bytes
            .checked_add(self.presence_key.len())
            .and_then(|bytes| bytes.checked_add(self.key.len()))
            .ok_or(AdapterError)?;
        let budget = self
            .admission
            .reserve(AdmissionClass::Inflight, charge)
            .map_err(|_| AdapterError)?;
        let (observed, budget) = self
            .group
            .inspect_scoped_pair(&self.presence_key, &self.key, limits, budget)
            .await
            .map_err(|_| AdapterError)?;
        Ok(budget.hold(observed))
    }

    #[expect(
        clippy::too_many_lines,
        reason = "one revision-fenced native publication and its exact ambiguous readback"
    )]
    pub(super) async fn publish_presence_inner(
        &self,
        presence: BootstrapPresence,
    ) -> Result<(), AdapterError> {
        if &presence.identity.node != self.group.local_node()
            || presence.remaining_ms != self.policy.claim_ttl_ms
        {
            return Err(AdapterError);
        }
        let mut published = self.presence_published.lock().await;
        if published
            .as_ref()
            .is_some_and(|(identity, _)| identity != &presence.identity)
        {
            return Err(AdapterError);
        }
        let value_len =
            encoded_presence_len(&self.scope, self.policy, &presence, self.max_value_bytes)
                .map_err(|_| AdapterError)?;
        let queued_bytes = self
            .presence_key
            .len()
            .checked_add(value_len)
            .ok_or(AdapterError)?;
        let mut queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)
            .map_err(|_| AdapterError)?;
        let retained = self
            .admission
            .reserve(AdmissionClass::Inflight, value_len)
            .map_err(|_| AdapterError)?;
        let value =
            encode_presence_value(&self.scope, self.policy, &presence, self.max_value_bytes)
                .map_err(|_| AdapterError)?;
        let value = retained.hold(value);
        let mut attempts = 0;
        loop {
            attempts += 1;
            let observed = self.inspect_local_presence().await?;
            let local = observed
                .get()
                .entries
                .iter()
                .find(|entry| &entry.node == self.group.local_node())
                .ok_or(AdapterError)?;
            if let Some(existing) = &local.first {
                let existing = decode_presence_value(
                    &self.scope,
                    self.policy,
                    &local.node,
                    existing,
                    self.max_value_bytes,
                )
                .map_err(|_| AdapterError)?;
                if existing.identity != presence.identity || existing.renewal > presence.renewal {
                    return Err(AdapterError);
                }
                if existing.renewal == presence.renewal {
                    if local.first_remaining_ttl_ms.is_none_or(|ttl| ttl == 0) {
                        return Err(AdapterError);
                    }
                    *published = Some((presence.identity, value));
                    return Ok(());
                }
            } else if published.is_some() {
                // A previously confirmed local presence lapsed. Do not silently
                // re-create it with a later renewal after the peer proof broke.
                return Err(AdapterError);
            }
            let expected = EntryRevision {
                key: local.first_version,
                member: local.member_state_version,
            };
            drop(observed);
            let result = self
                .group
                .set_entry_if_revision(
                    self.presence_key.clone(),
                    value.get().clone(),
                    Some(self.policy.claim_ttl_ms),
                    expected,
                    EntryMutationLimits {
                        max_key_bytes: self.presence_key.len(),
                        max_value_bytes: self.max_value_bytes,
                    },
                    queued,
                )
                .await;
            match result {
                Ok((true, budget)) => {
                    drop(budget);
                    break;
                }
                Ok((false, budget)) => {
                    // A confirmed rejection made no mutation. A first create
                    // binds the whole member revision, so an unrelated local
                    // write between the cut and the actor rejects it. Take a
                    // fresh cut and re-run every check above; a retained key
                    // revision or a newer presence still refuses.
                    if attempts >= MAX_PRESENCE_ATTEMPTS {
                        return Err(AdapterError);
                    }
                    queued = budget;
                }
                Err(_) => {
                    // Lost replies have an unknown outcome. Exact native readback
                    // can confirm this body's publication, never a blind retry.
                    let readback = self.inspect_local_presence().await?;
                    let matches = readback.get().entries.iter().any(|entry| {
                        &entry.node == self.group.local_node()
                            && entry.first.as_deref() == Some(value.get().as_slice())
                            && entry.first_remaining_ttl_ms.is_some_and(|ttl| ttl > 0)
                    });
                    if !matches {
                        return Err(AdapterError);
                    }
                    break;
                }
            }
        }
        *published = Some((presence.identity, value));
        Ok(())
    }

    pub(super) async fn withdraw_presence_inner(
        &self,
        identity: PresenceIdentity,
    ) -> Result<(), AdapterError> {
        let mut published = self.presence_published.lock().await;
        let Some((current, value)) = published.as_ref() else {
            return Ok(());
        };
        if current != &identity {
            return Ok(());
        }
        let queued_bytes = self
            .presence_key
            .len()
            .checked_add(value.get().len())
            .ok_or(AdapterError)?;
        let queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)
            .map_err(|_| AdapterError)?;
        let result = self
            .group
            .delete_entry_if_value(
                self.presence_key.clone(),
                value.get().clone(),
                self.presence_key.len(),
                self.max_value_bytes,
                queued,
            )
            .await;
        if let Ok((_, budget)) = result {
            drop(budget);
        } else {
            let readback = self.inspect_local_presence().await?;
            if readback.get().entries.iter().any(|entry| {
                &entry.node == self.group.local_node()
                    && entry.first.as_deref() == Some(value.get().as_slice())
            }) {
                return Err(AdapterError);
            }
        }
        *published = None;
        Ok(())
    }
}
