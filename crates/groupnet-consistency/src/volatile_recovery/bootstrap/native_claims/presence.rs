//! Revision-fenced native participation publication for one bootstrap scope.

use std::mem::size_of;

use groupnet_core::volatile_bootstrap::{
    BootstrapPresence, ObservedEntry, PresenceIdentity, decode_presence_value,
    encode_presence_value, encoded_presence_len,
};
use std::time::Duration;

use groupnet_runtime::{
    EntryInspectionLimits, EntryRemoval, EntryRevision, EntryWrite, InspectedPair,
    InspectedPairEntry,
};

use super::{
    AdapterError, AdapterErrorKind, AdmissionClass, Admitted, CAPACITY, MALFORMED,
    NativeClaimSource, UNAVAILABLE, key_bytes,
};

/// A newer or concurrent presence for this member holds the entry.
const FENCED: AdapterError = AdapterError::new(AdapterErrorKind::Fenced);

/// Confirmed CAS rejections re-inspected per publication. Each attempt is two
/// bounded actor round trips inside the caller's own operation deadline.
const MAX_PRESENCE_ATTEMPTS: usize = 4;

impl NativeClaimSource {
    fn pair_limits(&self) -> Result<EntryInspectionLimits, AdapterError> {
        let per_member = self
            .policy
            .max_member_bytes
            .checked_add(size_of::<InspectedPairEntry>())
            .and_then(|bytes| bytes.checked_add(self.max_value_bytes.get().checked_mul(2)?))
            .ok_or(CAPACITY)?;
        let max_response_bytes = self
            .policy
            .max_members
            .checked_mul(per_member)
            .and_then(|bytes| bytes.checked_add(size_of::<InspectedPair>()))
            .ok_or(CAPACITY)?;
        Ok(EntryInspectionLimits {
            max_key_bytes: key_bytes(self.longer_key())?,
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
            .get()
            .checked_add(self.presence_key.len())
            .and_then(|bytes| bytes.checked_add(self.key.len()))
            .ok_or(CAPACITY)?;
        let budget = self.admission.reserve(AdmissionClass::Inflight, charge)?;
        let (observed, budget) = self
            .group
            .inspect_scoped_pair(&self.presence_key, &self.key, limits, budget)
            .await
            .map_err(|_| UNAVAILABLE)?;
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
            return Err(AdapterErrorKind::InvalidInput.into());
        }
        let mut published = self.presence_published.lock().await;
        if published
            .as_ref()
            .is_some_and(|(identity, _)| identity != &presence.identity)
        {
            return Err(FENCED);
        }
        let value_len = encoded_presence_len(
            &self.scope,
            self.policy,
            &presence,
            self.max_value_bytes.get(),
        )
        .map_err(|_| AdapterError::new(AdapterErrorKind::InvalidInput))?;
        let queued_bytes = self
            .presence_key
            .len()
            .checked_add(value_len)
            .ok_or(CAPACITY)?;
        let mut queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)?;
        let retained = self
            .admission
            .reserve(AdmissionClass::Inflight, value_len)?;
        let value = encode_presence_value(
            &self.scope,
            self.policy,
            &presence,
            self.max_value_bytes.get(),
        )
        .map_err(|_| AdapterError::new(AdapterErrorKind::InvalidInput))?;
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
                .ok_or(MALFORMED)?;
            if let Some(existing) = &local.first {
                let existing = decode_presence_value(
                    &self.scope,
                    self.policy,
                    ObservedEntry {
                        author: &local.node,
                        value: existing,
                    },
                    self.max_value_bytes.get(),
                )
                .map_err(|_| MALFORMED)?;
                if existing.identity != presence.identity {
                    // The same boot token is another live session of this
                    // process, or this session already confirmed its own. A
                    // different boot token is a previous process life under
                    // this member name, which peers still gossip back until
                    // its TTL lapses; this life supersedes it at the observed
                    // revision.
                    if existing.identity.boot == presence.identity.boot || published.is_some() {
                        return Err(FENCED);
                    }
                } else if existing.renewal > presence.renewal {
                    return Err(FENCED);
                } else if existing.renewal == presence.renewal {
                    if local.first_remaining_ttl.is_none_or(|ttl| ttl.is_zero()) {
                        return Err(FENCED);
                    }
                    *published = Some((presence.identity, value));
                    return Ok(());
                }
            } else if published.is_some() {
                // A previously confirmed local presence lapsed. Do not silently
                // re-create it with a later renewal after the peer proof broke.
                return Err(FENCED);
            }
            let expected = EntryRevision {
                key: local.first_version,
                member: local.member_state_version,
            };
            drop(observed);
            let result = self
                .group
                .set_entry_if_revision(
                    EntryWrite::new(self.presence_key.clone(), value.get().clone())
                        .with_ttl(Duration::from_millis(self.policy.claim_ttl_ms))
                        .with_limits(self.mutation_limits(&self.presence_key)?),
                    expected,
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
                        return Err(FENCED);
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
                            && entry.first_remaining_ttl.is_some_and(|ttl| !ttl.is_zero())
                    });
                    if !matches {
                        return Err(UNAVAILABLE);
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
            .ok_or(CAPACITY)?;
        let queued = self
            .admission
            .reserve(AdmissionClass::Inflight, queued_bytes)?;
        let result = self
            .group
            .delete_entry_if_value(
                EntryRemoval::new(self.presence_key.clone(), value.get().clone())
                    .with_limits(self.mutation_limits(&self.presence_key)?),
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
                return Err(UNAVAILABLE);
            }
        }
        *published = None;
        Ok(())
    }
}
