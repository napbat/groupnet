//! One actor-state cut with response admission checked before any clone.

use std::mem::size_of;
use std::time::Instant;

use groupnet_core::{GroupEngine, Time};

use crate::group::{EntryInspectionError, EntryInspectionLimits, InspectedEntries, InspectedEntry};

pub(super) fn inspect_scoped_entry(
    engine: &GroupEngine,
    key: &str,
    limits: EntryInspectionLimits,
    now: Time,
    sampled_at: Instant,
) -> Result<InspectedEntries, EntryInspectionError> {
    let mut count = 0usize;
    // The result's Vec header is charged even for a quiet one-node roster.
    let mut charged = size_of::<InspectedEntries>();
    for (node, _, _) in engine.member_statuses_since() {
        count = count
            .checked_add(1)
            .ok_or(EntryInspectionError::TooManyMembers)?;
        if count > limits.max_members {
            return Err(EntryInspectionError::TooManyMembers);
        }
        let node_bytes = node.as_str().len();
        if node_bytes > limits.max_member_bytes {
            return Err(EntryInspectionError::IdentityTooLong);
        }
        let value = visible_value(engine, node, key, now);
        let value_bytes = value.map_or(0, <[u8]>::len);
        if value_bytes > limits.max_value_bytes {
            return Err(EntryInspectionError::ValueTooLong);
        }
        charged = charged
            .checked_add(size_of::<InspectedEntry>())
            .and_then(|bytes| bytes.checked_add(node_bytes))
            .and_then(|bytes| bytes.checked_add(value_bytes))
            .ok_or(EntryInspectionError::Capacity)?;
        if charged > limits.max_response_bytes {
            return Err(EntryInspectionError::Capacity);
        }
    }

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| EntryInspectionError::Capacity)?;
    for (node, status, _) in engine.member_statuses_since() {
        let value = visible_value(engine, node, key, now).map(<[u8]>::to_vec);
        let remaining_ttl_ms = value.as_ref().and_then(|_| {
            engine
                .node_entry_expires_at(node, key)
                .map(|expiry| expiry.0.saturating_sub(now.0))
        });
        entries.push(InspectedEntry {
            node: node.clone(),
            status,
            value,
            remaining_ttl_ms,
        });
    }
    Ok(InspectedEntries {
        sampled_at,
        entries,
    })
}

fn visible_value<'a>(
    engine: &'a GroupEngine,
    node: &groupnet_core::NodeId,
    key: &str,
    now: Time,
) -> Option<&'a [u8]> {
    let value = engine.node_entry(node, key)?;
    if engine
        .node_entry_expires_at(node, key)
        .is_some_and(|expiry| expiry <= now)
    {
        None
    } else {
        Some(value)
    }
}
