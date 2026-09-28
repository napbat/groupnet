//! One actor-state cut with response admission checked before any clone.

use std::mem::size_of;
use std::time::Instant;

use groupnet_core::{GroupEngine, Time};

use crate::group::{
    EntryInspectionError, EntryInspectionLimits, InspectedEntries, InspectedEntry, InspectedPair,
    InspectedPairEntry,
};

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
    let mut retained = response_vec_charge::<InspectedEntry>(
        entries.capacity(),
        limits,
        size_of::<InspectedEntries>(),
    )?;
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
        charge_entry(
            entries.last().ok_or(EntryInspectionError::Capacity)?,
            &mut retained,
            limits,
        )?;
    }
    Ok(InspectedEntries {
        sampled_at,
        entries,
    })
}

pub(super) fn inspect_scoped_pair(
    engine: &GroupEngine,
    first: &str,
    second: &str,
    limits: EntryInspectionLimits,
    now: Time,
    sampled_at: Instant,
) -> Result<InspectedPair, EntryInspectionError> {
    let mut count = 0usize;
    let mut charged = size_of::<InspectedPair>();
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
        let first_bytes = visible_value(engine, node, first, now).map_or(0, <[u8]>::len);
        let second_bytes = visible_value(engine, node, second, now).map_or(0, <[u8]>::len);
        if first_bytes > limits.max_value_bytes || second_bytes > limits.max_value_bytes {
            return Err(EntryInspectionError::ValueTooLong);
        }
        charged = charged
            .checked_add(size_of::<InspectedPairEntry>())
            .and_then(|bytes| bytes.checked_add(node_bytes))
            .and_then(|bytes| bytes.checked_add(first_bytes))
            .and_then(|bytes| bytes.checked_add(second_bytes))
            .ok_or(EntryInspectionError::Capacity)?;
        if charged > limits.max_response_bytes {
            return Err(EntryInspectionError::Capacity);
        }
    }

    let mut entries = Vec::new();
    entries
        .try_reserve_exact(count)
        .map_err(|_| EntryInspectionError::Capacity)?;
    let mut retained = response_vec_charge::<InspectedPairEntry>(
        entries.capacity(),
        limits,
        size_of::<InspectedPair>(),
    )?;
    for (node, status, _) in engine.member_statuses_since() {
        let first_value = visible_value(engine, node, first, now).map(<[u8]>::to_vec);
        let second_value = visible_value(engine, node, second, now).map(<[u8]>::to_vec);
        let first_remaining_ttl_ms = first_value.as_ref().and_then(|_| {
            engine
                .node_entry_expires_at(node, first)
                .map(|expiry| expiry.0.saturating_sub(now.0))
        });
        let second_remaining_ttl_ms = second_value.as_ref().and_then(|_| {
            engine
                .node_entry_expires_at(node, second)
                .map(|expiry| expiry.0.saturating_sub(now.0))
        });
        entries.push(InspectedPairEntry {
            node: node.clone(),
            status,
            member_incarnation: engine
                .member_incarnation(node)
                .ok_or(EntryInspectionError::Unavailable)?,
            member_state_version: engine
                .member_state_version(node)
                .ok_or(EntryInspectionError::Unavailable)?,
            first: first_value,
            first_version: engine.node_entry_version(node, first),
            first_remaining_ttl_ms,
            second: second_value,
            second_version: engine.node_entry_version(node, second),
            second_remaining_ttl_ms,
        });
        charge_pair_entry(
            entries.last().ok_or(EntryInspectionError::Capacity)?,
            &mut retained,
            limits,
        )?;
    }
    Ok(InspectedPair {
        sampled_at,
        entries,
    })
}

fn response_vec_charge<T>(
    capacity: usize,
    limits: EntryInspectionLimits,
    header: usize,
) -> Result<usize, EntryInspectionError> {
    if capacity > limits.max_members {
        return Err(EntryInspectionError::Capacity);
    }
    let charged = capacity
        .checked_mul(size_of::<T>())
        .and_then(|bytes| header.checked_add(bytes))
        .ok_or(EntryInspectionError::Capacity)?;
    if charged > limits.max_response_bytes {
        return Err(EntryInspectionError::Capacity);
    }
    Ok(charged)
}

fn charge_entry(
    entry: &InspectedEntry,
    retained: &mut usize,
    limits: EntryInspectionLimits,
) -> Result<(), EntryInspectionError> {
    let capacity = entry.value.as_ref().map_or(0, Vec::capacity);
    if capacity > limits.max_value_bytes {
        return Err(EntryInspectionError::Capacity);
    }
    *retained = retained
        .checked_add(entry.node.as_str().len())
        .and_then(|bytes| bytes.checked_add(capacity))
        .ok_or(EntryInspectionError::Capacity)?;
    if *retained > limits.max_response_bytes {
        return Err(EntryInspectionError::Capacity);
    }
    Ok(())
}

fn charge_pair_entry(
    entry: &InspectedPairEntry,
    retained: &mut usize,
    limits: EntryInspectionLimits,
) -> Result<(), EntryInspectionError> {
    let first = entry.first.as_ref().map_or(0, Vec::capacity);
    let second = entry.second.as_ref().map_or(0, Vec::capacity);
    if first > limits.max_value_bytes || second > limits.max_value_bytes {
        return Err(EntryInspectionError::Capacity);
    }
    *retained = retained
        .checked_add(entry.node.as_str().len())
        .and_then(|bytes| bytes.checked_add(first))
        .and_then(|bytes| bytes.checked_add(second))
        .ok_or(EntryInspectionError::Capacity)?;
    if *retained > limits.max_response_bytes {
        return Err(EntryInspectionError::Capacity);
    }
    Ok(())
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
