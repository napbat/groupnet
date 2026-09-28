//! Bounded, versioned native-entry encoding for advisory bootstrap claims.

use std::fmt::Write;

use super::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapPresence, BootstrapScope, ClaimIdentity,
    ClaimPhase, PresenceIdentity,
};
use crate::NodeId;

const MAGIC: &[u8; 4] = b"VBC1";
const PRESENCE_MAGIC: &[u8; 4] = b"VBP1";

/// A bootstrap claim key or value is invalid or exceeds its declared bound.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClaimCodecError {
    /// A name, identity, or encoded value exceeds a finite bound.
    Bound,
    /// Bytes are truncated, malformed, or contain trailing data.
    Malformed,
    /// The version, scope, policy, or observed author does not match.
    Mismatch,
}

/// Collision-free reserved key for one complete bootstrap scope.
///
/// # Errors
/// Rejects an empty scope or a key that exceeds `max_key_bytes`.
pub fn claim_entry_key(
    scope: &BootstrapScope,
    max_key_bytes: usize,
) -> Result<String, ClaimCodecError> {
    if scope.domain.is_empty() || scope.partition.is_empty() || max_key_bytes == 0 {
        return Err(ClaimCodecError::Bound);
    }
    let prefix_len = "~volatile-bootstrap/v1/".len();
    let size = prefix_len
        .checked_add(scope.domain.len().to_string().len())
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(scope.domain.len()))
        .and_then(|n| n.checked_add(scope.partition.len()))
        .ok_or(ClaimCodecError::Bound)?;
    if size > max_key_bytes {
        return Err(ClaimCodecError::Bound);
    }
    let mut key = String::new();
    key.try_reserve_exact(size)
        .map_err(|_| ClaimCodecError::Bound)?;
    write!(
        &mut key,
        "~volatile-bootstrap/v1/{}:{}{}",
        scope.domain.len(),
        scope.domain,
        scope.partition
    )
    .map_err(|_| ClaimCodecError::Bound)?;
    Ok(key)
}

/// Encodes an exact scoped, policy-bound claim without a wall-clock deadline.
///
/// The native entry's TTL supplies observer-local expiry. `remaining_ms` is
/// consequently not encoded and is filled from the observing actor's cut.
///
/// # Errors
/// Rejects invalid identity, oversized fields, or an encoded value larger
/// than `max_bytes` before allocating the result.
pub fn encode_claim_value(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    claim: &BootstrapClaim,
    max_bytes: usize,
) -> Result<Vec<u8>, ClaimCodecError> {
    let size = encoded_claim_len(scope, policy, claim, max_bytes)?;
    let domain = bounded_name(&scope.domain, policy.max_scope_bytes)?;
    let partition = bounded_name(&scope.partition, policy.max_scope_bytes)?;
    let node = bounded_name(claim.identity.node.as_str(), policy.max_member_bytes)?;
    let policy_fields = policy_fields(policy)?;
    let mut out = Vec::new();
    out.try_reserve_exact(size)
        .map_err(|_| ClaimCodecError::Bound)?;
    out.extend_from_slice(MAGIC);
    write_name(&mut out, domain)?;
    write_name(&mut out, partition)?;
    for field in policy_fields {
        out.extend_from_slice(&field.to_le_bytes());
    }
    write_name(&mut out, node)?;
    out.extend_from_slice(&claim.identity.incarnation.0.to_le_bytes());
    out.extend_from_slice(&claim.identity.session.to_le_bytes());
    out.extend_from_slice(&claim.identity.attempt.to_le_bytes());
    out.extend_from_slice(&claim.renewal.to_le_bytes());
    out.push(match claim.phase {
        ClaimPhase::Willing => 1,
        ClaimPhase::Building => 2,
        ClaimPhase::Ready => 3,
    });
    Ok(out)
}

/// Computes the exact bounded entry-value allocation before encoding.
///
/// # Errors
/// Rejects invalid identity, scope, policy representation, or byte cap.
pub fn encoded_claim_len(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    claim: &BootstrapClaim,
    max_bytes: usize,
) -> Result<usize, ClaimCodecError> {
    let domain = bounded_name(&scope.domain, policy.max_scope_bytes)?;
    let partition = bounded_name(&scope.partition, policy.max_scope_bytes)?;
    let node = bounded_name(claim.identity.node.as_str(), policy.max_member_bytes)?;
    let scope_bytes = domain
        .len()
        .checked_add(partition.len())
        .ok_or(ClaimCodecError::Bound)?;
    if scope_bytes > policy.max_scope_bytes || !valid_identity(claim) {
        return Err(ClaimCodecError::Bound);
    }
    let _ = policy_fields(policy)?;
    let size = 4usize
        .checked_add(2 * 3)
        .and_then(|n| n.checked_add(scope_bytes))
        .and_then(|n| n.checked_add(node.len()))
        .and_then(|n| n.checked_add(9 * 8 + 16 + 3 * 8 + 1))
        .ok_or(ClaimCodecError::Bound)?;
    if size > max_bytes {
        return Err(ClaimCodecError::Bound);
    }
    Ok(size)
}

/// Decodes one actor-observed claim, requiring exact scope, policy and author.
///
/// The caller sets `remaining_ms` only from the actor's native TTL sample.
///
/// # Errors
/// Rejects oversized, malformed, mismatched, or trailing bytes without
/// allocating a name or claim before validation.
pub fn decode_claim_value(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    observed_node: &NodeId,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<BootstrapClaim, ClaimCodecError> {
    if bytes.len() > max_bytes {
        return Err(ClaimCodecError::Bound);
    }
    let mut reader = Reader { bytes, offset: 0 };
    if reader.take(4)? != MAGIC {
        return Err(ClaimCodecError::Mismatch);
    }
    if reader.name(policy.max_scope_bytes)? != scope.domain.as_bytes()
        || reader.name(policy.max_scope_bytes)? != scope.partition.as_bytes()
    {
        return Err(ClaimCodecError::Mismatch);
    }
    for expected in policy_fields(policy)? {
        if reader.u64()? != expected {
            return Err(ClaimCodecError::Mismatch);
        }
    }
    if reader.name(policy.max_member_bytes)? != observed_node.as_str().as_bytes() {
        return Err(ClaimCodecError::Mismatch);
    }
    let incarnation = BootId(u128::from_le_bytes(reader.array()?));
    let session = reader.u64()?;
    let attempt = reader.u64()?;
    let renewal = reader.u64()?;
    let phase = match reader.take(1)?[0] {
        1 => ClaimPhase::Willing,
        2 => ClaimPhase::Building,
        3 => ClaimPhase::Ready,
        _ => return Err(ClaimCodecError::Malformed),
    };
    if reader.offset != bytes.len()
        || incarnation.0 == 0
        || session == 0
        || attempt == 0
        || renewal == 0
    {
        return Err(ClaimCodecError::Malformed);
    }
    Ok(BootstrapClaim {
        identity: ClaimIdentity {
            node: observed_node.clone(),
            incarnation,
            session,
            attempt,
        },
        renewal,
        phase,
        remaining_ms: 0,
    })
}

/// Collision-free reserved native key for a long-lived participation entry.
///
/// # Errors
/// Rejects empty or over-limit scope names.
pub fn presence_entry_key(
    scope: &BootstrapScope,
    max_key_bytes: usize,
) -> Result<String, ClaimCodecError> {
    if scope.domain.is_empty() || scope.partition.is_empty() || max_key_bytes == 0 {
        return Err(ClaimCodecError::Bound);
    }
    let prefix = "~volatile-presence/v1/";
    let size = prefix
        .len()
        .checked_add(scope.domain.len().to_string().len())
        .and_then(|n| n.checked_add(1))
        .and_then(|n| n.checked_add(scope.domain.len()))
        .and_then(|n| n.checked_add(scope.partition.len()))
        .ok_or(ClaimCodecError::Bound)?;
    if size > max_key_bytes {
        return Err(ClaimCodecError::Bound);
    }
    let mut key = String::new();
    key.try_reserve_exact(size)
        .map_err(|_| ClaimCodecError::Bound)?;
    write!(
        &mut key,
        "{prefix}{}:{}{}",
        scope.domain.len(),
        scope.domain,
        scope.partition
    )
    .map_err(|_| ClaimCodecError::Bound)?;
    Ok(key)
}

/// Exact encoded length of one participation value, excluding native TTL.
///
/// # Errors
/// Rejects invalid identity, policy representation, or capacity.
pub fn encoded_presence_len(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    presence: &BootstrapPresence,
    max_bytes: usize,
) -> Result<usize, ClaimCodecError> {
    let domain = bounded_name(&scope.domain, policy.max_scope_bytes)?;
    let partition = bounded_name(&scope.partition, policy.max_scope_bytes)?;
    let node = bounded_name(presence.identity.node.as_str(), policy.max_member_bytes)?;
    let scope_bytes = domain
        .len()
        .checked_add(partition.len())
        .ok_or(ClaimCodecError::Bound)?;
    if scope_bytes > policy.max_scope_bytes
        || presence.identity.boot.0 == 0
        || presence.identity.session == 0
        || presence.renewal == 0
    {
        return Err(ClaimCodecError::Bound);
    }
    let _ = policy_fields(policy)?;
    let size = 4usize
        .checked_add(2 * 3)
        .and_then(|n| n.checked_add(scope_bytes))
        .and_then(|n| n.checked_add(node.len()))
        .and_then(|n| n.checked_add(9 * 8 + 16 + 2 * 8))
        .ok_or(ClaimCodecError::Bound)?;
    if size > max_bytes {
        return Err(ClaimCodecError::Bound);
    }
    Ok(size)
}

/// Encode one policy-bound participation renewal with no wall timestamp.
///
/// # Errors
/// Rejects invalid or oversized fields before allocating the body.
pub fn encode_presence_value(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    presence: &BootstrapPresence,
    max_bytes: usize,
) -> Result<Vec<u8>, ClaimCodecError> {
    let size = encoded_presence_len(scope, policy, presence, max_bytes)?;
    let mut out = Vec::new();
    out.try_reserve_exact(size)
        .map_err(|_| ClaimCodecError::Bound)?;
    out.extend_from_slice(PRESENCE_MAGIC);
    write_name(&mut out, scope.domain.as_bytes())?;
    write_name(&mut out, scope.partition.as_bytes())?;
    for field in policy_fields(policy)? {
        out.extend_from_slice(&field.to_le_bytes());
    }
    write_name(&mut out, presence.identity.node.as_str().as_bytes())?;
    out.extend_from_slice(&presence.identity.boot.0.to_le_bytes());
    out.extend_from_slice(&presence.identity.session.to_le_bytes());
    out.extend_from_slice(&presence.renewal.to_le_bytes());
    Ok(out)
}

/// Decode one actor-observed participation value for an exact author/scope.
/// The caller fills remaining TTL only from the native actor sample.
///
/// # Errors
/// Rejects malformed, trailing, wrong-policy, or over-limit bytes without
/// first allocating an identity.
pub fn decode_presence_value(
    scope: &BootstrapScope,
    policy: BootstrapConfig,
    observed_node: &NodeId,
    bytes: &[u8],
    max_bytes: usize,
) -> Result<BootstrapPresence, ClaimCodecError> {
    if bytes.len() > max_bytes {
        return Err(ClaimCodecError::Bound);
    }
    let mut reader = Reader { bytes, offset: 0 };
    if reader.take(4)? != PRESENCE_MAGIC {
        return Err(ClaimCodecError::Mismatch);
    }
    if reader.name(policy.max_scope_bytes)? != scope.domain.as_bytes()
        || reader.name(policy.max_scope_bytes)? != scope.partition.as_bytes()
    {
        return Err(ClaimCodecError::Mismatch);
    }
    for field in policy_fields(policy)? {
        if reader.u64()? != field {
            return Err(ClaimCodecError::Mismatch);
        }
    }
    if reader.name(policy.max_member_bytes)? != observed_node.as_str().as_bytes() {
        return Err(ClaimCodecError::Mismatch);
    }
    let boot = BootId(u128::from_le_bytes(reader.array()?));
    let session = reader.u64()?;
    let renewal = reader.u64()?;
    if reader.offset != bytes.len() || boot.0 == 0 || session == 0 || renewal == 0 {
        return Err(ClaimCodecError::Malformed);
    }
    Ok(BootstrapPresence {
        identity: PresenceIdentity {
            node: observed_node.clone(),
            boot,
            session,
        },
        renewal,
        remaining_ms: 0,
    })
}

fn bounded_name(name: &str, max_bytes: usize) -> Result<&[u8], ClaimCodecError> {
    if name.is_empty() || name.len() > max_bytes || name.len() > usize::from(u16::MAX) {
        return Err(ClaimCodecError::Bound);
    }
    Ok(name.as_bytes())
}

fn valid_identity(claim: &BootstrapClaim) -> bool {
    claim.identity.incarnation.0 != 0
        && claim.identity.session != 0
        && claim.identity.attempt != 0
        && claim.renewal != 0
}

fn policy_fields(policy: BootstrapConfig) -> Result<[u64; 9], ClaimCodecError> {
    Ok([
        u64::try_from(policy.max_members).map_err(|_| ClaimCodecError::Bound)?,
        u64::try_from(policy.max_member_bytes).map_err(|_| ClaimCodecError::Bound)?,
        u64::try_from(policy.max_scope_bytes).map_err(|_| ClaimCodecError::Bound)?,
        policy.settle_ms,
        policy.renew_ms,
        policy.claim_ttl_ms,
        policy.observe_ms,
        policy.donor_wait_ms,
        policy.total_ms,
    ])
}

fn write_name(out: &mut Vec<u8>, name: &[u8]) -> Result<(), ClaimCodecError> {
    let len = u16::try_from(name.len()).map_err(|_| ClaimCodecError::Bound)?;
    out.extend_from_slice(&len.to_le_bytes());
    out.extend_from_slice(name);
    Ok(())
}

struct Reader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Result<&'a [u8], ClaimCodecError> {
        let end = self.offset.checked_add(len).ok_or(ClaimCodecError::Bound)?;
        let slice = self
            .bytes
            .get(self.offset..end)
            .ok_or(ClaimCodecError::Malformed)?;
        self.offset = end;
        Ok(slice)
    }

    fn array<const N: usize>(&mut self) -> Result<[u8; N], ClaimCodecError> {
        self.take(N)?
            .try_into()
            .map_err(|_| ClaimCodecError::Malformed)
    }

    fn u64(&mut self) -> Result<u64, ClaimCodecError> {
        Ok(u64::from_le_bytes(self.array()?))
    }

    fn name(&mut self, max_bytes: usize) -> Result<&'a [u8], ClaimCodecError> {
        let len = usize::from(u16::from_le_bytes(self.array()?));
        if len == 0 || len > max_bytes {
            return Err(ClaimCodecError::Bound);
        }
        self.take(len)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn scope() -> BootstrapScope {
        BootstrapScope {
            domain: "origin:account".into(),
            partition: "bucket".into(),
        }
    }

    fn policy() -> BootstrapConfig {
        BootstrapConfig {
            max_members: 8,
            max_member_bytes: 32,
            max_scope_bytes: 64,
            settle_ms: 20,
            renew_ms: 50,
            claim_ttl_ms: 200,
            observe_ms: 20,
            donor_wait_ms: 300,
            total_ms: 500,
        }
    }

    fn claim() -> BootstrapClaim {
        BootstrapClaim {
            identity: ClaimIdentity {
                node: NodeId::new("node-a"),
                incarnation: BootId(1),
                session: 2,
                attempt: 3,
            },
            renewal: 4,
            phase: ClaimPhase::Ready,
            remaining_ms: 99,
        }
    }

    #[test]
    fn exact_claim_round_trip_and_collision_free_scope_key() {
        let bytes = encode_claim_value(&scope(), policy(), &claim(), 256).unwrap();
        assert_eq!(
            encoded_claim_len(&scope(), policy(), &claim(), 256).unwrap(),
            bytes.len()
        );
        let decoded =
            decode_claim_value(&scope(), policy(), &claim().identity.node, &bytes, 256).unwrap();
        assert_eq!(decoded.remaining_ms, 0);
        assert_eq!(decoded.identity, claim().identity);
        assert_eq!(decoded.renewal, 4);
        assert_eq!(decoded.phase, ClaimPhase::Ready);

        let left = BootstrapScope {
            domain: "a".into(),
            partition: ":b".into(),
        };
        let right = BootstrapScope {
            domain: "a:".into(),
            partition: "b".into(),
        };
        assert_ne!(
            claim_entry_key(&left, 128).unwrap(),
            claim_entry_key(&right, 128).unwrap()
        );
    }

    #[test]
    fn malformed_or_mismatched_present_claim_never_becomes_absence() {
        let bytes = encode_claim_value(&scope(), policy(), &claim(), 256).unwrap();
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            decode_claim_value(&scope(), policy(), &claim().identity.node, &trailing, 256),
            Err(ClaimCodecError::Malformed)
        );
        let mut changed_policy = policy();
        changed_policy.renew_ms += 1;
        assert_eq!(
            decode_claim_value(
                &scope(),
                changed_policy,
                &claim().identity.node,
                &bytes,
                256
            ),
            Err(ClaimCodecError::Mismatch)
        );
        assert_eq!(
            decode_claim_value(&scope(), policy(), &NodeId::new("node-b"), &bytes, 256),
            Err(ClaimCodecError::Mismatch)
        );
        assert_eq!(
            decode_claim_value(&scope(), policy(), &claim().identity.node, &bytes, 8),
            Err(ClaimCodecError::Bound)
        );
        let mut bad_version = bytes;
        bad_version[3] = b'2';
        assert_eq!(
            decode_claim_value(
                &scope(),
                policy(),
                &claim().identity.node,
                &bad_version,
                256
            ),
            Err(ClaimCodecError::Mismatch)
        );
    }

    #[test]
    fn presence_is_separate_policy_bound_entry_without_ttl_timestamp() {
        let presence = BootstrapPresence {
            identity: PresenceIdentity {
                node: NodeId::new("node-a"),
                boot: BootId(17),
                session: 23,
            },
            renewal: 4,
            remaining_ms: 99,
        };
        let bytes = encode_presence_value(&scope(), policy(), &presence, 256).unwrap();
        assert_eq!(
            encoded_presence_len(&scope(), policy(), &presence, 256).unwrap(),
            bytes.len()
        );
        let decoded =
            decode_presence_value(&scope(), policy(), &presence.identity.node, &bytes, 256)
                .unwrap();
        assert_eq!(decoded.identity, presence.identity);
        assert_eq!(decoded.renewal, 4);
        assert_eq!(decoded.remaining_ms, 0);
        assert_ne!(
            presence_entry_key(&scope(), 256).unwrap(),
            claim_entry_key(&scope(), 256).unwrap()
        );
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert_eq!(
            decode_presence_value(&scope(), policy(), &presence.identity.node, &trailing, 256),
            Err(ClaimCodecError::Malformed)
        );
        let mut wrong_policy = policy();
        wrong_policy.claim_ttl_ms += 1;
        assert_eq!(
            decode_presence_value(&scope(), wrong_policy, &presence.identity.node, &bytes, 256,),
            Err(ClaimCodecError::Mismatch)
        );
    }
}
