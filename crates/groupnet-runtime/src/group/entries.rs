//! Bounded actor-side entry inspection and confirmed local entry mutation.
//!
//! Every operation here checks its request and limits, and the caller's owned
//! budget, before it allocates anything further or enqueues a command. The
//! budget then travels with the queued command and its reply, so a cancelled
//! requester never releases bytes the actor still owns.

use std::any::Any;
use std::fmt;
use std::num::NonZeroUsize;
use std::time::{Duration, Instant};

use groupnet_core::{NodeId, Status};
use tokio::sync::oneshot;

use super::{CommandRejected, Group};
use crate::clock::core_millis;
use crate::driver::Event;

/// Finite actor-side observation limits, checked before cloning any entry.
///
/// Every bound is nonzero by type; [`validate`](Self::validate) checks the
/// cross-field rules. The defaults admit a 1 024-member roster of short
/// identities and small values within a 1 MiB response.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryInspectionLimits {
    /// Maximum owned bytes in each requested key.
    pub max_key_bytes: NonZeroUsize,
    /// Maximum returned members, including the local node.
    pub max_members: NonZeroUsize,
    /// Maximum bytes in one member identity.
    pub max_member_bytes: NonZeroUsize,
    /// Maximum bytes in one entry value.
    pub max_value_bytes: NonZeroUsize,
    /// Maximum combined response charge, including the fixed result and
    /// per-entry record headers, identities, and value bytes.
    pub max_response_bytes: NonZeroUsize,
}

impl Default for EntryInspectionLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: NonZeroUsize::new(256).unwrap(),
            max_members: NonZeroUsize::new(1_024).unwrap(),
            max_member_bytes: NonZeroUsize::new(256).unwrap(),
            max_value_bytes: NonZeroUsize::new(4_096).unwrap(),
            max_response_bytes: NonZeroUsize::new(1 << 20).unwrap(),
        }
    }
}

impl EntryInspectionLimits {
    /// Checks the cross-field rules every inspection preflights.
    ///
    /// # Errors
    /// Returns the first violated [`EntryInspectionLimitsError`] rule: a
    /// per-identity or per-value bound larger than the whole response bound is
    /// inverted.
    pub fn validate(&self) -> Result<(), EntryInspectionLimitsError> {
        if self.max_member_bytes > self.max_response_bytes {
            return Err(EntryInspectionLimitsError::MemberAboveResponse {
                max_member_bytes: self.max_member_bytes.get(),
                max_response_bytes: self.max_response_bytes.get(),
            });
        }
        if self.max_value_bytes > self.max_response_bytes {
            return Err(EntryInspectionLimitsError::ValueAboveResponse {
                max_value_bytes: self.max_value_bytes.get(),
                max_response_bytes: self.max_response_bytes.get(),
            });
        }
        Ok(())
    }
}

/// A violated [`EntryInspectionLimits`] rule, naming the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryInspectionLimitsError {
    /// `max_member_bytes` exceeds `max_response_bytes`.
    MemberAboveResponse {
        /// The configured identity bound.
        max_member_bytes: usize,
        /// The configured response bound.
        max_response_bytes: usize,
    },
    /// `max_value_bytes` exceeds `max_response_bytes`.
    ValueAboveResponse {
        /// The configured value bound.
        max_value_bytes: usize,
        /// The configured response bound.
        max_response_bytes: usize,
    },
}

impl fmt::Display for EntryInspectionLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MemberAboveResponse {
                max_member_bytes,
                max_response_bytes,
            } => write!(
                f,
                "entry inspection max_member_bytes {max_member_bytes} exceeds \
                 max_response_bytes {max_response_bytes}"
            ),
            Self::ValueAboveResponse {
                max_value_bytes,
                max_response_bytes,
            } => write!(
                f,
                "entry inspection max_value_bytes {max_value_bytes} exceeds \
                 max_response_bytes {max_response_bytes}"
            ),
        }
    }
}

impl std::error::Error for EntryInspectionLimitsError {}

/// Actor-side byte limits for one confirmed or conditional entry mutation.
///
/// Both bounds are nonzero by type; [`validate`](Self::validate) checks that
/// a request at both bounds has a representable combined charge.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryMutationLimits {
    /// Maximum owned bytes in the scoped key.
    pub max_key_bytes: NonZeroUsize,
    /// Maximum owned bytes in its value (or expected value).
    pub max_value_bytes: NonZeroUsize,
}

impl Default for EntryMutationLimits {
    fn default() -> Self {
        Self {
            max_key_bytes: NonZeroUsize::new(256).unwrap(),
            max_value_bytes: NonZeroUsize::new(4_096).unwrap(),
        }
    }
}

impl EntryMutationLimits {
    /// Checks the rule every mutation preflights.
    ///
    /// # Errors
    /// [`EntryMutationLimitsError::ChargeOverflow`] when a key and value at
    /// both bounds could not be charged against a budget.
    pub fn validate(&self) -> Result<(), EntryMutationLimitsError> {
        if self
            .max_key_bytes
            .checked_add(self.max_value_bytes.get())
            .is_none()
        {
            return Err(EntryMutationLimitsError::ChargeOverflow {
                max_key_bytes: self.max_key_bytes.get(),
                max_value_bytes: self.max_value_bytes.get(),
            });
        }
        Ok(())
    }
}

/// A violated [`EntryMutationLimits`] rule, naming the fields.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryMutationLimitsError {
    /// `max_key_bytes + max_value_bytes` overflows `usize`.
    ChargeOverflow {
        /// The configured key bound.
        max_key_bytes: usize,
        /// The configured value bound.
        max_value_bytes: usize,
    },
}

impl fmt::Display for EntryMutationLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ChargeOverflow {
                max_key_bytes,
                max_value_bytes,
            } => write!(
                f,
                "entry mutation max_key_bytes {max_key_bytes} plus max_value_bytes \
                 {max_value_bytes} overflows a budget charge"
            ),
        }
    }
}

impl std::error::Error for EntryMutationLimitsError {}

/// Exact actor-cut revision precondition for one local entry mutation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EntryRevision {
    /// Retained key revision, including a tombstone. An absent revision also
    /// binds `member` to prevent an old create after a publish and removal.
    pub key: Option<u64>,
    /// Native member state high-water at the observation cut.
    pub member: u64,
}

/// One bounded local entry publication, for
/// [`Group::set_entry_confirmed`] and [`Group::set_entry_if_revision`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryWrite {
    /// The scoped key.
    pub key: String,
    /// The value to publish.
    pub value: Vec<u8>,
    /// If set, every receiver expires the entry that long after last adopting
    /// it (see [`Group::set_entry`] for the conversion to core time).
    pub ttl: Option<Duration>,
    /// The owned-byte bounds the key and value must fit.
    pub limits: EntryMutationLimits,
}

impl EntryWrite {
    /// A write of `value` at `key`, without a TTL, under the default
    /// [`EntryMutationLimits`].
    #[must_use]
    pub fn new(key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            value: value.into(),
            ttl: None,
            limits: EntryMutationLimits::default(),
        }
    }

    /// Expires the entry `ttl` after each receiver last adopted it.
    #[must_use]
    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = Some(ttl);
        self
    }

    /// Bounds the key and value with `limits`.
    #[must_use]
    pub fn with_limits(mut self, limits: EntryMutationLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Checks the limits and the key and value against them.
    ///
    /// # Errors
    /// The first violated rule as an [`EntryMutationError`].
    pub fn validate(&self) -> Result<(), EntryMutationError> {
        check_mutation(&self.key, &self.value, self.limits)
    }
}

/// One bounded conditional withdrawal, for [`Group::delete_entry_if_value`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EntryRemoval {
    /// The scoped key.
    pub key: String,
    /// The exact bytes the actor-visible entry must hold to be deleted.
    pub expected: Vec<u8>,
    /// The owned-byte bounds the key and expected value must fit.
    pub limits: EntryMutationLimits,
}

impl EntryRemoval {
    /// A withdrawal of `key` while it holds exactly `expected`, under the
    /// default [`EntryMutationLimits`].
    #[must_use]
    pub fn new(key: impl Into<String>, expected: impl Into<Vec<u8>>) -> Self {
        Self {
            key: key.into(),
            expected: expected.into(),
            limits: EntryMutationLimits::default(),
        }
    }

    /// Bounds the key and expected value with `limits`.
    #[must_use]
    pub fn with_limits(mut self, limits: EntryMutationLimits) -> Self {
        self.limits = limits;
        self
    }

    /// Checks the limits and the key and expected value against them.
    ///
    /// # Errors
    /// The first violated rule as an [`EntryMutationError`].
    pub fn validate(&self) -> Result<(), EntryMutationError> {
        check_mutation(&self.key, &self.expected, self.limits)
    }
}

fn check_mutation(
    key: &String,
    value: &Vec<u8>,
    limits: EntryMutationLimits,
) -> Result<(), EntryMutationError> {
    limits.validate()?;
    if key.is_empty() {
        return Err(EntryMutationError::EmptyKey);
    }
    // Capacity, not length: the owned allocation is what the budget pays for.
    if key.capacity() > limits.max_key_bytes.get() {
        return Err(EntryMutationError::KeyTooLong {
            bytes: key.capacity(),
            max: limits.max_key_bytes.get(),
        });
    }
    if value.capacity() > limits.max_value_bytes.get() {
        return Err(EntryMutationError::ValueTooLong {
            bytes: value.capacity(),
            max: limits.max_value_bytes.get(),
        });
    }
    Ok(())
}

/// The combined `charges`, if `available` budget bytes cover them.
fn charge(charges: &[usize], available: usize) -> Result<(), Option<usize>> {
    let required = charges
        .iter()
        .try_fold(0_usize, |sum, bytes| sum.checked_add(*bytes));
    match required {
        Some(required) if required <= available => Ok(()),
        required => Err(required),
    }
}

/// An owned admission guard carried through the actor queue and response.
///
/// A cancelled requester does not release its budget while the actor still
/// owns or allocates the reply. The runtime is independent of any particular
/// admission implementation.
pub trait EntryBudget: Any + Send {
    /// Maximum queued or response bytes already reserved by this owner.
    fn bytes(&self) -> usize;
}

/// One member and exactly one scoped entry from an actor-state cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedEntry {
    /// Member identity.
    pub node: NodeId,
    /// Membership status at the same actor cut.
    pub status: Status,
    /// Entry bytes, if present and unexpired.
    pub value: Option<Vec<u8>>,
    /// Observer-local remaining TTL, if the entry has one.
    pub remaining_ttl: Option<Duration>,
}

/// One complete bounded actor-state cut with its monotonic sample instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedEntries {
    /// Local monotonic instant at or before the actor's expiry calculation.
    pub sampled_at: Instant,
    /// Complete known roster in node-id order, including absent entries.
    pub entries: Vec<InspectedEntry>,
}

/// One member and two scoped entries from the same actor-state cut.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedPairEntry {
    /// Native member identity.
    pub node: NodeId,
    /// Native membership status.
    pub status: Status,
    /// SWIM refutation incarnation; the entry's boot token fences restarts.
    pub member_incarnation: u64,
    /// Native high-water version including deleted entries. Conditional local
    /// publication binds this value so an old absent-value create cannot
    /// revive after a newer publication and withdrawal.
    pub member_state_version: u64,
    /// First entry bytes, if present and unexpired.
    pub first: Option<Vec<u8>>,
    /// Retained first-entry revision, including an absent tombstone.
    pub first_version: Option<u64>,
    /// Native remaining TTL for the first entry at the sample.
    pub first_remaining_ttl: Option<Duration>,
    /// Second entry bytes, if present and unexpired.
    pub second: Option<Vec<u8>>,
    /// Retained second-entry revision, including an absent tombstone.
    pub second_version: Option<u64>,
    /// Native remaining TTL for the second entry at the sample.
    pub second_remaining_ttl: Option<Duration>,
}

/// Complete bounded two-entry roster at one observer-local actor instant.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct InspectedPair {
    /// Instant at or before the actor's native TTL calculation.
    pub sampled_at: Instant,
    /// Complete known roster in `NodeId` order, including absent entries.
    pub entries: Vec<InspectedPairEntry>,
}

/// A bounded actor-side inspection failed without returning a partial roster.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryInspectionError {
    /// The limits violate a cross-field rule.
    Limits(EntryInspectionLimitsError),
    /// A requested key is empty.
    EmptyKey,
    /// A pair inspection named the same key twice.
    DuplicateKeys,
    /// A requested key owns more bytes than `max_key_bytes`.
    KeyTooLong {
        /// Owned key bytes.
        bytes: usize,
        /// The configured bound.
        max: usize,
    },
    /// The caller's budget cannot cover the bounded response plus the keys.
    BudgetTooSmall {
        /// Bytes the request charges, `None` when that sum overflows.
        required: Option<usize>,
        /// Bytes the budget reserved.
        available: usize,
    },
    /// The complete roster exceeded `max_members`.
    TooManyMembers,
    /// One member identity exceeded `max_member_bytes`.
    IdentityTooLong,
    /// One present value exceeded `max_value_bytes`.
    ValueTooLong,
    /// The complete response exceeded `max_response_bytes`.
    Capacity,
    /// A listed member's native state vanished from the actor cut.
    MissingMemberState,
    /// The command was not enqueued.
    Rejected(CommandRejected),
    /// The actor dropped the reply (it shut down mid-request).
    ReplyLost,
    /// The runtime could not recover its exact type-erased budget owner.
    Internal,
}

impl fmt::Display for EntryInspectionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits(_) => f.write_str("invalid entry inspection limits"),
            Self::EmptyKey => f.write_str("entry inspection key is empty"),
            Self::DuplicateKeys => f.write_str("entry pair inspection names one key twice"),
            Self::KeyTooLong { bytes, max } => {
                write!(f, "entry inspection key owns {bytes} bytes, above {max}")
            }
            Self::BudgetTooSmall {
                required: Some(required),
                available,
            } => write!(
                f,
                "entry inspection charges {required} bytes, above the {available}-byte budget"
            ),
            Self::BudgetTooSmall {
                required: None,
                available,
            } => write!(
                f,
                "entry inspection charge overflows, above the {available}-byte budget"
            ),
            Self::TooManyMembers => f.write_str("roster exceeds max_members"),
            Self::IdentityTooLong => f.write_str("member identity exceeds max_member_bytes"),
            Self::ValueTooLong => f.write_str("entry value exceeds max_value_bytes"),
            Self::Capacity => f.write_str("inspection response exceeds max_response_bytes"),
            Self::MissingMemberState => f.write_str("member state vanished from the actor cut"),
            Self::Rejected(_) => f.write_str("entry inspection was not enqueued"),
            Self::ReplyLost => f.write_str("group actor dropped the inspection reply"),
            Self::Internal => f.write_str("entry inspection budget owner was not recovered"),
        }
    }
}

impl std::error::Error for EntryInspectionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Rejected(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EntryInspectionLimitsError> for EntryInspectionError {
    fn from(error: EntryInspectionLimitsError) -> Self {
        Self::Limits(error)
    }
}

/// A confirmed local entry mutation failed or its outcome is unknown.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EntryMutationError {
    /// The limits violate a rule.
    Limits(EntryMutationLimitsError),
    /// The key is empty.
    EmptyKey,
    /// The key owns more bytes than `max_key_bytes`.
    KeyTooLong {
        /// Owned key bytes.
        bytes: usize,
        /// The configured bound.
        max: usize,
    },
    /// The value (or expected value) owns more bytes than `max_value_bytes`.
    ValueTooLong {
        /// Owned value bytes.
        bytes: usize,
        /// The configured bound.
        max: usize,
    },
    /// The caller's budget cannot cover the owned key and value.
    BudgetTooSmall {
        /// Bytes the request charges, `None` when that sum overflows.
        required: Option<usize>,
        /// Bytes the budget reserved.
        available: usize,
    },
    /// A revision precondition at `u64::MAX` can never be followed by a write.
    RevisionExhausted,
    /// The command was not enqueued, so nothing was applied.
    Rejected(CommandRejected),
    /// The actor dropped the reply. The outcome is unknown: read back the
    /// exact value before deciding whether it applied.
    ReplyLost,
    /// The actor did not apply a local set operation.
    NotApplied,
    /// The runtime could not recover its exact type-erased budget owner.
    Internal,
}

impl fmt::Display for EntryMutationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Limits(_) => f.write_str("invalid entry mutation limits"),
            Self::EmptyKey => f.write_str("entry key is empty"),
            Self::KeyTooLong { bytes, max } => {
                write!(f, "entry key owns {bytes} bytes, above {max}")
            }
            Self::ValueTooLong { bytes, max } => {
                write!(f, "entry value owns {bytes} bytes, above {max}")
            }
            Self::BudgetTooSmall {
                required: Some(required),
                available,
            } => write!(
                f,
                "entry mutation charges {required} bytes, above the {available}-byte budget"
            ),
            Self::BudgetTooSmall {
                required: None,
                available,
            } => write!(
                f,
                "entry mutation charge overflows, above the {available}-byte budget"
            ),
            Self::RevisionExhausted => f.write_str("entry revision precondition is exhausted"),
            Self::Rejected(_) => f.write_str("entry mutation was not enqueued"),
            Self::ReplyLost => f.write_str("group actor dropped the mutation reply"),
            Self::NotApplied => f.write_str("group actor did not apply the entry"),
            Self::Internal => f.write_str("entry mutation budget owner was not recovered"),
        }
    }
}

impl std::error::Error for EntryMutationError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Limits(error) => Some(error),
            Self::Rejected(error) => Some(error),
            _ => None,
        }
    }
}

impl From<EntryMutationLimitsError> for EntryMutationError {
    fn from(error: EntryMutationLimitsError) -> Self {
        Self::Limits(error)
    }
}

impl Group {
    /// Applies a bounded local entry and waits until the actor has published
    /// the resulting local view. This confirms only local publication; gossip
    /// delivery and remote adoption remain advisory.
    ///
    /// A lost reply has an unknown outcome. Read back the exact scoped entry
    /// rather than issuing an unrelated claim with the same identity.
    ///
    /// # Errors
    /// Returns [`EntryMutationError`] for a write or budget that fails its
    /// bounds (checked before enqueueing), a refused enqueue, a lost reply, or
    /// a rejected local application.
    pub async fn set_entry_confirmed<B: EntryBudget>(
        &self,
        write: EntryWrite,
        budget: B,
    ) -> Result<B, EntryMutationError> {
        write.validate()?;
        charge_mutation(&write.key, &write.value, &budget)?;
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::SetEntryConfirmed {
                key: write.key,
                value: write.value,
                ttl_ms: write.ttl.map(core_millis),
                budget: Box::new(budget),
                reply,
            })
            .map_err(|error| EntryMutationError::Rejected(CommandRejected::from_send(&error)))?;
        let (applied, erased) = response.await.map_err(|_| EntryMutationError::ReplyLost)?;
        let budget = erased
            .downcast::<B>()
            .map_err(|_| EntryMutationError::Internal)?;
        if applied {
            Ok(*budget)
        } else {
            Err(EntryMutationError::NotApplied)
        }
    }

    /// Publishes one bounded local entry only while its retained per-key
    /// revision still matches the actor cut. An absent key also binds the
    /// member's state-version high-water mark, so a delayed old create cannot
    /// revive after a newer create and withdrawal have both disappeared.
    /// Unrelated local writes do not reject a renewal with a retained key
    /// revision. The actor checks the condition after processing expiries.
    ///
    /// A `false` result confirms rejection without mutation. A lost reply is
    /// unknown and requires exact readback. The owned budget follows queued
    /// work and the response even if the requester is cancelled.
    ///
    /// # Errors
    /// Rejects a write, revision or budget that fails its bounds (checked
    /// before enqueueing), a refused enqueue, or a lost reply.
    pub async fn set_entry_if_revision<B: EntryBudget>(
        &self,
        write: EntryWrite,
        expected: EntryRevision,
        budget: B,
    ) -> Result<(bool, B), EntryMutationError> {
        write.validate()?;
        if expected.member == u64::MAX || expected.key == Some(u64::MAX) {
            return Err(EntryMutationError::RevisionExhausted);
        }
        charge_mutation(&write.key, &write.value, &budget)?;
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::SetEntryIfRevision {
                key: write.key,
                value: write.value,
                ttl_ms: write.ttl.map(core_millis),
                expected,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|error| EntryMutationError::Rejected(CommandRejected::from_send(&error)))?;
        let (applied, erased) = response.await.map_err(|_| EntryMutationError::ReplyLost)?;
        let budget = erased
            .downcast::<B>()
            .map_err(|_| EntryMutationError::Internal)?;
        Ok((applied, *budget))
    }

    /// Deletes this node's entry only if its actor-visible bytes exactly match
    /// the removal's `expected`. A distinct renewal/incarnation must change
    /// those bytes for this to fence delayed withdrawals. A `false` result
    /// does not delete a newer claim. Publication precedes the successful
    /// reply.
    ///
    /// # Errors
    /// Returns [`EntryMutationError`] for a removal or budget that fails its
    /// bounds (checked before enqueueing), a refused enqueue, or a lost reply
    /// (unknown outcome; read back).
    pub async fn delete_entry_if_value<B: EntryBudget>(
        &self,
        removal: EntryRemoval,
        budget: B,
    ) -> Result<(bool, B), EntryMutationError> {
        removal.validate()?;
        charge_mutation(&removal.key, &removal.expected, &budget)?;
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::DeleteEntryIfValue {
                key: removal.key,
                expected: removal.expected,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|error| EntryMutationError::Rejected(CommandRejected::from_send(&error)))?;
        let (deleted, erased) = response.await.map_err(|_| EntryMutationError::ReplyLost)?;
        let budget = erased
            .downcast::<B>()
            .map_err(|_| EntryMutationError::Internal)?;
        Ok((deleted, *budget))
    }

    /// Samples one key across the complete group roster inside the group
    /// actor, including each entry's observer-local remaining TTL.
    ///
    /// The limits, key and budget are checked before the key is copied or
    /// anything is enqueued. The budget then moves into the bounded actor
    /// command before any response allocation. If this future is cancelled,
    /// that command or its reply keeps the budget until its cloned bytes are
    /// dropped.
    ///
    /// # Errors
    /// Rejects invalid limits, key or budget, a refused enqueue, a lost reply,
    /// or any response overflow, without returning a truncated roster.
    pub async fn inspect_scoped_entry<B: EntryBudget>(
        &self,
        key: &str,
        limits: EntryInspectionLimits,
        budget: B,
    ) -> Result<(InspectedEntries, B), EntryInspectionError> {
        limits.validate()?;
        check_inspection_key(key, key.len(), limits)?;
        let budget_too_small = |required| EntryInspectionError::BudgetTooSmall {
            required,
            available: budget.bytes(),
        };
        charge(
            &[limits.max_response_bytes.get(), key.len()],
            budget.bytes(),
        )
        .map_err(budget_too_small)?;
        let key = key.to_owned();
        check_inspection_key(&key, key.capacity(), limits)?;
        charge(
            &[limits.max_response_bytes.get(), key.capacity()],
            budget.bytes(),
        )
        .map_err(budget_too_small)?;
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::InspectScopedEntry {
                key,
                limits,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|error| EntryInspectionError::Rejected(CommandRejected::from_send(&error)))?;
        let (entries, erased) = response
            .await
            .map_err(|_| EntryInspectionError::ReplyLost)??;
        match erased.downcast::<B>() {
            Ok(budget) => Ok((entries, *budget)),
            Err(erased) => {
                drop(entries);
                drop(erased);
                Err(EntryInspectionError::Internal)
            }
        }
    }

    /// Samples two scoped keys and native membership incarnations from one
    /// complete actor cut, with the owned budget held through cancellation.
    ///
    /// The limits, keys and budget are checked before the keys are copied or
    /// anything is enqueued.
    ///
    /// # Errors
    /// Rejects invalid limits, keys or budget, a refused enqueue, a lost
    /// reply, or a response too large to retain, without returning a
    /// truncated roster.
    pub async fn inspect_scoped_pair<B: EntryBudget>(
        &self,
        first: &str,
        second: &str,
        limits: EntryInspectionLimits,
        budget: B,
    ) -> Result<(InspectedPair, B), EntryInspectionError> {
        limits.validate()?;
        check_inspection_key(first, first.len(), limits)?;
        check_inspection_key(second, second.len(), limits)?;
        if first == second {
            return Err(EntryInspectionError::DuplicateKeys);
        }
        let budget_too_small = |required| EntryInspectionError::BudgetTooSmall {
            required,
            available: budget.bytes(),
        };
        charge(
            &[limits.max_response_bytes.get(), first.len(), second.len()],
            budget.bytes(),
        )
        .map_err(budget_too_small)?;
        let first = first.to_owned();
        let second = second.to_owned();
        check_inspection_key(&first, first.capacity(), limits)?;
        check_inspection_key(&second, second.capacity(), limits)?;
        charge(
            &[
                limits.max_response_bytes.get(),
                first.capacity(),
                second.capacity(),
            ],
            budget.bytes(),
        )
        .map_err(budget_too_small)?;
        let (reply, response) = oneshot::channel();
        self.tx
            .try_send(Event::InspectScopedPair {
                first,
                second,
                limits,
                budget: Box::new(budget),
                reply,
            })
            .map_err(|error| EntryInspectionError::Rejected(CommandRejected::from_send(&error)))?;
        let (entries, erased) = response
            .await
            .map_err(|_| EntryInspectionError::ReplyLost)??;
        match erased.downcast::<B>() {
            Ok(budget) => Ok((entries, *budget)),
            Err(erased) => {
                drop(entries);
                drop(erased);
                Err(EntryInspectionError::Internal)
            }
        }
    }
}

/// Checks a non-empty `key` whose owned (or to-be-owned) size is `bytes`.
fn check_inspection_key(
    key: &str,
    bytes: usize,
    limits: EntryInspectionLimits,
) -> Result<(), EntryInspectionError> {
    if key.is_empty() {
        return Err(EntryInspectionError::EmptyKey);
    }
    if bytes > limits.max_key_bytes.get() {
        return Err(EntryInspectionError::KeyTooLong {
            bytes,
            max: limits.max_key_bytes.get(),
        });
    }
    Ok(())
}

fn charge_mutation<B: EntryBudget>(
    key: &String,
    value: &Vec<u8>,
    budget: &B,
) -> Result<(), EntryMutationError> {
    charge(&[key.capacity(), value.capacity()], budget.bytes()).map_err(|required| {
        EntryMutationError::BudgetTooSmall {
            required,
            available: budget.bytes(),
        }
    })
}
