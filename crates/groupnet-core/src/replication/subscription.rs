//! Bounded, source-native identities and receipts for durable subscriptions.

use std::fmt;
use std::num::{NonZeroU64, NonZeroUsize};

use super::{
    BoundComparison, Comparison, Cursor, IdentityError, Scope, SourceHistory, SourceProof,
};

mod delivery;
mod terminal;
pub use delivery::{
    CommitSubscriberAck, DeliveryRequest, DurableDeliveryReceipt, SubscriberAckReceipt,
};
pub use terminal::{TerminalReason, TerminalReceipt, TerminalRequest};

/// Stable subscriber name within an exact native source scope.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberId {
    /// Source-persisted subscriber name, unchanged across process restarts.
    pub name: String,
}

/// Exact native scope and named subscriber. No synthetic source partition is used.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SubscriberKey {
    /// Native source scope protected by this registration.
    pub scope: Scope,
    /// Stable name within the scope.
    pub subscriber: SubscriberId,
}

/// Source-enforced finite retention promise for one named subscriber.
///
/// Byte and event budgets are nonzero by type; [`RetentionPolicy::validate`]
/// checks the fingerprint and the age budget. A request additionally bounds
/// every field by the session's [`SubscriptionLimits`].
///
/// The fingerprint identifies the source's configured policy, so there is no
/// default policy: [`RetentionPolicy::new`] takes the fingerprint and fills
/// the budgets with the largest promise [`SubscriptionLimits::default`]
/// admits (64 MiB, a million events, a day, a million events of lag).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Stable canonical fingerprint of the source's configured policy; nonempty.
    pub fingerprint: Vec<u8>,
    /// Maximum protected suffix bytes before explicit terminal expiry.
    pub max_bytes: NonZeroU64,
    /// Maximum protected native events before explicit terminal expiry.
    pub max_events: NonZeroU64,
    /// Maximum age of an unacknowledged native event, in logical
    /// milliseconds; nonzero.
    pub max_age_ms: u64,
    /// Maximum native event lag before explicit terminal expiry.
    pub max_lag_events: NonZeroU64,
}

/// Invalid [`RetentionPolicy`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RetentionPolicyError {
    /// [`RetentionPolicy::fingerprint`] is empty.
    EmptyFingerprint,
    /// [`RetentionPolicy::max_age_ms`] is zero.
    ZeroMaxAge,
}

impl fmt::Display for RetentionPolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::EmptyFingerprint => "retention policy fingerprint must be nonempty",
            Self::ZeroMaxAge => "retention policy max_age_ms must be nonzero",
        })
    }
}

impl std::error::Error for RetentionPolicyError {}

impl RetentionPolicy {
    /// A policy identified by `fingerprint`, with the default budgets; refine
    /// them with struct-update syntax. An empty fingerprint is reported by
    /// [`validate`](Self::validate), never replaced.
    #[must_use]
    pub fn new(fingerprint: impl Into<Vec<u8>>) -> Self {
        let limits = SubscriptionLimits::default();
        Self {
            fingerprint: fingerprint.into(),
            max_bytes: limits.max_retained_bytes,
            max_events: limits.max_retained_events,
            max_age_ms: limits.max_retained_age_ms,
            max_lag_events: limits.max_lag_events,
        }
    }

    /// Checks the policy's own rules, independent of session limits.
    ///
    /// # Errors
    /// Returns the [`RetentionPolicyError`] variant naming the first violated rule.
    pub fn validate(&self) -> Result<(), RetentionPolicyError> {
        if self.fingerprint.is_empty() {
            return Err(RetentionPolicyError::EmptyFingerprint);
        }
        if self.max_age_ms == 0 {
            return Err(RetentionPolicyError::ZeroMaxAge);
        }
        Ok(())
    }
}

/// Finite identity and retained-history bounds for one subscription session.
///
/// Byte and event caps are nonzero by type; [`SubscriptionLimits::validate`]
/// checks the age cap.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionLimits {
    /// Maximum UTF-8 subscriber name bytes.
    pub max_subscriber_bytes: NonZeroUsize,
    /// Maximum stable request identifier bytes.
    pub max_request_bytes: NonZeroUsize,
    /// Maximum source-native opaque epoch bytes.
    pub max_epoch_bytes: NonZeroUsize,
    /// Maximum canonical retention-policy fingerprint bytes.
    pub max_policy_bytes: NonZeroUsize,
    /// Maximum protected suffix bytes advertised in a policy.
    pub max_retained_bytes: NonZeroU64,
    /// Maximum protected event count advertised in a policy.
    pub max_retained_events: NonZeroU64,
    /// Maximum protected event age advertised in a policy, in logical
    /// milliseconds; nonzero.
    pub max_retained_age_ms: u64,
    /// Maximum event lag advertised in a policy.
    pub max_lag_events: NonZeroU64,
}

impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_subscriber_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_request_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_epoch_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_policy_bytes: const { NonZeroUsize::new(256).unwrap() },
            max_retained_bytes: const { NonZeroU64::new(64 * 1024 * 1024).unwrap() },
            max_retained_events: const { NonZeroU64::new(1_000_000).unwrap() },
            max_retained_age_ms: 24 * 60 * 60 * 1000,
            max_lag_events: const { NonZeroU64::new(1_000_000).unwrap() },
        }
    }
}

/// Invalid [`SubscriptionLimits`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionLimitsError {
    /// [`SubscriptionLimits::max_retained_age_ms`] is zero.
    ZeroMaxRetainedAge,
}

impl fmt::Display for SubscriptionLimitsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ZeroMaxRetainedAge => {
                f.write_str("subscription max_retained_age_ms must be nonzero")
            }
        }
    }
}

impl std::error::Error for SubscriptionLimitsError {}

impl SubscriptionLimits {
    /// Checks that the retained-age cap admits a policy.
    ///
    /// # Errors
    /// Returns [`SubscriptionLimitsError::ZeroMaxRetainedAge`] for a zero age cap.
    pub fn validate(&self) -> Result<(), SubscriptionLimitsError> {
        if self.max_retained_age_ms == 0 {
            return Err(SubscriptionLimitsError::ZeroMaxRetainedAge);
        }
        Ok(())
    }
}

fn validate_request_metadata(
    key: &SubscriberKey,
    policy: &RetentionPolicy,
    request_id: &[u8],
    max_cursor_bytes: usize,
    limits: SubscriptionLimits,
) -> Result<(), SubscriptionError> {
    limits.validate().map_err(SubscriptionError::Limits)?;
    policy.validate().map_err(SubscriptionError::Retention)?;
    if key.subscriber.name.is_empty()
        || key.subscriber.name.len() > limits.max_subscriber_bytes.get()
        || request_id.is_empty()
        || request_id.len() > limits.max_request_bytes.get()
        || policy.fingerprint.len() > limits.max_policy_bytes.get()
        || policy.max_bytes > limits.max_retained_bytes
        || policy.max_events > limits.max_retained_events
        || policy.max_age_ms > limits.max_retained_age_ms
        || policy.max_lag_events > limits.max_lag_events
    {
        return Err(SubscriptionError::Bounds);
    }
    key.scope
        .validate(max_cursor_bytes)
        .map_err(SubscriptionError::Identity)
}

/// Rejected subscription identity, certificate, or source guarantee.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SubscriptionError {
    /// Scope or native cursor has invalid identity or exceeds its bound.
    Identity(IdentityError),
    /// The session's [`SubscriptionLimits`] are invalid.
    Limits(SubscriptionLimitsError),
    /// The requested [`RetentionPolicy`] is invalid on its own.
    Retention(RetentionPolicyError),
    /// A required finite field is empty, zero, or exceeds its limit.
    Bounds,
    /// Source-native history differs from the registered history.
    History,
    /// A reply does not bind the exact stable request, policy, or key.
    Binding,
    /// The source cannot protect the requested retained suffix.
    Unsupported,
    /// Requested native history is already unavailable.
    HistoryUnavailable,
    /// Source or sink cannot admit the finite retention/transaction budget.
    Backpressured,
    /// Conditional registration found a different durable prior epoch; no write occurred.
    ConditionalMismatch,
    /// Source durably expired this named subscriber before the requested cursor.
    Expired,
    /// Durable fence ordinal history was lost, reused, or exhausted.
    FenceLost,
}

impl fmt::Display for SubscriptionError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::Identity(_) => "invalid subscription scope or cursor identity",
            Self::Limits(_) => "invalid subscription limits",
            Self::Retention(_) => "invalid retention policy",
            Self::Bounds => "a subscription field is empty, zero, or exceeds its limit",
            Self::History => "source history differs from the registered history",
            Self::Binding => "reply does not bind the exact request, policy, or key",
            Self::Unsupported => "source cannot protect the requested retained suffix",
            Self::HistoryUnavailable => "requested native history is unavailable",
            Self::Backpressured => "source or sink cannot admit the retention budget",
            Self::ConditionalMismatch => "conditional registration found a different prior epoch",
            Self::Expired => "source expired this subscriber before the requested cursor",
            Self::FenceLost => "durable fence ordinal history was lost, reused, or exhausted",
        })
    }
}

impl std::error::Error for SubscriptionError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Identity(error) => Some(error),
            Self::Limits(error) => Some(error),
            Self::Retention(error) => Some(error),
            _ => None,
        }
    }
}

/// Exact request that must be read back by the same stable ID after ambiguity.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterSubscriber {
    /// Exact native scope and stable subscriber name.
    pub key: SubscriberKey,
    /// Fresh nonzero process incarnation for this stable subscriber.
    pub incarnation: NonZeroU64,
    /// Explicit native start; registration never silently attaches at head.
    pub start: Cursor,
    /// Finite source-enforced retention and canonical policy fingerprint.
    pub policy: RetentionPolicy,
    /// Stable request ID reused for readback and retry, never an operation token.
    pub request_id: Vec<u8>,
    /// Conditional prior source ordinal. When present, the source must also
    /// compare its current durable ack to `start` atomically; `None` claims
    /// an absent key for `StartAt`.
    pub expected_prior_ordinal: Option<NonZeroU64>,
    /// Explicit source-durable terminal predecessor for a reset. The source
    /// must compare this tombstone atomically with its per-name fence ledger.
    pub reset_from: Option<TerminalReceipt>,
}

/// Resume a stable name from its source-authoritative durable ack cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResumeSubscriber {
    /// Exact native scope and stable subscriber name to read back.
    pub key: SubscriberKey,
    /// Fresh nonzero process incarnation for conditional replacement.
    pub incarnation: NonZeroU64,
    /// Policy whose fingerprint must match the durable registration.
    pub policy: RetentionPolicy,
    /// Stable replacement request ID used for unknown-outcome readback.
    pub request_id: Vec<u8>,
}

impl ResumeSubscriber {
    /// Checks bounded identity and policy before source-current readback.
    ///
    /// # Errors
    /// Returns an identity or bounds error for malformed metadata.
    pub fn validate(
        &self,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        validate_request_metadata(
            &self.key,
            &self.policy,
            &self.request_id,
            max_cursor_bytes,
            limits,
        )
    }
}

impl RegisterSubscriber {
    /// Checks local structural bounds before the source is contacted.
    ///
    /// # Errors
    /// Returns an identity or bounds error for malformed scoped metadata.
    pub fn validate(
        &self,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        validate_request_metadata(
            &self.key,
            &self.policy,
            &self.request_id,
            max_cursor_bytes,
            limits,
        )?;
        self.start
            .validate(&self.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if let Some(prior) = &self.reset_from {
            prior.request.validate(max_cursor_bytes, limits)?;
            prior.validate_against(&prior.request)?;
            if prior.request.key != self.key
                || self.expected_prior_ordinal != Some(prior.tombstone_ordinal)
            {
                return Err(SubscriptionError::Binding);
            }
        }
        Ok(())
    }
}

/// Source-certified replacement fence for exactly one subscriber lineage.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubscriptionEpoch {
    /// Durable strictly increasing ordinal for this stable subscriber key.
    pub ordinal: NonZeroU64,
    /// Source-native epoch value, opaque to Groupnet and bound to the ordinal.
    pub native: Vec<u8>,
    /// Exact source history protected by this registration.
    pub history: SourceHistory,
}

/// Source-backed registration protecting a native suffix before delivery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterReceipt {
    /// Exact key requested by the subscriber.
    pub key: SubscriberKey,
    /// Exact process incarnation registered by the source.
    pub incarnation: NonZeroU64,
    /// Stable request ID that produced or read back this registration.
    pub request_id: Vec<u8>,
    /// Durable source epoch and monotonic sink-fence ordinal.
    pub epoch: SubscriptionEpoch,
    /// Source-acknowledged native cursor whose suffix is protected.
    pub protected: Cursor,
    /// Source-verified retained suffix and committed head at registration.
    pub proof: SourceProof,
    /// Bound retained boundary <= protected cursor relation.
    pub retained_to_protected: BoundComparison,
    /// Bound protected cursor <= committed head relation.
    pub protected_to_head: BoundComparison,
    /// Exact configured policy fingerprint accepted by the source.
    pub policy_fingerprint: Vec<u8>,
}

/// Source-certified current durable acknowledgement for an existing name.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SourceSubscriberState {
    /// Exact existing subscriber key.
    pub key: SubscriberKey,
    /// Current durable source epoch and monotonic fence ordinal.
    pub epoch: SubscriptionEpoch,
    /// Exact durable source ack from which retention is protected.
    pub acknowledged: Cursor,
    /// Canonical fingerprint of the still-active retention policy.
    pub policy_fingerprint: Vec<u8>,
    /// Source-verified retained suffix and committed head at readback.
    pub proof: SourceProof,
    /// Bound retained boundary <= acknowledged cursor relation.
    pub retained_to_ack: BoundComparison,
    /// Bound acknowledged cursor <= committed head relation.
    pub ack_to_head: BoundComparison,
}

impl SourceSubscriberState {
    /// Checks exact stable name/policy and proof-bound retained ack position.
    ///
    /// # Errors
    /// Returns a binding, history, identity, or bounds error on contradiction.
    pub fn validate_against(
        &self,
        request: &ResumeSubscriber,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        request.validate(max_cursor_bytes, limits)?;
        if self.key != request.key || self.policy_fingerprint != request.policy.fingerprint {
            return Err(SubscriptionError::Binding);
        }
        self.validate_current(&request.key, max_cursor_bytes, limits)
    }

    /// Checks one source-current name and protected ack without needing a
    /// sink or a policy chosen by a restarting process.
    ///
    /// # Errors
    /// Returns a binding, history, identity, or bounds error on contradiction.
    pub fn validate_current(
        &self,
        key: &SubscriberKey,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        self.validate_current_with_gap(key, max_cursor_bytes, limits, false)
    }

    /// Checks an exact source-current epoch and ack for conditional terminal
    /// release. A retained-event gap does not prevent relinquishing this
    /// lineage; it still forbids `EventComplete` replay or resume.
    ///
    /// # Errors
    /// Returns invalid or unbound source-current metadata.
    pub fn validate_terminal_target(
        &self,
        key: &SubscriberKey,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        self.validate_current_with_gap(key, max_cursor_bytes, limits, true)
    }

    fn validate_current_with_gap(
        &self,
        key: &SubscriberKey,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
        allow_gap: bool,
    ) -> Result<(), SubscriptionError> {
        key.scope
            .validate(max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if self.key != *key {
            return Err(SubscriptionError::Binding);
        }
        limits.validate().map_err(SubscriptionError::Limits)?;
        if key.subscriber.name.is_empty()
            || key.subscriber.name.len() > limits.max_subscriber_bytes.get()
            || self.policy_fingerprint.is_empty()
            || self.policy_fingerprint.len() > limits.max_policy_bytes.get()
        {
            return Err(SubscriptionError::Bounds);
        }
        if self.epoch.native.is_empty() || self.epoch.native.len() > limits.max_epoch_bytes.get() {
            return Err(SubscriptionError::Bounds);
        }
        self.acknowledged
            .validate(&key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        self.proof
            .validate(&key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if self.epoch.history != self.acknowledged.history
            || self.proof.head.history != self.acknowledged.history
        {
            return Err(SubscriptionError::History);
        }
        let retained_to_ack = self.retained_to_ack.for_operands(
            &self.proof.retained_from,
            &self.acknowledged,
            &self.proof.id,
        );
        if retained_to_ack == Some(Comparison::After) && !allow_gap {
            return Err(SubscriptionError::HistoryUnavailable);
        }
        if retained_to_ack.is_none()
            || !matches!(
                self.ack_to_head
                    .for_operands(&self.acknowledged, &self.proof.head, &self.proof.id,),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Err(SubscriptionError::Binding);
        }
        Ok(())
    }
}

impl RegisterReceipt {
    /// Checks exact request binding and bounded epoch metadata. Native cursor
    /// order remains the source adapter's proof-bound responsibility.
    ///
    /// # Errors
    /// Returns a binding, history, identity, or bounds error on contradiction.
    pub fn validate_against(
        &self,
        request: &RegisterSubscriber,
        max_cursor_bytes: usize,
        limits: SubscriptionLimits,
    ) -> Result<(), SubscriptionError> {
        request.validate(max_cursor_bytes, limits)?;
        if self.key != request.key
            || self.incarnation != request.incarnation
            || self.request_id != request.request_id
            || self.policy_fingerprint != request.policy.fingerprint
        {
            return Err(SubscriptionError::Binding);
        }
        if self.epoch.native.is_empty() || self.epoch.native.len() > limits.max_epoch_bytes.get() {
            return Err(SubscriptionError::Bounds);
        }
        if request
            .expected_prior_ordinal
            .is_some_and(|prior| self.epoch.ordinal <= prior)
        {
            return Err(SubscriptionError::FenceLost);
        }
        if self.epoch.history != request.start.history
            || self.protected.history != request.start.history
        {
            return Err(SubscriptionError::History);
        }
        self.protected
            .validate(&request.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        self.proof
            .validate(&request.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if self.protected != request.start {
            // First-slice registration protects the exact explicit cursor.
            // A source advance must be read back and submitted explicitly.
            return Err(SubscriptionError::Binding);
        }
        if self.proof.head.history != request.start.history
            || !matches!(
                self.retained_to_protected.for_operands(
                    &self.proof.retained_from,
                    &self.protected,
                    &self.proof.id,
                ),
                Some(Comparison::Before | Comparison::Equal)
            )
            || !matches!(
                self.protected_to_head.for_operands(
                    &self.protected,
                    &self.proof.head,
                    &self.proof.id,
                ),
                Some(Comparison::Before | Comparison::Equal)
            )
        {
            return Err(SubscriptionError::Binding);
        }
        Ok(())
    }
}

/// Sink's durable epoch fence and recovered application cursor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FencedCheckpoint {
    /// Exact subscriber key bound in the sink transaction.
    pub key: SubscriberKey,
    /// Exact source registration request that was durably fenced.
    pub request_id: Vec<u8>,
    /// Epoch and monotonic ordinal atomically stored by the sink.
    pub epoch: SubscriptionEpoch,
    /// Recovered durable effects/cursor, possibly ahead of the source ack.
    pub cursor: Cursor,
    /// Proof that the sink epoch and application cursor are atomically recoverable.
    pub durable: bool,
}

impl FencedCheckpoint {
    /// Checks the exact registration fence and scoped sink cursor.
    ///
    /// # Errors
    /// Returns a binding, history, or identity error on contradiction.
    pub fn validate_against(
        &self,
        receipt: &RegisterReceipt,
        max_cursor_bytes: usize,
    ) -> Result<(), SubscriptionError> {
        if !self.durable
            || self.key != receipt.key
            || self.request_id != receipt.request_id
            || self.epoch != receipt.epoch
        {
            return Err(SubscriptionError::Binding);
        }
        if self.cursor.history != receipt.epoch.history {
            return Err(SubscriptionError::History);
        }
        self.cursor
            .validate(&receipt.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::{ProofId, Stream};

    fn request() -> RegisterSubscriber {
        let scope = Scope {
            stream: Stream {
                group: "g".into(),
                topic: "t".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        };
        RegisterSubscriber {
            key: SubscriberKey {
                scope: scope.clone(),
                subscriber: SubscriberId {
                    name: "sink".into(),
                },
            },
            incarnation: NonZeroU64::new(7).expect("nonzero"),
            start: Cursor {
                scope,
                history: SourceHistory {
                    source: "native".into(),
                    generation: 1,
                },
                position: vec![1],
            },
            policy: RetentionPolicy {
                fingerprint: vec![9],
                max_bytes: NonZeroU64::new(100).unwrap(),
                max_events: NonZeroU64::new(10).unwrap(),
                max_age_ms: 1000,
                max_lag_events: NonZeroU64::new(10).unwrap(),
            },
            request_id: vec![3],
            expected_prior_ordinal: None,
            reset_from: None,
        }
    }

    #[test]
    fn registration_binding_is_exact_and_bounded() {
        let request = request();
        let limits = SubscriptionLimits::default();
        request.validate(256, limits).expect("bounded request");
        let mut receipt = RegisterReceipt {
            key: request.key.clone(),
            incarnation: request.incarnation,
            request_id: request.request_id.clone(),
            epoch: SubscriptionEpoch {
                ordinal: NonZeroU64::new(1).expect("nonzero"),
                native: vec![2],
                history: request.start.history.clone(),
            },
            protected: request.start.clone(),
            proof: SourceProof {
                id: ProofId(vec![4]),
                head: request.start.clone(),
                retained_from: request.start.clone(),
                read_authority: false,
            },
            retained_to_protected: BoundComparison {
                left: request.start.clone(),
                right: request.start.clone(),
                proof: ProofId(vec![4]),
                order: Comparison::Equal,
            },
            protected_to_head: BoundComparison {
                left: request.start.clone(),
                right: request.start.clone(),
                proof: ProofId(vec![4]),
                order: Comparison::Equal,
            },
            policy_fingerprint: request.policy.fingerprint.clone(),
        };
        receipt
            .validate_against(&request, 256, limits)
            .expect("exact receipt");
        receipt.policy_fingerprint = vec![8];
        assert_eq!(
            receipt.validate_against(&request, 256, limits),
            Err(SubscriptionError::Binding)
        );
        receipt.policy_fingerprint = request.policy.fingerprint.clone();
        receipt.protected.position = vec![2];
        assert_eq!(
            receipt.validate_against(&request, 256, limits),
            Err(SubscriptionError::Binding)
        );
        receipt.protected = request.start.clone();
        receipt.epoch.history.generation += 1;
        assert_eq!(
            receipt.validate_against(&request, 256, limits),
            Err(SubscriptionError::History)
        );
    }

    #[test]
    fn limits_and_policy_validate_their_own_rules() {
        let limits = SubscriptionLimits::default();
        assert_eq!(limits.validate(), Ok(()));
        let zero_age = SubscriptionLimits {
            max_retained_age_ms: 0,
            ..limits
        };
        assert_eq!(
            zero_age.validate(),
            Err(SubscriptionLimitsError::ZeroMaxRetainedAge)
        );
        let request = request();
        assert_eq!(
            request.validate(256, zero_age),
            Err(SubscriptionError::Limits(
                SubscriptionLimitsError::ZeroMaxRetainedAge
            ))
        );
        let error = SubscriptionError::Limits(SubscriptionLimitsError::ZeroMaxRetainedAge);
        let source = std::error::Error::source(&error).expect("nested limits error");
        assert_eq!(
            source.to_string(),
            "subscription max_retained_age_ms must be nonzero"
        );
        assert_eq!(error.to_string(), "invalid subscription limits");

        let mut policy = request.policy.clone();
        assert_eq!(policy.validate(), Ok(()));
        policy.max_age_ms = 0;
        assert_eq!(policy.validate(), Err(RetentionPolicyError::ZeroMaxAge));
        policy.max_age_ms = 1000;
        policy.fingerprint.clear();
        assert_eq!(
            policy.validate(),
            Err(RetentionPolicyError::EmptyFingerprint)
        );
        let mut empty = request.clone();
        empty.policy = policy;
        let error = empty.validate(256, limits).expect_err("empty fingerprint");
        assert_eq!(
            error,
            SubscriptionError::Retention(RetentionPolicyError::EmptyFingerprint)
        );
        assert_eq!(
            std::error::Error::source(&error).map(ToString::to_string),
            Some("retention policy fingerprint must be nonempty".to_owned())
        );
    }

    /// The fingerprint-only constructor yields a valid policy that the
    /// default session limits admit, and never replaces an empty fingerprint.
    #[test]
    fn new_policy_is_valid_and_admitted_by_default_limits() {
        let policy = RetentionPolicy::new(vec![9]);
        assert_eq!(policy.validate(), Ok(()));
        let mut request = request();
        request.policy = policy;
        request
            .validate(256, SubscriptionLimits::default())
            .expect("the default budgets fit the default limits");
        assert_eq!(
            RetentionPolicy::new(Vec::new()).validate(),
            Err(RetentionPolicyError::EmptyFingerprint)
        );
    }
}
