//! Bounded, source-native identities and receipts for durable subscriptions.

use std::num::NonZeroU64;

use super::{
    BoundComparison, Comparison, Cursor, IdentityError, Scope, SourceHistory, SourceProof,
};

mod delivery;
pub use delivery::{CommitSubscriberAck, DurableDeliveryReceipt, SubscriberAckReceipt};

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
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetentionPolicy {
    /// Stable canonical fingerprint of the source's configured policy.
    pub fingerprint: Vec<u8>,
    /// Maximum protected suffix bytes before explicit terminal expiry.
    pub max_bytes: u64,
    /// Maximum protected native events before explicit terminal expiry.
    pub max_events: u64,
    /// Maximum age of an unacknowledged native event, in logical milliseconds.
    pub max_age_ms: u64,
    /// Maximum native event lag before explicit terminal expiry.
    pub max_lag_events: u64,
}

/// Finite identity and retained-history bounds for one subscription session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SubscriptionLimits {
    /// Maximum UTF-8 subscriber name bytes.
    pub max_subscriber_bytes: usize,
    /// Maximum stable request identifier bytes.
    pub max_request_bytes: usize,
    /// Maximum source-native opaque epoch bytes.
    pub max_epoch_bytes: usize,
    /// Maximum canonical retention-policy fingerprint bytes.
    pub max_policy_bytes: usize,
    /// Maximum protected suffix bytes advertised in a policy.
    pub max_retained_bytes: u64,
    /// Maximum protected event count advertised in a policy.
    pub max_retained_events: u64,
    /// Maximum protected event age advertised in a policy.
    pub max_retained_age_ms: u64,
    /// Maximum event lag advertised in a policy.
    pub max_lag_events: u64,
}

impl Default for SubscriptionLimits {
    fn default() -> Self {
        Self {
            max_subscriber_bytes: 256,
            max_request_bytes: 256,
            max_epoch_bytes: 256,
            max_policy_bytes: 256,
            max_retained_bytes: 64 * 1024 * 1024,
            max_retained_events: 1_000_000,
            max_retained_age_ms: 24 * 60 * 60 * 1000,
            max_lag_events: 1_000_000,
        }
    }
}

impl SubscriptionLimits {
    /// Returns whether every bound is positive and usable.
    #[must_use]
    pub fn valid(self) -> bool {
        self.max_subscriber_bytes > 0
            && self.max_request_bytes > 0
            && self.max_epoch_bytes > 0
            && self.max_policy_bytes > 0
            && self.max_retained_bytes > 0
            && self.max_retained_events > 0
            && self.max_retained_age_ms > 0
            && self.max_lag_events > 0
    }
}

fn validate_request_metadata(
    key: &SubscriberKey,
    policy: &RetentionPolicy,
    request_id: &[u8],
    max_cursor_bytes: usize,
    limits: SubscriptionLimits,
) -> Result<(), SubscriptionError> {
    if !limits.valid()
        || key.subscriber.name.is_empty()
        || key.subscriber.name.len() > limits.max_subscriber_bytes
        || request_id.is_empty()
        || request_id.len() > limits.max_request_bytes
        || policy.fingerprint.is_empty()
        || policy.fingerprint.len() > limits.max_policy_bytes
        || policy.max_bytes == 0
        || policy.max_bytes > limits.max_retained_bytes
        || policy.max_events == 0
        || policy.max_events > limits.max_retained_events
        || policy.max_age_ms == 0
        || policy.max_age_ms > limits.max_retained_age_ms
        || policy.max_lag_events == 0
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
    /// A required finite field is empty, zero, or too large.
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
            .map_err(SubscriptionError::Identity)
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
        if self.epoch.native.is_empty() || self.epoch.native.len() > limits.max_epoch_bytes {
            return Err(SubscriptionError::Bounds);
        }
        self.acknowledged
            .validate(&request.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        self.proof
            .validate(&request.key.scope, max_cursor_bytes)
            .map_err(SubscriptionError::Identity)?;
        if self.epoch.history != self.acknowledged.history
            || self.proof.head.history != self.acknowledged.history
        {
            return Err(SubscriptionError::History);
        }
        if !matches!(
            self.retained_to_ack.for_operands(
                &self.proof.retained_from,
                &self.acknowledged,
                &self.proof.id,
            ),
            Some(Comparison::Before | Comparison::Equal)
        ) || !matches!(
            self.ack_to_head
                .for_operands(&self.acknowledged, &self.proof.head, &self.proof.id,),
            Some(Comparison::Before | Comparison::Equal)
        ) {
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
        if self.epoch.native.is_empty() || self.epoch.native.len() > limits.max_epoch_bytes {
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
                max_bytes: 100,
                max_events: 10,
                max_age_ms: 1000,
                max_lag_events: 10,
            },
            request_id: vec![3],
            expected_prior_ordinal: None,
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
}
