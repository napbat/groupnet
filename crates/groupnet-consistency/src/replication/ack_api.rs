//! Optional source-certified evidence for bounded named acknowledgement waits.

use std::future::Future;
use std::pin::Pin;

use groupnet_core::replication::{
    AckEvidence, AckKind, AckTarget, AckWaitError, AckWaitLimits, AckWaitOutcome, AckWaitRequest,
    CertifiedRoster, Operation, RequiredSubscriber, Scope,
};

/// Public wait request before the worker stamps its private logical deadline.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedAckRequest {
    /// Stable caller request ID for retries and authoritative readback.
    pub request_id: Vec<u8>,
    /// Exact externally committed native target or mutation intent.
    pub target: AckTarget,
    /// Milestone requested from every pinned named subscriber.
    pub kind: AckKind,
    /// Source-certified fixed roster, including pinned registration epochs.
    pub roster: CertifiedRoster,
}

/// Bound result of one exact named wait; no outcome changes the external
/// source commit or substitutes one acknowledgement kind for another.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NamedAckResult {
    /// Stable caller request ID whose fixed roster was certified.
    pub request_id: Vec<u8>,
    /// Exact source-native target or intent that was waited on.
    pub target: AckTarget,
    /// Required proof kind for every pinned subscriber.
    pub kind: AckKind,
    /// Typed fixed-set result or degradation.
    pub outcome: AckWaitOutcome,
}

pub(crate) fn validate_named(
    request: &NamedAckRequest,
    scope: &Scope,
    limits: AckWaitLimits,
) -> Result<(), AckWaitError> {
    if request.roster.required.len() > limits.max_required {
        return Err(AckWaitError::Backpressure);
    }
    if request.target != request.roster.target
        || request.kind != request.roster.kind
        || request.roster.policy_version == 0
        || !matches!(
            (&request.kind, &request.target),
            (AckKind::Invalidated, AckTarget::Intent { .. })
                | (AckKind::Materialized, AckTarget::Cursor(_))
        )
    {
        return Err(AckWaitError::Roster);
    }
    if request.target.scope() != scope || scope.validate(limits.max_identity_bytes).is_err() {
        return Err(AckWaitError::Identity);
    }
    let mut total = 0usize;
    let mut charge = |field: &[u8], cap: usize| -> Result<(), AckWaitError> {
        if field.is_empty() {
            return Err(AckWaitError::Identity);
        }
        if field.len() > cap {
            return Err(AckWaitError::Backpressure);
        }
        total = total
            .checked_add(field.len())
            .ok_or(AckWaitError::Backpressure)?;
        if total > limits.max_metadata_bytes {
            return Err(AckWaitError::Backpressure);
        }
        Ok(())
    };
    charge(&request.request_id, limits.max_identity_bytes)?;
    charge(
        request.roster.certificate.as_slice(),
        limits.max_certificate_bytes,
    )?;
    for target in [&request.target, &request.roster.target] {
        for name in [
            &scope.stream.group,
            &scope.stream.topic,
            &scope.stream.kind,
            &scope.partition,
            &target.history().source,
        ] {
            charge(name.as_bytes(), limits.max_identity_bytes)?;
        }
        match target {
            AckTarget::Intent { id, .. } => charge(id, limits.max_identity_bytes)?,
            AckTarget::Cursor(cursor) => {
                cursor
                    .validate(scope, limits.max_identity_bytes)
                    .map_err(|_| AckWaitError::Identity)?;
                charge(&cursor.position, limits.max_identity_bytes)?;
            }
        }
    }
    let mut prior = std::collections::BTreeSet::new();
    for required in &request.roster.required {
        charge(required.name.as_bytes(), limits.max_identity_bytes)?;
        charge(&required.epoch, limits.max_identity_bytes)?;
        if required.incarnation == 0 || !prior.insert(required.name.as_str()) {
            return Err(AckWaitError::Roster);
        }
    }
    Ok(())
}

/// Source result of one bounded evidence observation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AckObservation {
    /// One source-verified acknowledgement for the exact poll operation.
    Evidence(Box<AckEvidence>),
    /// No matching evidence is currently committed; the core schedules a retry.
    Pending,
    /// The source can no longer certify this target, roster, or history.
    AuthorityLost,
}

/// Failure before the core can treat source evidence as authoritative.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckSourceFailure {
    /// This source cannot certify named subscriber evidence.
    Unsupported,
    /// Source authority or the certified history was lost.
    AuthorityLost,
    /// A transient source error; the core's bounded polling budget still applies.
    Retryable,
    /// Invalid or contradictory source evidence; fail closed.
    Terminal,
}

/// Failure to admit a named wait; no external source commit is rolled back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AckWaitStartError {
    /// No source-certified evidence adapter was attached before sessions opened.
    Unsupported,
    /// A wait or its certification is already active, or a finite queue is full.
    Backpressured,
    /// The request exceeded a bound or contradicted its source certificate.
    Invalid(AckWaitError),
    /// The local worker was closed before admission.
    Closed,
}

/// Boxed standard future used only by the opt-in metadata evidence bridge.
pub type AckSourceFuture<'a, T> =
    Pin<Box<dyn Future<Output = Result<T, AckSourceFailure>> + Send + 'a>>;

/// Trusted local adapter that checks named rosters and individual evidence
/// against the authoritative source, never against gossip or peer assertions.
///
/// The source may own its own durable ack ledger. This trait does not register
/// `EventComplete` subscribers, retain their native history, or certify
/// lease-holder rosters without the separate source-ordered admission proof.
/// Futures must be cancellation-safe and must not retain uncharged background
/// allocations after their deadline or handle is dropped.
pub trait AckEvidenceSource: Send + Sync + 'static {
    /// Validates even an empty fixed roster before the core starts the wait.
    fn certify<'a>(
        &'a self,
        request: &'a NamedAckRequest,
        limits: AckWaitLimits,
    ) -> AckSourceFuture<'a, ()>;

    /// Reads at most one verified acknowledgement for this exact poll token.
    /// Empty checks return [`AckObservation::Pending`] so the core schedules
    /// the next source-tail check without requiring a feed hint.
    fn observe<'a>(
        &'a self,
        request: &'a AckWaitRequest,
        poll: Operation,
        waiting: &'a [RequiredSubscriber],
        limits: AckWaitLimits,
    ) -> AckSourceFuture<'a, AckObservation>;
}

#[cfg(test)]
mod tests {
    use super::{NamedAckRequest, validate_named};
    use groupnet_core::replication::{
        AckKind, AckTarget, AckWaitError, AckWaitLimits, CertifiedRoster, RequiredSubscriber,
        Scope, SourceHistory, Stream,
    };

    fn request() -> (Scope, AckWaitLimits, NamedAckRequest) {
        let scope = Scope {
            stream: Stream {
                group: "g".into(),
                topic: "t".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        };
        let target = AckTarget::Intent {
            scope: scope.clone(),
            history: SourceHistory {
                source: "native".into(),
                generation: 1,
            },
            id: vec![1],
        };
        let roster = CertifiedRoster {
            target: target.clone(),
            kind: AckKind::Invalidated,
            policy_version: 1,
            certificate: vec![2],
            required: vec![RequiredSubscriber {
                name: "reader".into(),
                incarnation: 1,
                epoch: vec![3],
            }],
        };
        let request = NamedAckRequest {
            request_id: vec![4],
            target,
            kind: AckKind::Invalidated,
            roster,
        };
        let limits = AckWaitLimits {
            max_required: 2,
            max_identity_bytes: 16,
            max_certificate_bytes: 16,
            max_metadata_bytes: 64,
            max_wait_ms: 1000,
            poll_ms: 10,
        };
        (scope, limits, request)
    }

    #[test]
    fn public_request_is_bounded_before_clone_or_source_call() {
        let (scope, limits, mut request) = request();
        assert_eq!(validate_named(&request, &scope, limits), Ok(()));
        request
            .roster
            .required
            .push(request.roster.required[0].clone());
        assert_eq!(
            validate_named(&request, &scope, limits),
            Err(AckWaitError::Roster)
        );
        request.roster.required[1].name = "other".into();
        request
            .roster
            .required
            .push(request.roster.required[0].clone());
        assert_eq!(
            validate_named(&request, &scope, limits),
            Err(AckWaitError::Backpressure)
        );
        request.roster.required.truncate(1);
        request.request_id = vec![9; 17];
        assert_eq!(
            validate_named(&request, &scope, limits),
            Err(AckWaitError::Backpressure)
        );
    }
}
