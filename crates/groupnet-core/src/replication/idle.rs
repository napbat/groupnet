//! Optional source-check backoff. A poll schedule never extends proof freshness.

use std::fmt;
use std::num::NonZeroU32;

use super::Scope;

/// Finite idle source-check policy; the independent read freshness cadence
/// remains `Config::tail_check_ms` even when polling backs off.
///
/// [`Default`] backs off after three unchanged checks to one check a minute
/// with up to one second of jitter, valid against the default replication
/// `Config::tail_check_ms` (5 s) and any cadence up to a minute.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdlePolicy {
    /// Consecutive source-certified unchanged checks before backoff starts.
    pub unchanged_checks: NonZeroU32,
    /// Maximum interval between idle source checks in logical milliseconds;
    /// nonzero and at least the session's `tail_check_ms`.
    pub max_interval_ms: u64,
    /// Maximum deterministic positive jitter, at most `max_interval_ms`.
    pub jitter_ms: u64,
}

impl Default for IdlePolicy {
    fn default() -> Self {
        Self {
            unchanged_checks: const { NonZeroU32::new(3).unwrap() },
            max_interval_ms: 60_000,
            jitter_ms: 1_000,
        }
    }
}

/// Invalid [`IdlePolicy`], naming the violated rule.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum IdlePolicyError {
    /// [`IdlePolicy::max_interval_ms`] is zero.
    ZeroMaxInterval,
    /// [`IdlePolicy::max_interval_ms`] is below the session's `tail_check_ms`.
    MaxIntervalBelowTailCheck,
    /// [`IdlePolicy::jitter_ms`] exceeds [`IdlePolicy::max_interval_ms`].
    JitterExceedsMaxInterval,
}

impl fmt::Display for IdlePolicyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            Self::ZeroMaxInterval => "idle max_interval_ms must be nonzero",
            Self::MaxIntervalBelowTailCheck => {
                "idle max_interval_ms must be at least the session tail_check_ms"
            }
            Self::JitterExceedsMaxInterval => "idle jitter_ms must not exceed max_interval_ms",
        })
    }
}

impl std::error::Error for IdlePolicyError {}

impl IdlePolicy {
    /// Checks the policy against the owning session's base `tail_check_ms`
    /// cadence, which backoff never undercuts.
    ///
    /// # Errors
    /// Returns the [`IdlePolicyError`] variant naming the first violated rule.
    pub fn validate(&self, tail_check_ms: u64) -> Result<(), IdlePolicyError> {
        if self.max_interval_ms == 0 {
            return Err(IdlePolicyError::ZeroMaxInterval);
        }
        if self.max_interval_ms < tail_check_ms {
            return Err(IdlePolicyError::MaxIntervalBelowTailCheck);
        }
        if self.jitter_ms > self.max_interval_ms {
            return Err(IdlePolicyError::JitterExceedsMaxInterval);
        }
        Ok(())
    }
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(super) struct IdleState {
    unchanged: u32,
    poll_sequence: u64,
    force_hot_once: bool,
}

impl IdleState {
    pub(super) fn reset(&mut self) {
        self.unchanged = 0;
    }

    pub(super) fn force_hot_next(&mut self) {
        self.reset();
        self.force_hot_once = true;
    }

    pub(super) fn checked_interval(
        &mut self,
        unchanged: bool,
        policy: Option<IdlePolicy>,
        base_ms: u64,
        scope: &Scope,
        session: u64,
    ) -> Option<u64> {
        let Some(policy) = policy else {
            return Some(base_ms);
        };
        self.poll_sequence = self.poll_sequence.checked_add(1)?;
        let unchanged = unchanged && !self.force_hot_once;
        self.force_hot_once = false;
        if unchanged {
            self.unchanged = self.unchanged.checked_add(1)?;
        } else {
            self.reset();
        }
        if self.unchanged < policy.unchanged_checks.get() {
            return Some(base_ms);
        }
        let shifts = self.unchanged - policy.unchanged_checks.get() + 1;
        let interval = if shifts >= 63 {
            u64::MAX
        } else {
            base_ms.saturating_mul(1u64 << shifts)
        }
        .min(policy.max_interval_ms);
        let jitter = if policy.jitter_ms == 0 {
            0
        } else {
            stable_jitter(scope, session, self.poll_sequence) % policy.jitter_ms.saturating_add(1)
        };
        Some(interval.saturating_add(jitter).min(policy.max_interval_ms))
    }
}

fn stable_jitter(scope: &Scope, session: u64, sequence: u64) -> u64 {
    // Fixed FNV-1a input and constants; no randomized process hasher, clock,
    // or RNG enters a deterministic engine decision.
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    let session_bytes = session.to_le_bytes();
    let sequence_bytes = sequence.to_le_bytes();
    for field in [
        scope.stream.group.as_bytes(),
        scope.stream.topic.as_bytes(),
        scope.stream.kind.as_bytes(),
        scope.partition.as_bytes(),
        &session_bytes,
        &sequence_bytes,
    ] {
        for byte in field {
            hash = (hash ^ u64::from(*byte)).wrapping_mul(0x0000_0100_0000_01b3);
        }
        hash = (hash ^ 0xff).wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::replication::Stream;

    fn scope() -> Scope {
        Scope {
            stream: Stream {
                group: "g".into(),
                topic: "t".into(),
                kind: "v1".into(),
            },
            partition: "p".into(),
        }
    }

    #[test]
    fn deterministic_bounded_interval_and_reset() {
        let policy = IdlePolicy {
            unchanged_checks: NonZeroU32::new(2).unwrap(),
            max_interval_ms: 80,
            jitter_ms: 4,
        };
        let mut a = IdleState::default();
        let mut b = IdleState::default();
        for unchanged in [true, true, true, true, true, false, true] {
            let x = a.checked_interval(unchanged, Some(policy), 10, &scope(), 1);
            let y = b.checked_interval(unchanged, Some(policy), 10, &scope(), 1);
            assert_eq!(x, y);
            assert!(x.unwrap() <= 80);
        }
        assert_eq!(a.unchanged, 1);
        assert_eq!(
            a.checked_interval(true, Some(policy), 10, &scope(), 1),
            b.checked_interval(true, Some(policy), 10, &scope(), 1)
        );
    }

    #[test]
    fn counter_exhaustion_fails_closed() {
        let policy = IdlePolicy {
            unchanged_checks: NonZeroU32::MIN,
            max_interval_ms: 80,
            jitter_ms: 0,
        };
        let mut state = IdleState {
            unchanged: u32::MAX,
            poll_sequence: 1,
            force_hot_once: false,
        };
        assert_eq!(
            state.checked_interval(true, Some(policy), 10, &scope(), 1),
            None
        );
        state.poll_sequence = u64::MAX;
        state.unchanged = 0;
        assert_eq!(
            state.checked_interval(false, Some(policy), 10, &scope(), 1),
            None
        );
    }

    #[test]
    fn replay_forces_one_hot_source_check_before_idle_count_resumes() {
        let policy = IdlePolicy {
            unchanged_checks: NonZeroU32::MIN,
            max_interval_ms: 80,
            jitter_ms: 0,
        };
        let mut state = IdleState::default();
        assert_eq!(
            state.checked_interval(true, Some(policy), 10, &scope(), 1),
            Some(20)
        );
        state.force_hot_next();
        assert_eq!(
            state.checked_interval(true, Some(policy), 10, &scope(), 1),
            Some(10)
        );
        assert_eq!(
            state.checked_interval(true, Some(policy), 10, &scope(), 1),
            Some(20)
        );
    }

    #[test]
    fn default_policy_is_valid_against_the_default_session_cadence() {
        let tail_check_ms = crate::replication::Config::default().tail_check_ms;
        assert_eq!(IdlePolicy::default().validate(tail_check_ms), Ok(()));
        assert_eq!(
            crate::replication::Config {
                idle: Some(IdlePolicy::default()),
                ..crate::replication::Config::default()
            }
            .validate(),
            Ok(())
        );
    }

    #[test]
    fn validate_names_each_violated_rule() {
        let policy = IdlePolicy {
            unchanged_checks: NonZeroU32::MIN,
            max_interval_ms: 80,
            jitter_ms: 4,
        };
        assert_eq!(policy.validate(10), Ok(()));
        assert_eq!(policy.validate(80), Ok(()));
        assert_eq!(
            IdlePolicy {
                max_interval_ms: 0,
                jitter_ms: 0,
                ..policy
            }
            .validate(0),
            Err(IdlePolicyError::ZeroMaxInterval)
        );
        assert_eq!(
            policy.validate(81),
            Err(IdlePolicyError::MaxIntervalBelowTailCheck)
        );
        assert_eq!(
            IdlePolicy {
                jitter_ms: 81,
                ..policy
            }
            .validate(10),
            Err(IdlePolicyError::JitterExceedsMaxInterval)
        );
        assert_eq!(
            IdlePolicyError::JitterExceedsMaxInterval.to_string(),
            "idle jitter_ms must not exceed max_interval_ms"
        );
    }
}
