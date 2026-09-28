//! Optional source-check backoff. A poll schedule never extends proof freshness.

use super::Scope;

/// Finite idle source-check policy; the independent read freshness cadence
/// remains `Config::tail_check_ms` even when polling backs off.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct IdlePolicy {
    /// Consecutive source-certified unchanged checks before backoff starts.
    pub unchanged_checks: u32,
    /// Maximum interval between idle source checks in logical milliseconds.
    pub max_interval_ms: u64,
    /// Maximum deterministic positive jitter, capped by `max_interval_ms`.
    pub jitter_ms: u64,
}

impl IdlePolicy {
    pub(super) fn valid(self, base_ms: u64) -> bool {
        self.unchanged_checks > 0
            && self.max_interval_ms >= base_ms
            && self.jitter_ms <= self.max_interval_ms
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
        if self.unchanged < policy.unchanged_checks {
            return Some(base_ms);
        }
        let shifts = self.unchanged - policy.unchanged_checks + 1;
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
            unchanged_checks: 2,
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
            unchanged_checks: 1,
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
            unchanged_checks: 1,
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
}
