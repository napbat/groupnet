//! The deadline every caller-bounded acknowledgement wait polls against.

use std::time::Duration;

use tokio::time::Instant;

/// The instant a wait bounded by a caller's `timeout` gives up.
///
/// A timeout longer than the clock can represent saturates to "never": the
/// wait then ends only on its own outcome, which is indistinguishable from a
/// deadline beyond the clock's range. The arithmetic is checked, so no
/// `Duration` — `Duration::MAX` included — overflows or panics.
#[derive(Clone, Copy, Debug)]
pub(crate) struct WaitDeadline(Option<Instant>);

impl WaitDeadline {
    /// `timeout` from now.
    pub(crate) fn after(timeout: Duration) -> Self {
        Self(Instant::now().checked_add(timeout))
    }

    /// Whether the deadline has been reached.
    pub(crate) fn passed(self) -> bool {
        self.0.is_some_and(|deadline| Instant::now() >= deadline)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::WaitDeadline;

    #[test]
    fn an_unrepresentable_timeout_saturates_instead_of_overflowing() {
        let forever = WaitDeadline::after(Duration::MAX);
        assert!(forever.0.is_none(), "Duration::MAX is beyond the clock");
        assert!(!forever.passed());
    }

    #[test]
    fn a_zero_timeout_has_already_passed() {
        assert!(WaitDeadline::after(Duration::ZERO).passed());
    }
}
