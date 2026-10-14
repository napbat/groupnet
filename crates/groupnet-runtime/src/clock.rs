//! The one conversion from async-layer [`Duration`] timers to the sans-IO
//! core's logical milliseconds.

use std::time::Duration;

/// `duration` in whole core milliseconds.
///
/// The core's clock is `u64` milliseconds and only advances when the driver
/// ticks it, so a sub-millisecond remainder is rounded **up** to the next clock
/// tick (a timer never fires before its `Duration`), and a duration longer
/// than `u64::MAX` milliseconds saturates there. Both differences are below
/// what the core can observe; the conversion never overflows or panics.
pub(crate) fn core_millis(duration: Duration) -> u64 {
    let whole = duration.as_millis();
    let rounded = if duration.subsec_nanos().is_multiple_of(1_000_000) {
        whole
    } else {
        whole.saturating_add(1)
    };
    u64::try_from(rounded).unwrap_or(u64::MAX)
}

/// Core milliseconds back as a [`Duration`], which represents them exactly.
pub(crate) const fn from_core_millis(ms: u64) -> Duration {
    Duration::from_millis(ms)
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::{core_millis, from_core_millis};

    #[test]
    fn whole_milliseconds_convert_exactly_and_round_trip() {
        for ms in [0, 1, 1_500, 86_400_000] {
            assert_eq!(core_millis(Duration::from_millis(ms)), ms);
            assert_eq!(from_core_millis(ms), Duration::from_millis(ms));
        }
    }

    #[test]
    fn a_sub_millisecond_remainder_rounds_up_to_the_next_tick() {
        assert_eq!(core_millis(Duration::from_micros(1)), 1);
        assert_eq!(core_millis(Duration::from_micros(1_500)), 2);
        assert_eq!(core_millis(Duration::new(1, 1)), 1_001);
    }

    #[test]
    fn an_unrepresentable_duration_saturates() {
        assert_eq!(core_millis(Duration::MAX), u64::MAX);
        assert_eq!(
            core_millis(Duration::from_millis(u64::MAX) + Duration::from_micros(1)),
            u64::MAX
        );
    }
}
