//! Shared finite-window clock-rate arithmetic without I/O or a clock source.

use super::PolicyError;

/// Duration and clock-rate bounds for one finite source admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ExpiryTiming {
    /// Maximum local admission duration in milliseconds.
    pub max_duration_ms: u64,
    /// Upper ratio numerator for the waiting clock against the holder clock.
    pub rate_numerator: u64,
    /// Upper ratio denominator for the waiting clock against the holder clock.
    pub rate_denominator: u64,
    /// Nonzero timer quantization and scheduling margin in milliseconds.
    pub clock_margin_ms: u64,
}

impl ExpiryTiming {
    /// Validate finite positive bounds and checked expiry arithmetic.
    ///
    /// # Errors
    /// Returns [`PolicyError`] for invalid or overflowing values.
    pub fn validate(self) -> Result<(), PolicyError> {
        self.expiry_wait_ms().map(|_| ())
    }

    /// Conservative wait before another participant may expire one version.
    ///
    /// The holder starts its local deadline before it publishes or renews
    /// that exact version. It self-fences at the deadline, including after a
    /// pause. The waiter observes the confirmed version and starts the full
    /// returned wait then. It may remove only that unchanged version with an
    /// atomic conditional write. A renewal creates a new version. A restart
    /// starts a new full wait. Both sides must use the same validated fleet
    /// policy and its rate bound. A persisted wall timestamp is not authority.
    ///
    /// # Errors
    /// Returns [`PolicyError`] for invalid or overflowing values.
    pub fn expiry_wait_ms(self) -> Result<u64, PolicyError> {
        if self.max_duration_ms == 0
            || self.rate_denominator == 0
            || self.rate_numerator < self.rate_denominator
            || self.clock_margin_ms == 0
        {
            return Err(PolicyError::Invalid);
        }
        let product = u128::from(self.max_duration_ms)
            .checked_mul(u128::from(self.rate_numerator))
            .ok_or(PolicyError::Overflow)?;
        let divisor = u128::from(self.rate_denominator);
        let rounded = product
            .checked_add(divisor - 1)
            .ok_or(PolicyError::Overflow)?
            / divisor;
        u64::try_from(rounded)
            .ok()
            .and_then(|wait| wait.checked_add(self.clock_margin_ms))
            .ok_or(PolicyError::Overflow)
    }
}

#[cfg(test)]
mod tests {
    use super::{ExpiryTiming, PolicyError};

    #[test]
    fn rounded_wait_is_conservative_and_rejects_invalid_bounds() {
        let timing = ExpiryTiming {
            max_duration_ms: 7,
            rate_numerator: 11,
            rate_denominator: 10,
            clock_margin_ms: 1,
        };
        assert_eq!(timing.expiry_wait_ms(), Ok(9));
        assert_eq!(timing.validate(), Ok(()));
        assert_eq!(
            ExpiryTiming {
                clock_margin_ms: 0,
                ..timing
            }
            .validate(),
            Err(PolicyError::Invalid)
        );
        assert_eq!(
            ExpiryTiming {
                max_duration_ms: u64::MAX,
                rate_numerator: u64::MAX,
                rate_denominator: 1,
                ..timing
            }
            .validate(),
            Err(PolicyError::Overflow)
        );
    }
}
