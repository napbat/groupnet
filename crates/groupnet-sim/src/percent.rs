//! A validated share in `0..=100`, the unit of every fault-rate knob.

use std::fmt;

/// A whole percentage in `0..=100`: the share of messages
/// [`Simulation::set_loss`](crate::Simulation::set_loss) drops or of anchor
/// writes [`Simulation::set_anchor_unknown_percent`](crate::Simulation::set_anchor_unknown_percent)
/// leaves ambiguous.
///
/// Every value is in range, so the knobs taking one never clamp. Build a
/// constant with [`of`](Self::of), and a runtime value — a seeded draw, say —
/// with [`TryFrom`], which reports a value above 100 instead of correcting it.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Percent(u8);

impl Percent {
    /// No share at all: the fault never fires.
    pub const ZERO: Self = Self(0);

    /// Every one: the fault always fires.
    pub const HUNDRED: Self = Self(100);

    /// A percentage known valid at compile time, for constants and literals.
    ///
    /// # Panics
    /// If `value` exceeds 100; in a `const` context this is a compile error.
    /// Runtime values go through [`TryFrom`].
    #[must_use]
    pub const fn of(value: u8) -> Self {
        assert!(value <= 100, "a percentage is at most 100");
        Self(value)
    }

    /// The share, in `0..=100`.
    #[must_use]
    pub const fn get(self) -> u8 {
        self.0
    }
}

/// Why a raw value is not a [`Percent`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PercentError {
    /// The value exceeds 100. Carries the rejected value.
    AboveHundred(u32),
}

impl fmt::Display for PercentError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::AboveHundred(value) => write!(f, "percentage {value} exceeds 100"),
        }
    }
}

impl std::error::Error for PercentError {}

impl TryFrom<u32> for Percent {
    type Error = PercentError;

    /// # Errors
    /// [`PercentError::AboveHundred`] for a value above 100.
    fn try_from(value: u32) -> Result<Self, PercentError> {
        u8::try_from(value)
            .ok()
            .filter(|value| *value <= 100)
            .map(Self)
            .ok_or(PercentError::AboveHundred(value))
    }
}

impl TryFrom<u8> for Percent {
    type Error = PercentError;

    /// # Errors
    /// [`PercentError::AboveHundred`] for a value above 100.
    fn try_from(value: u8) -> Result<Self, PercentError> {
        Self::try_from(u32::from(value))
    }
}

impl From<Percent> for u8 {
    fn from(percent: Percent) -> Self {
        percent.0
    }
}

impl fmt::Display for Percent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}%", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{Percent, PercentError};

    /// Every value through 100 is accepted as given; everything above is
    /// rejected with the value, never clamped.
    #[test]
    fn try_from_accepts_the_range_and_rejects_above_it() {
        for value in 0..=100_u8 {
            assert_eq!(Percent::try_from(value).map(Percent::get), Ok(value));
            assert_eq!(
                Percent::try_from(u32::from(value)).map(Percent::get),
                Ok(value)
            );
        }
        for value in 101..=u8::MAX {
            assert_eq!(
                Percent::try_from(value),
                Err(PercentError::AboveHundred(u32::from(value)))
            );
        }
        for value in [256, u32::MAX] {
            assert_eq!(
                Percent::try_from(value),
                Err(PercentError::AboveHundred(value))
            );
        }
        assert_eq!(
            PercentError::AboveHundred(101).to_string(),
            "percentage 101 exceeds 100"
        );
    }

    #[test]
    fn constants_and_const_constructor_agree() {
        assert_eq!(Percent::default(), Percent::ZERO);
        assert_eq!(Percent::of(0), Percent::ZERO);
        assert_eq!(Percent::of(100), Percent::HUNDRED);
        assert_eq!(u8::from(Percent::of(37)), 37);
        assert_eq!(Percent::of(37).to_string(), "37%");
    }

    #[test]
    #[should_panic(expected = "a percentage is at most 100")]
    fn const_constructor_refuses_above_hundred() {
        let _ = Percent::of(101);
    }
}
