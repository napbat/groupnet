//! Validated capacities for bounded queues, channels and semaphores.
//!
//! Every networking config expresses a bounded queue as a [`QueueCapacity`],
//! so the single rule — nonzero and representable by Tokio's channels and
//! semaphores — is checked once, here, instead of by each config.

use std::fmt;
use std::io;
use std::num::NonZeroUsize;

/// Largest capacity a Tokio bounded channel or semaphore accepts
/// (`tokio::sync::Semaphore::MAX_PERMITS`), restated so the default build
/// stays free of Tokio. A unit test pins the two together.
const MAX_PERMITS: usize = usize::MAX >> 3;

/// The capacity of one bounded queue, channel or semaphore.
///
/// The invariant `1..=tokio::sync::Semaphore::MAX_PERMITS` holds for every
/// value, so `tokio::sync::mpsc::channel(capacity.get())` and
/// `Semaphore::new(capacity.get())` never panic.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct QueueCapacity(NonZeroUsize);

impl QueueCapacity {
    /// The smallest capacity: one slot.
    pub const MIN: Self = Self::of(1);

    /// The largest capacity Tokio's channels and semaphores support.
    pub const MAX: Self = Self::of(MAX_PERMITS);

    /// Validates a capacity, returning `None` for zero or a value above
    /// [`Self::MAX`].
    #[must_use]
    pub const fn new(value: usize) -> Option<Self> {
        if value > MAX_PERMITS {
            return None;
        }
        match NonZeroUsize::new(value) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }

    /// A capacity known valid at compile time, for constants and defaults.
    ///
    /// # Panics
    /// If `value` is zero or above [`Self::MAX`]; in a `const` context this is
    /// a compile error.
    #[must_use]
    pub const fn of(value: usize) -> Self {
        match Self::new(value) {
            Some(capacity) => capacity,
            None => panic!("queue capacity must be within 1..=Semaphore::MAX_PERMITS"),
        }
    }

    /// The number of slots.
    #[must_use]
    pub const fn get(self) -> usize {
        self.0.get()
    }

    /// The number of slots, as a nonzero value.
    #[must_use]
    pub const fn get_nonzero(self) -> NonZeroUsize {
        self.0
    }

    /// Scales the capacity, saturating at [`Self::MAX`]. The factor is
    /// nonzero, so the product never leaves the valid range from below and
    /// saturation is the only adjustment.
    #[must_use]
    pub const fn saturating_mul(self, factor: NonZeroUsize) -> Self {
        Self::saturated(self.get().saturating_mul(factor.get()))
    }

    /// Adds slack slots, saturating at [`Self::MAX`].
    #[must_use]
    pub const fn saturating_add(self, extra: usize) -> Self {
        Self::saturated(self.get().saturating_add(extra))
    }

    /// A nonzero slot count, saturated at [`Self::MAX`]. Callers only pass
    /// sums and products of nonzero values, so `value` is never zero.
    const fn saturated(value: usize) -> Self {
        if value > MAX_PERMITS {
            Self::MAX
        } else {
            Self::of(value)
        }
    }
}

/// Why a raw value is not a [`QueueCapacity`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QueueCapacityError {
    /// A queue must hold at least one slot.
    Zero,
    /// The value exceeds [`QueueCapacity::MAX`].
    AboveMax {
        /// The rejected value.
        value: usize,
    },
}

impl fmt::Display for QueueCapacityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Zero => f.write_str("queue capacity must be nonzero"),
            Self::AboveMax { value } => write!(
                f,
                "queue capacity {value} exceeds Tokio's semaphore limit {MAX_PERMITS}"
            ),
        }
    }
}

impl std::error::Error for QueueCapacityError {}

impl From<QueueCapacityError> for io::Error {
    fn from(error: QueueCapacityError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

impl TryFrom<usize> for QueueCapacity {
    type Error = QueueCapacityError;

    /// # Errors
    /// Returns [`QueueCapacityError::Zero`] or [`QueueCapacityError::AboveMax`].
    fn try_from(value: usize) -> Result<Self, QueueCapacityError> {
        Self::new(value).ok_or(if value == 0 {
            QueueCapacityError::Zero
        } else {
            QueueCapacityError::AboveMax { value }
        })
    }
}

impl From<QueueCapacity> for usize {
    fn from(capacity: QueueCapacity) -> Self {
        capacity.get()
    }
}

impl From<QueueCapacity> for NonZeroUsize {
    fn from(capacity: QueueCapacity) -> Self {
        capacity.0
    }
}

impl fmt::Display for QueueCapacity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.0, f)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_matches_tokio_semaphore_limit() {
        assert_eq!(
            QueueCapacity::MAX.get(),
            tokio::sync::Semaphore::MAX_PERMITS
        );
        // The extremes are accepted by the primitives they size.
        let _ = tokio::sync::Semaphore::new(QueueCapacity::MAX.get());
        let _ = tokio::sync::mpsc::channel::<()>(QueueCapacity::MIN.get());
    }

    #[test]
    fn construction_rejects_zero_and_unsupported_values() {
        assert_eq!(QueueCapacity::new(0), None);
        assert_eq!(QueueCapacity::new(MAX_PERMITS + 1), None);
        assert_eq!(QueueCapacity::new(usize::MAX), None);
        assert_eq!(QueueCapacity::new(1), Some(QueueCapacity::MIN));
        assert_eq!(QueueCapacity::new(MAX_PERMITS), Some(QueueCapacity::MAX));
        assert_eq!(QueueCapacity::try_from(0), Err(QueueCapacityError::Zero));
        assert_eq!(
            QueueCapacity::try_from(MAX_PERMITS + 1),
            Err(QueueCapacityError::AboveMax {
                value: MAX_PERMITS + 1
            })
        );
        assert_eq!(
            io::Error::from(QueueCapacityError::Zero).kind(),
            io::ErrorKind::InvalidInput
        );
        let capacity = QueueCapacity::try_from(64).unwrap();
        assert_eq!(capacity.get(), 64);
        assert_eq!(usize::from(capacity), 64);
        assert_eq!(capacity.get_nonzero().get(), 64);
        assert_eq!(capacity.to_string(), "64");
    }

    #[test]
    fn const_construction_is_checked() {
        const DEFAULT: QueueCapacity = QueueCapacity::of(16);
        assert_eq!(DEFAULT.get(), 16);
        assert!(std::panic::catch_unwind(|| QueueCapacity::of(0)).is_err());
        assert!(std::panic::catch_unwind(|| QueueCapacity::of(MAX_PERMITS + 1)).is_err());
    }

    #[test]
    fn arithmetic_saturates_only_at_the_maximum() {
        let window = QueueCapacity::of(64);
        let two = NonZeroUsize::new(2).unwrap();
        assert_eq!(window.saturating_mul(two).get(), 128);
        assert_eq!(window.saturating_mul(two).saturating_add(8).get(), 136);
        assert_eq!(window.saturating_mul(NonZeroUsize::MIN), window);
        assert_eq!(window.saturating_add(0), window);
        assert_eq!(window.saturating_mul(NonZeroUsize::MAX), QueueCapacity::MAX);
        assert_eq!(QueueCapacity::MAX.saturating_add(1), QueueCapacity::MAX);
        assert_eq!(QueueCapacity::MAX.saturating_mul(two), QueueCapacity::MAX);
    }
}
