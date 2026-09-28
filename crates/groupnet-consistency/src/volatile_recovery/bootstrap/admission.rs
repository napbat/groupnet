//! Shared byte admission for optional volatile peer bootstrap.

use std::sync::{Arc, Mutex};

/// Distinct pools charged under one aggregate limit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionClass {
    /// Encoded image and returned chunk bytes.
    Encoded,
    /// Decoded private index state.
    Decoded,
    /// Donor suffix retained during transfer.
    Suffix,
    /// Follower native-overlap buffer.
    NativeOverlap,
    /// Returned batches and queued frame copies.
    Inflight,
}

impl AdmissionClass {
    const fn index(self) -> usize {
        match self {
            Self::Encoded => 0,
            Self::Decoded => 1,
            Self::Suffix => 2,
            Self::NativeOverlap => 3,
            Self::Inflight => 4,
        }
    }
}

/// Aggregate and per-class caps shared across all recovery scopes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AdmissionLimits {
    /// Maximum simultaneous bytes.
    pub max_total_bytes: usize,
    /// Maximum encoded image and chunk bytes.
    pub max_encoded_bytes: usize,
    /// Maximum decoded private-index bytes.
    pub max_decoded_bytes: usize,
    /// Maximum retained donor-suffix bytes.
    pub max_suffix_bytes: usize,
    /// Maximum native-overlap bytes.
    pub max_native_overlap_bytes: usize,
    /// Maximum returned-batch and queued-frame bytes.
    pub max_inflight_bytes: usize,
    /// Maximum simultaneous resource reservations.
    pub max_reservations: usize,
}

/// Failed admission never licenses an allocation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    /// Limits are zero or incoherent.
    InvalidLimits,
    /// Requested charge is zero or exceeds its class cap.
    InvalidCharge,
    /// Aggregate, class, or reservation capacity is exhausted.
    Full,
}

impl AdmissionLimits {
    const fn class_cap(self, class: AdmissionClass) -> usize {
        match class {
            AdmissionClass::Encoded => self.max_encoded_bytes,
            AdmissionClass::Decoded => self.max_decoded_bytes,
            AdmissionClass::Suffix => self.max_suffix_bytes,
            AdmissionClass::NativeOverlap => self.max_native_overlap_bytes,
            AdmissionClass::Inflight => self.max_inflight_bytes,
        }
    }

    const fn classes(self) -> [usize; 5] {
        [
            self.max_encoded_bytes,
            self.max_decoded_bytes,
            self.max_suffix_bytes,
            self.max_native_overlap_bytes,
            self.max_inflight_bytes,
        ]
    }
}

#[derive(Debug, Default)]
struct Used {
    total: usize,
    classes: [usize; 5],
    reservations: usize,
}

#[derive(Debug)]
struct Inner {
    limits: AdmissionLimits,
    used: Mutex<Used>,
}

/// Cloneable handle to one shared budget, not an independent pool.
#[derive(Clone, Debug)]
pub struct ByteAdmission(Arc<Inner>);

impl ByteAdmission {
    /// Constructs a bounded shared budget.
    ///
    /// # Errors
    /// Rejects zero or incoherent limits.
    pub fn new(limits: AdmissionLimits) -> Result<Self, AdmissionError> {
        if limits.max_total_bytes == 0
            || limits.max_reservations == 0
            || limits
                .classes()
                .iter()
                .any(|cap| *cap > limits.max_total_bytes)
        {
            return Err(AdmissionError::InvalidLimits);
        }
        Ok(Self(Arc::new(Inner {
            limits,
            used: Mutex::new(Used::default()),
        })))
    }

    /// Charges bytes and one slot before the caller allocates work.
    ///
    /// # Errors
    /// Rejects invalid charge or exhausted capacity without changing usage.
    pub fn reserve(
        &self,
        class: AdmissionClass,
        bytes: usize,
    ) -> Result<Reservation, AdmissionError> {
        let index = class.index();
        let cap = self.0.limits.class_cap(class);
        if bytes == 0 || bytes > cap {
            return Err(AdmissionError::InvalidCharge);
        }
        let mut used = self
            .0
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        let total = used.total.checked_add(bytes).ok_or(AdmissionError::Full)?;
        let class_total = used.classes[index]
            .checked_add(bytes)
            .ok_or(AdmissionError::Full)?;
        let reservations = used
            .reservations
            .checked_add(1)
            .ok_or(AdmissionError::Full)?;
        if total > self.0.limits.max_total_bytes
            || class_total > cap
            || reservations > self.0.limits.max_reservations
        {
            return Err(AdmissionError::Full);
        }
        used.total = total;
        used.classes[index] = class_total;
        used.reservations = reservations;
        Ok(Reservation {
            inner: Arc::clone(&self.0),
            class,
            bytes,
        })
    }

    /// Exact aggregate, per-class, and reservation usage.
    #[must_use]
    pub fn usage(&self) -> (usize, [usize; 5], usize) {
        let used = self
            .0
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        (used.total, used.classes, used.reservations)
    }
}

/// Non-cloneable charge retained until owned work retires.
#[derive(Debug)]
pub struct Reservation {
    inner: Arc<Inner>,
    class: AdmissionClass,
    bytes: usize,
}

impl Reservation {
    /// Wraps a value allocated after this reservation.
    #[must_use]
    pub fn hold<T>(self, value: T) -> Admitted<T> {
        Admitted {
            value,
            reservation: self,
        }
    }

    /// Exact charged bytes.
    #[must_use]
    pub const fn bytes(&self) -> usize {
        self.bytes
    }

    /// Class charged by this exact reservation.
    #[must_use]
    pub const fn class(&self) -> AdmissionClass {
        self.class
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        let mut used = self
            .inner
            .used
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        used.total -= self.bytes;
        used.classes[self.class.index()] -= self.bytes;
        used.reservations -= 1;
    }
}

/// Owned work whose value drops before its charge.
///
/// This wrapper cannot be cloned. Every returned batch copy requires another
/// reservation before allocation. Callers supply a truthful byte charge.
#[derive(Debug)]
pub struct Admitted<T> {
    value: T,
    reservation: Reservation,
}

impl<T> Admitted<T> {
    /// Borrows the admitted value.
    #[must_use]
    pub const fn get(&self) -> &T {
        &self.value
    }

    /// Consumes the value inside a scope that retains its charge through the
    /// callback. The callback must retire the value before returning; any
    /// separately retained clone needs a separate prior reservation.
    pub(crate) fn consume<R>(self, apply: impl FnOnce(T) -> R) -> R {
        let Self { value, reservation } = self;
        let result = apply(value);
        drop(reservation);
        result
    }

    /// Exact charge held through this value's lifetime.
    #[must_use]
    pub const fn charged_bytes(&self) -> usize {
        self.reservation.bytes
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;

    fn limits() -> AdmissionLimits {
        AdmissionLimits {
            max_total_bytes: 10,
            max_encoded_bytes: 10,
            max_decoded_bytes: 10,
            max_suffix_bytes: 10,
            max_native_overlap_bytes: 10,
            max_inflight_bytes: 10,
            max_reservations: 2,
        }
    }

    #[test]
    fn cloned_handles_share_byte_and_slot_caps_across_scopes() {
        let budget = ByteAdmission::new(limits()).unwrap();
        let other = budget.clone();
        let image = budget
            .reserve(AdmissionClass::Encoded, 6)
            .unwrap()
            .hold(vec![0; 6]);
        let batch = other
            .reserve(AdmissionClass::Inflight, 4)
            .unwrap()
            .hold(vec![0; 4]);
        assert_eq!(budget.usage(), (10, [6, 0, 0, 0, 4], 2));
        assert_eq!(
            budget.reserve(AdmissionClass::Suffix, 1).unwrap_err(),
            AdmissionError::Full
        );
        drop(batch);
        assert_eq!(other.usage(), (6, [6, 0, 0, 0, 0], 1));
        drop(image);
        assert_eq!(budget.usage(), (0, [0; 5], 0));
    }

    #[derive(Debug)]
    struct Retired(Arc<AtomicUsize>, ByteAdmission);

    impl Drop for Retired {
        fn drop(&mut self) {
            assert_eq!(self.1.usage().0, 1);
            self.0.store(1, Ordering::Release);
        }
    }

    #[test]
    fn payload_drops_before_charge_is_released() {
        let budget = ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 1,
            max_encoded_bytes: 1,
            max_decoded_bytes: 1,
            max_suffix_bytes: 1,
            max_native_overlap_bytes: 1,
            max_inflight_bytes: 1,
            max_reservations: 1,
        })
        .unwrap();
        let retired = Arc::new(AtomicUsize::new(0));
        let value = budget
            .reserve(AdmissionClass::Inflight, 1)
            .unwrap()
            .hold(Retired(Arc::clone(&retired), budget.clone()));
        assert_eq!(
            budget.reserve(AdmissionClass::Encoded, 1).unwrap_err(),
            AdmissionError::Full
        );
        drop(value);
        assert_eq!(retired.load(Ordering::Acquire), 1);
        assert_eq!(budget.usage(), (0, [0; 5], 0));
    }

    #[test]
    fn scoped_consume_keeps_charge_through_core_processing() {
        let budget = ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 1,
            max_encoded_bytes: 0,
            max_decoded_bytes: 0,
            max_suffix_bytes: 0,
            max_native_overlap_bytes: 0,
            max_inflight_bytes: 1,
            max_reservations: 1,
        })
        .unwrap();
        let retired = Arc::new(AtomicUsize::new(0));
        budget
            .reserve(AdmissionClass::Inflight, 1)
            .unwrap()
            .hold(Retired(Arc::clone(&retired), budget.clone()))
            .consume(drop);
        assert_eq!(retired.load(Ordering::Acquire), 1);
        assert_eq!(budget.usage(), (0, [0; 5], 0));
    }

    #[test]
    fn per_class_limit_rejects_without_leaking_total_charge() {
        let budget = ByteAdmission::new(AdmissionLimits {
            max_total_bytes: 10,
            max_encoded_bytes: 2,
            max_decoded_bytes: 10,
            max_suffix_bytes: 10,
            max_native_overlap_bytes: 10,
            max_inflight_bytes: 10,
            max_reservations: 2,
        })
        .unwrap();
        assert_eq!(
            budget.reserve(AdmissionClass::Encoded, 3).unwrap_err(),
            AdmissionError::InvalidCharge
        );
        let first = budget.reserve(AdmissionClass::Encoded, 2).unwrap();
        assert_eq!(
            budget.reserve(AdmissionClass::Encoded, 1).unwrap_err(),
            AdmissionError::Full
        );
        assert_eq!(budget.usage(), (2, [2, 0, 0, 0, 0], 1));
        drop(first);
        assert_eq!(budget.usage(), (0, [0; 5], 0));
    }

    #[test]
    fn empty_feed_needs_no_native_overlap_reservation() {
        let budget = ByteAdmission::new(AdmissionLimits {
            max_native_overlap_bytes: 0,
            ..limits()
        })
        .unwrap();
        assert_eq!(budget.usage(), (0, [0; 5], 0));
        assert_eq!(
            budget
                .reserve(AdmissionClass::NativeOverlap, 1)
                .unwrap_err(),
            AdmissionError::InvalidCharge
        );
        assert!(budget.reserve(AdmissionClass::Encoded, 1).is_ok());
    }
}
