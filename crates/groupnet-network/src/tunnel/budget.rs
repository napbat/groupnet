//! Node-wide accounting of retained tunnel ciphertext.
//!
//! Each session direction owns a [`Reservation`] whose floor is guaranteed by
//! admission: the budget sets aside `max_sessions × 2 × min_window` and lends
//! only the remainder. Growth never waits, so reliability processing never
//! blocks on memory.

use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

use super::TunnelLimits;

/// Shared accounting for one tunnel transport.
#[derive(Debug)]
pub(super) struct MemoryBudget {
    /// Bytes lent above floors: the budget minus every possible floor.
    lendable: usize,
    lent: AtomicUsize,
    floors: AtomicUsize,
    #[cfg(test)]
    peak: AtomicUsize,
}

impl MemoryBudget {
    /// A budget for validated `limits`.
    pub(super) fn new(limits: &TunnelLimits) -> Arc<Self> {
        Arc::new(Self {
            lendable: limits.memory_budget.get() - limits.floor_budget(),
            lent: AtomicUsize::new(0),
            floors: AtomicUsize::new(0),
            #[cfg(test)]
            peak: AtomicUsize::new(0),
        })
    }

    /// Grants a direction its guaranteed `floor`.
    ///
    /// The caller bounds concurrent reservations by `max_sessions × 2`, which
    /// validation covers with set-aside floor bytes.
    pub(super) fn reserve(self: &Arc<Self>, floor: usize) -> Reservation {
        self.floors.fetch_add(floor, Ordering::AcqRel);
        #[cfg(test)]
        self.record_peak();
        Reservation {
            budget: self.clone(),
            floor,
            extra: 0,
        }
    }

    /// Bytes currently reserved: held floors plus lent growth.
    pub(super) fn in_use(&self) -> usize {
        self.floors.load(Ordering::Acquire) + self.lent.load(Ordering::Acquire)
    }

    /// Lends up to `wanted` bytes without waiting; returns the amount granted.
    fn lend(&self, wanted: usize) -> usize {
        let mut granted = 0;
        let _ = self
            .lent
            .try_update(Ordering::AcqRel, Ordering::Acquire, |lent| {
                granted = wanted.min(self.lendable - lent);
                (granted != 0).then_some(lent + granted)
            });
        #[cfg(test)]
        self.record_peak();
        granted
    }

    #[cfg(test)]
    fn record_peak(&self) {
        self.peak.fetch_max(self.in_use(), Ordering::AcqRel);
    }

    #[cfg(test)]
    pub(super) fn peak(&self) -> usize {
        self.peak.load(Ordering::Acquire)
    }
}

/// One direction's reserved bytes: a guaranteed floor plus lent growth.
#[derive(Debug)]
pub(super) struct Reservation {
    budget: Arc<MemoryBudget>,
    floor: usize,
    extra: usize,
}

impl Reservation {
    /// Reserved bytes, never below the floor.
    pub(super) fn bytes(&self) -> usize {
        self.floor + self.extra
    }

    /// Grows toward `target` as far as the shared remainder allows; never waits.
    pub(super) fn grow_to(&mut self, target: usize) {
        if let Some(wanted) = target.checked_sub(self.bytes()).filter(|n| *n != 0) {
            self.extra += self.budget.lend(wanted);
        }
    }

    /// Returns bytes above `max(target, floor)` to the shared remainder.
    pub(super) fn shrink_to(&mut self, target: usize) {
        let keep = target.saturating_sub(self.floor).min(self.extra);
        let released = self.extra - keep;
        if released != 0 {
            self.extra = keep;
            self.budget.lent.fetch_sub(released, Ordering::AcqRel);
        }
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.shrink_to(0);
        self.budget.floors.fetch_sub(self.floor, Ordering::AcqRel);
    }
}

#[cfg(test)]
mod tests {
    use std::num::{NonZeroU32, NonZeroUsize};

    use super::*;

    fn limits() -> TunnelLimits {
        TunnelLimits {
            max_sessions: NonZeroUsize::new(2).unwrap(),
            sessions_per_peer: NonZeroUsize::new(2).unwrap(),
            min_window: NonZeroU32::new(100 * 1024).unwrap(),
            max_window: NonZeroU32::new(400 * 1024).unwrap(),
            memory_budget: NonZeroUsize::new(1024 * 1024).unwrap(),
            ..TunnelLimits::default()
        }
    }

    #[test]
    fn floors_are_set_aside_and_growth_is_partial_and_bounded() {
        let limits = limits();
        limits.validate().unwrap();
        let floor = 100 * 1024;
        let budget = MemoryBudget::new(&limits);
        let mut first = budget.reserve(floor);
        let mut second = budget.reserve(floor);
        assert_eq!(budget.in_use(), 2 * floor);
        // Two more floors are set aside for the second session's directions.
        first.grow_to(1024 * 1024);
        assert_eq!(first.bytes(), floor + 1024 * 1024 - 4 * floor);
        second.grow_to(2 * floor);
        assert_eq!(second.bytes(), floor);
        let third = budget.reserve(floor);
        let fourth = budget.reserve(floor);
        assert_eq!(budget.in_use(), 1024 * 1024);
        assert_eq!(budget.peak(), 1024 * 1024);
        first.shrink_to(floor + 10);
        assert_eq!(first.bytes(), floor + 10);
        second.grow_to(floor + 50);
        assert_eq!(second.bytes(), floor + 50);
        first.shrink_to(0);
        assert_eq!(first.bytes(), floor);
        drop((first, second, third, fourth));
        assert_eq!(budget.in_use(), 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn concurrent_growth_never_exceeds_the_budget() {
        let limits = limits();
        let budget = MemoryBudget::new(&limits);
        let tasks: Vec<_> = (0..4)
            .map(|_| {
                let budget = budget.clone();
                tokio::spawn(async move {
                    let mut reservation = budget.reserve(100 * 1024);
                    for round in 0..10_000_usize {
                        reservation.grow_to(100 * 1024 + (round % 7) * 50_000);
                        assert!(budget.in_use() <= 1024 * 1024);
                        reservation.shrink_to((round % 3) * 40_000);
                    }
                })
            })
            .collect();
        for task in tasks {
            task.await.unwrap();
        }
        assert!(budget.peak() <= 1024 * 1024);
        assert_eq!(budget.in_use(), 0);
    }
}
