//! Logical time.
//!
//! The engine never *reads* a clock; a driver feeds time in. Keeping it a plain
//! integer (rather than [`std::time::Instant`]) is what lets a simulator
//! fabricate and replay time deterministically.

/// A logical, monotonic timestamp in milliseconds since a driver-chosen epoch.
///
/// The engine never reads time — a driver passes it in via
/// [`GroupEngine::on_tick`](crate::GroupEngine::on_tick). Using a plain integer
/// (rather than [`std::time::Instant`]) is what lets a simulator fabricate and
/// replay time deterministically.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Debug, Default)]
pub struct Time(pub u64);

impl Time {
    /// The zero instant.
    pub const ZERO: Time = Time(0);

    /// The far future ("never" for expiry stamps).
    pub const MAX: Time = Time(u64::MAX);

    /// Returns `self + ms`, saturating at `u64::MAX`.
    #[must_use]
    pub fn saturating_add(self, ms: u64) -> Time {
        Time(self.0.saturating_add(ms))
    }
}
