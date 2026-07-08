//! A deterministic, seedable pseudo-random generator.
//!
//! The simulator's whole value is reproducibility, so its randomness is a fixed,
//! replayable [`SplitMix64`] stream rather than an external RNG crate — the same
//! primitive drives the built-in loss schedule and the DST/property tests.

/// A tiny, seedable **splitmix64** pseudo-random generator.
///
/// Exposed so DST and property tests (and your own fault schedules) draw from
/// one audited, dependency-free implementation instead of each re-deriving the
/// constants. Equal seeds yield equal streams, on every platform.
#[derive(Clone, Debug)]
pub struct SplitMix64(u64);

impl SplitMix64 {
    /// The golden-ratio stride (`⌊2^64 / φ⌋`) added to the state each step.
    const GAMMA: u64 = 0x9e37_79b9_7f4a_7c15;

    /// Seeds the generator. Equal seeds always produce equal streams.
    #[must_use]
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// The next pseudo-random 64-bit word.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(Self::GAMMA);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    /// A pseudo-random value in `0..n`; `n` is treated as at least `1`.
    pub fn below(&mut self, n: u32) -> u32 {
        (self.next_u64() % u64::from(n.max(1))) as u32
    }

    /// A stateless single-word draw, equal to `SplitMix64::new(x).next_u64()`.
    /// Turns a counter (e.g. a message sequence number) into a well-distributed
    /// value without keeping generator state, so a decision keyed on it doesn't
    /// alias with the (equally deterministic) order the counter was produced in.
    #[must_use]
    pub fn hash(x: u64) -> u64 {
        Self::new(x).next_u64()
    }
}
