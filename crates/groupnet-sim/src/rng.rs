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
    #[expect(
        clippy::cast_possible_truncation,
        reason = "`% n` bounds the draw below `n`, itself a u32, so the narrowing is \
                  exact — and the whole point here is a branch-free fixed stream"
    )]
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

#[cfg(test)]
mod tests {
    use super::SplitMix64;

    /// The canonical splitmix64 stream for seed 0 — the reference vector every
    /// implementation of this generator agrees on.
    const SEED_0: [u64; 8] = [
        0xe220_a839_7b1d_cdaf,
        0x6e78_9e6a_a1b9_65f4,
        0x06c4_5d18_8009_454f,
        0xf88b_b8a8_724c_81ec,
        0x1b39_896a_51a8_749b,
        0x53cb_9f0c_747e_a2ea,
        0x2c82_9abe_1f45_32e1,
        0xc584_133a_c916_ab3c,
    ];

    /// A second, arbitrary seed, pinned the same way so a constant typo that
    /// happened to leave seed 0 intact still gets caught.
    const SEED_42: [u64; 4] = [
        0xbdd7_3226_2feb_6e95,
        0x28ef_e333_b266_f103,
        0x4752_6757_130f_9f52,
        0x581c_e1ff_0e4a_e394,
    ];

    fn draws(seed: u64, n: usize) -> Vec<u64> {
        let mut rng = SplitMix64::new(seed);
        (0..n).map(|_| rng.next_u64()).collect()
    }

    /// Known-answer test: the stream is bit-for-bit fixed, so any change to
    /// the gamma or the finalizer constants breaks every recorded simulation
    /// trace loudly instead of silently.
    #[test]
    fn stream_matches_the_known_answer() {
        assert_eq!(draws(0, SEED_0.len()), SEED_0);
        assert_eq!(draws(42, SEED_42.len()), SEED_42);
    }

    /// The state wraps rather than overflowing: seeding at `u64::MAX` is a
    /// normal draw, not a panic in debug builds.
    #[test]
    fn seeding_at_the_maximum_wraps() {
        assert_eq!(SplitMix64::new(u64::MAX).next_u64(), 0xe4d9_7177_1b65_2c20);
    }

    /// The documented invariant: `hash(x)` is exactly a fresh generator's
    /// first draw, so a counter-keyed decision and a seeded stream cannot
    /// drift apart.
    #[test]
    fn hash_is_a_fresh_generators_first_draw() {
        for x in (0..64).chain([u64::MAX, u64::MAX / 2, 1 << 40]) {
            assert_eq!(SplitMix64::hash(x), SplitMix64::new(x).next_u64());
        }
    }

    /// Equal seeds replay equal streams, and a clone continues the stream from
    /// exactly where it was taken — the property replay depends on.
    #[test]
    fn equal_seeds_and_clones_replay_the_same_stream() {
        assert_eq!(draws(7, 32), draws(7, 32));

        let mut rng = SplitMix64::new(7);
        for _ in 0..5 {
            rng.next_u64();
        }
        let mut forked = rng.clone();
        assert_eq!(draws_from(&mut rng, 8), draws_from(&mut forked, 8));
    }

    fn draws_from(rng: &mut SplitMix64, n: usize) -> Vec<u64> {
        (0..n).map(|_| rng.next_u64()).collect()
    }

    /// Distinct seeds give distinct streams — the generator is not a constant
    /// dressed up as one.
    #[test]
    fn distinct_seeds_diverge() {
        assert_ne!(draws(1, 8), draws(2, 8));
    }

    /// `below` stays in range, treats `0` as `1` (no divide-by-zero), and maps
    /// the stream to bounded draws in a pinned way.
    #[test]
    fn below_is_bounded_and_pinned() {
        let mut rng = SplitMix64::new(7);
        let got: Vec<u32> = (0..10).map(|_| rng.below(10)).collect();
        assert_eq!(got, [7, 4, 6, 3, 4, 5, 8, 2, 5, 5]);

        let mut rng = SplitMix64::new(99);
        for _ in 0..256 {
            assert!(rng.below(3) < 3);
            assert_eq!(rng.below(0), 0, "n is treated as at least 1");
            assert_eq!(rng.below(1), 0);
        }
    }
}
