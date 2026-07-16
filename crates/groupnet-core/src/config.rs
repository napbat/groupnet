//! Per-group failure-detection and gossip tunables.

/// Tunables for a single group's gossip and failure detection.
///
/// All durations are in milliseconds of *logical* time (see
/// [`Time`](crate::Time)), so they mean the same thing whether the engine is
/// driven by a real clock or a simulator's virtual one. [`Default`] is tuned for
/// a small, low-latency cluster.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often the periodic anti-entropy round fires. Retained under its
    /// historic name because it *is* "the current gossip interval": since G3 the
    /// round disseminates compact **digests** (per-node version summaries), not
    /// the full view, and [`anti_entropy_interval_ms`](Self::anti_entropy_interval_ms)
    /// defaults to it.
    pub gossip_interval_ms: u64,
    /// How often to probe a member for liveness.
    pub probe_interval_ms: u64,
    /// How long to wait for a probe ack before escalating / suspecting.
    pub probe_timeout_ms: u64,
    /// How long a member may stay `Suspect` before it is declared `Dead`.
    pub suspect_timeout_ms: u64,
    /// How long a `Dead` tombstone (member *or* per-key entry) is gossiped. After
    /// this it stops being re-advertised; after `2×` it is reaped. This window is
    /// the **reap horizon**: it must exceed the longest partition a node can
    /// survive and still be reconciled, because a digest never resurrects an
    /// entry reaped past it (see [`GroupEngine`](crate::GroupEngine)).
    pub dead_timeout_ms: u64,
    /// How many indirect probers to enlist when a direct probe goes unanswered.
    pub indirect_probes: usize,
    /// Maximum peers a legacy full-view push would fan out to. Retained for
    /// source compatibility; the anti-entropy path uses
    /// [`anti_entropy_fanout`](Self::anti_entropy_fanout).
    pub fanout: usize,
    /// Cadence of the digest/delta **anti-entropy** exchange. Defaults to
    /// [`gossip_interval_ms`](Self::gossip_interval_ms); the `gossip_interval_ms`
    /// builder knob keeps the two in step, so tuning the gossip rate tunes
    /// anti-entropy with it.
    pub anti_entropy_interval_ms: u64,
    /// How many peers each anti-entropy round sends a digest to. A targeted pull
    /// needs far less redundancy than a blind push, so 1–2 is plenty; the default
    /// is 2. Fanout rotates round-robin so every peer is covered over successive
    /// rounds.
    pub anti_entropy_fanout: usize,
    /// Soft cap on the encoded byte size of any single emitted frame (digest,
    /// delta, or request). Deltas that would exceed it are split across
    /// successive rounds; the only frame permitted to exceed it is one carrying a
    /// single entry whose value alone is larger (it is sent whole rather than
    /// starved).
    pub max_delta_frame_bytes: usize,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            gossip_interval_ms: 200,
            probe_interval_ms: 100,
            probe_timeout_ms: 50,
            suspect_timeout_ms: 500,
            dead_timeout_ms: 10_000,
            indirect_probes: 2,
            fanout: 3,
            anti_entropy_interval_ms: 200,
            anti_entropy_fanout: 2,
            max_delta_frame_bytes: 60_000,
        }
    }
}
