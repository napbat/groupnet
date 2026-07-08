//! Per-group failure-detection and gossip tunables.

/// Tunables for a single group's gossip and failure detection.
///
/// All durations are in milliseconds of *logical* time (see
/// [`Time`](crate::Time)), so they mean the same thing whether the engine is
/// driven by a real clock or a simulator's virtual one. [`Default`] is tuned for
/// a small, low-latency cluster.
#[derive(Clone, Debug)]
pub struct Config {
    /// How often to disseminate the full view.
    pub gossip_interval_ms: u64,
    /// How often to probe a member for liveness.
    pub probe_interval_ms: u64,
    /// How long to wait for a probe ack before escalating / suspecting.
    pub probe_timeout_ms: u64,
    /// How long a member may stay `Suspect` before it is declared `Dead`.
    pub suspect_timeout_ms: u64,
    /// How long a `Dead` tombstone is gossiped. After this it stops being
    /// re-advertised; after `2×` it is reaped from the table.
    pub dead_timeout_ms: u64,
    /// How many indirect probers to enlist when a direct probe goes unanswered.
    pub indirect_probes: usize,
    /// Maximum peers to disseminate to per gossip round.
    pub fanout: usize,
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
        }
    }
}
