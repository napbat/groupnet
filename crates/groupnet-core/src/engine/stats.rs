//! Anti-entropy traffic counters.

/// Cumulative anti-entropy traffic counters for one engine (one node's view
/// of one group), read via [`GroupEngine::net_stats`](crate::GroupEngine::net_stats) (the runtime exposes
/// them per group). The ratio to watch at scale is
/// `digest_summaries_listed / digests_built`: with per-peer delta digests it
/// tracks recent churn, not membership size — if it tracks membership size,
/// the group has outgrown its cadence or `full_digest_every` (see the
/// README's scaling envelope).
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetStats {
    /// Digests built (one per fanout target per round; delta + full).
    pub digests_built: u64,
    /// How many of those were full digests (every gossipable member listed).
    pub full_digests_built: u64,
    /// Member summaries listed across all digests.
    pub digest_summaries_listed: u64,
    /// Encoded digest frames handed to the transport (budget chunking means
    /// one digest can span several frames).
    pub digest_frames_sent: u64,
    /// Delta frames handed to the transport (anti-entropy backfill, offers,
    /// and eager push).
    pub delta_frames_sent: u64,
    /// Delta-request frames sent (including truncation continuations).
    pub delta_requests_sent: u64,
    /// Total encoded bytes of the frames counted above. Constant-size probe
    /// frames (ping/ack) are excluded — this measures the traffic that
    /// scales with state, not liveness.
    pub anti_entropy_bytes_sent: u64,
}
