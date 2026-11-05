//! Anti-entropy traffic and tick counters.

/// Cumulative anti-entropy traffic counters for one engine (one node's view
/// of one group), read via [`GroupEngine::net_stats`](crate::GroupEngine::net_stats) (the runtime exposes
/// them per group). The ratio to watch at scale is
/// `digest_summaries_listed / digests_built`: with per-peer delta digests it
/// tracks recent churn, not membership size, and
/// [`Config::digest_window`](crate::Config::digest_window) caps it either way
/// (`max_digest_summaries` never exceeds the window). If it sits at the
/// window, the group has outgrown its cadence or `full_digest_every` (see the
/// README's scaling envelope). [`ticks`](Self::ticks) counts how often the
/// driver woke the engine, so an idle group's timer cost is visible too.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct NetStats {
    /// Digests built (one per fanout target per round; delta + full).
    pub digests_built: u64,
    /// How many of those were full digests: built on the rolling window
    /// rather than as a per-peer delta (a peer's first digest, every
    /// `full_digest_every`-th, and any delta digest that would have listed
    /// more than the window).
    pub full_digests_built: u64,
    /// Member summaries listed across all digests.
    pub digest_summaries_listed: u64,
    /// The most member summaries any single digest listed — at most
    /// [`Config::digest_window`](crate::Config::digest_window).
    pub max_digest_summaries: u64,
    /// Encoded digest frames handed to the transport (budget chunking means
    /// one digest can span several frames).
    pub digest_frames_sent: u64,
    /// Encoded bytes of those digest frames (also counted in
    /// `anti_entropy_bytes_sent`).
    pub digest_bytes_sent: u64,
    /// Delta frames handed to the transport (anti-entropy backfill, offers,
    /// and eager push).
    pub delta_frames_sent: u64,
    /// Delta-request frames sent (including truncation continuations).
    pub delta_requests_sent: u64,
    /// Total encoded bytes of the frames counted above. Constant-size probe
    /// frames (ping/ack) are excluded — this measures the traffic that
    /// scales with state, not liveness.
    pub anti_entropy_bytes_sent: u64,
    /// Times the engine was ticked ([`GroupEngine::on_tick`](crate::GroupEngine::on_tick)).
    /// A driver that ticks at [`GroupEngine::next_deadline`](crate::GroupEngine::next_deadline)
    /// keeps this at the engine's own cadence — about one tick per probe
    /// interval in an idle group.
    pub ticks: u64,
}
