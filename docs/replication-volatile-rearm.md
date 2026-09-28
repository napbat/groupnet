# Optional rearm for volatile origin recovery

Status: **design contract; implementation pending**. This extends
[volatile coherence recovery](replication-volatile-coherence.md) after a full
turn has exhausted its finite budget. It does not add a durable source, origin
metadata objects, or any authority to a gossip head. The default driver still
stays `OriginOnly` until an explicit restart or a new external recovery signal.

## Policy and safety

An opt-in `RecoveryRearm` has nonzero `initial_ms` and `max_ms`, with
`initial_ms <= max_ms`. One full attempt continues to use its original
`RecoveryConfig.total_ms` and per-operation deadlines. At `OriginOnly`, the
sans-IO engine clears the old operation and schedules a rearm at a checked
logical deadline. The first exhausted turn waits `initial_ms`; each later
unsuccessful turn doubles the delay with checked, saturating arithmetic up to
`max_ms`. A successful, generation-bound affirmation resets the next delay
to `initial_ms`. Only one rearm timer and one recovery operation exist per
domain. This bounds retained state and spacing between exhausted episodes
without pretending that a long origin outage has ended. Inside each active
episode, the existing failed-scan retry still uses `RecoveryConfig.poll_ms`;
this policy is not exponential backoff for every origin LIST request.

On the timer, the engine begins a **new full origin recovery generation**.
The shell's serving gate and all old publication permits remain closed until
that generation materializes state and receives an independent affirmation.
The timer itself grants no read permission. A failed automatic episode returns
to `OriginOnly` and schedules a later timer. An explicit operator `restart`
may start immediately and cancels the pending timer; it does not reset the
failure backoff until a turn succeeds. `Cancel` is terminal, including for
the timer. Generation, operation-token, or deadline arithmetic exhaustion
leaves the gate closed and disables automatic rearm rather than wrapping.

A feed gap or higher lease-lapse counter observed during an `OriginOnly`
cooldown updates the covered-lapse obligation but does not start another
scan ahead of the scheduled timer. This matters during outages: repeated
volatile gossip notifications cannot turn capped backoff into an unbounded
origin request loop. The next full generation covers the coalesced signal.
During an active turn, the existing gap/lapse supersession rules still apply
and revoke serving synchronously. With rearm disabled, existing signal and
explicit-restart behavior stays unchanged.

The source adapter remains responsible for bounded origin operations and
per-page publication permits. The optional policy is local to the recovery
driver; it does not elect a fleet-wide builder. Before peer bootstrap exists,
each s3cache node can create its own origin scan. s3cache may opt in while
keeping zero coordination/metadata writes to S3 by default, the origin
bucket free of control objects, and ordinary read fallbacks intact.

## API and execution boundary

`RecoveryEngine::with_rearm(policy)` validates the policy before the first
event. `RecoveryHandle::open_with_rearm` performs the matching platform
deadline check before spawning its worker. Existing constructors remain
unchanged and opt out. The core owns the rearm deadline in `next_deadline`
and emits `ArmTimer`; the runtime only maps that logical deadline to its
monotonic clock. No Tokio timer map, app retry loop, or second token allocator
is introduced. Source and application callbacks retain their exact
generation/operation correlation and total-attempt deadline.

Deterministic core and seeded simulation tests must prove finite attempts,
increasing capped intervals, prompt recovery after the source heals,
no serving between episodes, reset after success, coalesced gap/lapse during
cooldown, cancellation, and arithmetic exhaustion. Runtime tests with an
in-memory adapter must prove repeated failures do not spin, a healed source
eventually reaches `Ready` without external restart, and public cancel/drop
stops future origin work. An s3cache MinIO fault test must cover an outage
longer than one total budget, automatic rearm after recovery, unchanged
origin read fallback while closed, and no default control-object writes.
