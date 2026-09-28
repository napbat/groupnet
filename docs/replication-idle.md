# Idle source checks without weakening read gates

Status: **implemented optional replication policy**. This refines the
scale requirements in [replication.md](replication.md). It follows named
acknowledgement waits. The default cadence remains unchanged.

## Contract

The default source-check cadence and its freshness gate remain unchanged.
An optional `IdlePolicy` allows a registered, inactive scope to back off its
source checks after a configured number of unchanged, successful checks.
The policy has a finite maximum interval and bounded deterministic jitter.
Only explicitly opened scopes have workers. Consumers register resident or
requested shards, and close evicted scopes; possible shard count is not a
reason to create a worker for every shard.

Backing off checks does not extend any source proof, lease, admission,
checkpoint, or permission to serve. The original freshness deadline remains
an independent timer. At that deadline the core closes the local read gate
and emits serving revocation even if the next idle source check is later.
Runtime read verdicts also check proof age directly, including while an
adapter call is blocked. An idle replica may keep its materialized state but
must use fallback or refresh before serving again.

A read request or floor wait records activity. Activity resets idle backoff;
if the source proof is already stale, it requests prompt revalidation.
Activity while a proof is fresh does not start an additional source check per
read. A source-change hint can request an earlier check, as it does today.
Activity and hints coalesce independently in bounded worker notification
state. Concurrent floor requests continue to coalesce into one native catch-up
operation, with no per-request source polling loop.

Successful replay, a changed source position/history, or a source hint resets
the idle interval. Only a source-certified unchanged position advances the
idle count; a failed request, missing history, or incomplete snapshot cannot
be treated as evidence that the source is idle. Existing finite retry,
snapshot, and terminal-gap behavior still applies.

Idle backoff never disables anti-entropy. With no reads or hints, the core
continues scheduling source checks at no more than the configured maximum
idle interval after a successful check. Actual discovery latency also includes
bounded operation admission and source I/O, and failures follow the retry
policy. A commit that produced no notification is eventually discovered
without requiring another write. No latency claim may omit those costs.

## One clock and scheduler

The policy belongs to `SessionEngine`, alongside its existing tail, retry,
snapshot, cleanup, and acknowledgement deadlines. It consumes caller-supplied
logical time and emits timers and source operations. The runtime does not
maintain a second replay or idle state machine.

Keep source-proof freshness separate from the next scheduled poll. The
minimum live deadline covers freshness revocation, idle polling, current
operation timeout, snapshot cleanup, and named-ack polling. Every decision
that changes the earliest deadline emits the appropriate `ArmTimer`, so a
driver using effects alone has the same behavior as one querying
`next_deadline()`. Stale timers cannot revive cancelled or superseded work.

Jitter is deterministic from the scope, session incarnation, and checked
poll sequence. It is bounded by the configured maximum interval, uses no
clock read or random-number dependency in the core, and never delays a
demand-triggered check or the freshness revocation timer. Arithmetic at
counter/deadline limits fails closed. A new source history resets the idle
state and still follows the existing recovery proof requirements.

The shell samples activity before queueing work, coalesces wakeups, and keeps
its existing registration, operation, replay-byte, and candidate-byte caps.
Idle scopes retain bounded cursor/proof metadata only. Backoff does not keep
snapshot stages, batches, acknowledgement observations, or source holds alive
past their ordinary lifecycle. Named acknowledgement waits keep their own
finite poll cadence and deadline even while state-sync replay is idle.

## Evidence required before enabling consumers

Unit and seeded simulation tests drive only explicit events and emitted
timers. They cover unchanged idle scopes, hot scopes, a missed notification
with no later write, first read after idleness, coalesced floor demands,
authority changes, partitions, operation timeouts, history replacement,
clock/counter limits, and cancellation with queued old timers. Assertions
include both safety (no freshness extension or stale read permission) and
liveness (prompt demand recovery and eventual idle discovery under available
source capacity). Simultaneous named waits must keep their deadlines and make
progress while replay checks back off.

Runtime in-memory tests block source I/O while proof age expires, then show
that activity restores service only after a new verified check and application
policy. Tests observe the real source-call count: a burst of reads cannot
produce one source request per read, and an idle scope cannot poll at the hot
cadence indefinitely. They also verify that closing a scope drops its worker
and all associated permits.

Report measurements for increasing registered scope counts and fractions of
active scopes, with backoff disabled and enabled. Include source calls, replay
bytes, peak adapter-owned resident bytes and operations, catch-up/read latency,
throughput, and first-read delay after idleness. Separate simulated virtual
time costs from measured runtime latency. Use the same commit, configuration,
and workload for comparisons. Consumer tests additionally count native object
store requests and optional control-store writes. S3cache's default path
continues to perform zero coordination writes to S3; this scheduling policy
cannot introduce storage or a durable startup requirement.

## Virtual-time source-call measurements

The effects-driven `replication_idle_scale` test compares unchanged sources
for 0 through 100 logical milliseconds, with a 10 ms freshness/hot interval,
one unchanged check before backoff, an 80 ms maximum idle interval, and no
jitter. Active scopes receive activity every 5 ms. Source checks and
revocation receipts complete immediately in this model. Half-active means
`floor(scopes / 2)` active scopes. Counts include the initial source check.

| Registered scopes | Backoff off | All idle | Half active | All active |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 11 | 4 | 4 | 11 |
| 4 | 44 | 16 | 30 | 44 |
| 16 | 176 | 64 | 120 | 176 |

These are simulated request counts, not measured network throughput or
wall-clock latency. Quiet scopes stop serving when the original freshness
budget expires; a later read requires refresh. Runtime tests separately
exercise blocked I/O, missed hints, read bursts, and already-covered RYW
floor waits. Larger consumer load tests and measured latency/memory remain
required before making deployment capacity claims.
