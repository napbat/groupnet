# Volatile coherence recovery for origin-backed caches

[Documentation index](README.md) · [Peer bootstrap](replication-volatile-bootstrap.md)

Status: **sans-IO core, opt-in runtime, and first s3cache consumer implemented**.
This complements [replication.md](replication.md); it is not a durable replay source.
Optional OriginOnly retry is specified [below](#optional-rearm-for-volatile-origin-recovery).
The first consumer is s3cache's default mode, which performs zero coordination
or metadata writes to S3. Its origin bucket remains untouched by control data.

## Authority boundary

S3 supplies object contents and LIST, but no committed event cursor for proxy
mutations. A writer can commit at the origin and die before publishing its
volatile Groupnet feed event. Neither matching gossip heads nor a completed
origin LIST proves that no such write exists. This protocol therefore grants
no `SourceProof`, EventComplete delivery, authoritative absence, or persistent
resume cursor. S3cache keeps its existing origin fallback for unproved reads,
LIST/absence policy, and body validation. Optional separate durable
coordination can later supply stronger source guarantees without changing the
zero-write default.

The recovery core owns *when* a local serving licence is revoked, which
generation may restore it, whether a lease lapse may retain the index, and
when uncertain evidence forces an origin rescan. Its adapter supplies
membership/lease/feed facts, invalidates or rebuilds application state, and
decides whether a particular indexed key or body may be served. `Invalidated`
means prior state is unservable; `Materialized` means the rescan/index update
finished. A successful invalidation is never mislabeled as materialization.
The ordinary lease's own `valid` verdict remains an independent read gate.

## Finite protocol

One `RecoveryEngine` exists per opened coherence domain, with explicit
limits for member count, identities/heads, barrier rounds, operation duration,
settle interval, and generation/operation tokens. It consumes caller-supplied
logical time and returns typed effects. No clock, socket, S3 call, or Tokio
enters the core. A source operation response carries the exact current
generation and operation token; stale responses cannot affirm a newer gap.

`FeedGap` immediately emits `CloseGate` and `Invalidate { distrust_bodies: true }`.
After the correlated `Invalidated` receipt it requests bounded `RebuildOrigin`
(or opt-in `AcquireBaseline`). The shell must perform the revocation before
any more local reads: its shared read verdict and index-publication permit
are latched closed in the same public gap transition, before asynchronous
worker work is queued. It starts at most one rescan for that generation. The
application moves its trust generation, re-LISTs relevant index state, and
keeps hot/warm bytes as candidates subject to later index validation. The
engine requests `Affirm` only after the matching materialization receipt.
Every origin-rescan page callback and index publication is generation fenced,
not just the final receipt. A private candidate with guarded swap or a
generation-checked index transaction prevents an old rebuild from
overwriting a newer index or resurrecting a DELETE. The lease adapter may
still decline while renewal is unconfirmed. Decline retries within a finite
deadline. Final affirmation atomically checks the same recovery generation
and publication permit while updating lease state. A new gap supersedes every
old rescan or affirmation.

`LeaseLapse(counter)` starts the cheaper arm only in the lease-backed mode and
only from a previously affirmed, complete local baseline and if that counter
is not covered by a newer gap recovery. A cold node, an origin-only node, or a
node still invalidating/rebuilding after a gap retains the mandatory full
origin rebuild; a newer lapse supersedes it with another full rebuild rather
than downgrading to the cheap arm. A lapse arriving during an unfinished cheap
arm likewise forces full fallback. It revokes local
serving first through the same read-verdict and publication-permit latch. The
adapter supplies a bounded initial membership and per-granter renewal
sample, including members not currently `Alive` and a
time-qualified set of peers already non-live before this lapse. The latter is
an explicit s3cache policy assumption, not proof that a reaped peer was never
writing. The core waits for **every** relevant granter to adopt a later
renewal; a roster-wide minimum advancing is insufficient. It then waits the
configured gossip settle interval, checks whether any previously relevant
writer vanished, samples advertised per-writer feed heads, and waits for the
application's applied frontier to reach them. It resamples boundedly if
heads move, checks vanished writers again, and requests affirmation for the
same generation. Successful affirmation retains validated cached bodies and
avoids origin LIST. The existing volatile-feed policy assumes a peer with no
advertised feed head contributes no frontier target; this does not prove it
made no unpublished origin mutation. An unreadable feed observation is a
failure, not an empty head. A head observed earlier in the same recovery turn
cannot disappear into an empty feed or leave its life and erase its barrier
obligation; that forces the full fallback. The exceptions are a writer's life
the observer delivered through its seal: a sealed restart it crossed
(`Peer::renewal`: it delivered the old life's seal after the sampled head and
renewed into the life the head names now), which is a progression the barrier
follows, and a seal it delivered before any next life (`Peer::sealed`), which
lets the writer's head disappear, name a later life, or the writer leave the
roster without losing a write; see "Sealed restarts" in
`consistency-modes.md`. A frozen grant, vanished unsealed writer, failed
barrier, capacity error, or deadline takes the full gap-style fallback.

The adapter reports a stable local `ObservedLapse` counter and must bind its
lease samples to the selected recovery turn. The core's `covered_lapses`
watermark and generation advance together on gap fallback, so a lapse and
its covering gap do not start duplicate origin rescans. Cancel, supersede,
timeout, and token exhaustion fail closed. Effect admission and application
callbacks have bounded deadlines and correlation; late completions cannot
reopen serving. Cancel is terminal for that session, including an explicit
`Start`; reopening requires a new session. No whole-fleet barrier is imposed
on ordinary reads or writes.

The core exposes `RecoveryConfig`, `RecoveryEvent`, `RecoveryEffect`,
`RecoveryStage`, `RecoveryState`, and `RecoveryEngine::step`. Events include
`Start`/`StartWithLapses`, `FeedGap`, `LeaseLapse`, `PeersObserved` (bounded
membership, individual renewals and heads), `FrontiersReached`, `Invalidated`,
`Materialized`, `Progressed`, `Affirmed { accepted }`, `Failed`, `Tick`, and
`Cancel`. Effects include `CloseGate`, `Invalidate`, `RebuildOrigin`,
`ObservePeers`, `WaitFrontiers`, `Affirm`, `FellBack`, and `ArmTimer`.
Settle and renewal waits use core stages and the same logical timer, not
separate uncorrelated callbacks. Optional bootstrap adds typed baseline and
peer-head/handoff transitions described in the
[runtime contract](replication-volatile-transfer-runtime.md).
Invalidation and materialization remain distinct correlated completion
receipts.

## Consumer cutover and evidence

The full-origin path retries a failed invalidation or rebuild with a fresh
operation token and a bounded poll delay under the episode's total deadline.
An operation timeout follows the same rule. At total expiry it ends in
`OriginOnly`; an explicit `Start` can begin another full rebuild.
The runtime's publication permit checks that exact token and generation,
plus the operation's current deadline, on every page publication. A callback
from a failed attempt cannot publish during its retry. The consumer must not
recreate a separate scan-retry state machine.

Rebuild liveness is progress-based, never a fixed bound that must exceed one
bucket's scan time. After each committed page the adapter calls
`PublicationPermit::progress`; the worker steps `RecoveryEvent::Progressed`
for that exact operation, which restarts both `attempt_ms` (the operation's
stall bound) and `total_ms` (the episode's budget) from that instant and moves
the permit's deadline with them. A scan that keeps committing pages therefore
runs to completion in one pass however long it takes; only a scan that
commits nothing for `attempt_ms` fails and retries, and only an episode with
no progress for `total_ms` ends in `OriginOnly`. A retry after a stall is a
fresh scan from the first page; there is no resume cursor. Progress from a
fenced operation (gap, lapse, cancel, expiry, or a newer token) is discarded
by the permit and refused by the core, so it can never revive old work.

The opt-in runtime is `groupnet-consistency/volatile-recovery` (facade feature
`groupnet/consistency-volatile-recovery`). `RecoveryHandle::open` starts closed
and synchronously revokes the adapter's independent serve grant before
returning. `feed_gap` and `lease_lapse` atomically close the read/publication
gate and retain a bounded coalesced signal before returning; a gap dominates
lapses and a newer lapse counter replaces an older one. A duplicate or
unsupported lapse cannot revoke an already affirmed gate. The worker owns
the core and bounded adapter calls. Every index page and final swap must use
the operation-bound `PublicationPermit::publish`; the permit checks its
generation, exact operation, and absolute deadline even if the worker has not
polled a timeout yet. `RecoveryAdapter::revoke_serving` and `affirm` run under
the same short control lock as public signal closure, so they must not call
the handle's status/signal methods or a permit method and must not block on
network or origin I/O. The last public handle drop fences serving and stops
the worker; an unexpected worker exit also closes it.

By default, at `OriginOnly` serving stays closed until an explicit `restart`
starts a new full turn, or a new handle/session is opened. `Cancel` is terminal for that
handle: subsequent automatic signals and `restart` reject, and reopening
requires a new handle with a fresh session incarnation. The shell does not
claim durable source completeness or change the application's independent
lease, index-completion, body-validation, or origin-fallback decisions.


The s3cache shell replaces `sync::recovery::ResyncGate`, `LapseWatch`, and
their staged retry/generation loops with one driver of these effects. It
retains the existing Groupnet lease, volatile `WriteFeed`/`FrontierView`,
origin rescan, index, body trust generation, and read fallbacks as adapters.
The driver must never synthesize a native durable cursor or call the separate
replication `SessionEngine` with gossip counters. A default startup with no
control store performs no S3 metadata writes. The first consumer cutover uses
one origin builder **per node**. Reusable fleet claim, takeover, and transfer
contracts are implemented in [peer bootstrap](replication-volatile-bootstrap.md);
S3 fleet adoption remains pending and supplies no extra read authority.
A joined peer still needs its own applicable
index/recovery proof before local serving.

Deterministic core simulations must cover overlapping gap and lapse,
superseded rescan/affirmation, frozen individual granter while the minimum
advances, vanished peer before and during the frontier barrier, moving heads,
lost responses, retries, token/capacity exhaustion, and bounded fallback.
S3cache's former lapse/retry state-machine cases are now Groupnet core/sim
properties. MinIO proxy tests assert origin fallback and body/index
validation during recovery, no default control writes, and guarded retry
pages. Fleet single-builder startup with takeover remains a follow-up.
Tests may show retained state
and fewer origin LISTs when the lapse proof succeeds; they must not infer
durable event completeness from the volatile feed.

## Optional rearm for volatile origin recovery

The core and runtime are implemented; s3cache opt-in remains pending. Rearm
applies after a full turn exhausts its finite budget. It adds no durable source,
origin metadata objects, or authority to a gossip head. Without rearm, the
driver stays `OriginOnly` until an explicit restart or an applicable new
external recovery signal.

### Policy and safety

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
driver; it does not elect a fleet-wide builder. Without consumer peer-bootstrap
integration, each s3cache node can create its own origin scan. s3cache may opt in while
keeping zero coordination/metadata writes to S3 by default, the origin
bucket free of control objects, and ordinary read fallbacks intact.

### API and execution boundary

`RecoveryEngine::with_rearm(policy)` validates the policy before the first
event. A coalesced `StartWithLapses` event preserves an explicit restart and
its highest observed lapse counter in one full recovery generation.
`RecoveryHandle::open_with_rearm` performs the matching platform
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
