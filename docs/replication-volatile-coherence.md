# Volatile coherence recovery for origin-backed caches

Status: **sans-IO core, opt-in runtime, and first s3cache consumer implemented**.
This complements [replication.md](replication.md); it is not a durable replay source.
Optional OriginOnly retry is specified in
[replication-volatile-rearm.md](replication-volatile-rearm.md).
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

One `VolatileRecoveryEngine` exists per opened coherence domain, with explicit
limits for member count, identities/heads, barrier rounds, operation duration,
settle interval, and generation/operation tokens. It consumes caller-supplied
logical time and returns typed effects. No clock, socket, S3 call, or Tokio
enters the core. A source operation response carries the exact current
generation and operation token; stale responses cannot affirm a newer gap.

`FeedGap` immediately emits `RevokeServing` and `DistrustBodies`, then a
bounded `OriginRescan` request. The shell must perform the revocation before
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
cannot disappear into an empty feed and erase its barrier obligation; that
forces the full fallback. A frozen grant, vanished writer, failed
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

The API should expose a small core `RecoveryConfig`, `RecoveryEvent`,
`RecoveryEffect`, `RecoveryState`, and `RecoveryEngine::step`. Events include
`FeedGap`, `LeaseLapse`, per-granter `Renewals`, `Settled`, `Membership`,
`AdvertisedHeads`, `FrontiersReached`, `Invalidated`, `Materialized`,
`Affirmed/Declined`, `Failed`, `Tick`, and `Cancel`. Effects include
`RevokeServing`, `DistrustBodies`, `RescanOrigin`, `SampleRenewals`,
`WaitSettle`, `SampleMembership`, `SampleHeads`, `WaitFrontiers`, `Affirm`,
and `ArmTimer`. Exact names may change during implementation, but the two
distinct completion receipts and every stage's correlation may not.

## Consumer cutover and evidence

The full-origin path retries a failed invalidation or rebuild with a fresh
operation token and a bounded poll delay under its **original** total
deadline. An operation timeout follows the same rule. At total expiry it
ends in `OriginOnly`; an explicit `Start` can begin another full rebuild.
The runtime's publication permit checks that exact token and generation,
plus the operation's absolute deadline, on every page publication. A callback
from a failed attempt cannot publish during its retry. The consumer must not
recreate a separate scan-retry state machine.

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
one origin builder **per node**. Fleet-wide single-builder startup, takeover,
and safe duplicate builders under partition remain a scheduling follow-up,
not evidence of read authority. A joined peer still needs its own applicable
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
