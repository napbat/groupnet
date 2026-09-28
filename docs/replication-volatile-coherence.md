# Volatile coherence recovery for origin-backed caches

Status: **implementation contract** for a separate sans-IO recovery slice. This
complements [replication.md](replication.md); it is not a durable replay source.
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
only if that counter is not covered by a newer gap recovery. It revokes local
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
avoids origin LIST. A frozen grant, vanished writer, absent head, failed
barrier, capacity error, or deadline takes the full gap-style fallback.

The adapter reports a stable local `ObservedLapse` counter and must bind its
lease samples to the selected recovery turn. The core's `covered_lapses`
watermark and generation advance together on gap fallback, so a lapse and
its covering gap do not start duplicate origin rescans. Cancel, supersede,
timeout, and token exhaustion fail closed. Effect admission and application
callbacks have bounded deadlines and correlation; late completions cannot
reopen serving. No whole-fleet barrier is imposed on ordinary reads or writes.

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

The s3cache shell replaces `sync::recovery::ResyncGate`, `LapseWatch`, and
their staged retry/generation loops with one driver of these effects. It
retains the existing Groupnet lease, volatile `WriteFeed`/`FrontierView`,
origin rescan, index, body trust generation, and read fallbacks as adapters.
The driver must never synthesize a native durable cursor or call the separate
replication `SessionEngine` with gossip counters. A default startup with no
control store performs no S3 metadata writes. Ordinary connected cold-cluster
startup coordinates exactly one origin builder; takeover and safe duplicate
builders under partition are scheduling concerns, not
evidence of read authority. A joined peer still needs its own applicable
index/recovery proof before local serving.

Deterministic core simulations must cover overlapping gap and lapse,
superseded rescan/affirmation, frozen individual granter while the minimum
advances, vanished peer before and during the frontier barrier, moving heads,
lost responses, retries, token/capacity exhaustion, and bounded fallback.
S3cache's existing `sync::tests` cases for `lapse_barrier_retains`,
`lapse_barrier_fallbacks`, `lease_lapse_resyncs`, and superseded resync remain
black-box expectations. MinIO proxy tests must also assert origin fallback
and body/index validation during recovery, no default control writes, and
single-builder normal startup with takeover. Tests may show retained state
and fewer origin LISTs when the lapse proof succeeds; they must not infer
durable event completeness from the volatile feed.
