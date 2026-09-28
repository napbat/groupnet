# Source-ordered admission decision core

This is the sans-IO contract for the optional source-backed pre-mutation fence
described in [replication.md](replication.md#7-consumer-obligations-and-limits).
It is backend-neutral and does not require an S3 bucket. For s3cache, the
default remains **zero coordination or metadata writes to S3**; the origin
bucket contains only ordinary user-object mutations. A durable coordination
store and its admission/intent writes require explicit opt-in. Without that
store or an equivalent authoritative reconciliation source, the current
origin fallback remains; no complete peer index or event-complete subscription
is claimed. Event-complete delivery additionally requires retained committed
history and cannot be replaced by a snapshot.
The source adapter, not gossip or this core, certifies a contiguous native
history prefix and the exact immutable records at its positions. A single
fleet policy binds source history, an opaque policy fingerprint, maximum
admission duration, and a conservative integer clock-rate ratio. Unsupported
policy or history refuses admission and dispatch.
The adapter must reject reused admission incarnations across the retained
history; the core retains only its current and in-flight local windows and a
bounded writer waitset, not an unbounded lifetime identity ledger.

A reader samples its monotonic clock **before** requesting the durable
admission append. Its original deadline never moves when an append reply or
source catch-up is delayed. A confirmed append authorizes nothing until a
contiguous application projection has advanced through that exact native
admission cursor. Its ordinary Groupnet serve lease and domain read policy
are separate gates. In particular, an unresolved intent fences its key and
relevant LIST/absence answers, while unrelated keys need not lose admission.
Renewal starts another bounded admission; the old one can serve only until
its original deadline and cannot be silently extended by the new append.
A cancelled, expired, or previous-process reply never revives admission.
The future shell must pass a fresh process-monotonic clock sample at each read
capability check and each writer fence decision. Acknowledging invalidation
means the matching admission stopped local serving for the affected intent;
a later renewal may serve that key only after it replays the pending intent
into the application's separate key-level gate.

After the exact intent append, a writer obtains an adapter-certified prefix
through that intent and a bounded set of preceding admissions. Each responsive
reader must acknowledge revocation bound to the intent and its **admission
incarnation**, or the writer waits a global maximum-duration expiry. That
wait starts after intent confirmation on the writer's own monotonic clock.
If writer and reader clock rates differ by at most the configured ratio
`rate_numerator / rate_denominator`, the writer waits at least
`ceil(max_duration * rate_numerator / rate_denominator) + clock_margin_ms`
of its clock units. The nonzero, persisted margin covers millisecond sample
quantization and driver scheduling uncertainty; overflow rejects the policy.
No per-node wall-clock comparison or persisted wall timestamp is used. A
restarted writer restarts the entire wait. Admission records after the intent
must replay the pending intent before serving. An invalidation acknowledgement
proves local serving revoked, not application state materialized or an origin
mutation committed. The core grants only permission to dispatch an origin
request; an ambiguous dispatched request must not be automatically repeated.

The first core slice bounds its in-memory waitset but does **not** issue
durable expiry-through-cut attestations. A writer can safely use the global
expiry wait even for an old admission, but an adapter whose roster projection
exceeds the configured bound must refuse the fast path. Persisted source
retention also needs a separate covered checkpoint/compaction protocol. Until
then, capacity exhaustion fails closed; neither an omitted roster member nor
an expired-looking wall timestamp can grant permission. Idle readers need no
renewal records; active readers pay a source append per renewal and writers
pay the intent append, invalidation round or conservative wait, and outcome
append. Implementations must measure this source traffic and contention.
