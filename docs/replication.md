# Source-backed replication and resumable state sync

Status: **accepted implementation contract for [issue #3](https://github.com/napbat/groupnet/issues/3), 2026-09-27**.
This is an additive tier above the consistency modes in
[`consistency-modes.md`](consistency-modes.md). Its first implementation slice
must preserve the current feed, ack, lease, Hosted, and handoff APIs. The
protocol may later reuse their internals, but their existing wire bodies do not
change. This document is the contract of record for the replication work; the
Hosted-mode build order in `consistency-modes.md` Section 6 remains unchanged.
The bounded source-ordered admission decision core is specified in
[`replication-admission.md`](replication-admission.md).

## 1. Boundary and promise

The source of truth is an application-supplied committed store: shardstore's
CAS slot log, an S3 origin plus a durable publication/reconciliation mechanism,
or a Hosted epoch-fenced source. Groupnet does not commit an application write,
elect a second writer, impose a mandatory log, or interpret application state.
It owns the *replica session*: discover source changes independently of gossip,
coalesce demand, choose replay or snapshot, drive bounded transfer and ordered
apply, persist progress through the adapter, recover after restart, and decide
when a scope is eligible to serve. The derived coordinator can schedule work but
never proves commit or read authority. Eventual, Hosted, and external authority
remain choices of the source adapter and read policy.

A small gossiped `WriteFeed` remains a prompt wakeup. Its per-writer
`WriteToken` orders only that writer's publications; it is not the source
cursor, a durable event journal, or proof that every source commit was
published. The source tail is checked on an independent, bounded schedule and
when a read floor demands it. Existing `PeerWrites::new` attach-at-head and
`PeerWrite::Gap` semantics remain intact. The new tier uses a separate durable
subscription API and never silently changes old subscribers' meaning.

For a particular scope, distinguish these milestones:

1. `SourceCommitted`: the source can prove the record is committed, including
   resolution of an ambiguous write outcome. The record may have no feed hint.
2. `Enqueued` and `Delivered`: transport progress only; neither is an apply
   proof.
3. `Invalidated`: stale serving for the affected scope has been revoked. An
   invalidation acknowledgement may release a coherence writer, but it does
   not claim rebuilt state.
4. `Materialized`: the application has made state visible through the reported
   native cursor. Live materialization can be volatile; a persisted cursor
   advances only with recoverable state or an equivalent idempotent replay
   proof. A volatile receipt never licenses checkpoint resume.
5. `ReadPermitted`: a current generation has installed complete scope state,
   reached its required source floor, and passed the selected authority policy.

Each acknowledgement declares its proof kind. API names and telemetry must not
call invalidation, delivery, or an optimistic feed frontier `Materialized`.
Write completion reports the external commit separately from the chosen ack
proof. An ack timeout cannot roll back that commit.

## 2. Native identity, order, and durable obligations

`StreamId` is a stable, named, typed topic under a `GroupId`; `ScopeId` selects
one independently recoverable shard, bucket, or partition. A `SourceId` and
`SourceGeneration` identify the authoritative source history. `Position<P>`
contains that identity, scope, and a source-native `P`, plus an optional
subposition only when the source exposes records within a committed batch.
`P` is generic in the local typed API and encoded by a versioned adapter codec
for storage or transfer. Encoded cursor bytes have a configured maximum length
and are rejected before allocation or comparison when they exceed it. It can
be a shard CAS `(slot_lsn, batch_index)`, or an origin
journal offset. No global `u64`, cross-writer feed token, or lexical byte
comparison is imposed. The source adapter defines successor/contiguity,
comparison within one history, and whether a whole CAS batch applies atomically.
Comparing different scope or source generations returns `Incomparable` until
the adapter proves a continuity mapping. Every comparison verdict binds the
exact two operand cursor encodings, their scope and source identity, and the
source proof used; the core rejects an unbound relation enum or a verdict for
different operands. A changed *feed* incarnation alone
does not invalidate an independent CAS cursor.

The adapter's `SourceProof` names the authority, source generation, contiguous
retained interval, and a stable high-water barrier. For external CAS it binds
to the actual committed slot chain and identifies retained checkpoints and
readback of unknown outcomes. For Hosted it binds to the epoch and fencing
verdict. For an origin source it binds to durable mutation history or a complete
reconciliation cut; peer agreement on a feed head is insufficient. Proofs are
opaque to Groupnet's domain logic but their comparison and verification hooks
are mandatory. A source that cannot supply them may offer best-effort hints,
but cannot enable a complete-index or event-complete read gate.

An application checkpoint is an *atomic state/cursor pair*: versioned state,
scope and source identity, applied position or vector, coverage proof, and
integrity metadata. The adapter either commits state and cursor together or
declares an idempotency key/equivalent replay proof. A cursor must never get
ahead of recoverable state. A source-native CAS checkpoint plus replay proof
can satisfy this obligation; no second Groupnet cursor file is required. The
adapter must say when replayed changes are query-visible, including unflushed
shardstore state. A restart validates the
pair against its source and schema; a missing, corrupt, stale, wrong-scope, or
unsupported checkpoint closes the local gate and starts recovery. Warm S3
object bodies are separate from an index checkpoint and remain suspect after
restart until independently validated.

## 3. Subscription guarantees and retention

`StateSync` means a correct, complete state for a scope at a proven cut and
catch-up to an explicit source barrier, subject to source availability and
processing capacity. It replays a retained suffix where available; otherwise
it obtains an authoritative snapshot and suffix. It does **not** promise that
an application observer receives each old event. An ephemeral `LiveHints`
attachment starts at head and retains the existing feed contract.

`EventComplete` means delivery of every committed source event in native order
from an accepted checkpoint. It is available only when the source declares
durable, seekable history and a retention policy covering this subscriber. A
snapshot never counts as event delivery. Records carry a stable `MessageId`
derived from source identity and native position, including batch subposition
where needed. `EventComplete` checkpoints name a stable `SubscriberId` and
`StreamId`/scope, and are durable at the source or in
an adapter-provided store whose compaction proof is reliable. Required
subscriber cursors protect history until acknowledged, or the subscriber
expires under its configured byte/age/lag policy with a terminal `Expired` or
`IrrecoverableGap` result. No silent skip or infinite storage promise.

Retirement of source history requires a durable checkpoint/snapshot that covers
state-sync consumers, and the minimum nonexpired required event-complete
subscriber cursor for event retention. A delivered or enqueued watermark is
never enough. Existing `WriteFeed::retire_through` remains a control-plane
ring operation and does not retire source history. A deployment may retire a
healthy s3cache feed to an anchor quickly; state sync must be prepared to take
a snapshot after even a brief outage.

Write acknowledgement waits explicitly name `AckRequirement`: a set of stable
subscriber IDs, a configured roster, or current eligible lease holders. The
chosen proof kind (`Invalidated` or `Materialized`), membership snapshot or
lease generation, deadline, and cancellation token are part of the request.
`AckOutcome` distinguishes `Satisfied`, `Pending { waiting }`,
`TimedOut { waiting }`, `Cancelled`, and `AuthorityLost`. Timeout reports
degradation and preserves the already committed source outcome. Batch acks may
amortize writes, but each acknowledged position has the declared proof.

## 4. Session state machine and race closure

The first native snapshot slice is specified in
[replication-snapshots.md](replication-snapshots.md).

One active catch-up session exists per `(StreamId, ScopeId, local replica)`;
multiple read floors and feed hints coalesce into its highest comparable
target. An incomparable target starts a new generation. The sans-IO engine in
`groupnet-core` consumes explicit events and emits effects; virtual time enters
only as an event. Its core states are `Unready`, `CheckingTail`, `Replaying`,
`Capturing`, `Transferring`, `Staging`, `CatchingUp`, `Affirming`, `Ready`, and
`RetryWait`. Failure, gap, proof loss, or supersession revokes serving before
further work. Generation numbers fence every source response, chunk, apply
completion, checkpoint, and affirmation. Each issued operation also has a
unique correlation token, so a late retry within the same generation cannot
satisfy a newer request. The engine accepts a completion only for its current
generation and outstanding token; only that pair may publish readiness.

The ordered recovery protocol is:

1. Load and validate the atomic checkpoint. Obtain a source proof and tail
   barrier independently of feed gossip. If a continuous retained interval
   starts after the checkpoint, replay under byte/event/time bounds. Otherwise
   choose a verified source snapshot or an authoritative source rebuild.

The initial replay core owns checkpoint bootstrap correlation as well: it
issues distinct load and guarded-install operations from the same session
token allocator used by tail, scan, and apply. A private state/cursor load is
never published until a durable exact-cursor install receipt arrives for that
install operation. A missing checkpoint still checks the source before
entering snapshot recovery. Transient load/install failures retry within the
session budget; cancellation and expiry reject late responses. Runtime workers
must check the core's live operation and next logical deadline before acting
on queued effects, rather than inventing a bootstrap token or retaining old
timer entries. A revocation acknowledgement has separate correlation.
2. Before snapshot cut `C0`, acquire retained replay or live capture with a
   lease/hold that guarantees every committed record after `C0` until attach.
   If the source cannot protect this interval, abort and retry from a newer
   cut. A source that can only scan and cannot prove concurrent mutations
   complete is ineligible for a read-authoritative snapshot.
3. Obtain snapshot metadata: protocol and schema version, scope, source
   generation, cut `C0`, coverage/authority proof, total bytes/chunks, and
   integrity digest. Transfer bounded frames into a private staging area.
   Validate every offset, count, size, digest, scope, and source incarnation.
   A partial transfer never mutates the live committed checkpoint.
4. Read committed records strictly after `C0` through an explicit source
   catch-up barrier `B`. On retention overrun, capture lapse, discontinuity,
   or failed proof, discard staged state and restart recovery. Serialized
   staging apply prevents a late snapshot from overwriting a newer delete.
5. Atomically install staged state plus its final native cursor and coverage
   proof, conditional on current recovery generation and install operation
   token. A rollback or failed atomic install cannot mark the scope ready.
   Attach ongoing source
   delivery with a no-gap barrier at or after `B`; recheck source tail to
   close the capture-to-subscribe race. Only then affirm materialization and
   evaluate the read gate. If cutover cannot be atomic in the application,
   keep the scope unready while a fenced commit protocol finishes.

The source's authoritative replay/snapshot transport uses a framed stream
over `BulkTransport` by default. An adapter can bind an existing gRPC or object
store data path by implementing the same bounded `ReplicationPlane` contract;
Groupnet still owns session ordering, cursor/proof validation, cancellation,
and cutover. Targeted, correlated bootstrap requests and replies carry
`session_id`, `generation`, operation token, scope, source proof, and deadline.
They do not replace application RPC for document search or peer forwarding.
If control-plane replication messages are needed, they add new frame kinds
inside the current `FRAME_VERSION`; old nodes drop unknown kinds, and existing
frame bodies stay unchanged. Bulk data has its own versioned framing and
limits. The initial replay-only slice needs no new control-plane frame.

## 5. Adapter and public API sketch

The shape below states ownership and proof obligations; exact Rust signatures
may evolve while preserving these semantics. The typed `P`/`Record` stay local
to the runtime adapter; the core receives size-bounded opaque cursor bytes plus
proof-bound adapter comparison verdicts, so core remains free of I/O and
application code. Every effect has a generation and operation token.

```rust,ignore
pub struct ReplicationConfig {
    pub max_cursor_bytes: usize,
    pub max_history_bytes: usize,
    pub max_history_events: usize,
    pub max_snapshot_bytes: u64,
    pub max_chunk_bytes: usize,
    pub max_inflight_bytes: usize,
    pub max_inflight_batches_per_scope: usize,
    pub max_active_scopes: usize,
    pub tail_check_interval: Duration,
    pub retry_budget: RetryBudget,
}

pub enum Subscription { LiveHints, StateSync, EventComplete(EventRetention) }
pub enum ApplyProof { Invalidated, Materialized }
pub enum ReadDecision { Serve { through: Cursor }, Wait { for_floor: Cursor },
                        Fallback { reason: Refusal }, Refuse { reason: Refusal } }
pub enum SessionOutcome { Ready { through: Cursor }, CatchingUp { stage: Stage },
                          Backpressured { retry_after: Duration },
                          Retryable { reason: RecoveryError },
                          Expired, IrrecoverableGap, Cancelled, Superseded }
pub struct Operation { pub generation: u64, pub token: OperationToken }

pub trait SourceAdapter {
    type Position; type Record; type Snapshot;
    // All answers bind scope + source generation and are verified before use.
    async fn reconcile_outcome(&self, key: IdempotencyKey) -> CommitOutcome<Self::Position>;
    async fn tail(&self, scope: &ScopeId) -> Result<SourceBarrier<Self::Position>>;
    async fn scan_after(&self, cursor: &Position<Self::Position>, limit: ScanLimit)
        -> Result<ContiguousBatch<Self::Record, Self::Position>>;
    async fn hold_and_snapshot(&self, scope: &ScopeId, limit: SnapshotLimit)
        -> Result<SnapshotOffer<Self::Snapshot, Self::Position>>;
    async fn renew_hold(&self, hold: HoldId) -> Result<RetainedThrough<Self::Position>>;
    async fn release_hold(&self, hold: HoldId);
    fn compare_and_verify(&self, left: &Cursor, right: &Cursor,
                          proof: &SourceProof) -> BoundCursorVerdict;
}

pub trait ApplicationAdapter<P, R> {
    // Every completion is fenced by generation and operation token.
    async fn load_checkpoint(&self, scope: &ScopeId, op: Operation)
        -> Result<Option<Checkpoint<P>>>;
    async fn revoke_serving(&self, scope: &ScopeId, op: Operation) -> Result<()>;
    async fn stage_snapshot(&self, offer: &SnapshotMeta<P>, op: Operation)
        -> Result<StageHandle>;
    async fn append_chunk(&self, stage: StageHandle, chunk: VerifiedChunk,
                          op: Operation) -> Result<()>;
    async fn apply_records(&self, target: ApplyTarget, records: &[R],
                           op: Operation) -> Result<ApplyReceipt<P>>;
    async fn install(&self, stage: StageHandle, checkpoint: Checkpoint<P>,
                     op: Operation) -> Result<MaterializedReceipt<P>>;
    async fn abort_stage(&self, stage: StageHandle, op: Operation);
    async fn persist_live(&self, receipt: ApplyReceipt<P>, op: Operation)
        -> Result<MaterializedReceipt<P>>;
    fn read_policy(&self, request: ReadRequest<P>, progress: &Progress<P>)
        -> DomainReadVerdict;
}

let handle = replication.subscribe(stream, scope, subscriber_id,
                                   Subscription::StateSync, source, app, plane)?;
let resumed = handle.resume_from(saved_checkpoint).await; // validates proof
let outcome = handle.catch_up(CatchUpTarget::AtLeast(native_floor)).await;
let checkpoint = handle.checkpoint().await; // materialized state/cursor proof
let decision = handle.read_decision(read_request); // includes mode authority
let wait = handle.wait_for_acks(required, ApplyProof::Invalidated, deadline).await;
```

`SourceAdapter` has no `ack(event)` shortcut: the source must show committed
records, authoritative ordering, retained coverage, and an ambiguous-outcome
readback. `ApplicationAdapter` owns domain representation and conflict rules,
but Groupnet drives retries, source choice, replay, snapshot capture, staging,
and affirmation. A standalone callback that asks the app to "recover now" is
not this API. The implementation may split traits by source capability so
simple retained-replay sources need not implement snapshots; the builder
rejects `StateSync` when neither a complete retained path nor a proven
snapshot/rebuild path exists, and rejects `EventComplete` without durable
history. Adapter errors have typed retryable, terminal, and authority-lost
classes. Bounded queues return backpressure, never silently drop committed
work. Cancellation stops work, aborts private staging, and leaves the last
committed checkpoint intact.

The read gate intersects five predicates: complete installed scope,
materialized cursor at the requested native floor, current generation,
continuity through the latest checked source barrier, and selected mode's
authority. A tail check is a progress bound, not proof that no later source
commit exists. Strict local authority also requires a source fence,
lease/read barrier, or pre-mutation revocation proof; otherwise the read uses
its configured source fallback. A shardstore `ConsistencyToken` maps to a
per-shard CAS LSN floor;
TTL `SeqFloors` are route hints only. A strong s3cache read additionally needs
its valid serve lease and feed/read barrier. A bounded read retains its
freshness bound and origin fallback. An incomplete index cannot prove LIST or
absence, and its current replicated-miss origin fallback remains. Origin or
valid source reads may continue while local recovery is unready.

## 6. Scheduling and capacity

The replay implementation enforces 8 MiB or 4096 events per batch, one
in-flight operation per scope, 1024 registered scopes, 16 concurrent adapter
operations, and a 32 MiB global native-batch reserve. Checkpoint recovery has
a separate 64 MiB per-candidate limit and 256 MiB global reserve held through
installation. Adapters must account for retained native state, including fork
heap allocations; encoded replay bytes alone do not bound a domain's full
replica memory. Domain memory admission remains necessary. Queues hold at most
32 commands per scope. An operation has a 30-second deadline including capacity
waits; three retries use a one-second delay. Replay-only tail checks currently
run every five seconds for each registered scope. These are configurable
starting limits, not measured throughput or latency guarantees.

Snapshot defaults planned for the next slice are a 1 GiB offer, 256 KiB chunks,
and four concurrent bulk transfers, with separately reserved live catch-up
capacity. Idle backoff to 60 seconds, deterministic jitter, and sparse active
scope scheduling remain required scalability work; the current runtime does
not implement them. Source-owned retained-history byte and age limits remain
separate. Snapshot builders must validate nonzero limits and require the source
hold to cover the complete attempt, including capacity waits and cutover.
There is no unbounded queue, transfer, retry loop, or required-ack wait.
History has byte, event, and age caps. Snapshot offers and chunks have encoded
byte caps and an integrity algorithm/version. A session has maximum in-flight
bytes, batches, elapsed time, and retry attempts before reporting a typed
stalled outcome. A bounded fair scheduler rotates active scopes, reserves
capacity for live catch-up while snapshots run, and coalesces wakeups and read
floors into one session per scope. It caps active streams, per-peer connections,
and concurrent origin builders.

Tail checks are event-driven on hints, source wakeups, and read floors, plus a
bounded anti-entropy cadence per *active* stream. The cadence must discover a
commit that was never enqueued, even if no later writes occur; jitter and
coalescing avoid a synchronized fleet scan. Idle scopes use an indexed active
set or sparse sweep, not a high-rate per-shard timer. The scheduler reports
the configured and observed tail-check lag. A slow subscriber is
backpressured, retried, and eventually explicitly expired if its retention
budget says so. Strong ack/lease deployments pay their selected per-write
cost; ordinary state sync does not wait for the whole fleet.

When no complete peer snapshot exists for an S3 bucket, a best-effort builder
lease chooses one origin scanner in a connected group. Expiry permits takeover;
a partition may create duplicate builders. Builder selection is only a cost
control. Neither builder nor peer snapshot grants source authority until
coverage and source reconciliation are proven.

Metrics expose per-scope source versus materialized cursor, retained low/head,
hint age, tail-check lag, event lag, gaps, replay/retry counts, snapshot bytes
and stage, generation, ack latency and waiting identities, source requests,
in-flight budgets, and every read-refusal reason. Latency reporting separates
source commit, hint publication, invalidation, materialization, and read
admission.

## 7. Consumer obligations and limits

For shardstore, the CAS slot log remains the serializer. Its adapter must
expose batch positions or prove atomic whole-slot application, checkpoint
retention, readback by idempotency key for unknown outcomes, and a
query-visible apply receipt. Existing `ReplicaFork::refresh_with_lower_bound`
and `refresh`, backed by `caslog` fork/checkpoint recovery, provide
authoritative replay and checkpoint
recovery; Groupnet schedules and gates them instead of duplicating CAS slot
ordering. Their current whole-refresh API does not enforce session byte or
event budgets. Integration first adds a bounded native replay step that applies
whole CAS slots to a private fork. It reports the native interval, encoded
bytes, commit count, provenance, and whether it reached a requested floor,
observed the next slot absent, exhausted a budget, or needs checkpoint recovery.
A slot that exceeds a budget is an explicit limit result; it is never split.
Checkpoint recovery has a separate transfer budget. An observed absent slot is
a source-prefix observation, not proof that a damaged log contains no later
slots. `Provenance::Recovered` cannot establish complete state. Shardstore's
existing degraded-read policy remains a separate domain decision and cannot
produce Groupnet's complete-state read permission.
The adapter retains Lean-to-Full fallback when provenance cannot
prove a Lean fold, and its floor-bearing segment search decides whether
unflushed documents are actually servable. Groupnet replaces the existing
detached and per-serve refresh scheduling with one coalesced session, waits
for a token's shard floor or chooses a valid source, and never interprets a
TTL LSN hint as proof. Docres may keep gRPC for forwarding/search and bind a
replication data plane if it meets the framed and bounded contract; existing
peer RPC alone does not provide a snapshot stream.

**s3cache deployment constraint (owner-confirmed 2026-09-28): zero coordination
or metadata writes to S3 by default.** Ordinary proxied user-object mutations
are unchanged. No mode puts Groupnet journals, admissions, checkpoints, or
snapshot objects in the origin bucket. A durable coordination store is an
explicit opt-in; a separate control bucket is one possible adapter, not a
startup requirement. Default recovery retains origin validation/reconciliation
and fallback. Durable event-complete delivery is exposed only when a configured
source supplies its history and retention obligations. Tests must prove that
default startup, reads, recovery, and user mutations issue no extra metadata
PUTs. The optional mode's storage traffic must be measured separately.

For s3cache, origin commit precedes feed publication. A configurable durable
shared intent/outcome journal in separate control storage, or a complete
origin reconciliation contract, must close the crash window before peer
snapshots can license an authoritative index. In the optional journal-backed
coherence mode, persist the
intent before forwarding the mutation and revoke local serving among current
lease holders by acknowledgement or lease lapse before origin mutation. The
intent and unresolved operation IDs must survive total fleet restart and be
inherited by joins and snapshots. An origin HEAD/LIST while an old request can
still commit cannot clear such an intent; resolution needs operation quiescence
or an idempotency/commit proof. Without a durable proof covering unresolved
operations, the affected scope stays origin-routed; neither a gossip head nor
a peer snapshot can clear it. Batch DELETE partial outcomes, COPY, and
multipart completion need the same treatment. This control journal is an
optional s3cache authority mechanism, not hidden user-object metadata or a
mandatory Groupnet log. The source adapter must identify the origin cut and
all mutations during a LIST
scan, including deletes and uncertain per-key writes. The application adapter
keeps PUT/DELETE conflict ordering, tombstones, hot-body invalidation, LIST
page merge, and absence rules. Mutations must pass through the coherent proxy
fleet; out-of-band origin writes require a separately proven reconciliation
source. A journal must not be added as a mandatory second log when the source
already provides safe committed replay.

The optional journal-backed pre-mutation protocol also needs a reader-admission
rule. Sampling current
lease holders leaves a race when a reader joins after the wait completes but
before the origin mutation starts. The control source orders reader admissions
with mutation intents. A reader starts a bounded local monotonic deadline
before appending its admission, confirms that exact append, and replays through
its admission before it can serve. Its gate requires both the ordinary serving
lease and this source admission. A renewal is a new source admission; ordinary
lease renewal cannot extend it. Earlier pending intents remain fenced.

A writer waits for each earlier admission to acknowledge the specific intent
and admission incarnation, or for its maximum permitted duration to elapse.
The expiry wait starts when the writer confirms the intent and accounts for the
documented clock-rate bound. Later admissions must replay the intent before
serving. Admission versions and duration bounds are validated across the
cluster; an unknown or unsupported bound fails closed. A restarted writer
restarts the full expiry wait. A restarted reader must obtain a fresh admission.
No persisted wall-clock timestamp is treated as a serving capability. This
protocol needs deterministic race tests before it can grant authority.

The optional s3cache control-source adapter uses a conditional-create slot chain per
bucket. It verifies ambiguous appends by reading the exact slot and comparing
the operation identity and payload. It never skips an uncertain slot. Intent,
outcome, and admission records share this ordering. This choice adds control
storage writes and contention to the coherent path; measurements must report
that cost. The source is optional, but absence of a proven publication and
admission mechanism cannot authorize a complete local multi-node index.

## 8. Build slices and verification

Each slice ends with the workspace's `AGENTS.md` verification matrix green.
Protocol decisions live in `groupnet-core`, without tokio, a clock read, or a
socket; the `groupnet-consistency` `replication` feature and facade
`consistency-replication` provide thin async I/O. New modules are split before
any Rust source reaches 1000 lines. The existing `handoff` helper remains an
optional gap remediator for Hosted group state, with no native resume cursor;
this protocol does not silently redefine it.

1. **Core vocabulary and replay.** Native scoped cursor codec/validation,
   source proof and retention verdicts, generation-fenced replay scheduler,
   typed outcomes, and a source-tail timer effect. Deterministic unit and
   seeded simulation tests cover skipped hints, duplicate/out-of-order
   records, unknown commit readback, restart, partitions, fair scope work,
   and source-generation discontinuity. No new wire kind is required in this
   slice; codec tests come with the first actual protocol message.
2. **Runtime replay and persistence.** Thin async source/app shells,
   `StateSync` subscription, bounded queues, durable checkpoint resume,
   source-tail anti-entropy, and floor/read gate. In-memory integration tests
   crash before/after apply and cursor persistence; a missed publication is
   found with no subsequent write. No snapshot or event-complete claim yet.
3. **Snapshot session.** Capture hold, bounded framed transfer, staged apply,
   replay-through-barrier, generation-fenced atomic install, attach and
   affirmation. Simulate retention overrun and every cutover crash phase;
   transfer concurrent PUT and DELETE in integration tests. Corrupt or late
   transfer must leave the scope unready and never resurrect a deleted row.
4. **Subscriptions and acknowledgements.** Stable named subscribers,
   event-complete retention/expiry, checkpoint resume, named required ack
   sets, proof kinds, deadlines/cancellation, correlated bootstrap messages,
   and metrics. Test slow/stopped readers, compaction, explicit terminal gap,
   invalidation before rebuild, lease lapse, and timeout after external commit.
5. **Consumers, one green integration at a time.** Migrate shardstore's
   CAS replay, floor wait, and refresh orchestration while preserving document
   semantics and docres tokens. Migrate s3cache's index recovery/coherence
   orchestration after proving its origin publication gap and checkpoint
   source; retain body validation and fallback. Remove the displaced app
   state machines, then run each consumer's fault tests.

Load gates measure p50/p99 catch-up and read latency, throughput, memory,
retained bytes, bootstrap traffic, added origin GET/LIST/HEAD requests, and
recovery under increasing scope count, write rate, slow peers, partitions,
restarts, and large snapshots. Verify configured bounds and live-update
progress during bulk transfer; publish measured results rather than invented
targets. A single origin builder is expected when connected, takeover after
failure, and duplicate builders are allowed under partition.

## 9. Implementation status

The first slice adds `groupnet_core::replication::SessionEngine`. It accepts
scoped opaque cursors, exact-operand source comparisons, and replay batch
metadata. Native payloads stay with the driver. Its effects request tail checks,
bounded scans, application materialization, serving revocation, and logical
timers. Read floors coalesce within one source history. Materialized and durable
checkpoint positions remain separate.

Each operation carries a caller-supplied session incarnation, a recovery
generation, and a monotonically allocated token. The caller must use a fresh
incarnation when recreating an engine that could receive old replies. Cancelled
sessions and exhausted retries do not resume on hints or authority updates.
Cancellation closes the gate before issuing a new-generation revocation that
the application can acknowledge. Source and mode authority both gate reads.

Core tests exercise floor waits, proof binding, bounded retries, cancellation,
restart identity, and retention gaps. A standalone seeded simulator drives
multiple scopes through missed hints, delayed and duplicate replies,
partitions, and restart. The existing membership simulator and wire frames are
unchanged.

The `groupnet-consistency` `replication` feature now supplies the replay-only
async manager; the facade exposes it with `consistency-replication`. Typed
source and application adapters keep native positions and batches outside the
core. Core-issued load and install operations restore an atomic checkpoint
before replay. The manager bounds registration, commands, operation admission,
replay bytes, and private checkpoint bytes. Coalesced hints and an independent
source check discover commits without a notification or later write.

Install and revocation permits fence synchronous publication against session
closure, supersession, and operation deadlines. An external asynchronous store
must enforce the same fence inside its state/cursor transaction; a callback
receipt alone cannot make that transaction safe. Read verdicts additionally
check current mode authority, source-check age, the requested floor, and the
application's independent domain predicate.

Runtime tests cover missed hints, floor waits, stale source checks, authority
loss, cancellation, late detached installs, checkpoint restore, and bounded
resource release. Snapshot recovery, durable event-complete retention, named
acknowledgements, scalable idle scheduling, and consumer migration remain in
the build order above. Selecting the core's `EventComplete` gap policy does not
itself establish a durable subscription or protect source history from
retirement.
