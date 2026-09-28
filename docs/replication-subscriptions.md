# Durable named subscriptions and required acknowledgements

Status: **accepted contract for the next implementation slice**. This refines
[replication.md](replication.md#3-subscription-guarantees-and-retention). It
adds an opt-in `EventComplete` capability above source-backed replay. It does
not change the existing attach-at-head feed, make `StateSync` snapshots count
as delivered events, or create another required log above a native CAS log.

## Observable contract

An `EventComplete` subscription has a stable `SubscriberId` (bounded group,
named topic, type/version, scope, and subscriber name), a fresh nonzero
subscriber **incarnation** on each process restart, a source history, and a
source-native checkpoint. `MessageId` binds that history to the exact native
record position, including batch subposition unless the source proves that a
whole batch is one atomic event. Bytes of native cursor positions are opaque;
the source adapter supplies proof-bound comparisons and contiguous batches.
Per-publisher feed numbers cannot be substituted. Delivery is at least once;
exactly once effects require the application's own atomic effect/receipt or
idempotency transaction.
Several named subscribers may consume the **same native scope** in one
process. The runtime registry keys their sessions by `(Scope, SubscriberId)`;
it never alters source scope or fabricates a subscriber-specific suffix,
which would invalidate native cursor and source proofs.

The five milestones are separate: `SourceCommitted` is a source proof;
`Delivered` reports transport acceptance; `Invalidated` says stale serving
has been revoked for the declared operation/domain; `Materialized` says
application effects are visible through an exact native cursor; and
`ReadPermitted` is a later intersection of complete state, floor, source/mode
authority, and domain policy. A delivered or invalidated acknowledgement
cannot advance a materialized checkpoint. A volatile materialization receipt
cannot advance a **durable** resume cursor. Every public receipt and metric
names its proof kind and exact scope/history.

## Register, retain, deliver, acknowledge

1. Before the first batch is delivered, the source atomically accepts a
   `RegisterSubscriber` request for `(SubscriberId, incarnation, source
   history, explicit start cursor, retention policy fingerprint)`. The
   resulting source-backed registration protects the requested retained
   suffix. Registration fails with `HistoryUnavailable`, `Unsupported`, or
   `Backpressured` if that protection cannot be established. It never silently
   attaches at head. A restart reads back the stable subscriber checkpoint,
   replaces the old incarnation conditionally, and begins at its exact next
   native position. Concurrent old-incarnation acks and deliveries are fenced.
   Registration uses a stable request ID; an unknown source response is read
   back by that ID before delivery or a conflicting incarnation starts.
   A confirmed registration alone does not fence a separate application
   store. The engine next enters `BindingSink` and emits a correlated
   `BindSinkEpoch` effect. The sink atomically installs that exact registered
   epoch as its durable fence together with its recovered application cursor
   metadata, under the local install permit. No delivery or source ack is
   allowed before the matching `SinkEpochBound` receipt. A crash between
   source registration and sink binding retries/readbacks the **same**
   source epoch and cannot invent a fresh cursor.
   Registration uses a stable request ID; an unknown source response is read
   back by that ID before delivery or a conflicting incarnation starts.
   A source may use its existing CAS checkpoint/retention machinery; Groupnet
   does not write a second copy of every event.
2. The session checks the source tail independently of gossip and requests
   bounded contiguous whole batches after the protected cursor. It verifies
   scope, history, proof ID, native advance, batch subposition, event count,
   encoded bytes, and retention boundary before exposing records. The worker
   holds no more than configured per-subscriber and global in-flight byte and
   event budgets. Demand and hints coalesce; bounded source-tail checks still
   find a committed event that never produced a hint. A slow consumer receives
   backpressure rather than dropped records.
3. The application durably records effects plus exact cursor, or proves
   deterministic idempotent replay, **before** the session asks the source to
   advance its protected subscriber cursor. Local `InstallPermit` guards the
   operation's generation and deadline, but cannot fence an external
   transaction delayed across process takeover. The sink must atomically
   condition its state/effects/cursor transaction on the source-registered
   subscription epoch and exact previous cursor. A new incarnation installs
   that epoch fence in the sink before it applies or acknowledges new records.
   An older in-flight transaction then cannot overwrite newer effects. A
   source-specific monotonic idempotency proof qualifies only if it enforces
   the same ordered effects/cursor invariant. The durable apply receipt binds
   subscriber key, epoch/incarnation, previous cursor, through cursor, and
   operation. The source's conditional
   `CommitSubscriberAck` binds `SubscriberId`, incarnation, previous cursor,
   new cursor, proof kind `Materialized`, source proof, and a stable ack request
   ID. The source must prevent retention from passing the old cursor until it
   durably accepts that change. An ambiguous ack response is resolved by
   readback of that exact request ID and cursor; until then, the core treats
   progress as unconfirmed. The source may already have durably advanced and
   compacted from the old cursor, which is safe only because application
   effects and their matching checkpoint preceded the ack. A crash after
   effects but before source
   ack redelivers the batch and relies on application deduplication. A crash
   after source ack is safe only because the effects/checkpoint were already
   recoverable. The source may coalesce adjacent acks, never skip an
   unproven batch or split a source-atomic event.
   If the recovered sink cursor is ahead of a source subscriber ack after a
   lost response, bind/reconciliation uses its exact durable effect proof to
   redeliver idempotently or conditionally advance the source ack. It never
   rolls application state backward to the older source ack.
4. Source compaction uses the minimum **durably acknowledged** cursor of
   nonexpired required subscribers and a state-sync checkpoint covering other
   replica needs. Enqueue, transport delivery, and feed retirement have no
   compaction authority. If a byte, event, age, or lag cap is reached, the
   source durably records explicit `Expired`/`IrrecoverableGap` for that
   subscription before dropping the protected history. The session then
   stops EventComplete delivery; it cannot repair the missing events using a
   snapshot under the same subscriber generation. An explicit reset may reuse
   the stable name only with a new incarnation and source-registered epoch, a
   chosen new start, and the old epoch's durable terminal tombstone visible to
   callers. It cannot silently resume the expired promise. Failure to
   persist expiry keeps retention or fails closed; it cannot silently skip.
   Ambiguous expiry is read back before the source reclaims history.

The first implementation requires an adapter capability that interlocks
registration, subscriber cursor updates, terminal expiry, and native history
retirement. If the source cannot provide that interlock, the builder rejects
`EventComplete`. The source's total retained byte/event/age budgets and its
policy for slow required subscribers are configured and observable. There is
no promise of unlimited retention or unlimited disconnected time. Subscriber
name lengths, registered subscriber count, protected histories, queued
events, and pending ack requests are all capped. An idle subscriber consumes
retention capacity even with no live worker; an explicit detach preserves its
registered cursor until its policy expires, while explicit unsubscribe
durably relinquishes protection.

## Named required acknowledgement waits

`AckRequirement` names a fixed, bounded set of stable `SubscriberId`s, a
versioned configured roster snapshot, or source-certified eligible lease
holders. A `WaitId` contains the caller's stable request ID, the same session
incarnation/generation and operation token already used by the replay
engine, scope/source history, and the exact committed target or mutation
intent. The request pins each required subscriber's incarnation or
source-certified subscription epoch when the wait starts. It fixes that
bounded set, proof kind (`Invalidated` or
`Materialized`), deadline, and cancellation token. Later gossip membership
changes neither add an unbounded waiter nor erase a required one. An ack
counts once only when its subscriber/incarnation, exact wait target, source
history, proof kind, and correlated operation match. Invalidation refers to
the specific intent and prior admission being fenced; it does not assert an
index rebuild. Materialization refers to an exact source-native cursor and
query-visible effects. `ReadPermitted` remains a separate read-policy result.
An ack from a restarted process with the same stable name cannot satisfy the
old wait by name alone; the first slice waits for its pinned epoch or times
out.

The sans-IO wait core returns `Satisfied`, `Pending { waiting }`,
`TimedOut { waiting }`, `Cancelled`, or `AuthorityLost` and emits a finite
timer. Duplicate, late, cross-generation, wrong-kind, or contradictory acks
are rejected. Cancellation ends the wait and drops its bounded roster; it
does not cancel an externally committed write or erase its durable event.
Timeout reports the unmet identities and degradation without claiming the
selected guarantee or rolling back the source commit. The write result
reports `SourceCommitted`/unknown commit separately from this wait outcome.
The first slice permits one active named wait per scope with bounded batch
targets; concurrent waits receive backpressure instead of creating an
unbounded operation map.

For a strong write waiting on **current lease holders**, gossip membership is
not a sound roster. The source-ordered admission protocol in
[replication-admission.md](replication-admission.md) supplies an intent-bound
reader set and join fence. Earlier admissions must acknowledge the exact
invalidation or conservatively lapse; later admissions inherit the pending
intent before serving. The wait core consumes that certified roster/proof and
cannot replace it with an observed peer list. If the source cannot certify
it, `AuthorityLost` closes the strong local path. Named static subscribers do
not require a whole-fleet barrier unless explicitly selected.
Admission-lapse fallback uses the checked `ExpiryTiming` clock-rate and
quantization-margin policy from the admission core; gossip lease timestamps
cannot provide that proof.

## Smallest implementable API slice

Keep the existing `StateSync` manager's per-scope registration. Register
`EventComplete` sessions separately by `(Scope, SubscriberId)` so two named
consumers of one source partition have independent protected cursors,
incarnations, and retry budgets. Each uses the same `SessionEngine`
implementation and its existing `(session, generation, token)` allocator;
add an internal `EventSubscription` facet and `AckWait` facet with bounded
state, not a third token space, second replay scheduler, or global
coordinator. The initial core
states are `Registering`, `BindingSink`, `Protected`, `Scanning`, `Delivering`, `Acking`,
`ReadingBackAck`, `RetryWait`, `Expired`, and `Cancelled`. Every effect has
one absolute deadline covering capacity wait and adapter I/O. The core
retains bounded cursor/proof/identity metadata only; native records and
transactions stay with the worker. Separate source/application capability
traits preserve the existing replay-only adapters:

`RegisterReceipt` binds the key, new source epoch, protected native cursor,
policy fingerprint, and exact register request. `FencedCheckpoint` binds that
same epoch to the recovered application cursor. The core accepts
`SinkEpochBound` only for its outstanding `BindSinkEpoch` operation and that
exact registration; all later apply receipts bind the same epoch and expected
previous cursor.

```rust,ignore
trait DurableSubscriptionSource: SourceAdapter {
    // Each result includes source history, exact request binding and proof.
    async fn register(&self, key: SubscriberKey, start: Cursor,
                      policy: RetentionPolicy, request_id: RegisterRequestId,
                      op: Operation) -> RegisterReceipt;
    async fn read_registration(&self, key: SubscriberKey,
                               request_id: RegisterRequestId,
                               op: Operation) -> RegisterReceipt;
    async fn protected_tail(&self, key: SubscriberKey, op: Operation) -> SourceProof;
    async fn scan_protected(&self, key: SubscriberKey, from: Cursor,
                            limit: ScanLimit, op: Operation) -> ContiguousBatch;
    async fn commit_ack(&self, key: SubscriberKey, prior: Cursor, through: Cursor,
                        request_id: AckRequestId, op: Operation) -> AckCommit;
    async fn read_ack(&self, key: SubscriberKey, request_id: AckRequestId,
                      op: Operation) -> AckCommit;
    async fn expire_or_unsubscribe(&self, key: SubscriberKey,
                                   request_id: TerminalRequestId, op: Operation)
        -> TerminalReceipt;
    async fn read_terminal(&self, key: SubscriberKey,
                           request_id: TerminalRequestId, op: Operation)
        -> TerminalReceipt;
}
trait DurableEventSink<P, R> {
    // Atomically bind the source registration epoch and recovered app cursor
    // in the sink's durable fence before delivery or source ack.
    async fn bind_epoch(&self, registration: RegisterReceipt,
                        permit: &InstallPermit) -> FencedCheckpoint<P>;
    // Conditional exact state/effects + cursor transaction, or equivalent
    // monotonic ordered idempotency proof; permit checks local lifecycle.
    async fn apply_and_checkpoint(&self, key: SubscriberKey,
                                  epoch: SubscriptionEpoch, previous: P,
                                  records: &[R], through: P,
                                  permit: &InstallPermit) -> DurableApplyReceipt<P>;
}
handle.event_complete(subscriber_id, retention_policy, explicit_start)?;
handle.wait_for_acks(wait_id, committed_target, fixed_required_set,
                     ApplyProof::Invalidated, deadline, cancellation)?;
```

The first runtime adapter can use shardstore's CAS log and native checkpoint
retention, with its batch positions or whole-batch atomicity declaration and
unknown-outcome readback. It must prove native protected-history behavior
before enabling EventComplete. A source-backed ack ledger may be required for
named waits, but this does not create a mandatory second durable application
log. S3cache keeps zero coordination/metadata S3 writes by default; a
separate durable coordination source is explicitly opt-in and its traffic is
measured. Ordinary S3 origin objects are never repurposed as control files.

## Tests that determine completion

Deterministic seeded core simulation varies committed events, partitions,
restarts, unknown ack outcomes, missed hints, native batch boundaries, slow
subscriber lag, compaction, cancellation, and deadline races. It asserts that
every committed message from the accepted checkpoint is delivered at least
once and never marked acknowledged before recoverable application effects;
duplicate delivery has one idempotent effect. Crash before effect commit,
after effect commit/before source ack, and after source ack/before response
must each recover correctly. Unknown ack readback cannot advance by a
different request or source history. Compaction cannot pass a protected
cursor; exceeding configured retention produces an explicit terminal result,
and a snapshot cannot disguise the missing event.
Delay an old-incarnation sink transaction through takeover: after the new
worker fences its registered epoch and applies a later record, the old
transaction must fail the shared epoch/previous-cursor condition and cannot
overwrite the newer effects.
Crash between source registration and `BindSinkEpoch`, then retry/read back
the same registered epoch; no delivery occurs before sink binding. Recover a
sink cursor ahead of the source ack after an unknown response and prove
idempotent reconciliation without rolling back the sink or skipping effects.

Ack-wait simulations use two named readers, an invalidation followed later
by materialization, duplicate/wrong-kind/late acks, a vanished reader, and a
join after writer intent. They assert exact fixed-set counting, no stale
incarnation revival, source-ordered lease admission for the joining reader,
timeout with the external commit still reported, and bounded roster/wait
memory. Runtime in-memory tests use real adapter transactions and forced
unknown responses; wire codec tests are added only if new control frames are
actually introduced. Before consumer migration, measure retained bytes,
source scan/readback requests, ack wait latency, and slow-subscriber
backpressure as subscriber and stream counts rise.
