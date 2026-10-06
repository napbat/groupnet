# Durable named subscriptions and required acknowledgements

[Documentation index](README.md) · [Replication contract](replication.md)

**Navigation:** [status](#implementation-status) ·
[observable contract](#observable-contract) ·
[registration and retention](#register-retain-deliver-acknowledge) ·
[required waits](#named-required-acknowledgement-waits) ·
[public API](#public-api-and-ownership) ·
[wait runtime](#named-acknowledgement-wait-runtime).

## Implementation status

The delivery core and runtime implement explicit `StartAt` and `ResumeExisting`,
source registration and exact ambiguous-outcome readback, durable sink epoch
binding, protected replay, and source acknowledgement after durable sink
application. `with_event_complete` adds these opt-in capabilities to the
replication manager. State-sync and named subscriptions share registration,
operation, and byte admission. A named session disables snapshot repair even
when state-sync sessions in the same manager support snapshots.

Local cancellation and `close_named_if` stop delivery and retain the durable
source registration. A bounded `unsubscribe` conditionally writes and reads
back an exact source tombstone; `unsubscribe_detached` does the same from a
source-current durable ack without a healthy sink. Source expiry or a proven
retention gap stops delivery immediately. A separate bounded terminal-ledger
read exposes a source-certified tombstone, and `ResetAt` atomically compares it
before claiming a new lineage with a higher persistent ordinal. Unknown
terminal writes remain unconfirmed until exact readback. A 64-seed
virtual-time test covers lost, delayed, duplicated and reordered delivery
responses, crash after durable sink effects before source ack, higher-epoch
resume, and healed progress without duplicate effects. Production retained-history
adapters and consumer migration remain unfinished. Source-certified fixed-roster
acknowledgement waits are implemented separately from durable delivery; see
[the runtime contract](#named-acknowledgement-wait-runtime).

A confirmed unsubscribe releases the named lineage's source retention. It
does not roll back sink effects or prove that an already-issued remote sink
transaction has quiesced; sink transactions must still enforce their durable
epoch and previous-cursor conditions. Detached unsubscribe does no sink I/O.

This accepted core/runtime contract refines
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
   The public start choice is explicit: `StartAt` conditionally claims an
   absent name and must protect exactly the requested native cursor;
   `ResumeExisting` first reads the source's current durable named ack and
   ordinal, then atomically compares both that prior ordinal and durable ack
   cursor before replacing them with a fresh
   incarnation and a stable new request ID. A conclusive prior-ordinal
   mismatch performs no write and stops this attempt; an unknown replacement
   response is read back by its exact request ID. Missing, expired, or
   unverifiable prior state never falls back to an attach-at-head claim.
   A confirmed registration alone does not fence a separate application
   store. The engine next enters `BindingSink` and emits a correlated
   `BindSinkEpoch` effect. The sink atomically installs that exact registered
   epoch as its durable fence together with its recovered application cursor
   metadata, under the local install permit. No delivery or source ack is
   allowed before the matching `SinkEpochBound` receipt. A crash between
   source registration and sink binding retries/readbacks the **same**
   source epoch and cannot invent a fresh cursor.
   Each accepted registration also carries a durable, source-certified
   **per-subscriber fence ordinal**. The source strictly increments it across
   incarnation replacement, terminal reset, and source-history replacement;
   exhaustion is terminal and an old ordinal is never reused.
   Loss, rollback, or recreation of that ordinal ledger is authority loss,
   never permission to restart ordinals; a source cannot advertise
   `EventComplete` unless this persistent fence history survives native-log
   history replacement. The sink's
   `BindSinkEpoch` transaction atomically stores that ordinal with its epoch
   and recovered application cursor. An exact retry of the same
   ordinal/epoch/request is a no-op or readback that cannot lower the sink
   cursor. It accepts a greater ordinal only with the certified
   registration and an explicit old-lineage reconciliation or reset, and
   rejects every lower ordinal. A delayed old bind therefore cannot replace
   a newer fence even if it began before the newer incarnation registered.
   Binding a new history creates an explicit new sink lineage; it never
   relabels effects from the old history as materialized in the new one.
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

The implemented capability requires an adapter interlock between
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
old wait by name alone; the implemented wait uses its pinned epoch or times
out.

The sans-IO wait core returns `Satisfied`, `Pending { waiting }`,
`TimedOut { waiting }`, `Cancelled`, or `AuthorityLost` and emits a finite
timer. Duplicate, late, cross-generation, wrong-kind, or contradictory acks
are rejected. Cancellation ends the wait and drops its bounded roster; it
does not cancel an externally committed write or erase its durable event.
Timeout reports the unmet identities and degradation without claiming the
selected guarantee or rolling back the source commit. The write result
reports `SourceCommitted`/unknown commit separately from this wait outcome.
The runtime permits one active named wait per scope with bounded batch
targets; concurrent waits receive backpressure instead of creating an
unbounded operation map.

For a strong write waiting on **current lease holders**, gossip membership is
not a sound roster. The source-ordered admission protocol in
[replication.md](replication.md#source-ordered-admission-decision-core) supplies an intent-bound
reader set and join fence. Earlier admissions must acknowledge the exact
invalidation or conservatively lapse; later admissions inherit the pending
intent before serving. The wait core consumes that certified roster/proof and
cannot replace it with an observed peer list. If the source cannot certify
it, `AuthorityLost` closes the strong local path. Named static subscribers do
not require a whole-fleet barrier unless explicitly selected.
Admission-lapse fallback uses the checked `ExpiryTiming` clock-rate and
quantization-margin policy from the admission core; gossip lease timestamps
cannot provide that proof.

## Public API and ownership

`StateSync` keeps its per-scope registration. `EventComplete` sessions register
separately by `(Scope, SubscriberId)` so two named consumers of one source
partition have independent protected cursors, incarnations, and retry budgets.
Each uses the same `SessionEngine` implementation and its existing
`(session, generation, token)` allocator. The internal subscription and ack-wait
facets retain bounded state; they do not create another token space, replay
scheduler, or global coordinator. Subscription stages include `Registering`,
`BindingSink`, `Protected`, `Scanning`, `Delivering`, `Acking`,
`ReadingBackAck`, `RetryWait`, `Expired`, and `Cancelled`. Every effect has
one absolute deadline covering capacity wait and adapter I/O. The core retains
bounded cursor/proof/identity metadata only; native records and transactions
stay with the worker. Separate source/application capability traits preserve
the replay-only adapters.

`RegisterReceipt` binds the key, new source epoch, monotonic fence ordinal,
protected native cursor, policy fingerprint, and exact register request.
`FencedCheckpoint` binds that same epoch and ordinal to the recovered
application cursor. The core accepts
`SinkEpochBound` only for its outstanding `BindSinkEpoch` operation and that
exact registration; all later apply receipts bind the same epoch and expected
previous cursor.

The implemented traits are in
[`subscription_api.rs`](../crates/groupnet-consistency/src/replication/subscription_api.rs).
`DurableSubscriptionSource: SourceAdapter` provides `read_current_subscriber`,
`register_subscriber`, exact `read_subscriber_registration`, `subscriber_tail`,
`scan_subscriber`, `commit_subscriber_ack`, exact `read_subscriber_ack`,
`commit_subscriber_terminal`, exact `read_subscriber_terminal`, and
`read_current_terminal`. `SubscriptionSourceResult` distinguishes accepted
conditional writes, conclusive no-write rejections, and ambiguous failures.
`DurableEventSink<P, B>` provides `bind_subscriber_epoch` and
`apply_subscriber_batch`; the latter takes both the batch's native interval and
the previous sink cursor, and persists the stable source-ack request ID with
the effects. Standard futures keep typed native records in the adapter.

The [manager API](../crates/groupnet-consistency/src/replication/shell/event_complete.rs)
uses an explicit fresh nonzero session incarnation:

```rust,ignore
let subscriptions = replication.with_event_complete(sink, subscription_limits)?;
let handle = subscriptions.open_named(
    &scope,
    subscriber_id,
    fresh_session_id,
    SubscriptionStart::StartAt { position, policy, request_id },
)?;
// Restart: ResumeExisting { policy, request_id }.
// Terminal reset: ResetAt { position, policy, request_id, prior: tombstone }.
```

`NamedSubscriptionHandle` reports source ack separately from sink cursor and
terminal state. Local cancellation/conditional close retain source protection;
bounded unsubscribe writes an exact durable tombstone. Detached unsubscribe
reads the source-current ack and performs no sink I/O. Terminal inspection is
read-only; only a later conditional `ResetAt` comparing that exact tombstone
can claim a new lineage. Named waits use the separate
[`wait_named`](#boundary-and-public-shape) API, not a durable-delivery shortcut.

A shardstore adapter can use its CAS log and native checkpoint
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
Delay an old incarnation's **bind** across the new bind and apply as well:
its lower source-certified fence ordinal must be rejected without replacing
the new epoch or its recovered cursor.
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

Evidence entry points:
[core subscription transitions](../crates/groupnet-core/src/replication/session/subscription.rs),
[terminal transitions](../crates/groupnet-core/src/replication/session/subscription/terminal.rs),
[seeded delivery schedules](../crates/groupnet-sim/tests/replication_subscription_delivery.rs),
[queued fault schedules](../crates/groupnet-sim/tests/replication_subscription_faults.rs),
and [runtime adapter scenarios](../crates/groupnet-consistency/tests/replication_event_complete.rs).
They do not establish retention or atomic sink behavior for a production
consumer adapter.

## Named acknowledgement wait runtime

Status: **implemented fixed-roster core/runtime support**. This runtime waits
on source-certified evidence; it does not register durable `EventComplete`
subscribers or infer lease-holder eligibility from gossip. Existing replay,
native snapshot adapters, and wire formats remain unchanged.

### Boundary and public shape

`SessionHandle::wait_named(request, deadline)` accepts a runtime request
envelope whose `CertifiedRoster` was obtained from the authoritative source.
The envelope contains all `AckWaitRequest` fields except its core-clock
`due`; callers cannot know the worker's clock origin. The request includes a
stable caller request ID, exact target and source history, proof kind
(`Invalidated` or `Materialized`), policy version, and the fixed bounded set of
subscriber names with pinned incarnations and registration epochs. The runtime
never turns a current peer list into that set. The handle returns the core's
typed `AckWaitOutcome` plus the wait's exact target and proof kind; a timeout or
cancelled wait does not change the result of the external write. A second active
wait on the same session is backpressured. The caller must retain its request
ID for retry/readback; this API does not claim durable wait recovery after
process loss.

The public method samples its monotonic `deadline` **before enqueue**. The
worker converts that original deadline to core-logical `due` before source
certification. Certification, capacity admission, and observation consume
that same absolute budget; each source call ends by the earlier of the wait
deadline and the normal operation timeout. An expired request is never started
merely because certification returned.

The opt-in bridge is attached to a `Replication` manager once, before any
session opens, so existing `SourceAdapter`, `ApplicationAdapter`, and snapshot
mode signatures stay source compatible. The public shape is:

```rust,ignore
pub trait AckEvidenceSource: Send + Sync + 'static {
    fn certify<'a>(&'a self, request: &'a NamedAckRequest,
                   limits: AckWaitLimits)
        -> Pin<Box<dyn Future<Output = Result<(), AckSourceFailure>> + Send + 'a>>;
    fn observe<'a>(&'a self, request: &'a AckWaitRequest, poll: Operation,
                   waiting: &'a [RequiredSubscriber], limits: AckWaitLimits)
        -> Pin<Box<dyn Future<Output = Result<AckObservation, AckSourceFailure>> + Send + 'a>>;
}
pub enum AckObservation {
    Evidence(Box<AckEvidence>),
    Pending,
    AuthorityLost,
}
let manager = manager.with_ack_evidence(source, limits)?; // before open()
handle.wait_named(request, deadline).await;
```

`certify` verifies the certificate and every pinned registration against the
source, including scope, history, target, proof kind, policy version, and
source epoch. `observe` checks the same binding for each acknowledgement;
the sans-IO core additionally checks exact equality against the active wait
and rejects duplicate, stale, wrong-kind, and cross-generation evidence. A
peer's unsigned assertion is never passed as trusted evidence. The bridge
uses boxed **standard futures** for object-safe opt-in dispatch; it stores no
erased native records and uses no runtime downcasts. Invalid source evidence
fails closed as authority loss or terminal adapter failure. Source uncertainty
never counts as an acknowledgement.

The bridge returns at most one bounded acknowledgement per observation. Each
query includes the current unmet subset so a stateless source does not keep
returning an already counted member. `NamedAckResult` binds the request ID,
exact target, proof kind, and outcome. A timeout reports the last core-confirmed
unmet identities, including when the caller deadline fires while I/O is blocked.
Source operations share the existing global operation semaphore, absolute
operation deadline, and FIFO worker turn with replay and snapshot work. A
slow source call cannot hold the whole manager's capacity; its future must
be cancellation-safe. Certification and each observation also carry their
own bounded byte/identity limits and consume a finite worker queue slot.
The manager checks `AckWaitLimits` and request metadata before accepting
work; no source call may allocate an unbounded roster or proof. A certified
roster larger than the limit is rejected, not truncated. Pending certification
occupies the session's **one active wait slot**. Even an empty roster needs
source certification; the shell cannot infer satisfaction.

### Core-driven lifecycle

The manager's per-scope worker retains the current `SessionEngine` and its
`(session, generation, token)` allocator. It sends `StartAckWait` only after
source certification. The core holds one stable wait operation for
cancellation/outcome and allocates a **fresh poll operation** for every
`ObserveNamedAcks` effect. Both use that same allocator; a delayed empty poll
cannot clear or satisfy a later poll. The core emits an absolute timer for
each poll, capped at the earlier of its attempt timeout and whole-wait
deadline, and eventually `AckWaitFinished` for the stable wait operation.
The worker forwards only source-verified `AckObserved` or `AckAuthorityLost`
replies, after advancing the core's logical clock and checking that the
poll operation is still current. `AckAuthorityLost` and `CancelAckWait` bind
the stable wait operation. Empty source checks have an explicit core event
and finite core-scheduled next poll; neither hints nor replay progress are
needed to discover a later acknowledgement. An empty poll returns `AckChecked`;
the core arms the next poll at the finite configured `poll_ms`, capped by the
original wait deadline. A source hint may coalesce an earlier check without
changing the roster or deadline.

Cancellation ends only this wait and releases its waiter; it cannot cancel a
committed source mutation or revoke a live state-sync session. The worker
resolves all waiters on manager close, driver failure, cancellation, and timeout
so no caller can hang after its bounded deadline. Cancellation, timeout, and
dropped caller futures fence only their exact wait envelope, including while
certification is stalled. Pre-start shell correlation uses a local request
generation plus the caller's stable request ID, never a fabricated protocol
`Operation`. Once certified, the core's operation is the sole protocol token.
Stale cancellation from an old envelope cannot cancel a later wait on the same
scope.

The wait target is source-native and opaque. `Invalidated` evidence is tied
to the exact intent and admission being fenced; `Materialized` evidence is
tied to the exact native cursor and query-visible effect. Neither kind is
substituted for the other, nor does a satisfied wait itself grant local read
permission. Loss of the source certificate returns `AuthorityLost`. A local
state-sync `Authority(false)` only closes that session's read path; it cannot
negate source-certified evidence for an external committed target. The
separate state-sync read gate continues to follow its own source/domain
authority rules.

### Completion evidence

Core unit and seeded simulations cover fixed-set counting with two required
names, duplicate and wrong-kind/epoch rejection, stale generation and late
response fencing, cancellation, authority loss, and deadline behavior.
Runtime in-memory tests use one source-certified roster and delayed per-name
acks, including a commit with **no gossip hint** discovered by polling. They
also check concurrent replay fairness, a stalled evidence source, bounded
roster/identity rejection, one-active-wait backpressure, and a closed worker
resolving the caller. The source adapter remains opt-in; an unsupported source
or uncertifiable lease-holder roster must return `Unsupported` before the wait
starts. This implementation does not supply a production lease-holder adapter.

References: [public acknowledgement API](../crates/groupnet-consistency/src/replication/ack_api.rs),
[runtime driver](../crates/groupnet-consistency/src/replication/shell/driver/ack.rs),
[core wait transitions](../crates/groupnet-core/src/replication/session/ack_wait.rs),
[core scenarios](../crates/groupnet-core/src/replication/session/ack_wait_tests.rs),
[seeded waits](../crates/groupnet-sim/tests/replication_ack_wait.rs),
[wait fault schedules](../crates/groupnet-sim/tests/replication_ack_wait_faults.rs),
and [runtime scenarios](../crates/groupnet-consistency/tests/replication_ack.rs).
