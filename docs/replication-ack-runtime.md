# Named acknowledgement wait runtime slice

Status: **implemented fixed-roster runtime slice**. This implements only the
named, fixed-roster wait in [replication-subscriptions.md](replication-subscriptions.md).
It does not register durable `EventComplete` subscribers or infer lease-holder
eligibility from gossip. The existing replay and native snapshot adapters and
wire formats remain unchanged.

## Boundary and public shape

`SessionHandle::wait_named(request, deadline)` accepts a runtime request
envelope whose `CertifiedRoster` was obtained from the authoritative source.
The envelope contains all `AckWaitRequest` fields except its core-clock
`due`; callers cannot know the worker's clock origin. The
request includes a stable caller request ID, exact target and source history,
proof kind (`Invalidated` or `Materialized`), policy version, and the fixed
bounded set of subscriber names with pinned incarnations and registration
epochs. The runtime never turns a current peer list into that set. The handle
returns the core's typed `AckWaitOutcome` plus the wait's exact target and
proof kind; a timeout or cancelled wait does not change the result of the
external write. A second active wait on the same session is backpressured.
The caller must retain its request ID for retry/readback; this slice does not
claim durable wait recovery after process loss.
The public method samples its monotonic `deadline` **before enqueue**. The
worker converts that original deadline to core-logical `due` before source
certification. Certification, capacity admission, and observation consume
that same absolute budget; each source call ends by the earlier of the wait
deadline and the normal operation timeout. An expired request is never
started merely because certification returned.

The opt-in bridge is attached to a `Replication` manager once, before any
session opens, so existing `SourceAdapter`, `ApplicationAdapter`, and snapshot
mode signatures stay source compatible. One concrete shape is:

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
manager.with_ack_evidence(source, limits)?; // before open()
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
roster larger than the limit is rejected, not truncated. Pending
certification occupies the session's **one active wait slot**. Even an empty
roster needs source certification; the shell cannot infer satisfaction.

## Core-driven lifecycle

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
the stable wait operation. Empty source checks must have an explicit core
event and finite core-scheduled next poll; neither hints nor replay progress
are needed to discover a later acknowledgement. Specifically, an empty poll
returns `AckChecked`; the core arms the next poll at the finite configured
`poll_ms`, capped by the original wait deadline. A source hint may coalesce
an earlier check without changing the roster or deadline. Cancellation
ends only this wait and releases its waiter; it cannot cancel a committed
source mutation or revoke a live state-sync session. The worker resolves all
waiters on manager close, driver failure, cancellation, and timeout so no
caller can hang after its bounded deadline.
Cancellation, timeout, and dropped caller futures fence only their exact
wait envelope, including while certification is stalled. Pre-start shell
correlation uses a local request generation plus the caller's stable request
ID, never a fabricated protocol `Operation`. Once certified, the core's
operation is the sole protocol token. Stale cancellation from an old
envelope cannot cancel a later wait on the same scope.

The wait target is source-native and opaque. `Invalidated` evidence is tied
to the exact intent and admission being fenced; `Materialized` evidence is
tied to the exact native cursor and query-visible effect. Neither kind is
substituted for the other, nor does a satisfied wait itself grant local read
permission. Loss of the source certificate returns `AuthorityLost`. A local
state-sync `Authority(false)` only closes that session's read path; it cannot
negate source-certified evidence for an external committed target. The
separate state-sync read gate continues to follow its own source/domain
authority rules.

## Completion evidence

Core unit and seeded simulations prove fixed-set counting with two required
names, duplicate and wrong-kind/epoch rejection, stale generation and late
response fencing, cancellation, authority loss, and deadline behavior.
Runtime in-memory tests use one source-certified roster and delayed per-name
acks, including a commit with **no gossip hint** discovered by polling. They
also check concurrent replay fairness, a stalled evidence source, bounded
roster/identity rejection, one-active-wait backpressure, and a closed worker
resolving the caller. The first source adapter remains opt-in; an unsupported
source or lease-holder roster returns `Unsupported` before the wait starts.
