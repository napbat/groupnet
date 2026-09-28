# Source-backed snapshot recovery: first native slice

Status: **accepted implementation contract for the first native slice**. This refines the snapshot protocol in
[replication.md](replication.md#4-session-state-machine-and-race-closure). It
does not change the replay-only public API or require every source to support
snapshots. Groupnet owns the order, bounds, operation correlation, and read
gate. The source adapter owns committed source history and retention proof;
the application adapter owns private state and atomic state/cursor install.

## Opt-in boundary

Only a `StateSync` session with a validated `Config.snapshot: Some(...)` may
start snapshot recovery after a missing checkpoint or retention gap.
`EventComplete` still requires every retained committed event: a snapshot
cannot replace a missing event. `Config.snapshot` defaults to `None`, and the
existing `SourceAdapter` and `ApplicationAdapter` signatures stay usable as
they are. A `ReplayOnly` runtime mode remains the default type parameter of
`Replication<S, A, M = ReplayOnly>`; `SessionHandle` need not carry that mode
when its public methods do not expose snapshot resources. `NativeSnapshot`
selects the optional capabilities below. This uses ordinary explicit
associated types; it needs no unstable associated-type defaults, external
dependencies, or new wire frame.

```rust,ignore
pub trait SnapshotSourceAdapter: SourceAdapter {
    type Hold: Send + 'static;
    type ReadHandle: Send + 'static;
    type Attachment: Send + 'static;

    fn acquire_hold(&self, scope: Scope, limit: SnapshotLimit)
        -> impl Future<Output = Result<Self::Hold, AdapterFailure<Self::Error>>> + Send;
    fn offer(&self, hold: &mut Self::Hold, limit: SnapshotLimit)
        -> impl Future<Output = Result<SnapshotOffer<Self::Position, Self::ReadHandle>,
                                      AdapterFailure<Self::Error>>> + Send;
    fn read_chunk(&self, read: &mut Self::ReadHandle, offset: u64, max_bytes: usize)
        -> impl Future<Output = Result<Vec<u8>, AdapterFailure<Self::Error>>> + Send;
    fn barrier(&self, hold: &mut Self::Hold)
        -> impl Future<Output = Result<SourceProof, AdapterFailure<Self::Error>>> + Send;
    fn attach(&self, hold: &mut Self::Hold, after: Self::Position)
        -> impl Future<Output = Result<Attached<Self::Attachment>,
                                      AdapterFailure<Self::Error>>> + Send;
    fn release_hold(&self, hold: Self::Hold)
        -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;
}

pub trait SnapshotApplicationAdapter<P, B>: ApplicationAdapter<P, B> {
    type Stage: Send + 'static;

    fn begin_stage(&self, scope: Scope, metadata: SnapshotMetadata)
        -> impl Future<Output = Result<Self::Stage, AdapterFailure<Self::Error>>> + Send;
    fn write_chunk(&self, stage: &mut Self::Stage, offset: u64, bytes: Vec<u8>)
        -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;
    fn verify_image(&self, stage: &mut Self::Stage, digest: &[u8])
        -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;
    fn apply_private(&self, stage: &mut Self::Stage, from: P, through: P, batch: B)
        -> impl Future<Output = Result<(), AdapterFailure<Self::Error>>> + Send;
    fn seal_stage(&self, stage: Self::Stage, through: P)
        -> impl Future<Output = Result<Checkpoint<P, Self::Recovery>,
                                      AdapterFailure<Self::Error>>> + Send;
}
```

The snapshot `Hold`, `ReadHandle`, `Attachment`, and `Stage` are owned directly by one
per-scope worker. The adapter does not hide an unbounded handle registry.
`SnapshotOffer` owns the read handle and has a bounded metadata record:
protocol/schema, scope and source history, source-proven cut `C0`, coverage
certificate, total encoded bytes, chunk count, and digest.
`Attached<T>` owns `T` plus the source-proven attach barrier/cursor and
retention-continuity certificate. The shell reserves its global byte permits
**before** each source read. The source must enforce
`max_bytes` while producing a chunk; the shell checks the returned size again
before delivering it to the private stage. The first slice transfers chunks
sequentially and in exact offset order. It checks nonzero progress, each
offset, count, size, total, checksum receipt, and final digest; an empty or
truncated transfer cannot install. The application stage may spool to bounded
private disk. `max_snapshot_bytes`, `max_chunks`, `max_chunk_bytes`,
`max_metadata_bytes`, and `max_snapshot_time_ms` are finite config limits;
`max_chunk_bytes` fits the shared in-flight byte reserve. The adapter reports
actual charged bytes and refuses a source object larger than the declared
total. Merely trusting an advertised `Content-Length` is insufficient.
A separate `CheckpointLimit` caps decoded/private stage bytes and resident
native candidate bytes. The shell reserves that candidate budget before
creating the stage and holds it until install or abort; the adapter reports
charged bytes while decoding and aborts at the bound. Encoded chunk limits
alone do not bound a decoded `ReplicaFork` or adapter heap allocation.

## Recovery order and authority

1. Close the local read gate and sample the monotonic total-recovery deadline
   **before requesting** a source retention/capture hold. Obtain that hold
   **before** choosing `C0`. For this first slice, a source qualifies only
   when that hold guarantees every committed record after `C0` through the
   configured total recovery deadline. A finite hold must cover the entire
   transfer, staged replay, install, attach, and final tail check; otherwise
   the source is ineligible. Its guarantee is measured conservatively from
   the pre-request local instant with the backend's documented monotonic
   clock-rate/latency margin; no node compares unsynchronized wall timestamps.
   A lost or late hold response may leave a finite source-side reservation,
   but it must expire under the same proven bound rather than leak forever.
   Renewal of shorter holds is a later extension.
   The source must prove that the snapshot represents a consistent cut while
   writes continue. A peer's snapshot bytes or matching gossip heads alone
   are not source authority.
2. Validate the offer against the held source, exact scope/history, schema,
   cut, coverage, and all budgets. Read ordered bounded chunks into private
   stage and verify their complete integrity. The live checkpoint does not
   change during transfer.
3. Obtain explicit source barrier `B` under the same hold. Replay the complete
   native committed interval `(C0, B]` in bounded whole batches into the
   private stage. Every batch uses the existing source-native comparison and
   coverage proofs; source retention overrun, continuity failure, or hold
   loss discards the stage. A late snapshot cannot overwrite a later delete.
4. Seal the stage as an atomic state/cursor candidate at `B` and use the
   existing guarded checkpoint-install permit. Only a durable receipt for
   exactly `B` may enter post-install catch-up. An install that committed
   before cancellation still remains read-unready until a new generation
   proves its state; a stale operation cannot mark it Ready.
5. While the hold remains valid, attach ongoing source delivery at or after
   `B`, with a no-gap proof and a worker-owned attachment handle (which may be
   `()` for a source whose normal retained-log polling itself supplies the
   continuation). Recheck the authoritative source tail and reuse
   the current session's bounded replay loop to cover the attach barrier.
   The snapshot transition stays in the **same** session generation; every
   effect receives a fresh token from the existing allocator. Read admission
   remains closed until attach, tail recheck, materialization, source/mode
   authority, and application `may_serve` all agree. Only then release the
   hold. An absent source event hint cannot substitute for the tail check.

`SessionEngine` is the only transition scheduler. A snapshot module may add
private handlers and stages, but must not create a second token allocator or
readiness state machine. The core validates bounded metadata and proof-bound
cursor relations without parsing native cursor bytes or hashing source data.
Trusted adapters verify source-specific coverage and digest algorithms.
The first native adapter may read an existing CAS checkpoint through bounded
range chunks and decode it into a private `ReplicaFork`; no new mandatory
log or peer data-plane wire format is needed. `BulkTransport` framing can be
bound later for peer transfer. Hosted handoff's opaque coverage logic does
not prove this source's committed cut or retained suffix.

## Cancellation, cleanup, and failure

Every hold, offer, chunk, stage apply, install, attach, and tail completion is
bound to `(session, generation, token)` and its one absolute logical deadline.
The worker checks current operation before executing a queued effect and
ticks the core before accepting a response. Cancellation, supersession,
timeout, changed source history, corrupt bytes, missing chunk, failed install,
or attach gap closes admission and drops private state. Normal abort explicitly
releases the hold and discards the stage under bounded cleanup time.
`ReadHandle`, `Attachment`, and `Stage` must also clean local resources on drop; a remote
hold needs finite source-side expiry if the process dies before explicit
release. A late successful hold reply after cancellation is released rather
than retained. No cleanup result can retroactively authorize a read.

S3cache keeps **zero coordination or metadata writes to S3 by default**.
An optional durable coordination source may be configured separately, but
this snapshot API neither writes snapshot files into the origin bucket nor
turns that source on by default. Without a source-proven mutation cut and
retained suffix, a peer snapshot does not authorize a complete local index;
existing origin fallback continues.

## Deterministic completion criteria

Core tests and seeded virtual-time schedules must prove: hold is acquired
before cut; a concurrent commit at every boundary is either in the image or
replayed after `C0`; bounded chunks reject zero progress, duplicate/reordered
offsets, byte/count overflow, truncated final data, bad digest, wrong schema,
and cross-scope/history offers; a late delete survives staged replay;
retention overrun or hold loss never installs; cancel/restart during every
phase rejects old tokens and drops resources; a failed or volatile install
never advances the checkpoint; attach after `B` catches commits made between
capture and subscription; no node reports Ready before attach and tail
recheck. One schedule keeps writes arriving through cutover and still reaches
Ready under the declared hold/deadline bounds. An in-memory runtime adapter
test checks byte permits, one per-scope stage, bounded cleanup, and exact
guarded install. The native CAS adapter then proves the same cut/retention
contract against its real checkpoint and log APIs before claiming authority.
