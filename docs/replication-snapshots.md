# Source-backed snapshot recovery

[Documentation index](README.md) · [Replication contract](replication.md)

Status: **implemented native snapshot core/runtime contract; real storage and
consumer adapters remain integration obligations**. This refines the protocol in
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

The public capabilities live in
[`snapshot_api.rs`](../crates/groupnet-consistency/src/replication/snapshot_api.rs).
`SnapshotSourceAdapter` supplies typed `Hold`, `ReadHandle`, and `Attachment`
handles. Its `acquire_hold` receives the exact acquire `Operation`, the core's
absolute logical deadline, the corresponding runtime `Instant`, and bounded
`SnapshotConfig`. It returns a `SnapshotHold` containing both the handle and
an operation-bound `HoldReceipt`. `offer` returns `SnapshotImage<ReadHandle>`;
`attach` returns `SnapshotAttachment<Attachment>`. `barrier`, bounded
`read_chunk`, and `release_hold` complete the source capability.

`SnapshotApplicationAdapter` supplies a private `Stage`. `begin_stage` receives
the validated offer and a `CheckpointLimit`, and returns `SnapshotStage`
with its initial native-state charge. `write_chunk`, `verify_image`, and
`apply_private` report the updated total charge. `seal_stage` returns the
existing typed `Checkpoint`; its publication uses the normal application
`install_checkpoint` method and `InstallPermit`.

```rust,ignore
let replication = Replication::<_, _, NativeSnapshot>::new_native(
    group, source, application, limits,
)?;
```


The snapshot `Hold`, `ReadHandle`, `Attachment`, and `Stage` are owned directly by one
per-scope worker. The adapter does not hide an unbounded handle registry.
A read handle normally contains bounded streaming metadata. If the source
retains a complete image or native fork, it must hold separate source-domain
memory admission until that image and any background work are released. The
manager's chunk and application-stage quotas do not account for source-owned
image memory.
`SnapshotImage` owns the read handle and a bounded `SnapshotOffer` metadata record:
protocol/schema, scope and source history, source-proven cut `C0`, coverage
certificate, total encoded bytes, chunk count, and digest.
`SnapshotAttachment<T>` owns `T` plus the source-proven attach barrier/cursor and
retention-continuity certificate. The shell reserves its global byte permits
**before** each source read. The source must enforce
`max_bytes` while producing a chunk; the shell checks the returned size again
before delivering it to the private stage. The runtime transfers chunks
sequentially and in exact offset order. It checks nonzero progress, each
offset, count, size, total, checksum receipt, and final digest; an empty or
truncated transfer cannot install. The application stage may spool to bounded
private disk. `max_snapshot_bytes`, `max_chunks`, `max_chunk_bytes`,
`max_metadata_bytes`, and `max_total_ms` are finite config limits;
the metadata bound covers combined variable-length fields, with fixed struct
overhead bounded by one offered image per session;
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
   **before** choosing `C0`. A source qualifies only when that hold guarantees
   every committed record after `C0` through the
   configured total recovery deadline. A finite hold must cover the entire
   transfer, staged replay, install, attach, and final tail check; otherwise
   the source is ineligible. Its guarantee is measured conservatively from
   the pre-request local instant with the backend's documented monotonic
   clock-rate/latency margin; no node compares unsynchronized wall timestamps.
   A lost or late hold response may leave a finite source-side reservation,
   but it must expire under the same proven bound rather than leak forever.
   Renewal of shorter holds is not supported by this native snapshot contract.
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
   remains closed until attach, tail recheck, and materialization complete.
   At that point `Stage::Ready` means replay readiness and the finite hold
   can be released. Local read permission still separately intersects
   source/mode authority and application `may_serve`; an unrelated authority
   denial does not prolong retention. An absent source event hint cannot
   substitute for the tail check.

`SessionEngine` is the only transition scheduler. A snapshot module may add
private handlers and stages, but must not create a second token allocator or
readiness state machine. The core validates bounded metadata and proof-bound
cursor relations without parsing native cursor bytes or hashing source data.
Trusted adapters verify source-specific coverage and digest algorithms.
A native CAS adapter may read an existing checkpoint through bounded range
chunks and decode it into a private `ReplicaFork`; no new mandatory log or
peer data-plane wire format is needed. That adapter still has to prove its real
storage retention/cut contract. `BulkTransport` framing is an optional peer
transfer binding, not a transport automatically supplied by `NativeSnapshot`.
Hosted handoff's opaque coverage logic does not prove this source's committed
cut or retained suffix.

## Cancellation, cleanup, and failure

Every hold, offer, chunk, stage apply, install, attach, and tail completion is
bound to `(session, generation, token)` and its one absolute logical deadline.
The worker checks current operation before executing a queued effect and
ticks the core before accepting a response. Cancellation, supersession,
timeout, changed source history, corrupt bytes, missing chunk, failed install,
or attach gap closes admission and drops private state. Normal abort explicitly
releases the hold and discards the stage under bounded cleanup time.
Successful cutover releases the temporary hold, read handle, and private
stage while retaining the attached live continuation; abort, cancellation,
and supersession discard the attachment too. Cleanup and final local
disposal name the original acquire attempt, so an old operation cannot
remove a newer attempt's resources. Failed or expired release triggers
synchronous local disposal before another attempt may start; the finite
source-side hold still expires if the process dies.
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

Implementation and evidence references:

- [public source/application capabilities](../crates/groupnet-consistency/src/replication/snapshot_api.rs),
  [snapshot runtime driver](../crates/groupnet-consistency/src/replication/shell/driver/snapshot.rs),
  and [core transitions](../crates/groupnet-core/src/replication/session/snapshot.rs);
- [core recovery scenarios](../crates/groupnet-core/src/replication/snapshot_tests.rs),
  [seeded snapshot schedules](../crates/groupnet-sim/tests/replication_snapshot.rs),
  and [runtime adapter scenarios](../crates/groupnet-consistency/tests/replication_snapshot.rs).

These references cover the generic implementation and its test adapters.
They do not certify a real native checkpoint store, origin LIST scan, or
consumer application transaction. Enabling a production adapter still requires
the cut, suffix retention, memory charging, cancellation/cleanup interlock,
and atomic fenced install proofs described above.
