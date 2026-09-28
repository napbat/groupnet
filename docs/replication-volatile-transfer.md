# Bounded transfer of a volatile index image

Status: **claim/transfer sans-IO cores and deterministic tests implemented;
runtime source, bulk transport, and consumer integration pending**. This refines the peer
bootstrap contract in [replication-volatile-bootstrap.md](replication-volatile-bootstrap.md).
It transfers an already guarded donor index. It does not certify origin
freshness, durable writer history, or local read authority.

## One session and exact identity

The claim session selects a donor and emits `DonorAvailable { op, selected }`.
An opt-in transfer child is constructed only for that exact operation and
selected `ClaimIdentity`. The composite claim/transfer engine owns one
`next_token` counter. It must expose a private `allocate_token` operation
that increments this counter **without** replacing the parent's selected
donor, `DonorAvailable` operation, or deadline. The existing claim
`operation()` can use that allocator and then set its own outstanding work;
transfer phases can use the same allocator for typed child operations bound
to the parent `op`. No second timer loop or unscoped token counter is added.
Selection replacement, claim expiry, `CancelWork`, and ordinary recovery
supersession synchronously invalidate the transfer child and all its tokens.
The child emits exact cleanup, never an independent origin-retry decision.
The claim parent chooses a bounded donor takeover or its existing guarded
origin fallback after the child reports failure.

The first `DonorAvailable` operation's absolute `operation_due` and the
original follower episode's `total_due` are retained when transfer starts.
Its effective transfer deadline is their minimum. The ordinary claim poll
timer pauses while transfer is active; `ClaimEngine::Tick` drives the child
deadline, local claim renewals, and a separately correlated native TTL
refresh for the exact selected donor. An absent, expired, contradictory, or
unrefreshed claim aborts transfer without replacing the parent operation
with `ObserveClaims`. Transfer success completes the candidate handoff;
failure excludes that exact donor and resumes bounded observation/takeover
within the original total budget. Neither a delayed offer nor a later ready
advertisement extends either deadline. A self-built `Ready` donor may remain
available to others, but its own claim grants no serving authority.

The donor offer binds the scope, exact donor boot/session/attempt, fresh
`CaptureId`, schema/version, captured index coverage, image cut `C`, encoded
and decoded image bounds, exact sorted membership identities, native writer
cuts, chunk count, and an adapter-verified full-image commitment. The
follower declares its exact supported application schema at construction;
an offer with a different schema is rejected before any stage reservation.
The follower reserves its private encoded/decoded stage and a global runtime
memory permit before receiving any chunk. An image with no admissible finite
bound is rejected before transfer. The source-facing adapter supplies a
bounded, current offer; the core checks identity, budgets, and stage order,
not application bytes or a hash algorithm.

## Ordered effects and callbacks

The core issues one chunk request at a time. Each response must match its
current operation, sequence, declared bytes, and remaining total budget.
The runtime verifies the chunk's integrity and stages it privately before
reporting `ChunkStored`; a response-lost retry of the same chunk requires
exact readback or verified byte identity. Different bytes for the same
sequence abort. The core charges both compressed and decoded limits; the
runtime retains real permits until its buffer and stage are dropped. A
terminal transfer, timeout, or cancellation emits exact `DiscardStage` and
`ReleaseReservation` effects. Their local resource-disposal callbacks cannot
revive a cancelled session. Local buffers and permits are dropped by a worker
exit guard even if a cleanup callback is lost; remote donor reservations have
their own finite journal TTL and cannot be extended by the abandoned child.

The follower attaches to the donor's live delta stream with an exact
`AttachToken` **before** requesting barrier `B`. The donor returns its
stored `BarrierReceipt`: one atomic `B`, native covered cuts, and member set
bound to that reservation and attachment. The follower applies bounded
contiguous journal batches after `C` through this receipt. Each batch is
applied to the volatile private stage before its exact ack; no durable ack
is earned. The runtime drops the batch buffer before retiring its in-flight
memory charge. A delayed ack,
release, or barrier callback from another reservation is ignored. Later
`advance_barrier` calls are permitted only after the previous `B` was fully
acknowledged and no batch is outstanding. Each next `B` binds its own cuts;
cuts sampled after a stored `B` cannot be paired with that older image.

The follower simultaneously attaches to its ordinary native writer feeds
and buffers their effects within a finite separate budget. It first replays
the donor suffix through one exact sampled `B` and its writer cuts, then
skips buffered native effects only when their full writer/epoch/sequence is
covered by those exact cuts. It applies uncovered effects afterward under
the application's existing conflict/tombstone rule. Native events cannot
overwrite a newer private staged effect merely because they arrived later.
Two same-key effects from incomparable writers without a source-certified
ordering or authoritative origin reconciliation abort this image to origin
routing. An unchanged native effect still advances coverage through a
bounded no-op.
The donor stream stays attached until the follower proves its native feed
coverage through one exact sampled `B` receipt and independently passes its
current lease/frontier affirmation. An unknown writer, membership change,
donor lapse/gap/rebuild, source hole, journal overflow, donor death, or
uncertain overlap aborts to the existing guarded origin fallback.

The final application install is a private-stage swap under a current
recovery publication permit and lease/domain check. Every page, native
effect, and donor batch uses the same application generation fence; a stale
private stage cannot overwrite a newer index or resurrect a delete. The
core emits `InstallCandidate` only after complete image verification and
barrier replay and an exact `NativeCoverageReceipt` binding the full B
receipt, claim-selected parent, private stage position B, proven per-writer
cuts, complete membership, and bounded buffered
effects. The application executes the swap only inside the current
`PublicationPermit::publish` closure and rechecks its own lease/frontier
authority there; `Installed` is emitted only after that guarded closure
commits. This means *eligible for an application check*, not
`ReadPermitted`; origin fallback and replicated-miss policy continue until
the application's own gate opens. The donor reservation may be released
only after continuation coverage is proved or the transfer is discarded.

The runtime requires a fresh boot identity across restarts for each node
identity. The claim core now uses typed `BootId(u128)` for claim, operation,
and capture correlation, while retaining the current nonzero per-open
session and capture serial. For s3cache, the concrete default fleet-mode
binding is one 128-bit nonce from the operating system's cryptographic
random source at each process start, with a test injection hook; it writes
no S3 metadata. The probabilistic assumption is that this nonce does not
collide for one `NodeId` while old claims, transfers, or delayed callbacks
can still exist. A wall-clock sample or process-local counter is
insufficient. If the OS source fails, fleet transfer stays disabled and
the existing guarded origin scan remains available. An operator-supplied
monotonic incarnation provider may replace random boot tokens when that
authority already exists.

The shell uses one per-scope worker and bounded command queue. An opt-in
`BulkTransport`/`DataPlane` stream carries framed offer, chunk, delta, and
barrier records. Existing TCP or in-memory bulk transports are optional
bindings; an application may supply its own gRPC stream. The core depends on
none of them. Both ends bound frame length before body collection and limit
one chunk plus one delta batch in flight per follower. The shell reserves
real global encoded, decoded, stage, and batch memory permits before source
reads or private clones, and releases them only after buffers/streams are
actually dropped. The donor's live index keeps its ordinary request behavior
while transfer works. A follower that cannot complete transfer continues
origin-routed, retains its normal lease/feed ingestion, and falls back to its
existing guarded origin scan; fallback is not a second app retry FSM.

## Smallest implementation slice

Add a sans-IO `TransferSession` child under `volatile_bootstrap/transfer/`.
Its finite states are `RequestingOffer`, `Receiving`, `Attaching`,
`Replaying`, `AwaitingNativeCoverage`, `Installing`, `Completed`, and
`Aborted`. It holds only bounded metadata, counters, receipts, and current
correlation; the runtime owns image/stream/permit resources. Add deterministic
core and seeded queue tests for offer and chunk limits, missing/corrupt/
duplicate chunks, attach-before-barrier, append between `B` and metadata
readback, replay across `B2`, stale callbacks, loss and expiry, and guarded
install cancellation. The first runtime integration should use the existing
per-scope recovery worker and source-selected donor; it must replace the
follower's origin scan only when a bounded private image reaches the same
application affirmation. No separate application retry scheduler is added.
