# Peer bootstrap for a volatile application index

[Documentation index](README.md) · [Recovery](replication-volatile-coherence.md) · [Runtime](replication-volatile-transfer-runtime.md) · [Membership](replication-volatile-membership.md)

Status: **claim/takeover, donor-journal and transfer cores, optional recovery-worker
runtime, native TTL source, and bulk adapter implemented; S3 fleet consumer
integration and performance measurements remain pending**. This extends the
[volatile recovery contract](replication-volatile-coherence.md). It is a
state-sync optimization for an application whose initial state can be built
from an origin and then updated by bounded peer feeds. It adds no durable
source, S3 control objects, or authority to a gossip head. The first consumer
is s3cache, whose default origin bucket remains untouched by coordination
metadata.

## Outcome and authority boundary

For a connected, converged cold fleet with a healthy origin and enough bounded
transfer capacity, one node performs the origin LIST build for the configured
bootstrap scope. Other nodes obtain an independently installable index image
and catch up before their local recovery gates open. If no donor can prove a
continuous cut, a follower stays origin-routed and may perform its own guarded
scan. During a
partition, separate components may each select a builder; duplicate scans
are safe, and reconnection cannot install an older image over a newer index.
An expired builder claim only schedules work. It cannot grant reads.

The donor must provide the actual provenance of a complete, current guarded
origin scan: bucket/scope, schema, donor identity and recovery generation,
index coverage, a donor-local index mutation position `C`, observed native
per-writer frontiers, and an integrity commitment.
A derived coordinator or two matching gossip heads do not certify an index.
The follower also needs its own lease/domain affirmation after installation.
Replicated index misses continue to reach the origin under s3cache's current
policy. This mechanism does not fix an origin commit whose proxy dies before
publishing its feed event. A direct external origin mutation never enters a
Groupnet feed at all; peer bootstrap cannot detect or certify its absence.
Deployments with such mutations retain origin validation/fallback and cannot
claim local absence/LIST freshness from this transfer. Any unknown mutation
or broken continuity keeps affected keys and absence/LIST authority closed
until authoritative origin reconciliation.

## Builder selection and transfer

Groupnet owns a sans-IO `ClaimEngine` per scope. It
consumes complete bounded member/TTL-claim observations, logical ticks, and
exact builder/follower callbacks. Each claim identity binds node, fresh boot
incarnation, fresh session, and attempt. Monotone renewal high-water marks
remain boundedly retained for the episode so an expired stale claim cannot be
revived by repeated observation. `CancelWork` fences exact old work before a
new build or origin fallback. A `Ready` claim advertises only a potential
donor; it grants no serving authority. The caller supplies a genuinely fresh
boot identity across process restarts, not uniqueness inferred from a
process-local counter or wall-clock sample. Fleet-wide startup and
request-volume improvements remain unmeasured until consumer integration.

The protocol consumes bounded, versioned member/claim observations, logical
ticks, capture and
transfer receipts, and loss signals. It emits claim, hold, request, transfer,
install, release, and fallback effects. A deterministic rank among converged
healthy members selects one provisional builder after a bounded claim-settle
window. Followers wait for that builder's certified image for as long as its
Building claim keeps advertising build progress, however long the origin scan
takes; a disappeared, failed, or stalled builder (no advance for its stall
bound) triggers a bounded takeover round with a fresh generation.
Claims and liveness hints never open the serving gate. The runtime owns one
worker/session allocator and bounded fair per-scope queues, not a second
application retry state machine. Builder identity and attempt incarnation bind
every effect, callback, and image; late responses are ignored.

The donor first reaches its own guarded `Ready` state from a complete origin
scan. It then enables a **donor-local** bounded delta journal under the same
index transaction/lock that publishes every later key mutation and tombstone.
The donor takes an immutable index image at local position `C`; there is no
capture-to-journal race. Each delta is generation-bound and includes a stable
sequence, key, operation and final index effect. This local sequence is only
an index-transfer cursor, not a source-committed position or durable cursor.
The journal has explicit event, byte, time, and per-follower reservation caps.
It never suppresses normal feed retirement. If it fills, expires, or loses a
contiguous position, the candidate fails closed and the follower uses origin
fallback. No all-publisher retention hold or new writer-admission protocol is
required solely for this cache bootstrap optimization.

The donor-local journal uses a fresh capture identity containing the donor's
boot/session/attempt, recovery generation, and a capture serial that cannot
repeat within that session. A follower reservation has its own increasing
serial, so an ack, read, release, or attachment callback from an earlier
reservation cannot advance a reopened follower. The source-native writer
identity is optional on a journal entry: origin-validated repairs and other
index changes still receive distinct stable local mutation identities and
must be recorded. Bounded native per-writer covered cuts are carried as
separate metadata; they are not inferred from the local journal sequence.

At image capture, the adapter holds the same index publication lock used by
every page and feed mutation. It reserves the encoded image, decoded image,
and live suffix budgets **before** cloning; captures state, cursor `C`, the
bounded current writer cuts, and the complete membership roster under that lock;
and attaches a pre-reserved journal in `Capturing` state before releasing the
lock. The bounded immutable state clone is charged and measured while the
publication lock is held; large encoding runs on a blocking worker with the
image and its permits owned by that job. Feed publications after `C` append
to the same suffix during encoding, including native no-ops. The image's
immutable native cuts at C remain separate from the journal's advancing
barrier cuts; an offer must never label the C image with later B coverage.
This cannot make
the initial clone itself nonblocking, so the configured clone cap and measured
lock latency must keep that critical section finite. No follower may reserve,
attach, read a batch, or treat a claim as `Ready` while the journal is
`Capturing`. The encoding job's cancellation does not release its permits
until its actual buffers are dropped. On completion the application rechecks
the exact ingress identity, recovery generation, original episode deadline,
and journal validity under the publication lock, then calls `finish_capture`
with actual encoded/decoded charges; the image's cut remains `C = 0` even if
later suffix positions were already appended. Failure, cancellation, expiry,
or overflow invalidates the candidate and cannot publish a partial image.
The capture binds a complete bounded sorted native membership roster, not an
unchecked short hash. Across cuts, node and presence boot/session bind
membership; SWIM status/incarnation changes alone do not invalidate it (see
[membership binding](replication-volatile-membership.md)). Groupnet tracks the
reservation and candidate
budgets, while the application adapter owns the concrete index bytes and
must enforce the bound during serialization. A follower's stream attachment
has a fresh correlated token and must be acknowledged before Groupnet may
issue barrier `B`. Each returned batch has separately reserved in-flight
bytes/events and one exact ack token; releasing a follower or acknowledging
one batch never truncates the candidate's shared suffix.
The pure core's copyable charge is only logical accounting; the runtime must
also hold and retire a real global memory permit until old image/batch buffers
are dropped. Expiry alone cannot reclaim bytes still held by an asynchronous
transfer. Replaying an unchanged native event records an explicit bounded
no-op so the covered writer sequence still advances. The adapter decides
same-ID idempotence under the index lock before changing live state; a second
effect with the same ID and different bytes invalidates the candidate. An
`append` invocation means an actual index publication occurred: a mismatched
recovery generation invalidates that capture, while a stale callback blocked
before publication never enters the journal.

The donor must abort a candidate when its own feed gaps, lease lapses, index
rebuilds, or observed membership continuity changes invalidate its existing
volatile index permission. A newly observed writer cannot be silently erased
from the candidate. Gossip cannot prove that no writer joined unseen; the
follower's independent current roster/lease and frontier affirmation remains
mandatory, and this transfer makes no stronger freshness claim than the
donor's existing volatile index contract. Reusable runtime transfer is
implemented; S3 fleet adoption still requires the consumer's guarded index
capture and atomic native-feed handoff.

The donor sends bounded metadata and sequential chunks over a peer data
plane. Chunks carry sequence, length, an adapter-verified integrity check,
and a final image commitment; no crypto or runtime dependency enters the
sans-IO core. The follower stages privately under separate reserved encoded,
decoded, and live-delta byte budgets. It rejects missing,
duplicate-with-different-bytes, corrupt, excessive, or out-of-order chunks;
cancellation discards the stage.
The donor journal retains the suffix throughout transfer and catch-up. The
follower attaches to its live delta stream before requesting a bounded donor
barrier `B`, then applies every local index delta after `C` through `B` and
continues receiving later deltas. It also attaches to its ordinary Groupnet
peer feeds. The donor bridge remains active until the follower proves its
native feeds cover the sampled per-writer cuts and completes its own current
lease/frontier affirmation; releasing it at `B` alone would create an attach
gap. If the donor fails during that bridge, the candidate aborts to origin
fallback. The follower rechecks donor incarnation/generation and membership
continuity at cutover.
Donor deltas and native feed events may describe the same operation and may
arrive in either order. The follower buffers native events privately and uses
their writer/epoch/sequence identities and the donor's exact covered
per-writer cuts to skip effects already represented in donor state. It applies
uncovered native events only after the covered donor suffix. The donor-local
sequence orders the bridge only and is never treated as a global write order.
s3cache's present `KeyIndex` resolves ordinary put/delete conflicts with
timestamps and delete-tie priority; equal-time puts from different writers
are not globally comparable. An incomparable same-key overlap must remain
origin-routed or abort the candidate for authoritative reconciliation before
complete-index admission. A stale donor delta cannot overwrite a newer
native update or resurrect a DELETE. The cutover checks that the exact
merged state and covered frontiers were atomically installed under the
recovery generation.
Every mutation/page/install is fenced by one recovery generation and
publication permit. Only an atomic, generation-checked index swap can mark
the transferred image complete. An interrupted transfer cannot mark ready.

If a cut cannot be made or the local suffix cannot be retained for the
bounded total attempt, Groupnet starts the ordinary full origin recovery path. Requests
remain able to use the origin during all phases. No whole-fleet read barrier
is introduced. Transfer backpressure and one in-flight image per scope bound
memory and network use; a donor may reuse an immutable captured image across
followers only while its local journal reservation and candidate budgets
remain valid.

The cold follower may grant peer leases and ingest feeds while its own local
serving gate is `OriginOnly`: granting another node's lease is independent of
licensing this node's index reads. s3cache already starts the lease and feed
tasks at gossip attach, before recovery readiness. The first integration test
must prove donor recovery and follower grant progress without waiting for the
follower's image, otherwise the one-builder schedule would deadlock.

## Implementation status and evidence

The claim/takeover core, bounded donor journal, transfer child, and optional
runtime composition exist in `groupnet-core/src/volatile_bootstrap/` and
`groupnet-consistency/src/volatile_recovery/bootstrap/`. The runtime's
`BootstrapSession` owns the child and resources inside the existing recovery
worker; `NativeClaimSource` and `BulkDonorPort` provide reusable bindings.
Deterministic suites cover claim selection, partitions/takeover, journal
continuity and bounds, transfer ordering, cancellation and stale callbacks.
Runtime and bulk adapter suites exercise private handoff correlation,
resource ownership, and real in-memory network request mapping. These are
protocol implementation evidence, not evidence of S3 fleet deployment.

The remaining consumer acceptance is to:

1. Replace s3cache's per-node cold scan selection with the Groupnet session;
   keep the existing guarded scan as fallback and preserve positive GET,
   LIST/absence, body-validation, and lease gates. MinIO tests must show one
   connected builder, follower local readiness after validated transfer,
   takeover, partition duplicates, no healthy donor fallback, and zero
   coordination writes to S3. Measure startup local-hit latency, origin
   GET/LIST counts, transfer bytes, retained delta bytes, and behavior as
   followers and buckets increase. No performance claim is complete without
   those measurements.

The API remains source-capability based: a consumer without a transactionally
captured local image and mutation stream can still use the existing origin
recovery engine. A future
optional durable coordination store may supply stronger replay contracts,
but is not required for s3cache's default mode or docres/shardstore's native
CAS source.

## Bounded transfer of a volatile index image

The sans-IO transfer child and reusable runtime/bulk bindings are implemented.
This transfers an already guarded donor index; S3 consumer capture and atomic
feed handoff remain integration obligations. Transfer does not certify origin
freshness, durable writer history, or local read authority.

### One session and exact identity

The claim session selects a donor and emits `DonorAvailable { op, selected }`.
An opt-in transfer child is constructed only for that exact operation and
selected `ClaimIdentity`. The composite claim/transfer engine owns one
`next_token` counter. Its private `allocate_token` operation
increments this counter **without** replacing the parent's selected
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
original follower episode's `total_due` bound the transfer when it starts:
its initial stall deadline is their minimum. Each real advance of the
transfer (an offer, a reservation, a stored chunk, a verified image, an
attached stream, a staged or acknowledged batch, native coverage) restarts
that stall bound as `donor_wait_ms` from the advance, renews the parent
operation and claim episode, and reports progress to the parent recovery, so
only a transfer that stops advancing expires. The ordinary claim poll
timer pauses while transfer is active; `ClaimEngine::Tick` drives the child
deadline, local claim renewals, and a separately correlated native TTL
refresh for the exact selected donor. An absent, expired, or contradictory
claim aborts transfer without replacing the parent operation with
`ObserveClaims`; a refresh read that fails or times out is sampled again, and
the donor's last observed claim expiry still bounds the transfer. Transfer
success completes the candidate handoff. A failure reports
`Released { reason: TransferAborted(error) }` and resumes bounded
observation/takeover; it excludes that exact donor attempt unless the error
is `Unavailable`, an operation that failed or timed out without a verdict on
the image, and the first Ready selection's donor-wait deadline still holds.
Then the follower samples a fresh cut one observation interval later and may
transfer from the same live attempt again
(`replication-volatile-membership.md`). A later ready advertisement alone
extends no deadline.
A self-built `Ready` donor may remain
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

### Ordered effects and callbacks

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
routing. The same rule applies when a donor/native effect overlaps a
follower's origin-validated local repair: local publication order alone
cannot order effects observed on different nodes. An unchanged native
effect still advances coverage through a bounded no-op.
The donor stream stays attached until the follower proves its native feed
coverage through one exact sampled `B` receipt and atomically hands off to
normal native delivery. The follower then independently passes its current
head/frontier and lease/domain affirmation before local serving opens; a
private candidate may be installed while that serving gate remains closed.
An unknown writer, membership change,
donor lapse/gap/rebuild, source hole, journal overflow, donor death, or
uncertain overlap aborts to the existing guarded origin fallback.

The final application install is a private-stage swap under a current
recovery publication permit and exact continuous native handoff. Every page, native
effect, and donor batch uses the same application generation fence; a stale
private stage cannot overwrite a newer index or resurrect a delete. The
core emits `InstallCandidate` only after complete image verification and
barrier replay and an exact `NativeCoverageReceipt` binding the full B
receipt, claim-selected parent, private stage position B, proven per-writer
cuts, complete membership, and bounded buffered
effects. `Installed` binds to a typed `NativeHandoffReceipt`
certifying that under one guarded publication section, the private
image through B received every bounded uncovered native/local effect and
the normal feed applier took ownership without a gap. The application
executes the swap only inside the current `PublicationPermit::publish`
closure; the serving lease/domain check follows as a separate step.
`Installed` is emitted only after that guarded closure commits. This means
*eligible for an application check*, not
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

### Transfer states and evidence

The sans-IO `TransferSession` child lives under `volatile_bootstrap/transfer/`.
It holds only bounded metadata, counters, receipts, and current correlation;
the runtime owns image, stream, and permit resources. Its finite states cover
offer, stage/donor reservation, receiving and verification, attachment,
barrier request and replay/acknowledgement, native coverage and barrier
advance, install, completion, and abort.

Core tests and seeded schedules cover offer/chunk limits, missing/corrupt/
duplicate chunks, attach-before-barrier, exact B readback, replay across B2,
stale callbacks, loss/expiry, and guarded install cancellation. Runtime
composition uses the existing per-scope worker and selected donor; the
follower's origin scan is replaced only when the bounded private candidate
reaches the independent application affirmation. No application retry
scheduler is added, and these tests do not establish S3 fleet readiness.
