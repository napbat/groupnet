# Peer bootstrap for a volatile application index

Status: **claim/takeover, bounded donor-journal, and transfer sans-IO cores implemented;
runtime image capture, bulk transfer, and consumer integration pending**. This extends the
[volatile recovery contract](replication-volatile-coherence.md). It is a
state-sync optimization for an application whose initial state can be built
from an origin and then updated by bounded peer feeds. It adds no durable
source, S3 control objects, or authority to a gossip head. The first consumer
is s3cache, whose default origin bucket remains untouched by coordination
metadata.

## Outcome and authority boundary

For a connected, converged cold fleet with a healthy origin and enough bounded
transfer capacity, one node performs the origin LIST build for a bucket. Other
nodes obtain an independently installable index image and catch up before
their local recovery gates open. If no donor can prove a continuous cut, a
follower stays origin-routed and may perform its own guarded scan. During a
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

Groupnet owns a sans-IO `ClaimEngine` per scope. Its implemented first slice
consumes complete bounded member/TTL-claim observations, logical ticks, and
exact builder/follower callbacks. Each claim identity binds node, fresh boot
incarnation, fresh session, and attempt. Monotone renewal high-water marks
remain boundedly retained for the episode so an expired stale claim cannot be
revived by repeated observation. `CancelWork` fences exact old work before a
new build or origin fallback. A `Ready` claim advertises only a potential
donor; it grants no serving authority. The eventual runtime must obtain a
genuinely fresh boot identity across process restarts, not infer uniqueness
from a process-local counter or wall-clock sample. Fleet-wide startup and
request-volume improvements remain unmeasured until the consumer integration.

The completed protocol will consume
bounded, versioned member/claim observations, logical ticks, capture and
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
bounded current writer cuts, and the exact member identities under that lock;
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
The capture binds the exact bounded sorted
member identities, rather than trusting a short membership hash; any changed
identity invalidates the candidate. Groupnet tracks the reservation and candidate
budgets, while the application adapter owns the concrete index bytes and
must enforce the bound during serialization. A follower's stream attachment
has a fresh correlated token and must be acknowledged before Groupnet may
issue barrier `B`. Each returned batch has separately reserved in-flight
bytes/events and one exact ack token; releasing a follower or acknowledging
one batch never truncates the candidate's shared suffix in this first slice.
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
donor's existing volatile index contract. Until runtime transfer and consumer
integration exist, every node continues its guarded origin scan.

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

## First implementation slices and verification

1. Add a sans-IO claim/takeover core and seeded partition simulations. Prove
   one origin builder in a connected converged fleet, bounded takeover, and
   safe duplicate builders under partition. Claims must not grant authority.
2. Add a donor-local, byte/event/time-bounded index delta journal sharing the
   index publication linearization. Exercise healthy feed retirement, writer
   joins, deletes, gap/lapse cancellation, journal overflow, and donor death.
   A failed reservation keeps the origin fallback.
3. Add bounded image metadata/chunks, private staging, integrity checks,
   local cut/replay barrier, and fenced atomic install. Simulate writes
   and DELETEs at every phase, cross-stream duplicate/reorder, stale pages,
   corruption and loss,
   cancellation, donor death, and late replies across takeover.
4. Replace s3cache's per-node cold scan selection with the Groupnet session;
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
