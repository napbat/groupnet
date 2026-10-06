# Optional runtime for volatile peer index bootstrap

[Documentation index](README.md) · [Bootstrap protocol](replication-volatile-bootstrap.md) · [Bulk transport](replication-volatile-bulk.md)

Status: **optional recovery-worker composition, native TTL claim/participation
source, and bulk donor adapter implemented; S3 fleet consumer integration
and its acceptance measurements remain pending**. The pure engines and
transfer contract are in [replication-volatile-bootstrap.md](replication-volatile-bootstrap.md).
This runtime remains opt-in. The default recovery handle uses its current
origin rebuild and performs no extra coordination writes to S3.

## One recovery episode and one worker

`RecoveryHandle::open_with_bootstrap` opens the same public safety gate and
cancellation semantics as `open_with_rearm`, accepting a
`Box<dyn BootstrapDriver>`. The combined constructor is
`open_with_bootstrap_and_rearm`. `BootstrapSession::new` constructs the
concrete driver from `BootstrapCapabilities<C: ClaimSource, D: DonorPort>`,
`BootstrapRuntimeConfig`, scope, node, a 128-bit `BootId`, and session, and
returns its bounded `DonorSender`. The capabilities share a `ByteAdmission`;
configuration pins finite claim/transfer, observation, and inbox limits,
including `require_participation`. Journal/capture and bulk limits are pinned
by their respective bindings. Existing `RecoveryAdapter` implementations
and callers of `open` need not implement transfer methods.
An application may inject a boot identity for tests. Production s3cache fleet
integration must obtain one token per process start from the operating system CSPRNG;
failure disables peer bootstrap and leaves guarded origin recovery available.
An operator-backed monotonic incarnation provider is also valid. A wall
clock sample or resettable process counter is not.

The existing recovery worker alone drives the selected `ClaimEngine` and
its `TransferSession` child. There is no spawned per-bucket claim loop and no
application-owned retry state machine. With bootstrap enabled the pure
recovery engine emits `AcquireBaseline { op }` after full invalidation;
the default configuration still emits `RebuildOrigin`. The shell drives the
claim/transfer effects under the current recovery generation and the
`AcquireBaseline` operation's deadline, which is the episode's `total_ms`
budget and which the child reads from its permit. The selected claim's finite
episode deadline must be no later than that recovery deadline. The worker
uses the minimum of claim renewal, exact selected-claim refresh, child work,
and recovery operation deadlines for one timer. The claim and transfer
engines separately bound each source and stage operation. Applying ordinary
`attempt_ms` to the entire parent would abort healthy multi-step transfers.

Only progress renews these bounds: build progress, and the progress of a
peer transfer. The builder's local build reports
each committed origin page through the parent `PublicationPermit::progress`,
which renews the recovery episode exactly as for `RebuildOrigin`. While the
build runs the worker keeps publishing claim and presence renewals, and on
each renewal turn converts new permit progress into `BuildProgressed`: that
restarts the build's stall bound `donor_wait_ms` and the claim episode, and a
fresh renewal advertises the claim's monotone `progress`. A follower that
observes the selected Building claim's `progress` advance renews its own claim
episode and emits `BuilderProgressed`, which the worker turns into a progress
report on its own parent permit. A follower therefore waits for a builder that
keeps advancing, however long its scan runs. It releases the builder only when
the claim disappears (the builder's own stall bound ended it and withdrew the
claim) or when no advance has been seen for `donor_wait_ms + claim_ttl_ms +
observe_ms` — the builder's own bound plus the time its last advance takes to
become visible and be sampled — and then scans the origin itself.

A peer transfer from a Ready donor is bounded the same way. It starts with
the stall bound min(`donor_wait_ms`, the claim episode's total), but every
real advance of the transfer (`Offered`, `StageReserved`, `DonorReserved`,
`ChunkStored`, `ImageVerified`, `StreamAttached`, `BatchStaged`,
`BatchAcknowledged`, `NativeCovered`) restarts that stall bound, the parent
operation deadline and the claim episode, and emits `BuilderProgressed`, which
the worker turns into `PublicationPermit::progress` on its parent permit. A
large image on a loaded node may therefore take longer than `donor_wait_ms`,
or than the recovery `total_ms`, as long as it keeps advancing. Pending native
coverage and coverage re-polls are not advances, so a stalled transfer still
aborts at its last bound (`TransferError::Expired`); a failed or timed-out
runtime operation aborts it with `TransferError::Unavailable`. An abort is
reported as `Released { reason: TransferAborted(error) }`. A failed or
timed-out refresh read of the donor's claim during the transfer
(`SelectedClaimUnobserved`) is not evidence that the donor left: the transfer
continues while the claim last observed is unexpired and samples again.

A failed or declined donor
returns a typed `BootstrapDeclined { op }` to the recovery engine, which then
emits a fresh guarded `RebuildOrigin` in the **same** original recovery
episode. It never starts an unbounded new origin attempt. The session reports
each decline to the `BootstrapObserver` as `BootstrapDecision::Declined {
reason }` (`ParentExpired`, `Unbound`, `SelectionEnded`, `Cancelled`,
`Refused`, `ClaimPublishFailed`, `PresencePublishFailed`, `NoParticipation`,
`BuildNotAccepted` or `Admission`), and every path on which the recovery
engine abandons its current stage emits `RecoveryEffect::FellBack { from,
reason }` (`EpisodeExpired`, `OperationExpired`, `OperationFailed`,
`BaselineDeclined`, `HandoffRejected`, `EvidenceRejected`,
`MembershipChanged`, `BarrierExhausted` or `Exhausted`). The shell delivers it
to the required `RecoveryAdapter::fell_back(from, reason)`, also when a signal
supersedes the queued fallback work, so no origin fallback is silent. A local provisional
builder also uses the existing `rebuild_origin` callback and publication
permit; its correlated success supplies the local baseline and may publish
`Ready` as donor availability, never as read authority. It reports
`LocalBaselineBuilt { op }` to recovery, which proceeds directly to its
ordinary `Affirm` without a second origin scan. The one worker continues
driving bounded `ClaimEngine` TTL renewals and donor journal expiry after
local recovery reaches `Ready`; a source gap, restart, or final handle drop
withdraws that exact claim. An invalidated capture withdraws it too, unless
participation is required: then a renewed Building claim supersedes it until
the recapture (see `replication-volatile-membership.md`). Claim renewal after a
completed local build advertises an available image and does not extend the
already finished recovery episode or license local reads. A peer install emits
`PeerBaselineInstalled { op, handoff }` only after the atomic native-delivery
handoff below. It enters a **new peer-specific** recovery check: sample a
complete bounded peer/head roster, wait the sampled native heads through the
normal feed path, recheck the roster and heads, then request independent
lease/domain affirmation. The worker composes each roster sample itself:
member identities come from the bootstrap child's fresh native
participation cut, and feed heads from the adapter's ordinary
`observe_peers`. Without required participation there is no roster to
compare with the handoff's covered members, so the check fails closed to
guarded origin recovery. The current full-origin `Materialized` path goes
straight to `Affirm`; it does not already contain this peer barrier. A
changed roster, missing head that was previously observed, feed gap, lapse,
or handoff loss closes the candidate and uses guarded origin recovery within
the original deadline. Neither branch skips final application read-policy
checks. This is volatile coherence for participating writers, not proof of
external origin completeness or authoritative absence.

`Start`, gap, lapse, rearm, cancellation, and last-handle drop synchronously
close the shared serving/publication fence before the worker can touch an
old page. The recovery engine emits `CancelBaseline` for the original
baseline operation before replacement origin work; the worker translates
it to exact `CancelWork` before discarding its private
stage or source reservation. Late source responses, queued chunks, image
verification, and `Installed` replies carry both the recovery operation and
the claim/transfer operation. A newer generation rejects them. The worker
must not drain a public signal and then accidentally adopt a later fence
version. It must not extend the total deadline when a donor changes, a
claim renewal without new progress arrives, or the worker waits for cleanup.

## Bounded capability surface

`BootstrapCapabilities` shares the claim source, donor port and byte admission
with the session. `ClaimSource` publishes and withdraws native TTL claims,
returns a **complete** member/claim snapshot with count and identity-byte
limits enforced before allocation, and refreshes one exact selected claim
from native TTL storage. It distinguishes absent from unreadable or malformed
observations. Claims and gossip are liveness hints;
they cannot certify origin freshness or absence. The current live lease and
write-feed implementation continues running while a follower is origin
routed or waiting for a donor.

`DonorPort` supplies typed offer, reservation, sequential chunk,
attachment, atomic barrier, bounded batch, exact ack/readback, and native
coverage operations. The donor journal is captured under the application's
index publication lock: first reserve real encoded, decoded, suffix, and
in-flight memory, then start a `Capturing` journal at C and clone a bounded
private state under that lock. The pre-reserved journal accepts later index
effects during off-lock blocking encoding, but serves no follower RPC or
`Ready` claim before `finish_capture` revalidates the exact ingress and
original deadline. Cancelled blocking work retains its real buffer permits
through completion. The journal refuses time that runs backwards, and both
the worker and the application's publication path drive it, so every journal
time — at C, on each journaled effect, and at `finish_capture` — comes from
the session's one `LogicalClock`, handed to the application in its capture
request. A second clock, even one sampling the same instant, can round a step
ahead and fail the capture. Every
subsequent actual index effect, including origin-validated repair, native
no-op, and tombstone, enters the same guarded journal. A source gap, lapse,
rebuild, changed complete membership identity, or budget overflow invalidates
the candidate. The runtime stores one owned capture and at most the
configured follower reservations per scope; the adapter cannot hide an
unbounded transfer map.

A worker-owned `PrivateStage` receives one verified chunk or one bounded
`JournalBatch` at a time. It charges encoded input, decoded index state,
native overlap buffer, and returned batch clones against a shared global
byte admission object before allocation. Dropped or expired work releases
actual memory permits only after buffers and stage handles are dropped.
The runtime retains the exact donor reservation and attached stream until
the follower has applied through B, independently verified native writer
cuts and current complete membership, and attached ongoing native delivery.
The normal feed ingress must register each decoded event in a bounded
overlap buffer before any asynchronous body invalidation or per-key wait.
Origin-validated local repairs and other index effects made while staging
also enter that coordinator-ordered buffer; otherwise a candidate swap could
erase them. Overflow, a missing event, or a feed gap aborts the candidate.
It skips only native effects covered by the sampled B cuts and applies
uncovered native and local effects after the donor suffix.
For a same-key donor/native effect and follower-local origin repair, the
follower's coordinator order is not a remote commit order. Unless source
identities prove their order, the adapter must reconcile that key freshly
against the origin under the current generation or abort the candidate to
origin recovery. The same rule applies to incomparable native writers; a
newer event cannot be overwritten by a delayed donor delta. A sampled
gossip head alone cannot provide this coverage receipt.

The stage's final `install` runs only through the current
`PublicationPermit::publish` closure. Under one short publication critical
section, the consumer validates the recovery generation, index/body trust
generation, schema, exact B/cuts, privately staged-through-B position, and
attached native stream; applies the bounded buffered effects not covered by
B; swaps the private index; and switches the normal feed applier
to that installed index. There is **no interval** between candidate swap and
normal-feed ownership in which an arriving native event can be discarded or
applied only to the old index. `NativeHandoffReceipt` binds the
transfer parent, exact B/cuts, native attachment incarnation and contiguous
position (including valid position zero for a quiet feed), fresh
normal-applier generation, exact accepted schema, and bounded buffer charge.
It also binds the exact outer `RecoveryOperation` that admitted the baseline.
Recovery and claim-child sessions remain independent; the worker records
their active mapping, and the recovery core rejects a receipt for any other
outer operation even if its child transfer is internally valid.
`Installed` must present that exact receipt; a caller boolean or sampled
head does not qualify. The transfer core may release the donor reservation
only after accepting it. A delayed old-generation receipt cannot release a
new reservation or install over a newer index. If atomic handoff cannot be
established, the private image is discarded and reads stay origin-routed.
`InstallCandidate` carries the exact accepted `NativeCoverageReceipt` to the
worker, so the guarded callback does not reconstruct B from a mutable donor
head or an unbounded adapter-owned map.

Coverage compares the follower's live native writer positions with B through
the core's `align_cuts`. An exact match (a writer known to one side only
aligns as a quiet zero-position feed) may install. Any other pair of
positions is `Pending`: a position behind or ahead of B in the same writer
life, and a position in a different life. Positions are `(epoch, sequence)`,
ordered epoch-major, and a restart opens a new epoch. A B cut in an older epoch
than the live one covers the writer's old life only up to that cut and none
of the new life, whose writes are native effects B must cover; a live cut in
the older epoch has not applied the new life B covers. Neither side may
install until both stand at one position of one life, and no pair of
positions is itself a refusal: the side behind crosses into the newer life
either by a sealed renewal (`WriteFeed::seal`, `PeerWrite::Renewed`), after
which the positions compare within one life, or by a gap, which withdraws the
donor's capture or restarts the follower's recovery and discards its stage.
A donor journal records a renewal with `DonorJournal::renew` only from the
exact sealed cut, as a native delta at sequence zero of the new epoch, and a
follower replaying the suffix crosses with it; any other move into a new
epoch withdraws the candidate. The rejoin this matters for: a restarted node
joining a Ready peer holds its own writer at `(new epoch, 0)` while the peer's
image still covers the old life through its last write. That pair is
`Pending` until the peer applies the restarted writer's seal and renewal, and
if the old life ended without a seal the peer takes the restart gap instead,
whose unknown tail only an origin scan can remediate. The adapter answers
`NativePending`, keeps its stage, and the transfer samples a later barrier with
`AdvanceBarrier` after `coverage_poll_ms`, bounded by the transfer deadline. An
unsorted cut list or an incomparable local repair is a conflict and aborts the
candidate. The install repeats the same check under the index write lock
and swaps only if it still holds; if a live effect landed after the coverage
check it answers `NativePending` too, again keeping the stage.

A donor's own writes are native effects of its own feed writer. The
consumer declares that writer at its current position before any capture
starts, and assigns each own write's feed position under the same index
publication lock that applies the write. The index, the donor journal, and
the feed therefore see own writes in one contiguous order, so B covers them
exactly and a follower that applied the same feed events aligns without a
separate donor-local writer rule.

Candidate publication and serving permission are distinct. The follower
need not hold a serving lease before this candidate swap, which permits a
cold follower to keep ingesting feeds and seeking grants while origin-routed.
The gate remains closed through the new peer head/frontier check and current
lease/domain affirmation. Passing affirmation cannot repair a dropped feed
event; it may open reads only after the exact continuous handoff and sampled
barrier have succeeded. The donor attachment can be released after that
handoff even if the independent lease check has not yet passed. The native
attachment and normal applier continue while the donor
reservation is released.

Lock order is `RecoveryHandle` control/publication permit, then the short
index publication coordinator, then `KeyIndex` write lock. No one awaits
under these locks. Every normal feed effect, origin page/repair, and private
swap takes the publication coordinator before mutating `KeyIndex`; the donor
journal records the resulting effect in that same critical section. Feed
ingress does not call back into recovery control while holding the
coordinator or `KeyIndex`. The existing per-key body `FillFence` may already
be held when a feed effect reaches the coordinator, but the handoff never
acquires a `FillFence` while holding recovery control or the coordinator.
`LocalCache::distrust_all` is an atomic generation change; hot-body eviction
and other asynchronous work happen after releasing the publication locks.
Read decisions capture the recovery generation before index/tier work and
recheck it outside the index lock before returning a local answer. This
preserves the existing body-fill path, whose `can_commit` callback can consult
recovery control while holding a per-key fill fence, without a reverse lock
edge.
Every callback that could mutate a page or discard replacement state uses
the same generation fence. The shell checks the absolute deadline before
and after any synchronous affirmation. A delayed successful callback after
the deadline or a gap is revoked and cannot open the read gate. Existing
origin fallback for indexed misses, LIST uncertainty, and origin-validated
positive bodies remains in force unless a separate absence proof is built.

Bulk transfer is a separate opt-in transport feature. It uses bounded frames
for offer, chunk, delta, barrier, ack, and cancellation; peers upgrade
together, so frame kinds and bodies change with a bulk codec version bump
rather than through compatibility paths. The core has no Tokio,
network, clock, hash, or S3 dependency. The adapter verifies chunk length
and commitment before private staging, and corrupted or interrupted streams
discard the stage. The origin bucket receives no control object in either
default or optional peer-bootstrap mode.

## S3 scope and bounded donor lifetime

The current S3 `WriteSync` owns one recovery handle and one `KeyIndex`; its
`rebuild_origin` walks the bounded union of configured buckets and current
`KeyIndex` bucket names. The S3 integration contract therefore claims **one
whole-index scope per `WriteSync`**, not a fictitious per-bucket recovery
handle. Its partition identity is a canonical, length-prefixed encoding of
that exact sorted union, index schema, and the configured origin
endpoint/account namespace. The adapter must have a stable, explicit origin
namespace identity; it cannot infer equivalence merely from equal bucket
names. A new bucket discovered or created during capture changes that union
and invalidates/rekeys the candidate before any offer or install. It must fit
the declared scope-byte limit exactly; it is never truncated or replaced by
an unchecked short hash. A changed bucket universe cannot adopt an old donor
image. A deployment whose complete union exceeds the scope, image, capture,
or global memory caps uses its existing guarded origin path. This makes the
consumer integration finite as bucket count grows; splitting into independent
bucket scopes requires an explicit recovery-handle and publication-gate design.

The local provisional builder may publish a `Ready` claim only after the
guarded origin scan finished every bucket, the bounded private capture is
complete at C, and the donor journal is attached to **every** subsequent
index publication. A completed recovery lease alone is insufficient. The
same worker services its TTL renewal and finite journal lifetime even after
the local read gate opens; the donor image remains an availability hint, not
authority. When the capture or global admission expires, withdraw or let the
claim expire and reject new reservations. Continued local serving still
depends on the ordinary recovery, lease, and application gates.

## Implementation status and consumer acceptance

The optional recovery transitions, `BootstrapSession` worker composition,
shared byte admission, `NativeClaimSource`, and `BulkDonorPort` exist.
`volatile_bootstrap_runtime.rs` and its scenario children exercise baseline
acquisition, expiry, stale install, cancellation, donor withdrawal, slow build
progress, presence maintenance, and roster recapture. Core receipt types bind
the exact outer recovery operation and child handoff. Bulk adapter tests
exercise all generic journal request branches over `MemBulkNet`, including
nonempty suffix and B2/ack readback. These are reusable protocol implementation
evidence, not a claim of completed S3 consumer integration.

The S3 acceptance boundary remains:

1. Wire the index adapter under an explicit fleet opt-in. The normal
   connected cold fleet elects one provisional origin builder after claim
   convergence; followers transfer and then independently affirm. A failed
   builder permits takeover, while partitioned duplicates remain safe. A
   donor without complete coverage, a failed bulk stream, or unavailable
   source falls back to each follower's guarded origin scan. MinIO tests
   assert exact GET/LIST behavior, no control writes to the origin bucket,
   private-image deletion order, and local-hit availability after recovery.

Consumer acceptance requires the repository's feature-specific checks,
deterministic fault schedules, and measurements before claiming fleet
bootstrap complete. It must exercise a late native DELETE and local
origin-validated repair between B and handoff, incomparable equal-time
cross-writer overlap, cancellation racing publication, concurrent body fill,
and multiworker faults. Every source effect must appear exactly once or the
candidate remain unservable. Follower lease/feed ingestion continues while
reads route to origin; concurrent-scope memory and source-call counts must fit
configured bounds. This design supplies neither event-complete subscriptions
nor a durable source cursor for S3's ordinary object writes.

## Native TTL source for volatile bootstrap

`NativeClaimSource` is implemented as an optional Groupnet-backed claim and
participation adapter. It does not make claims, membership, or a donor image
authoritative for S3 object state. S3 fleet consumer adoption remains pending;
default s3cache operation still writes no coordination metadata to S3 or to a
separate control store.

### One coherent, bounded observation

`Group::set_entry` and `delete_entry` enqueue app-state commands and
`Group::node_entry` reads a byte-only watch snapshot. That watch does not
contain the entry's observer-local expiry, and `node_entries` clones values
before a caller can cap them. `statuses_held_bounded` bounds a roster but is a
separate watch, so pairing it with entries is not one actor-state cut. The
native adapter therefore uses generic async actor-side inspection in
`groupnet-runtime`, with no dependency on `groupnet-consistency`:
`Group::inspect_scoped_entry<B: EntryBudget>` returns
`Result<(InspectedEntries, B), EntryInspectionError>`. Its participation-aware
counterpart, `inspect_scoped_pair`, returns `InspectedPair` from a fixed
two-key cut; the complete roster contract is in
[membership binding](replication-volatile-membership.md).
`EntryBudget: Any + Send` exposes the owned reservation's `bytes()`.

The caller validates the scoped key length and reserves the complete maximum
response budget **before enqueueing** inspection. The generic runtime query
carries that owned, type-erased reservation through its bounded actor inbox,
the actor's response allocation, and the oneshot reply. The adapter recovers
the reservation and turns the response into its admitted `ClaimSnapshot`;
if its awaiting future is cancelled, the queued command or reply still owns
the reservation until its cloned bytes are dropped. The actor validates the
requested cap against the supplied budget without depending on
`groupnet-consistency` for accounting. A response with no surviving receiver
is dropped with its charge.

This pool bounds source-owned commands, response bytes, decoded observations
and retained published values, not the Group engine's adopted entry storage
or watch snapshots. Deployments must separately bound configured scope/claim
count.

The actor samples its current `GroupEngine` membership and exactly that key
for every retained member at one logical time. It checks member count, each
`NodeId` byte length, each value byte length, and the checked sum of value,
identity, and fixed record charges **before cloning** any member or value. It
then returns bounded status, optional value, the entry's remaining native
TTL, and its local monotonic sample instant. Overflow, unavailable actor,
unreadable state, and an unrepresentable clock result are typed failures;
none means an empty claim. An expired entry is absent even if the next engine
reap has not run. A present entry without finite TTL is not a valid claim.
The same bounded actor query handles exact selected-claim refresh; no
adapter-owned polling roster or unbounded `LIST` exists. An actor inspection
is a local snapshot, not a fleet barrier.

`ClaimSource::observe_claims` maps the complete inspected roster and valid
claims into the existing admitted result. Alive status supplies provisional
eligibility; Suspect/Dead remain visible but ineligible. A malformed present
claim, mismatched scope/policy, duplicate identity, or omitted member is an
error rather than silence. The `ClaimSource` returns the sample instant with
both full-roster and selected-claim observations. After the await,
`BootstrapSession` first ticks its own logical clock, then subtracts elapsed
actor-to-core transit from each sampled remaining TTL, rounding elapsed
milliseconds **up** and allowing one extra millisecond for the actor's
logical-time quantization. Zero remaining time is absence/expiry. The worker
passes only this aged duration into `ClaimEngine`, which adds it to its own
logical `Time`; it never compares the Group actor's time origin with its own,
or one node's wall clock with another's. The core's high-water renewal rule
remains decisive: repeated observation of the same claim revision cannot
restart its lifetime merely because gossip arrived again.

### Entry identity and exact withdrawal

One node-owned entry key is derived from a length-delimited encoding of
`BootstrapScope`, under a reserved bootstrap namespace. The bounded value
contains a codec version, exact scope and policy fingerprint, `NodeId`,
128-bit boot nonce, session, attempt, renewal, phase, build progress, and no wall-clock
deadline. `Group::set_entry` supplies a finite TTL duration; each observer
measures remaining life on its own monotonic clock. The codec rejects trailing
bytes, oversized fields, unknown versions, zero identity components, and a
value whose embedded scope disagrees with the requested key. It adds no new
frame kind and changes no existing `EntryDelta` body.

Local enqueue success is not proof that a claim was adopted.
`Group::set_entry_confirmed` acknowledges actor adoption; an unknown outcome
is read back through the bounded actor query before reporting publish
success. The actor samples current monotonic time before arming a confirmed
claim TTL, publishes the resulting view, then acknowledges; it must not use a
stale prior periodic-tick stamp for a newly written TTL. An ambiguous
acknowledgment may be read back by exact value and
identity. It cannot be reinterpreted as network-wide agreement. Publication
failure stops this provisional selection episode or causes bounded core
fallback to origin, never an unbounded source retry loop.

`WithdrawClaim` must not delete a newer local claim on the same key. The
generic actor command conditionally deletes only when its current value
matches the caller's last exact published value. The source retains that
bounded value and serializes local publish/withdraw enqueueing. An old
incarnation's delayed withdrawal is harmless after a new value replaces it.
Remote duplicate nodes or partitions can still create competing provisional
builders; their images remain private until the independent recovery and
serving gates affirm. Claim expiry/withdrawal does not revoke healthy local
read permission by itself.

### Wakeup, transport, and tests

The existing `NodeStateChanged`/`MembershipChanged` stream is only a coalesced
wakeup. On lag, the worker repeats the complete bounded actor inspection;
core `Tick` keeps finite observation and claim-expiry deadlines even when
every wakeup is lost. Source reads/publishes are charged to exact child
operations, never to a fresh outer deadline. `ClaimSource` remains a reusable
Groupnet capability. The existing recovery worker drives the paired bulk
protocol as well; no application claim loop or mandatory S3 control object is
introduced.

### Bounded bulk request/reply

The [bulk data-plane contract](replication-volatile-bulk.md) owns the framing,
codec and donor adapter details. Both receive and send are bounded to the
operation's caps; the codec does not alter control-plane `FRAME_VERSION` or
Hosted handoff bodies. Requests bind complete scope/identities and parent/
child operations; typed replies, exact capture/reservation/barrier correlation,
in-band termination and an end-of-stream check are mandatory. Admission
precedes every frame read and decoded clone.

The donor worker samples its journal under the short ingress lock and sends
owned admitted buffers off-lock. Full inbox capacity is a typed refusal.
Offer/barrier metadata and chunk/batch copies retain charges through send or
cancellation. Connect, request, reply and send use the original correlated
deadline; only the core decides retry/readback. No claim, response, EOF, or
matching head grants reads.

`volatile_native_claims.rs` exercises actor-backed native publication,
participation, expiry, malformed-present versus absent, exact withdrawal,
and restarted identities. Runtime, bulk codec/fault, and adapter suites
exercise correlation, cancellation and admission ownership. Those suites
establish reusable protocol behavior, not S3 fleet acceptance.

Consumer/multiworker acceptance must still partition/heal claim gossip,
drop/duplicate wakeups, expire a builder, and show one connected builder
under timely convergence with finite takeover otherwise. No claim or donor
Ready event may open serving without separate recovery and native-feed
handoff proof. Fault schedules must inject truncated, duplicated and
wrong-operation frames, stalled sends, a full donor inbox, lost reservation
response, and cancellation during install; charges retire and neither EOF
nor matching peer heads can install or serve an incomplete image.
