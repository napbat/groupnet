# Optional runtime for volatile peer index bootstrap

Status: **implementation contract for the next slice; runtime and consumer
integration pending**. The pure claim, donor journal, and transfer engines are
specified in [replication-volatile-bootstrap.md](replication-volatile-bootstrap.md)
and [replication-volatile-transfer.md](replication-volatile-transfer.md).
This runtime remains opt-in. The default recovery handle uses its current
origin rebuild and performs no extra coordination writes to S3.

## One recovery episode and one worker

Add an opt-in `RecoveryHandle::open_with_bootstrap` constructor with the same
public safety gate and cancellation semantics as `open_with_rearm`. It accepts
an owned `BootstrapSetup` containing finite claim, journal, transfer, bulk,
and global-memory limits, a 128-bit `BootId`, and source, donor, and private
stage capabilities. Existing `RecoveryAdapter` implementations and callers
of `open` remain source-compatible; they need not implement transfer methods.
An application may inject a boot identity for tests. Production s3cache fleet
mode obtains one token per process start from the operating system CSPRNG;
failure disables peer bootstrap and leaves guarded origin recovery available.
An operator-backed monotonic incarnation provider is also valid. A wall
clock sample or resettable process counter is not.

The existing recovery worker alone drives the selected `ClaimEngine` and
its `TransferSession` child. There is no spawned per-bucket claim loop and no
application-owned retry state machine. Add a pure recovery effect for
`AcquireBaseline { op }` after a full invalidation when bootstrap is enabled;
the default configuration still emits `RebuildOrigin`. The shell drives the
claim/transfer effects under the current recovery generation and the
original `RecoveryConfig::total_ms` deadline. The selected claim's finite
episode deadline must be no later than that recovery deadline. The worker
uses the minimum of claim renewal, exact selected-claim refresh, child work,
and recovery operation deadlines for one timer. The `AcquireBaseline` parent
operation lasts only until that original total
deadline; the claim and transfer engines separately bound each source and
stage operation. Applying ordinary `attempt_ms` to the entire parent would
abort healthy multi-step transfers. A failed or declined donor
returns a typed `BootstrapDeclined { op }` to the recovery engine, which then
emits a fresh guarded `RebuildOrigin` in the **same** original recovery
episode. It never starts an unbounded new origin attempt. A local provisional
builder also uses the existing `rebuild_origin` callback and publication
permit; its correlated success supplies the local baseline and may publish
`Ready` as donor availability, never as read authority. It reports
`LocalBaselineBuilt { op }` to recovery, which proceeds directly to its
ordinary `Affirm` without a second origin scan. The one worker continues
driving bounded `ClaimEngine` TTL renewals and donor journal expiry after
local recovery reaches `Ready`; a source gap, invalidated capture, restart,
or final handle drop withdraws that exact claim. Claim renewal after a
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
claim renewal arrives, or the worker waits for cleanup.

## Bounded capability surface

`BootstrapSetup` contains an object-safe `ClaimSource` that can publish and
withdraw native TTL claims, return a **complete** member/claim snapshot with
count and identity-byte limits enforced before allocation, and refresh one
exact selected claim from native TTL storage. It distinguishes absent from
unreadable or malformed observations. Claims and gossip are liveness hints;
they cannot certify origin freshness or absence. The current live lease and
write-feed implementation continues running while a follower is origin
routed or waiting for a donor.

An opt-in `DonorSource` supplies typed offer, reservation, sequential chunk,
attachment, atomic barrier, bounded batch, exact ack/readback, and native
coverage operations. The donor journal is captured under the application's
index publication lock: first reserve real encoded, decoded, suffix, and
in-flight memory, then start a `Capturing` journal at C and clone a bounded
private state under that lock. The pre-reserved journal accepts later index
effects during off-lock blocking encoding, but serves no follower RPC or
`Ready` claim before `finish_capture` revalidates the exact ingress and
original deadline. Cancelled blocking work retains its real buffer permits
through completion. Every
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
applied only to the old index. A new typed `NativeHandoffReceipt` binds the
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
aligns as a quiet zero-position feed) may install. A same-incarnation
position that differs in either direction is `Pending`: the follower is
still applying those feeds, or its live index already holds effects past B
that the stage lacks. The adapter answers `NativePending`, keeps its stage,
and the transfer samples a later barrier with `AdvanceBarrier` after
`coverage_poll_ms`, bounded by the transfer deadline. A changed writer
incarnation or an incomparable local repair is a conflict and aborts the
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
for offer, chunk, delta, barrier, ack, and cancellation; new frame kinds may
be added without altering existing frame bodies. The core has no Tokio,
network, clock, hash, or S3 dependency. The adapter verifies chunk length
and commitment before private staging, and corrupted or interrupted streams
discard the stage. The origin bucket receives no control object in either
default or optional peer-bootstrap mode.

## S3 scope and bounded donor lifetime

The current S3 `WriteSync` owns one recovery handle and one `KeyIndex`; its
`rebuild_origin` walks the bounded union of configured buckets and current
`KeyIndex` bucket names. The first fleet slice therefore claims **one
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
first slice finite as bucket count grows; splitting into independent bucket
scopes requires a later explicit recovery-handle and publication-gate design.

The local provisional builder may publish a `Ready` claim only after the
guarded origin scan finished every bucket, the bounded private capture is
complete at C, and the donor journal is attached to **every** subsequent
index publication. A completed recovery lease alone is insufficient. The
same worker services its TTL renewal and finite journal lifetime even after
the local read gate opens; the donor image remains an availability hint, not
authority. When the capture or global admission expires, withdraw or let the
claim expire and reject new reservations. Continued local serving still
depends on the ordinary recovery, lease, and application gates.

## First implementation and verification slices

1. Extend the pure recovery engine with optional baseline acquisition and
   typed declined/installed transitions, preserving one original total
   deadline and existing default outputs. Test gap, lapse, rearm, cancel,
   donor timeout, local build, and origin fallback in deterministic schedules.
2. Add the thin claim/transfer driver to the existing recovery worker with
   bounded source callbacks and a shared byte admission object. In-memory
   fault tests hold a chunk, source callback, and install callback across a
   gap or cancellation; none may publish. Exercise a late native DELETE and
   an origin-validated repair between B and handoff, equal-time cross-writer
   overlap that must abort, cancellation racing the publication critical
   section, and concurrent body fill. An installed candidate must have every
   source effect exactly once or remain unservable; stale callbacks cannot
   replace it. Verify follower lease and feed ingestion continue while reads
   route to origin. Measure concurrent scope memory and source-call counts
   against configured bounds.
3. Wire the S3 index adapter under an explicit fleet opt-in. The normal
   connected cold fleet elects one provisional origin builder after claim
   convergence; followers transfer and then independently affirm. A failed
   builder permits takeover, while partitioned duplicates remain safe. A
   donor without complete coverage, a failed bulk stream, or unavailable
   source falls back to each follower's guarded origin scan. MinIO tests
   assert exact GET/LIST behavior, no control writes to the origin bucket,
   private-image deletion order, and local-hit availability after recovery.

All three slices require the repository's feature-specific tests, strict
Clippy, formatting, docs, and deterministic fault schedules before claiming
fleet bootstrap complete. The design does not provide event-complete
subscriptions or a durable source cursor for S3's ordinary object writes.
