# Bounded peer bootstrap data plane

[Documentation index](README.md) · [Bootstrap runtime](replication-volatile-transfer-runtime.md)

Status: **opt-in framing, typed codec, bounded client/listener, recovery-worker
donor adapter, and fault suites implemented; S3 fleet consumer integration
and its acceptance measurements remain pending**. This is an opt-in transport for the volatile donor protocol in
[`replication-volatile-transfer-runtime.md`](replication-volatile-transfer-runtime.md).
It does not change the default S3 origin path or write metadata to origin.

The existing `BulkTransport` carries reliable ordered byte streams. One
bootstrap request uses one stream with one request frame and a bounded reply
sequence. A chunk or batch uses one payload frame; every reply ends with an
**in-band terminator** containing the exact frame count and total byte count.
EOF before the terminator is truncation, never success. After the terminator,
the responder closes its write half and the caller checks for EOF under its
original local deadline. A duplicate terminator, trailing frame or byte,
wrong count, cancelled write, oversized frame, or malformed stream fails the
operation and the stream is discarded. The donor's finite reservation expires
or is released by a later exact `Release` request when a connection dies. A terminal
reply can report success, refusal, expiry, or resource exhaustion; none is a
serving-authority proof. An unknown protocol kind or version fails closed.

The envelope binds the exact scope, selected donor and follower
`ClaimIdentity` (node, boot, session, attempt), original claim
`BootstrapOperation`, and current transfer child operation. The accept
side additionally checks the `BulkTransport` peer identity. Replies echo the
request correlation; a delayed reply from a prior donor, claim generation,
boot, transfer operation, or scope cannot complete current work. Native
`CaptureId`, `ReservationId`, `AttachToken`, barrier receipt, batch operation,
and cursor are encoded in the operation payload whenever that phase requires
them. The wire encoding has a version, bounded length-prefixed strings and
vectors, checked integer conversion, canonical ordering for member and
native-cut sets, and rejects trailing bytes. The bulk codec carries its own
version byte, which only rejects a mis-deployed peer; the control-plane
`FRAME_VERSION` and frame bodies are untouched.

The typed request/reply pairs mirror `DonorRequest`: `Offer`, `Reserve`,
`Chunk`, `Attach`, `Barrier`, `AdvanceBarrier`, `Batch`, `Ack`, and `Release`.
The donor service executes them against its current exact capture and
reservation. It cannot synthesize a newer barrier from a stored older one,
nor turn an ack for one batch into progress on another. `Offer`, barrier,
chunk, and batch replies retain their admitted source ownership through
encoding and send completion. A response succeeds only after its complete
terminator and the bounded end-of-stream check; unknown write outcomes
require a fresh correlated request, with exact donor-journal readback for
`Ack` when available, never an assumption of success.
Typed refusals distinguish stale capture or reservation, deadline expiry,
capacity/backpressure, unsupported schema or version, and continuity loss.
Only the exact matching operation may consume a refusal; it never silently
advances a transfer phase.

The caller supplies a finite maximum frame and phase-specific encoded and
decoded byte/event limits. `DataStream::recv_bounded` rejects the declared
frame length before allocating. The worker reserves from its shared global
byte admission **before** the read, actor dispatch, clone, or encode that can
allocate a returned frame. The reservation follows the actual bytes through
the queued request, decoded response, private stage, or outgoing send and is
released only after those values are retired. A second simultaneously held
copy takes a second reservation. The current listener accepts one stream at
a time under a finite server deadline, and its worker inbox has finite
request and byte admission. This serial setting may limit donor throughput
and has no measured fleet capacity claim. Dropped caller futures do
not detach uncharged server work. A request deadline cannot be extended by
stream reconnect, new donor selection, or claim renewal.
Only the caller's monotonic local deadline controls its connect, send,
receive, EOF check, and retry. No `Instant` or wall timestamp crosses the
wire. The responder separately clamps work to its own finite configured
request budget and current capture/reservation expiry. Relative requested
limits cannot refresh the donor's existing reservation.

The driver belongs to Groupnet consistency, with a separate opt-in feature
`volatile-bootstrap-bulk` that implies `volatile-recovery` and pulls the
existing `groupnet-transport/bulk`, `bytes`, and `futures-util` dependencies.
The facade exposes it as `consistency-volatile-bootstrap-bulk`, implying
`consistency-volatile-recovery` and `bulk`; like `consistency-handoff`, the
weak `groupnet-transport-mem?/bulk` edge supplies `MemBulkNet` when `mem` is
selected, without requiring it for no-default builds. Feature-specific tests,
strict Clippy, no-default facade checks, and rustdoc must run in addition to
the workspace default matrix. Applications
provide encoded index chunks, private stage writes, native-feed continuity,
and the generation-fenced atomic handoff. They do not implement a second
network request/retry state machine. The sans-IO claim and transfer engines
remain the only protocol decision makers; the runtime maps one current effect
to one correlated request and returns the exact result or failure.

The listener serves one accepted stream at a time and shares the worker's
bounded inbox. It reserves worst-case frame bytes before reading a payload,
then reserves decoded bytes before trying the finite inbox; it never spawns
an unbounded task per stream. Pending requests and
outbound replies retain their permits through send or cancellation. It serves
only the active `DonorCapture` identity and rejects incoming work after
withdrawal, journal overflow, or capture expiry. Bounded batches yield to
claim renewal, local feed ingestion, and recovery timers. Closing the worker
drains or rejects its inbox and drops each stream and permit; an old listener
cannot route a late request into a replacement capture.

The public seam is `BootstrapBulkClient::<B>::new(DataPlane<B>, ByteAdmission,
BulkLimits)` plus `request(Correlation, &DonorRequest, Instant)`, returning an
`Admitted<WireReply>` or typed error. It does not choose a retry time. The
donor side is `BootstrapBulkListener::<B>::new(DataPlane<B>, DonorSender,
ByteAdmission, BulkLimits)` and `run(watch::Receiver<bool>)`. The listener
reads `DonorSender::current_identity()` for each request; the owning worker
updates that identity when a guarded capture becomes ready, and clears it
when that capture retires. One transport binding can therefore survive a
new donor attempt/session without admitting old requests. The listener owns
one admitted stream at a time; `DonorSender::try_submit` keeps the decoded
request charge through the bounded worker queue and callback. The worker
still owns the only `DonorInbox`, active capture, and claim/transfer engines.

Codec and `MemBulkNet` suites cover malformed/truncated/oversized frames,
wrong correlation, duplicate/trailing/missing terminators, stalled EOF,
lost reservation response, full/closed inbox, caller cancellation, and donor
identity rotation. Worker and adapter suites cover expiry/cleanup, stale
install, cancellation, donor withdrawal, journal reply readback, and all
generic donor-journal request branches. Consumer acceptance still requires
concurrent multiworker network faults and application-level partial-write
cancellation with
`groupnet-testkit::MemCluster`. A follower stays origin-routed
until its separate native coverage, handoff, lease, and recovery gates admit
local serving. Healthy connected schedules must complete donor transfer;
faulted schedules may fall back to origin within the original deadline.

## Bulk donor adapter inside the existing recovery worker

The reusable `BulkDonorPort` adapter is implemented; S3 fleet integration
remains pending. It binds the bounded bulk client/listener to
`BootstrapSession`. The S3 consumer must not turn each `TransferEffect` into
an application-owned network/retry state machine. The default S3 path still
makes zero coordination or metadata writes to origin.

`BootstrapSession` remains the sole scheduler. It supplies the exact current
effect, `TransferContext` (original parent and full donor/follower identities),
owned `TransferResources`, shared `ByteAdmission`, and the **absolute child
operation deadline** to one `DonorPort::execute` call. The worker already
intersects that deadline with the original recovery episode, ticks the core
after every await, and ignores stale callbacks. The deadline is an explicit
port argument, not a fresh duration derived inside a network adapter.
`BulkDonorPort<A, B>` wraps a consumer `A: BootstrapStatePort` and a
`BootstrapBulkClient<B: BulkTransport>`; it implements `DonorPort` for the
existing worker without a new task, timer, map of stages, or operation
allocator. Construction also pins a complete `BootstrapScope` and bulk limits.
It constructs the client from the **same** admission handle passed to the
worker, or rejects a separately supplied handle whose pool identity differs;
a mismatched pool cannot evade the global cap. Local capture and guarded
unlink delegate to `A`. Groupnet handles incoming Reserve, Attach, Barrier,
Advance, Batch, Ack, and Release against `DonorJournal` under the shared
ingress lock. `A` supplies only a charged immutable image offer and bounded
image chunks. Native writer cuts must enter that journal under the
application's index publication coordinator before the atomic B sample;
the generic handler never samples separate native state after B.
The image-offer callback receives the exact metadata cap before allocation;
Groupnet checks the completed offer as well. Donor journal limits for roster,
cuts, and batches must fit that same local policy before any mutable request.

The Groupnet wrapper owns these translations. `FetchOffer`, `ReserveDonor`,
`FetchChunk`, `AttachStream`, `FetchBarrier`, `AdvanceBarrier`, `FetchBatch`,
and `AckBatch` create one exact typed `DonorRequest`, build `Correlation` from
the selected donor, follower, original parent, current child operation, and
scope, then call `client.request(..., child_deadline)`. A returned reply must
have the expected phase and exact nested capture/reservation/B/cursor before
it becomes a `TransferEvent`. `ReleaseReservation` sends a bounded best-effort
exact release; it produces no progress event. The reservation identity, not a
newly invented child token, binds this cleanup. Its local deadline is the
minimum of the existing reservation expiry and original episode budget; it
cannot extend recovery, and a lost cleanup response cannot prevent bounded
worker shutdown. A response lost after the
donor acted is uncertain: only the existing core retry/readback decisions may
advance the phase. Network errors, refusal, cancellation, and deadline expiry
become `Failed` for the current core operation or leave cleanup incomplete;
none affirms a read or installs a candidate.

`ReserveStage`, `VerifyImage`, `CheckNativeCoverage`, `InstallCandidate`, and
`DiscardStage` stay consumer-state callbacks. A fetched chunk passes to the
consumer's private-stage write with its `Admitted<Vec<u8>>` charge intact;
the wrapper then emits `ChunkStored` only for an exact successful write and
verified per-chunk bytes. A fetched batch passes through a private replay
callback with its in-flight charge intact; `BatchStaged` means every exact
effect through that batch was applied privately. `AckBatch` follows, never
precedes, successful private replay. The consumer alone supplies native feed
buffer/coverage proofs, application schema, image commitment check, and the
single generation-fenced candidate swap plus normal-feed handoff. These
callbacks own no network request selection or retry timer.

The wrapper charges the checked sum of a request and its exact correlation
before cloning either, and holds that permit through the network await.
Retained reservation and attachment identities have a separate worker-owned
charge until cleanup;
source replies are charged before journal clones enter the inbox. The wrapper
uses an ownership-preserving conversion from
`Admitted<WireReply>` into an admitted chunk, batch, or `TransferEvent`.
The shared decoded charge uses a checked maximum of the actual static
`size_of::<WireReply>()` and `size_of::<TransferEvent>()` headers, bounded
vector element headers, and their variable bytes. Inline batch staging keeps
the admitted batch and its charge through application and ack preparation. No
clone may outlive that charge; a second retained copy reserves again first.
The application receives borrowed or owned admitted values and cannot move
buffers out of their permit. `TransferResources` remains the sole owner of
private stage, logical donor attachment, at most one batch, and native overlap.
The journal counts one logical outstanding batch across lost-response
readbacks; each physical batch copy reserves a separate runtime byte charge
before the journal clones it. The donor's whole-journal storage reservation
includes bounded delta and
follower vector slots with growth headroom, retained roster/cut bodies,
identities, and every saved follower barrier. It is acquired before the
guarded index capture and persists while ingress is attached. Image buffers
and physical response/readback copies have separate admissions; this is a
conservative finite ownership bound, not an exact process RSS estimate.
Requests with metadata or batch caps other than the donor's configured local
policy are refused before source state changes.
Heterogeneous policy caps can therefore decline peer transfer and route the
follower to origin; there is no cap negotiation in this first protocol.
The network `Attach` reply is a source reservation/attachment token: this
first protocol uses one short-lived request stream per operation, while the
donor journal retains and serves the continuous bounded suffix. It is not an
unbounded open socket per follower.

`volatile_bootstrap_bulk_adapter.rs` exercises the real `MemBulkNet` mapping
and all nine generic donor-journal request branches, including nonempty C→B
and B2, reservation/ack readback, and exact wrong-operation refusal.
`volatile_bootstrap_journal_readback.rs` checks seeded lost, duplicate, and
reordered source replies; `volatile_bootstrap_runtime.rs` checks worker
expiry, stale install, cancellation, and donor withdrawal. The earlier bulk
client/listener tests cover correlation and listener identity rotation.
These are reusable protocol boundaries, not a claim that a connected S3
fleet already bootstraps from peers. Actor-backed native claim/participation
is implemented; concurrent multiworker network faults and S3 index/feed
handoff remain consumer acceptance work. They must prove failures cannot
publish or grant local reads.
The S3 integration keeps origin-routed reads, active lease grants, and native
feed ingestion while this child runs; it supplies bounded index bytes and
atomic capture/publication semantics, never a synthetic durable source cursor.
