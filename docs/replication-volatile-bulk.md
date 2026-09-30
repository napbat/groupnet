# Bounded peer bootstrap data plane

Status: **opt-in framing, typed codec, bounded client/listener, and fault tests
implemented; claim/transfer worker wiring and fleet consumer integration
pending**. This is an opt-in transport for the volatile donor protocol in
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

Codec and `MemBulkNet` tests now cover malformed/truncated/oversized frames,
wrong correlation, duplicate/trailing/missing terminators, stalled EOF,
lost reservation response, full/closed inbox, caller cancellation, and donor
identity rotation. The subsequent worker/consumer slice must test reservation
expiry and cleanup, delayed/reordered native replies, actual partial-write
cancellation, and multiworker donor exchange with
`groupnet-testkit::MemCluster`. A follower stays origin-routed
until its separate native coverage, handoff, lease, and recovery gates admit
local serving. Healthy connected schedules must complete donor transfer;
faulted schedules may fall back to origin within the original deadline.
