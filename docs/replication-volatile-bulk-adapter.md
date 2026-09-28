# Bulk donor adapter inside the existing recovery worker

Status: **design contract; implementation pending**. This completes the
reusable mapping between the opt-in [bulk client and listener](replication-volatile-bulk.md)
and the already implemented `BootstrapSession`. The S3 consumer must not turn
each `TransferEffect` into an application-owned network/retry state machine.
The default S3 path still makes zero coordination or metadata writes to the
origin.

`BootstrapSession` remains the sole scheduler. It supplies the exact current
effect, `TransferContext` (original parent and full donor/follower identities),
owned `TransferResources`, shared `ByteAdmission`, and the **absolute child
operation deadline** to one `DonorPort::execute` call. The worker already
intersects that deadline with the original recovery episode, ticks the core
after every await, and ignores stale callbacks. Add the deadline as an explicit
port argument rather than deriving a fresh duration inside a network adapter.
`BulkDonorPort<A, B>` wraps a consumer `A: BootstrapStatePort` and a
`BootstrapBulkClient<B: BulkTransport>`; it implements `DonorPort` for the
existing worker without a new task, timer, map of stages, or operation
allocator. Construction also pins a complete `BootstrapScope` and bulk limits.
It constructs the client from the **same** admission handle passed to the
worker, or rejects a separately supplied handle whose pool identity differs;
a mismatched pool cannot evade the global cap. Local capture, guarded
unlink, and incoming follower preparation delegate to `A`.

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

The wrapper needs an ownership-preserving conversion from
`Admitted<WireReply>` into an admitted chunk, batch, or `TransferEvent`.
The shared decoded charge uses a checked maximum of the actual static
`size_of::<WireReply>()` and `size_of::<TransferEvent>()` headers, bounded
vector element headers, and their variable bytes. Inline batch staging keeps
the admitted batch and its charge through application and ack preparation. No
clone may outlive that charge; a second retained copy reserves again first.
The application receives borrowed or owned admitted values and cannot move
buffers out of their permit. `TransferResources` remains the sole owner of
private stage, logical donor attachment, at most one batch, and native overlap.
The network `Attach` reply is a source reservation/attachment token: this
first protocol uses one short-lived request stream per operation, while the
donor journal retains and serves the continuous bounded suffix. It is not an
unbounded open socket per follower.

Safety tests use a real `MemBulkNet` binding and the existing
`BootstrapSession`/recovery worker. They cover all nine phases, C→B nonempty
suffix, B2 advancement after exact ack, lost reservation/ack response,
duplicate or wrong-correlation replies, cancellation during chunk and install,
and donor identity rotation. Faults must not produce `Installed`, `Ready`, or
local serving; healthy schedules finish within the original episode deadline.
`groupnet-testkit::MemCluster` exercises simultaneous claims and donor
requests once the native claim source and consumer adapters are connected.
The S3 integration keeps origin-routed reads, active lease grants, and native
feed ingestion while this child runs; it supplies bounded index bytes and
atomic capture/publication semantics, never a synthetic durable source cursor.
