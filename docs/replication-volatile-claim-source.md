# Native TTL source for volatile bootstrap

Status: design contract; implementation pending. This is an optional Groupnet
adapter for the claim/transfer runtime described in
[`replication-volatile-transfer-runtime.md`](replication-volatile-transfer-runtime.md).
It does not make claims, membership, or a donor image authoritative for S3
object state. Default s3cache operation still writes no coordination metadata
to S3 or to a separate control store.

## One coherent, bounded observation

`Group::set_entry` and `delete_entry` enqueue app-state commands and
`Group::node_entry` reads a byte-only watch snapshot. That watch does not
contain the entry's observer-local expiry, and `node_entries` clones values
before a caller can cap them. `statuses_held_bounded` bounds a roster but is a
separate watch, so pairing it with entries is not one actor-state cut. The
native adapter therefore needs a generic, async, actor-side inspection API in
`groupnet-runtime`, with no dependency on `groupnet-consistency`:

```rust
trait InspectionBudget: Send + 'static {
    fn bytes(&self) -> usize;
}
Group::inspect_scoped_entry<B: InspectionBudget>(key, limits, budget: B)
    -> Result<(ObservedEntries, B), InspectError>
```

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

## Entry identity and exact withdrawal

One node-owned entry key is derived from a length-delimited encoding of
`BootstrapScope`, under a reserved bootstrap namespace. The bounded value
contains a codec version, exact scope and policy fingerprint, `NodeId`,
128-bit boot nonce, session, attempt, renewal, phase, build progress, and no wall-clock
deadline. `Group::set_entry` supplies a finite TTL duration; each observer
measures remaining life on its own monotonic clock. The codec rejects trailing
bytes, oversized fields, unknown versions, zero identity components, and a
value whose embedded scope disagrees with the requested key. It adds no new
frame kind and changes no existing `EntryDelta` body.

Local enqueue success is not proof that a claim was adopted. Add an actor
acknowledgment/readback for this generic entry mutation, or read back the
exact local entry through the bounded actor query before reporting publish
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

## Wakeup, transport, and tests

The existing `NodeStateChanged`/`MembershipChanged` stream is only a coalesced
wakeup. On lag, the worker repeats the complete bounded actor inspection;
core `Tick` keeps finite observation and claim-expiry deadlines even when
every wakeup is lost. Source reads/publishes are charged to exact child
operations, never to a fresh outer deadline. `ClaimSource` remains a reusable
Groupnet capability. The existing recovery worker drives the paired bulk
protocol as well; no application claim loop or mandatory S3 control object is
introduced.

## Bounded bulk request/reply

The existing `BulkTransport`/`DataPlane` provides ordered byte streams and
`MemBulkNet` provides the in-memory transport. The current `DataStream::recv`
accepts a transport-wide 256 MiB frame before allocating it, much larger
than a bootstrap operation's budget. Add a generic `recv_bounded(max_bytes)`
that rejects the length header before buffer allocation, and use an equally
bounded send path. The bootstrap codec uses its own magic/version and typed
kinds inside that bounded payload. This is a new bulk-stream protocol; it
does not alter the control-plane `FRAME_VERSION` or the existing Hosted
handoff body.

One request and one terminal reply run on each stream. The request header
binds scope, exact donor and follower boot/session/attempt, parent and child
operations, capture/reservation/attachment/barrier identity where relevant,
kind, and caller response limits. Kinds map exactly to the existing
`DonorRequest` variants: `Offer`, `Reserve`, `Chunk`, `Attach`, `Barrier`,
`AdvanceBarrier`, `Batch`, `Ack`, `Release`. The response is either the
matching `DonorReply` with the same correlation fields, or a typed refusal.
A chunk or batch carries one bounded payload. Every response ends with an
in-band terminator containing exact frame and byte counts; EOF without it is
a truncated operation, never success. The decoder rejects duplicate terminal
frames, unknown kinds/versions, trailing bytes, noncontiguous chunks, wrong
reservation, and any claimed count or length above both the per-effect cap
and the shared admission reservation. It checks metadata and frame lengths
before allocating decoded vectors or buffers.

The donor accept path admits the bounded request into the existing
`DonorInbox`; full capacity returns a typed refusal and drops its charge.
The worker samples the current captured journal under its short synchronous
lock, then sends from owned admitted buffers without holding that lock across
an await. `Offer`/`Barrier` response metadata and returned batch/chunk copies
retain their own charges until send completes or cancels. No response itself
grants read authority, and a late reply cannot supersede the exact child
operation. Outbound connect, request, response, and send completion all use
the original selected claim/transfer deadline. An interrupted request can be
retried or read back only through the core's exact correlated effect; the
port never invents an independent schedule.

First implementation tests should cover bound failures before allocation,
remaining-TTL monotonic decay, same-revision regossip, malformed-present
versus absent, delayed conditional withdrawal after a new session, actor
backpressure and ambiguous mutation readback. An in-memory multiworker
`MemCluster` schedule must partition/heal claim gossip, drop/duplicate wakeups,
expire a builder, and show one connected builder under timely convergence
with finite takeover otherwise. It must also demonstrate that no claim or
donor Ready event opens the local serving gate without the separate recovery
and native-feed handoff proof. `MemBulkNet` tests must inject truncated,
duplicated, and wrong-operation frames, stalled sends, a full donor inbox,
lost response after donor reservation, and cancellation during install. They
assert that all admission charges retire and that neither EOF nor two peers
agreeing on a head can install or serve an incomplete image.
