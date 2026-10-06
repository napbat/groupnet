# Volatile peer-bootstrap membership binding

[Documentation index](README.md) · [Bootstrap protocol](replication-volatile-bootstrap.md) · [Native claim source](replication-volatile-transfer-runtime.md#native-ttl-source-for-volatile-bootstrap)

Status: **native participation source, membership-bound capture/recapture,
and worker maintenance implemented; S3 fleet consumer integration remains
pending**. This contract does not itself establish S3 deployment readiness.

Builder claims and membership are different lifetimes. A follower withdraws
its builder claim after transfer, while it remains a healthy Groupnet member.
Therefore a donor image cannot use the currently visible claim identities as
its complete membership roster. Requiring one claim per member would make a
later joining follower rebuild from origin even in a healthy connected fleet.

An opted-in participant publishes one separate, scoped participation entry
with its NodeId, a fresh caller-supplied 128-bit boot identity, a fresh worker
session, scope/policy binding, and strictly increasing renewal sequence.
The attempt number belongs only to transient builder claims. The same
sans-IO bootstrap engine schedules presence renewal and expiry with its
existing logical timer/operation allocator, including after the recovery
engine reaches Ready or follower transfer completes; the runtime does not
add a second application scheduler. The entry is advisory: it grants neither
serving authority nor an origin-write position. A new process using the same
NodeId has a distinct boot identity; a new worker session under the same boot
is distinct too. An old entry, reply, or image cannot be
reinterpreted as belonging to the new process. Publication uses the existing
Groupnet actor's bounded TTL entry mutation and exact readback on unknown
outcome. Shutdown withdraws the exact entry; lost withdrawal remains bounded
by native TTL. A participant cannot claim readiness for a donor image from
this entry alone.

The core owns one presence identity, renewal high-water mark and finite
next-renewal deadline per scope, not an unbounded history of old entries.
Re-observing an unchanged native renewal never extends its expiry. An
unreadable entry, backward renewal, clock arithmetic exhaustion, lost current
presence or finite capacity failure withdraws donor availability and makes
peer transfer unavailable until fresh source proof; local read authority
remains governed by its separate recovery/lease gates. The actor-owned queue,
raw response, decoded response and core-retained roster each have a checked
pre-allocation charge in the shared byte pool. A cancelled caller does not
release actor-response admission before the actor drops the response.

Presence mutation is conditional within the Group actor. The paired actor
cut returns the retained per-key revision, including a tombstone, and the
member's state-version high-water mark. A renewal compares only the exact
presence-key revision, so unrelated hot coherence entries cannot starve it.
An initial create after no retained per-key revision also compares the member
high-water mark, preventing absent-value ABA: an old queued create cannot
publish after a newer presence was installed and then withdrawn or expired.
The actor checks after advancing expiries and refuses revision exhaustion;
an unknown response requires exact readback rather than blind retry. Because
an initial create binds the whole member revision, any unrelated local entry
write between the cut and the actor rejects it without mutation. The source
answers that confirmed rejection by taking a fresh cut and re-running every
check, a bounded number of times inside the caller's operation deadline; a
retained key revision or newer presence still refuses. The queued request
and exact response own admission through actor completion.

A process restarted under the same member name finds its previous life's
presence gossiped back by peers until that entry's TTL lapses. A local
presence with a different boot token is that previous life, and a source that
has not yet confirmed a presence of its own supersedes it at the observed key
revision. The same boot token under another session of this process still
refuses, as does any foreign identity once this source has published.

The source exposes one **complete, bounded actor cut** of native membership
status/incarnation, participation entries, and builder claims for a bootstrap
scope. Limits on member count, NodeId bytes, entry bytes, and total owned
response bytes are checked before cloning inside the actor. Its response
carries an observer-local monotonic sample instant and native remaining TTL,
which ages through the actor, adapter, and worker before a core event accepts
it. Missing, malformed, duplicate, expired, or wrong-policy participation for
an eligible member refuses that cut; it never creates a synthetic boot
identity from NodeId or SWIM incarnation. A retained old-process entry
conflicting with a new-process entry is refused likewise until the source
resolves the conflict. The worker reports a refused, failed or timed-out
sample to the core as `ObservationFailed`. A follower still inside its grace
for a selected remote builder samples again at the observation interval,
since under load a read times out or a peer's presence renewal lands late
without ending the build it follows; neither the grace nor the episode budget
is renewed by a failed sample, so both still bound the wait. Any other
selection declines peer bootstrap and falls back to origin; ordinary coherence
and origin reads remain available.

Each selection sample is a fresh decision over its own complete cut, pinned
for that decision only: a member refuting a suspicion with a higher
incarnation between two samples does not refuse the next one. Only the
verification of an already selected candidate (before capture, Ready, and
each barrier) holds a decision to the cut it was verified at. A followed
builder whose node is merely not eligible in a sample, as while SWIM suspects
a loaded peer, is sampled again inside the grace rather than replaced; a
builder that withdraws its claim while it is still an eligible member ended
its build and is taken over at once. The core reports every wait it gives up
as an informational `Released { builder, reason }` effect (`Stalled`,
`Withdrawn`, `DonorUnavailable`, `Unverified`, `Ended` or
`TransferAborted(error)` when the transfer from a Ready donor aborts), which
the runtime passes to an optional `BootstrapObserver` for operator logs. The
observer also receives `BootstrapDecision::Declined { reason }` whenever the
session ends without an image, and the recovery adapter's `fell_back(from,
reason)` names the origin path the episode then takes. A transfer that keeps
advancing renews its own stall bound and the parent budgets, so a slow but
live transfer is never released for outliving `donor_wait_ms` (see
`replication-volatile-transfer-runtime.md`).

The narrow runtime primitive is a fixed two-key
`Group::inspect_scoped_pair(presence_key, claim_key, limits, owned_budget)`.
It returns each member's NodeId, status, native membership incarnation, two
optional raw values, their retained revisions, the member version high-water,
and both native remaining TTLs at one actor sample
instant. The actor validates the complete result and retains the supplied
owned budget through its queue and response; it never gathers two independent
snapshots and pretends they were atomic. `NativeClaimSource` decodes both
policy-bound values into a new complete observation. Claim selection may use
only claim-bearing candidates. The journal, Offer, Barrier, and coverage roster
records every native member at that cut with exact status and membership
incarnation, including noneligible members without a participation entry.
Eligible members require a current compatible presence. Across cuts the
roster binds each member's node and presence, not its status or incarnation:
a join, a leave, a restart's new boot or session, or a presence that lapses or
appears invalidates the old capture; a suspicion, a Dead verdict, or a
refutation alone does not (see "What a donor roster binds" below).
The current one-key inspection and builder-claim APIs remain usable unchanged.

The donor journal records a sorted, exact, count-and-byte-bounded native
membership roster (NodeId, incarnation/status, and optional fresh presence
boot/session). Builder selection separately retains the exact
`ClaimIdentity` of donor and follower. Under the guarded index publication
lock, image C and the journal ingress become one candidate; a fresh source
roster is checked immediately before C and again before Ready and each
barrier. Changed or unprovable membership invalidates that candidate and
withdraws donor availability. Source gossip is not a linearizable admission
barrier: a new writer unseen at C can still join. The follower therefore
keeps native overlap/continuity and its independent current lease/frontier
affirmation; it must origin-route if those proofs cannot cover the join.

**What a donor roster binds.** Each cut is validated exactly as above. Two
cuts, at C and after encoding, at C and at Ready, a barrier or the donor's
maintenance recheck, or the donor's C roster and a follower's post-handoff
cut, bind the same membership (`same_membership`) when they list the same
members in order and each member has the same presence (boot and session) or
none in both. Status and incarnation are left out because they are the
observing node's SWIM opinion, not a fact about the member: a suspicion or a
Dead verdict changes only the observer's status for the member, and a
refutation raises only the member's incarnation, while the process, its
presence, and the writes it can make stay the same. Nothing a donor image must
cover depends on them. Writes are bound by native cuts: C records each
writer's epoch and sequence under the same publication lock, every later
effect enters the journal contiguously, and a new writer or epoch
(`Invalidation::Membership`) or a sequence gap (`Invalidation::Gap`) retires
the capture. Lease and feed continuity are bound by the outer recovery gate,
which closes on a gap, a lapse, or a join and revokes the Ready guard, and by
the follower's own lease/frontier affirmation and native overlap. What the
roster does bind still catches every change of the membership itself: a join
adds a node, a reap removes one, a restart shows a new boot or session (a
restarted process starts again at incarnation zero, so its incarnation never
reliably revealed it), and a member that really died loses its presence
within one claim TTL even while peers only suspect it. A noneligible member
without presence is bound only by its node; it can neither donate nor
receive, and any write from a restart of it arrives as a new native writer.
Binding SWIM opinions made the check fail on churn it cannot use: on a loaded
one-CPU pod the capture itself pauses the node (below), a peer suspects it, and
it refutes, so the donor's own incarnation rose between C and the
post-encode cut, and a loaded follower went from Suspect to Dead in the
donor's view, in almost every attempt. Different observers also disagree on
both fields until gossip converges, which a follower comparing the donor's
roster with its own cut would otherwise have to wait out.

If the origin LIST completed but donor capture is refused or the roster changed
while C was encoded, the worker reports a local-only baseline. It does not
repeat the LIST or advertise Ready. Once that baseline reaches the independent
local serving gate, the existing worker waits for a complete fresh source cut
before starting one finite Ready recapture. The builder's Building claim is
not withdrawn at the local-only baseline: it stays renewed until the
recapture's claim supersedes it. A follower that samples between the two
therefore keeps waiting for this image instead of scanning the origin again.
A pending recapture makes no progress, so that claim is renewed for at most
the builder's own stall bound, `donor_wait_ms`, and then withdrawn; a follower
stops waiting within its own bound, `donor_wait_ms + claim_ttl_ms +
observe_ms`, of the last advance it saw.
A transient newly Alive member without presence leaves participation renewing;
the existing maintenance timer rechecks without repeating the LIST. The
recapture renews its claim and presence while C is encoded, so a large image
cannot let either lapse. Before it checks the encoded image against a fresh
participation cut, the worker publishes every renewal the engine has
scheduled, including one that came due while the capture held the worker:
that cut is verified against the worker's own claim sequence. A recapture that
fails or outlives its donor-wait bound is pending again, never an origin
fallback. While a recapture is pending, every maintenance turn samples a
complete cut and offers it to the core, which starts the attempt or nothing.
A failed attempt's retry waits a backoff of one observation interval after the
first failure, doubling, capped at a quarter of `donor_wait_ms` but never
under one interval. The backoff paces only the participants the failures were
taken under. The core keeps the presence identities (process boot and worker
session) the failed attempts' cuts named, at most one per listed node, and a
cut naming any other participant, a joiner or a restarted peer's new life,
ends the backoff and starts the failures over: its first capture starts at
once. A follower must be in its donor's roster, so no failed attempt could
have served it, and most failures it would otherwise wait out are its own
arrival's: its predecessor's leave lapses the donor's lease and fails a running
attempt, the reap changes the roster, and the join itself lapses the donor's
lease and retires its capture, each within a claim window of the last. No stall
of this node can mint a participant: a member whose presence lapses and
returns is the same one, so a capture that costs its own lapse cannot reset
its own backoff.
The claim window opened when the image became pending (the local-only build,
the adoption of an installed image, or a retired capture) is not extended by
failures: inside it the claim stays renewed and a retry runs under any
complete cut, so a transient failure or a join that interrupts a recapture is
retried once its backoff has passed. When the window closes the claim is
withdrawn, and only a cut binding a different membership than the one the
last attempt failed under starts another attempt, which opens a fresh window;
a failure the membership did not cause never loops, and no joiner waits on it.
A Ready capture proves itself by staying Ready for one whole claim window.
One retired sooner, by a lapse, an expiry or a membership change, is one more
failed attempt: failures accumulate and the backoff keeps doubling until a
capture has proved itself. Its retirement still opens a fresh claim window,
so a joiner waits through the lapse that retired it, but once the failures'
backoffs add up to a whole window, a cut binding the membership the last
attempt failed under starts nothing, as after a closed window. A capture that
stalls its own node into a lease lapse therefore costs at most one window's
worth of attempts per membership, not one per lapse. With `observe_ms` = 1 s
and `donor_wait_ms` = 30 s, starts are at least 1, 2, 4, 7.5, 7.5 and 7.5 s
apart after each failure, so one window's backoff holds at most seven
attempts, and after it each membership change among the same participants
grants one attempt at most every 7.5 s. A new participant grants one
immediate attempt and starts its own such series; participants come only from
process boots and worker sessions elsewhere, so this adds no loop.

A node Ready on a peer's installed image is a donor as well. When its outer
recovery affirms Ready, the core binds the retired candidate's installed image
to the child as its local baseline (`AdoptLocalBaseline`), held as a completed
local image whose Ready recapture is pending at once: the core opens a fresh
claim window and publishes a fresh attempt's Building claim for it, and the
next verified complete cut captures it, unchanged membership and all, exactly
as for an origin build. Every lapse or membership change after that retires
and replaces the capture as it does a built one. A rejoiner is so a donor
within one capture of its own Ready, whichever node built the index first: the
next pod a rolling update stops leaves behind a node whose image is Ready or
advertised, not one waiting for a membership change to capture it.

Each attempt's cut at C is taken under the publication fence and the index
write lock, and C must stay one atomic cut with its journal ingress and native
cuts. A runtime task needing either waits meanwhile, so a node on one runtime
worker pauses for as long as C holds them, and a pause past the lease makes
the capture cause its own lapse. The consumer therefore must keep C to O(1)
work in its row count. For S3 this requires sizing the image from maintained
per-bucket index tallies and snapshotting structurally shared persistent maps,
then measuring and encoding that snapshot off-lock. These are consumer
integration obligations, not evidence supplied by the generic worker.
While the worker awaits any adapter operation that does not itself drive the
bootstrap child (invalidation, the origin rebuild, peer observation and
frontier waits), it keeps the child's maintenance turns running on the
child's own deadline and wake. A node's own origin scan can outlast its
presence TTL many times over; its presence must stay renewed meanwhile, or
every peer's complete participation cut, which a donor's Ready recapture
needs, misses this live member for the whole scan, and the lapsed presence is
refused when the scan ends. No Ready recapture starts during such an
operation: the worker offers no Ready capture guard while one is armed.
The worker also runs this maintenance once immediately after the outer
recovery affirms Ready, so an already-complete local image need not wait for
its next presence renewal. Each turn obtains a read-only Ready capture guard
from the current outer recovery control version and generation. The original
origin-build permit remains expired or revoked and cannot authorize recapture.
A gap, lapse, or join closes the gate and invalidates that guard; no donor
recapture occurs until the new recovery episode reaffirms and its exact child
generation matches the new guard. Every attempt keeps a fresh finite
donor-wait deadline and never repeats the completed origin LIST.
For a lease lapse that retains a locally built or adopted baseline, the
sans-IO recovery engine suspends the child first: its Ready capture, if it
holds one, is retired, its origin publication permit is discarded, and its
claim, a Ready one or the Building claim of a recapture still pending, is
superseded by a fresh attempt's Building claim, renewed for a fresh window
under the same bound as above while presence renewal continues. A pending
recapture's claim is renewed even if its window had closed and the claim was
withdrawn. A join commonly causes exactly such a lapse, so leaving the claim
withdrawn here would let the joiner, sampling during the lapse, scan the
origin. The retirement also grants the next verified cut one recapture, even
one equal to a cut a recapture failed under, because that failure belonged to
the suspended Ready generation, unless the retired capture is itself a failed
attempt (above) whose retries are spent. A lapse during a running attempt
retires nothing: that attempt fails at its guarded finish and counts as a
failure, so a capture that costs its own lapse earns no extra attempt from it.
The engine issues a new child
binding only after that same lapse episode passes renewal, head, frontier,
and final affirmation checks. The child then uses the new Ready guard and a new
claim/capture identity. A feed
gap, full rebuild, or failed lapse retires the suspended candidate instead.
Candidate retirement withdraws its claim and transfer resources but preserves
the process's bounded presence renewal. A completed peer transfer follows the
same rule: its candidate is retired, while presence remains through later
lapses and can participate in a third join. A later acquisition starts a fresh
candidate under its own generation without republishing or duplicating that
presence. Only terminal worker shutdown withdraws presence. This carry-forward
never reuses an old capture or authorizes reads
while the outer gate is closed.

A follower may select a Ready claim just as that donor loses its outer lease.
A transfer the donor refuses or that fails verification (`Continuity`,
`Stale`, a schema or capacity refusal) excludes that exact claim attempt; it
does not grant another use of its C. A transfer operation that only failed
or timed out (`Unavailable`) is no verdict on the image: the request may
never have reached the donor. A transient connection failure on an otherwise
healthy donor is not a continuity verdict; excluding its still-live claim
would make a follower wait out the donor bound and scan origin unnecessarily.
Such an attempt
therefore stays eligible while the first Ready selection's donor-wait
deadline holds. The follower samples a fresh complete cut one observation
interval later and transfers from that attempt again if it is still live
there; a donor that did retire its capture has superseded the attempt with
a new claim by then. Neither the deadline nor the episode budget is
renewed, so a donor that keeps failing ends the wait at that deadline like
any other, and every new transfer starts from an Offer the donor serves only
after rechecking its roster. The sans-IO selection engine may sample fresh
complete source cuts at the configured finite observation interval, waiting
for a new eligible claim identity only until the first Ready attempt's
original donor-wait deadline, capped by the episode's total deadline. It does
not extend either budget on subsequent refusals. If no independently valid
replacement arrives, the ordinary origin fallback proceeds. Origin reads
remain available during this wait; no stale donor permission is accepted.

A healthy donor checks its roster on the existing worker maintenance turn,
and also before serving an Offer or Barrier. A cut binding a different
membership, or an unprovable cut,
retires its old capture and unlinks the old journal before offering any
replacement; its Ready claim is superseded by a renewed Building claim, as
after a lapse, so joiners wait for the replacement. Once a complete fresh cut
is available, the existing recovery worker may issue a finite recapture
operation against its already-Ready complete index; this does not repeat
origin LIST. This is a distinct trusted
`recapture_current_index` callback, not a reuse or extension of the expired
origin-build `PublicationPermit`. It binds the new claim/capture identity,
recovery generation, complete participation roster, and original operation
deadline. The consumer reserves image and suffix memory before entering the
short recovery-control then index-publication critical section. In that
section it proves the current generation/lease/feed gate and complete bucket
coverage, captures C, and attaches the new journal. Encoding may run
off-lock while live mutations enter the journal. A fresh roster and the same
generation/lease/feed proof are checked before new Ready publication. Any
refusal leaves the healthy local read gate unchanged and peer transfer
unavailable; it never silently starts another origin scan. The S3 consumer
must decline recapture on incomplete/uncertain buckets, changed scope, a feed
gap, or insufficient admission.

The scoped participation entry uses a new reserved key and body kind. The
existing builder-claim value is unchanged (codec `VBC2` since it gained
build progress). The canonical bulk Offer
and Barrier bodies carry the single canonical full-native roster under bulk
codec version 2. Peers upgrade together, so there is no parallel bulk-body
path for another version; the version byte only rejects a mis-deployed peer.
A healthy two-node run with a transferred follower must still
select the already-Ready donor for a third joining follower. Tests must prove
that schedule, old/new boot and same-boot new-session overlap, stale TTL or
renewal replay, missing participation fallback, cancellation and admission
release, and member change during C encoding or B replay; and that SWIM
suspicion, Dead verdicts and refutations during C encoding fail no capture,
while failed recaptures are paced and bounded as above.
