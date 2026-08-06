# Consistency modes

Status: **accepted 2026-08-05 — all decisions resolved (Section 8);
implementation begins with Milestone 0.** The milestone list in Section 6 is
the build order of record.

This document designs groupnet's consistency-mode surface: what each mode
honestly guarantees, where each lives, and the order it gets built. It is
grounded in a review of the two real consumers (docres/shardstore and s3cache)
rather than in taxonomy for its own sake.

---

## 1. Terminology

Two words are load-bearing and must never blur:

* **Coordinator** — the existing derived, non-authoritative role (rendezvous
  hash over live members). It stays exactly as it is: no fencing, no
  authority, bifurcation under partition remains harmless *because it can do
  nothing binding*. Retrofitting authority onto it would silently change the
  contract under every current consumer.
* **Host** — the new elected, **epoch-fenced** role introduced by the Hosted
  mode. In the common case the host lands on the same node the coordinator
  ranking picks, but it is a distinct concept with a distinct API.

The two **coexist** in a Hosted group: the derived coordinator never goes
away and never gains authority; the host is additive, opt-in, and the only
bearer of authority.

Likewise **modes** vs **tiers**:

* A **mode** is per-group and changes the write path (Eventual, Hosted).
* A **tier** is a composable opt-in layer above a mode (session feeds,
  applied-acks). Today's `consistency` and `acks` features are tiers, not
  modes, and stay that way.

## 2. Consumer ground truth

The design must "retain docres support and support strong". What the two
consumers actually do today (verified in their code, 2026-08):

### docres / shardstore

docres is the host binary for the shardstore engine. From groupnet it consumes
**liveness only**: SWIM membership (`Group::members()`), TTL'd per-node
entries (`set_entry`/`node_entry` — gossiped applied-LSN hints for read
routing, explicitly "safe to lose"), and `groupnet_core::placement` (weighted
HRW) for derived writer/replica/maintainer roles. It does **not** use the
`consistency` crate — shardstore hand-rolled a session layer because its
floors are shard-LSN-typed.

Its "strong" writes exist already and come from somewhere else entirely:
**object-store CAS**. `caslog` claims commit-log slots with put-if-absent;
epoch-fenced writer records make one node the *normal* single appender per
shard; placement is "a liveness input, never a safety one"; its own docs say
"no election, no coordination round". The store's create-exclusivity is the
only safety primitive. That is: docres's strong guarantee is **fenced
single-serializer-per-shard, safety anchored in external CAS** — precisely the
`Activation::External` shape below, already proven in production against
R2/MinIO conditional writes.

Consequences: docres needs no in-fabric quorum, and "retaining docres
support" means keeping the eventual fabric + liveness surface stable while
formalizing the fencing pattern shardstore already implements by hand.

### s3cache

A clustered S3 caching proxy, and the deepest consumer of groupnet's
consistency tiers: `WriteFeed`/`PeerWrites` (index invalidation events),
`Frontier` (freshness barriers), `AckLedger` + `applied_cluster_wide` (its
"strong" mode holds each write until every alive peer applied the
invalidation), gaps → full flush + origin resync. Every uncertainty degrades
to "serve via the origin — slower, never wrong". It needs the **acks tier**,
never elected leadership; conflicting durable writes are arbitrated by the
origin's own conditional-write support, passed through untouched.

Two concrete API gaps groupnet owes it (Section 7):

1. **Detector-timing introspection.** s3cache sizes its authoritative-404
   trust window from `groupnet::core::Config::default()` because the *built*
   node's effective config is not reachable. If timings change or become
   per-node, its honesty claim silently breaks.
2. **Ack-capability visibility.** Bounded-mode nodes never ack, so a
   mixed-mode cluster makes every strong write eat its full timeout. Ack
   participation should be advertised in membership so writers can exclude
   non-acking peers.

### The language mapping

The owner's "strongly consistent mode" decomposes into two different things:

* For docres-shaped consumers: **single serializer per shard with epoch
  fencing**, safety delegated to an external CAS-capable store. Not CP
  consensus in the fabric.
* As a generic base-library capability (p2p games, future consumers):
  **elected host with real authority**, where the activation policy decides
  the CAP posture.

Both are the same epoch/fencing skeleton with different activation policies —
which is the core of this design.

## 3. Mode taxonomy and honest guarantees

The contract in three sentences (owner-confirmed 2026-08-05): a group can
stay a pure metadata/membership group — `Eventual` is the default mode and
its contract never changes. A group can opt into an elected, epoch-fenced
leader (Hosted mode), with the activation policy setting the CAP posture. And
a small-roster group can compose Quorum activation with a quorum-applied
commit level (see *Commit levels* under M3) to get genuinely strong
guarantees — every write pays a majority round-trip, which is exactly right
for a game session or a small cache cluster and deliberately wrong at fleet
scale.

### The dial: what you want → what you pay

The whole surface, as a practitioner chooses it (owner-confirmed
2026-08-05). Configurations are per group and stack top-down; rows link to
the sections that define them.

| You want | You configure | The write path pays |
|---|---|---|
| Converges eventually, free | `Eventual` (default; derived coordinator only) — M0 | nothing — gossip amortizes it |
| Read-your-writes, loss surfaced | + session tier — T1 | nothing extra |
| One authoritative serializer, "good enough" | Hosted `Settle` × `Commit::Local` — M3 | nothing extra; a migration may lose the acked tail, surfaced as a loud `Gap` |
| No node serves stale (cluster coherence) | acks tier — T2 (lease tier — T3 — proposed as its successor) | one cluster round per write |
| **Guaranteed**: no acked write ever lost, no split-brain | Hosted `Quorum` × `Commit::QuorumApplied` — M3 | one voter-majority round per write |
| **Guaranteed and fast** | Hosted `External` (CAS-anchored fencing) — M3 | ~nothing extra when writes already target a CAS-capable store |

The organizing principle: **you pay for a guarantee where the guarantee
lives.** In-fabric consensus costs a majority round-trip per write — right
for small rosters with no external store (a game session). External CAS
costs a store round-trip the consumer was usually already paying — docres's
fencing rides on the durability writes themselves, which is why shardstore
can honestly say "no election, no coordination round" while staying safe;
that is the *guaranteed-and-fast* quadrant. Coherence tiers cost a cluster
round because their guarantee is about **every reader**, not one writer —
s3cache, which uses no leader of either kind. Eventual is free because it
promises only convergence. And the knobs being per-group means one
deployment mixes them: docres runs shard groups with External fencing while
the fabric group underneath stays pure Eventual metadata.

### M0 — Eventual (base fabric; today; unchanged)

SWIM membership, per-key LWW metadata registers, single-writer per-node keyed
entries, digest/delta anti-entropy. AP; PA/EL. Guarantees: eventual
convergence, per-key deterministic single winner (LWW), per-node-per-key total
order. Gives up: cross-node ordering, freshness bounds, commit, conflict
surfacing (the LWW loser vanishes silently).

### T1 — Session tier (`consistency` feature; today)

Per-writer sequenced feeds with explicit `Gap` on loss/restart; `Frontier`
barriers where "reached" means *applied*, not delivered. Adds read-your-writes,
monotonic reads, per-writer order, detected-never-silent loss. Still AP/EL.

### T2 — Write-coherence tier (`consistency-acks` feature; today)

Applied-watermark ledgers; `applied_cluster_wide` waits on every member the
writer currently believes Alive. Bounded-time (not absolute) under asymmetric
partition inside the probe window — the crate's honesty box stays verbatim.
This tier is what s3cache's strong mode is built on today; the owner judges
that construction brittle, and T3 below is the proposed successor. T2 itself
stays: it is the right tool when the writer only needs *responsive* peers
coherent and degradation-on-timeout is acceptable.

### T3 — Coherence-lease tier (proposed; motivated by s3cache's brittle strong mode)

The owner's assessment — shared after reviewing s3cache — is that its current
strong mode is brittle. The diagnosis: T2's `applied_cluster_wide` is
**unanimity over a rumor-derived set**. Every write blocks on every peer the
writer currently believes Alive (N-of-N, the most fragile quorum), one
degraded-but-alive peer taxes every write cluster-wide, and an ack timeout
ends in a *degradation* (writer proceeds; s3cache's stability clock suspends
authoritative 404s everywhere) rather than a guarantee — correctness during
the window depends on the stale peer *learning* it should stand down, which
is exactly what an asymmetrically-partitioned peer cannot do. The root cause
is structural: **the read side has no self-expiring right to serve**, so the
write side has no choice but to chase acks from everyone, forever.

The fix is read-side **freshness leases** (Gray–Cheriton): a node may serve
locally-cached state (including authoritative negatives) only while holding
an unexpired lease. A writer's invalidation blocks on responsive
lease-holders (fast path — identical cost to T2 acks when healthy) or on the
lapse of a silent peer's lease (slow path — bounded, and the exposure is
ended by the *stale node's own clock*, a bounded-clock-**rate** assumption,
not a connectivity assumption). Consequences:

* Write-wait under failure becomes `min(acks, lease remainder)` with a real
  guarantee at the end, instead of a timeout with a hope at the end.
* "May I answer a 404 authoritatively?" becomes "do I hold a valid lease" —
  a mechanism, replacing s3cache's hand-rolled view-stability heuristic
  (`Stability`/`settled()` over `detection_window()`).
* Mixed-mode deployments stop being a convention: lease participation is
  advertised in membership (Section 7, item 2), so writers know exactly whom
  to wait for.

This tier is the **same lease machinery Hosted mode's Milestone 1 builds**
(grant/renew/expire, DST-provable disjointness in virtual time), pointed the
other way: instead of one host holding a lease to *write*, every reader
holds a lease to *serve*. Lease duration is the knob trading
write-stall-under-failure against renewal traffic; renewals piggyback on the
existing gossip cadence.

Status: **delivered** (Milestone 2) — feature `leases` in
`groupnet-consistency`, `consistency-leases` on the facade. The as-built
protocol refines the sketch above in ways that are now contract:

* **Renewal is confirmed, not asserted.** A reader's serving right derives
  only from renewals that came back: every member folds the renewals it has
  *adopted* into a wholesale grant-map entry (`~lease:g`), and the reader
  serves until `D − rate_margin` after the publish instant of the newest
  renewal confirmed by **every** not-reaped `CAP_LEASE` member. A reader
  that merely re-publishes cannot extend its own lease — the unilateral-
  extension hole a naive TTL-entry lease would have.
* **The writer's countdown starts at adoption.** A silent reader's lapse
  instant on the writer's side is the writer's *own engine's* TTL expiry of
  that reader's `~lease` entry — armed at adoption, so propagation delay is
  free safety margin in the safe direction. Zero wire changes: the whole
  tier rides existing TTL'd entries, and non-upgraded nodes relay them.
* **Lapse ⇒ NeedsResync ⇒ affirmation.** A lapsed (or booting) reader stays
  invalid even with a fresh confirmed lease until it affirms catch-up
  (`mark_caught_up`, accepted only while a lease is live) — a lapsed reader
  missed exactly the invalidations whose writers proceeded at its lapse.
* **Roster rule:** Suspect and Dead-but-unreaped granters stay in the
  confirmation min-set (either may still be writing); only a reap removes
  one. Boot guards narrow membership divergence, and both are **enforced in
  the shell** rather than asked of the deployment: for its first
  `detection_window_ms + 2 × anti_entropy_interval` of participation a reader
  cannot reach `Serving` at all (`mark_caught_up` declines and no serve
  deadline is published — without that gate the empty roster a booting node
  holds confirms vacuously, and it can serve under a window no granter gave),
  and a writer refuses the no-known-holders fast path over the same window.
* **What the roster rule costs, stated as an outage:** one unreaped
  `CAP_LEASE` member that stops granting freezes *every* reader's
  confirmation cluster-wide. Each reader's window closes within one `D` of
  the freeze and cannot reopen until membership reaps the silent member — at
  the reap horizon, `2 × dead_timeout_ms` past the `Dead` verdict, itself up
  to `detection_window_ms` past the silence. At the defaults (`D = 2s`,
  `dead_timeout_ms = 10s`, three members) that is `0.9 + 20 − 2` ≈ **19s of
  cluster-wide origin-serving** — correct reads throughout, none of them
  cached. Sizing: a lease deployment wants a short `dead_timeout_ms`, on the
  order of `D` (at `D = 2s`, `dead_timeout_ms = 2s` turns those 19s into
  ≈ 3s), bounded below by the longest partition it must survive and still
  reconcile — the reap horizon is also the window past which a returning
  node's entries can no longer be recovered by a digest.
* **Honesty:** production safety rests on bounded clock-*rate* error over
  one lease duration (`rate_margin`, reader-side, default
  `max(D/100, 5ms)`); every failure inside the assumptions degrades to
  origin-serving or writer over-waiting, never a stale serve. The two ways it
  costs availability instead — the fail-slow reader (renewing but not
  applying: no ack, no lapse, so the writer waits to its own deadline) and
  the unreaped-granter outage above — are named in the tier's honesty box.
  Proven in virtual time by `tests/lease_dst.rs` (224 chaos seeds: the
  Gray–Cheriton lapse contract, no-unconfirmed-extension, previous-life
  ghosts never serve) and `tests/lease_dst_liveness.rs` (64 liveness seeds);
  mutation-tested for falsifiability.

### M3 — Hosted mode (new)

One elected **host** per group serializes the group's authoritative writes for
the duration of an **epoch**. Epochs are totally ordered and fenced: any node
that has adopted epoch *e* rejects host-scoped state fenced with *e′ < e*.

What "strong" honestly means here, stated the way consumers must read it:

* **Single-serializer-per-epoch.** Within an epoch, the host's application
  order is *the* order; followers observe a prefix of it. (This is the
  existing session guarantee applied to the host's feed.)
* **Fencing, not prevention.** A deposed host's state stops propagating the
  moment any peer holds a higher epoch — but a client talking to a
  not-yet-deposed host during the detection window can be served stale reads.
  The window is bounded by probe/suspect timing plus lease expiry.
* **Not linearizability.** Follower reads may be stale; even host reads are
  linearizable only if the host proves it still holds the lease at read time.
  A documented knob, never an implied default.
* **Split-brain is an activation-policy choice**, made explicit below.

Key structural alignment: `WriteToken` in `groupnet-consistency` is already
`(epoch, seq)` with epoch-major ordering, and an epoch change already
surfaces to subscribers as a `Gap` covering the previous life. The leadership
epoch drops directly into that slot: **host migration is, to every
subscriber, exactly a writer-restart gap** — handled by machinery that
already exists and is already tested.

#### Activation policies (one skeleton, three ways to close an epoch)

| Policy | Partition behavior | CAP posture | Split-brain |
|---|---|---|---|
| `Quorum { voters }` | only a side holding a majority of the **static voter roster** activates a host; the minority side fails hosted writes fast (`NoLeader`, once M4's write path exists) while base-fabric gossip continues | CP for the hosted domain | none outside the lease window (lease expiry + bounded clock-*rate* skew) |
| `External` (CAS-anchored lease) | whoever wins the external conditional write is host; partition sides are irrelevant | CP; consensus outsourced to the anchor | **none in epochs, absolutely** — and on the steal path a bounded, always-cross-epoch, always-fenced overlap in *instants* † |
| `Settle { claim_settle_ms }` (lobby-style) | each side elects its own host after the settle window | AP + serialization per side | yes — bounded and **fenced**: at heal exactly one epoch survives; the loser surfaces as a `Gap` + demotion event for the app to reconcile |

† **What "absolutely" covers, and what it does not.** The absolute statement is
about **epochs**, and it is the strongest one in this table. The anchor is a
linearizable CAS register and it *allocates* the epoch — an epoch number exists
only because one conditional write created it — so no two nodes ever hold the
same epoch, at any instant, at disjoint times, across any partition, and **with
no node-local storage of any kind**. That is **S1-strict, unconditional**, and
it is strictly stronger than Quorum's: there S1-strict is *storage-conditional*,
holding only while voters remember what they granted (an amnesiac restart can
re-issue an epoch — see the M3 property matrix). External needs no `GrantStore`,
no boot blackout, and no persisted ledger, because the property that durability
was standing in for is the anchor's job.

What is *not* absolute is instantaneous non-overlap on the **steal** path.
A claimant supersedes an expired record when `now_wall_ms ≥ expires_at_wall_ms +
steal_margin_ms` — shardstore's `caslog/epoch.rs` TTL + skew-margin rule, lifted
verbatim — so the deposed holder can still believe itself live for as long as
the two nodes' **wall clocks disagree**, and the honest assumption is
*claimant wall-clock skew ≤ the configured steal margin*. Read that as the
**pairwise** condition it is — `|skew(claimant) − skew(holder)| ≤
steal_margin_ms`, plus the anchor round trip — because "every node is within
`steal_margin_ms` of true time" is a strictly weaker premise that does **not**
imply it: two nodes at opposite ends of that band disagree by *two* margins.
(The DST's within-margin family draws its per-node offsets inside ±`margin/2`
for exactly this reason, and pins the boundary at the pairwise limit.) Three
things bound
what that costs, and they are why the residue is acceptable rather than papered
over:

* it is **bounded** by the margin, not open-ended;
* it is **always cross-epoch** — the successor holds a strictly higher epoch by
  construction, so the two overlapping beliefs are never a same-epoch duel and
  every ordering question between them has an answer;
* it is **always fenced** — the fence token stamped onto the external store
  rejects the older epoch's writes at the store, whatever either node believes.

Stated as the design rule the whole tier rests on: **the anchor record is
*succession*; the fence at the store is *safety*.** A margin sized wrong costs a
slower or a slightly overlapping handover; it cannot cost a lost write, because
nothing about epoch uniqueness or fencing consults a clock.

A gossip-derived majority is **not** an option: SWIM views can diverge across
a partition, so "majority of who I think is alive" is unsafe. Quorum mode
requires an explicit roster; if a consumer can't name one, `External` is the
CP path.

#### Election design (reuse, don't duplicate)

* **Candidate priority = the existing rendezvous ranking.** The top-ranked
  live member is *the* candidate. Deterministic — no dueling-candidate
  randomized timeouts — and it inherits the coordinator's stability under
  churn. The derived coordinator itself is untouched.
* **Epochs** are `u64`, monotone per group. A candidate claims
  `epoch = highest_seen + 1` via a new `LeadClaim` frame; peers answer
  `LeadGrant` (Quorum: at most one grant per epoch per voter; Settle: grants
  are defined on the wire but inert — activation is
  settle-window-absence-of-a-beating-claim). Current `(epoch, host)` rides a
  small `LeadState` frame on the anti-entropy cadence for repair.
* **The fencing key is the pair `(epoch, host)`**, ordered epoch-major with
  the deterministic rendezvous tiebreak at equal epochs (M1 finding, forced:
  two symmetric partition sides with identical state can claim the same
  integer, so a bare-`u64` global "no two hosts per epoch" is unachievable
  in Settle mode — it returns as a strict guarantee under Quorum). Same-
  epoch cross-partition activations heal deterministically: exactly one
  pair survives, the loser is fenced and demoted.
* **Self-naming state is learned, never adopted** (M1 finding, from DST
  seed 45): a node that receives a `LeadState` naming **itself** at a
  **strictly higher epoch** must not re-adopt its own hostship from an echo
  — but it must still learn the epoch, step down to `(epoch, None)`, and
  re-claim above it. Strictly-higher-*epoch*, not higher-*pair*: acting on
  an equal-epoch echo of one's own hostship would either regress the
  adopted pair or re-emit the step-down effect on every repair round — the
  equal-epoch echo is deliberately inert (the fixed point the rule leaves
  behind). Without this rule a restarted host wedges the cluster into
  permanent disagreement on the fencing epoch while agreeing on the host.
* **Leases and step-down.** The host's authority expires `lease_ms` after its
  last successful renewal; on expiry it demotes itself *before* the rest of
  the cluster can elect a successor (lease < election timeout). In the sim,
  lease disjointness is checked exactly in virtual time; in production it
  rests on bounded clock-rate error — the standard assumption, stated
  plainly.
* **Voter durability (Quorum), stated honestly.** A voter that grants,
  crashes, and restarts within a claim window could double-grant (the classic
  persistent-vote problem). Raft solves it by *requiring* a persisted vote;
  that is the one place this design deviates from the standard safety model,
  and the deviation must not be the default posture. The rule: **a driver
  with durable storage persists the grant** (a recovered-state constructor,
  `GroupEngine::with_recovered(...)`, restores it on boot — docres-shaped
  deployments always have a store); the **post-restart grant blackout** — a
  freshly booted voter refuses to grant for ≥ `lease_ms` — is the documented
  fallback for genuinely storage-free deployments, converting durability
  into a timing rule DST can prove sufficient in logical time (but which,
  in production, rests on "restart + boot exceeds the blackout" instead of
  on nothing at all). *As built this is sharper than the sketch: the two are
  not alternatives — a recovered voter still serves the boot blackout, and
  what persistence buys is epoch uniqueness across restarts plus immediate
  re-grants to the incumbent. See the Quorum as-built subsection below.*
* **Fencing surface.** A `Fence { epoch, host }` token exposed to the
  application, stamped onto data-plane operations and — critically —
  **external stores** (S3/R2 `If-Match`/`If-None-Match`). Gossip cannot
  reject a doomed writer's disk I/O; the fence token is the bridge that makes
  "strong" real end-to-end. Same philosophy the README already holds: gossip
  carries liveness and coherence signals; stores own truth.
* **Host migration / handoff.** Milestone 1 surfaces
  `LeadershipChanged { epoch, host, role }`; the write path surfaces
  migration as the epoch `Gap`. Milestone 6 added optional snapshot handoff
  over the existing `BulkTransport` data plane — a `Gap` remediator, not a
  replay (see the M6 as-built subsection).

#### Quorum activation, as built (Milestone 3)

Status: **delivered (pending review)** — `Activation::Quorum { voters }` +
`VoterRoster`, the voter ledger and grant rounds in
`engine/election/quorum.rs`, `Effect::PersistGrant`, `RecoveredGrant` +
`GroupEngine::with_recovered` in `groupnet-core`; the `GrantStore` trait and
`GroupProfile::with_voter_storage` in `groupnet-runtime`. The as-built rules
refine the sketch above and are now contract:

* **What carries global single-lease safety is the grant *promise*, not the
  lease-vs-detection arithmetic.** A voter refuses every **new** claimant for
  `lease_ms` after any grant it makes (a re-grant of the same pair slides the
  promise; the claimant already granted is exempt, so a host may always
  advance its own epoch). That promise, plus one-grant-per-epoch-per-voter and
  the intersection of two majorities, is the whole argument. The Settle-era
  sizing rule `lease_ms < detection_window_ms + settle window` is therefore
  **demoted under Quorum to a liveness guideline**: it governs how fast a dead
  host's group recovers, and no safety property rests on it. (It remains a
  safety rule under `Settle`, where nothing else bounds split-brain.)
* **A lease is attributed to the instant the claim was *sent*.** An activation
  runs to `round_sent_at + lease_ms`, never `now + lease_ms` at the moment the
  majority landed: every voter in that majority promised from an instant at or
  after the send, so the host's authority expires no later than the earliest
  promise that made it host. Anchoring on the last grant's *arrival* would
  hand the host an overhang past the promises it was built from — exactly
  where a second host fits. **The assumption:** a *renewal* round re-anchors
  every anti-entropy tick, so a grant answering the previous round can be
  counted into the current one and over-attribute the lease by at most one
  anti-entropy interval. That is safe precisely while the **claim→grant round
  trip — ≈2·(latency + jitter) — stays under `anti_entropy_interval_ms`**: the
  grant being mis-attributed answered a claim sent one round earlier, so it is a
  round trip, not a one-way hop, that has to fit inside the cadence. A deployment
  that cannot
  hold that sizing needs per-round identity on the wire; the escape hatch is a
  nonce carried by a **new frame kind** (no existing body changes, so still
  `FRAME_VERSION 3`). An election round has no such slack — its anchor is the
  instant the claim was opened, and re-offers deliberately do not move it.
* **Recovery restores the pair, not the time** — correcting the M1 sketch
  above, which read as though persistence replaced the blackout. It does not:
  a store records *what* was granted, never *when*, so **even a recovered
  voter applies the boot-anchored window to every new claimant** (boot is at or
  after the crash, which is at or after the grant, so a blackout measured from
  boot always covers the promise the lost grant implied). What recovery buys is
  the other two things: **epoch uniqueness across restarts** (the recovered
  pair is a floor no restart can forget) and **immediate re-grants to the
  incumbent** (the claimant named in the pair is exempt from the promise, so a
  restarted voter stops starving the sitting host for a lease).
  `RecoveredGrant::none()` — storage attesting this voter has *never* granted —
  is the one statement that lifts the blackout outright, and only a driver that
  really did persist every grant may make it.
* **The honest property matrix.** Two different guarantees, and they are not
  the same strength:

  | Property | Storage-free (blackout) | `GrantStore` + recovery |
  |---|---|---|
  | **S4c-global** — at most one unexpired lease per group at any instant, partitions included | **holds** | **holds** |
  | **S1-strict** — no two nodes ever host the same *epoch*, even at disjoint times | holds unless a voter restarts amnesiac | **holds**, assuming persists succeed |

  The right-hand column's S1-strict is conditional on the store actually
  accepting what it is asked to write: a persist *failure* leaves the two
  self-grant shapes below (row Q4b's retry, and a roster of one) hosting on an
  undurable grant, which risks S1-strict across an amnesiac restart — and only
  that. **S4c is carried by the grant promise and the boot blackout and never
  depends on the store**, so it stays in the "holds" column however the disk
  behaves.

  The gap is exactly one scenario: a voter grants epoch *e*, crashes with no
  store, waits out its blackout, and grants *e* to a different claimant. The
  first host's lease expired before the blackout ended, so no two leases ever
  overlap — S4c is intact — but the epoch has stopped being a unique name for
  a hostship. That matters to anything that treats an epoch as an identity
  rather than as an ordering (a fence token stamped into an external store, for
  instance), which is why a deployment with a disk should use one.
* **The write-ahead driver contract, and fail-closed on persist error.**
  `Effect::PersistGrant` is emitted immediately before the frames the grant it
  records licenses, in two shapes: a peer's claim answered (the `LeadGrant`
  itself) and this node's own claim opened (a self-grant is counted straight
  into the round, so what follows is the `LeadClaim` broadcast). A driver with
  a store **must complete the persist before those frames leave**, and if the
  store errors it **must drop them** — and keep dropping re-offers of that pair
  until a later persist succeeds, since the engine re-answers a recorded grant
  without re-persisting it. The runtime driver does exactly this: the persist
  runs on the blocking pool and the actor waits for it; a store error (or a
  panicking store) arms a guard that swallows the matching grant/claim frames,
  matched by decoding rather than by position. The drop is **silent by
  design** — nothing in `NetStats` counts it (those counters are the sans-IO
  engine's, and it cannot see a driver's disk), and a frame never sent is not
  traffic; the `io::Error` the store returned is the operator's signal. Cost of
  a failing store: that voter stops closing epochs and (with the caveat below)
  cannot become host, so a roster that needs it stalls. A driver with **no**
  store ignores `PersistGrant` entirely — the supported blackout posture, not a
  bug.
* **The caveat: a self-grant is not always a frame.** The two shapes above are
  the frames a grant *licenses*; a claimant's own grant is counted straight into
  its round instead of being sent, so a failed persist has nothing left to
  withhold in two cases. Row **Q4b** re-attempts the self-grant on every tick the
  round is open — long after the claim went out — and a roster of **one** closes
  its round on the self-grant before the claim is broadcast at all. In both, a
  refused persist still leaves the round closed and **the activation's
  `LeadState` is not withheld**: the node hosts on a grant its disk refused. On a
  roster of two or more that is bounded to one lease (the renewal round's claim
  *is* withheld, so no voter re-grants and the host demotes on lapse); on a
  roster of one it is unbounded, because a solo voter's renewal closes in-engine
  and emits no frame to drop. What this costs is **S1-strict across an amnesiac
  restart** and nothing else — S4c-global is carried by the promise and the
  blackout, timing rules that never consult a store.
* **Voters and members are different sets, and both directions are legal.** The
  roster names who votes, not who is alive and not who is in the membership
  view. A voter gossip has never shown alive is still sent every claim (claim
  targets are the live peers **unioned** with the roster) — a roster member
  nobody has heard from is precisely the grant an election cannot afford to
  skip. A member outside the roster never grants (and cannot count itself),
  but is otherwise a full participant and may still be elected host, since
  candidacy is the rendezvous ranking and hosting and voting are independent.
  An **empty roster** asks for one grant it can never collect, so the group
  never activates a host at all — the fail-safe answer, chosen over a `0`
  threshold that would turn a misconfiguration into a silent loss of the very
  property Quorum is picked for.
* **The minority freeze, as it is observable *today*.** There is no `NoLeader`
  error yet: it belongs to M4's write path, and until that exists a minority
  side has nothing to fail fast. What M3 surfaces is `leadership()`. If the
  incumbent is on the minority side it cannot renew, so its lease lapses, it
  demotes, and every observer there moves to `(epoch, None)` and stays hostless
  — a minority candidate can claim but never collects a majority. If the
  incumbent is on the *majority* side, the minority keeps reporting the stale
  `(epoch, host)` pair it last adopted, with no local way to tell that it is
  stale; base-fabric gossip continues underneath either way. Both are correct
  and neither is an error return, so **a consumer must not read a non-`None`
  host as permission to serve** until M4 gives the write path its own verdict.

Proven at this layer by `groupnet-core/tests/election_quorum.rs` (the grant
rules and round arithmetic), the Quorum DST seeds in `groupnet-sim`, and
`groupnet-runtime/tests/quorum.rs` — which is where the *driver* half is
falsifiable: a majority of real stores holding the winning pair, a voter whose
store always fails putting no grant on the wire at all (counted on its own
transport) while the other two still close the epoch, the *same* failing store
on the **candidate** withholding the `LeadClaim` instead and stalling the group
hostless (candidacy is rank-gated, so no second-ranked node steps in), and a
voter restarting with its persisted ledger re-granting the incumbent an order of
magnitude inside the lease a blackout would have cost. The guard's own truth
table — including the `LeadState` it deliberately does **not** withhold — is
unit-tested beside it in `groupnet-runtime/src/driver.rs`.

#### Commit levels — the second half of "strong"

Activation answers *who may serve as host*; it does not answer *when a hosted
write may be acknowledged*. Those are orthogonal knobs, and true strong
consistency requires both. If the host acks from local state alone and dies
before any follower applied the write, the write is lost even though the
election was perfectly CP — so `HostedWrites` carries a commit level:

* **`Commit::Local`** — acked once the host applied it. Followers trail via
  the session tier; a migration may lose the acked tail, surfaced honestly as
  the epoch `Gap`. The game-lobby default: cheap, and clients rebase.
* **`Commit::QuorumApplied`** — acked once a majority of the voter roster has
  *applied* it (the acks-tier machinery scoped to voters). Combined with
  `Activation::Quorum`, the grant majority and the commit majority intersect,
  so an activating candidate can — and **must** — recover the newest
  committed state from the majority it heard before serving. That
  **leader-completeness rule** is what upgrades "single serializer with
  fencing" to a real guarantee: no write acked at this level is ever lost,
  and no split-brain exists outside the lease window. This is the
  small-roster strong profile.
* **`Commit::AllApplied`** — unanimity via T2, for read-anywhere-after-ack.
  Subject to the T3 brittleness diagnosis; prefer it leased, and only on
  small fixed rosters.

Reads, stated per level: host reads are linearizable only under a valid
lease (or a per-read renewal); follower reads at a commit watermark are
sequentially consistent, never linearizable. The strong profile's cost
envelope — a majority round-trip per write, majority grants per election —
is the deliberate, documented trade: right for single-digit rosters (a game
session, a small cache cluster), wrong past that. Named plainly: this
profile **is consensus** — view-stamped primary-backup over the existing
feed — and it is **the ceiling**: constrained to fixed small rosters, with
no general replicated log behind it (see M4 for the honest Raft
comparison).

#### Hosted write path, as built (Milestone 4)

Status: **delivered (pending review)** — both sans-IO cores, the `HostedWrites`
/ `HostedReads` / `CommitLedger` shells, the DST, and the runnable
fenced-ownership example
(`crates/groupnet-consistency/examples/fenced_ownership.rs`) are built.
Everything below is the contract of record for them: it amends, and in three
places narrows, the sketch above.

**Feature naming.** The write path ships as `hosted` in `groupnet-consistency`
and as **`consistency-hosted`** on the `groupnet` facade — the same
crate-feature / facade-feature pairing `leases` / `consistency-leases` uses.
Where this document writes a bare `hosted` it means the crate-level feature;
the facade name is always the prefixed one.

##### What "gating activation" means — and what it does not

Section 6's Milestone 4 line reads "the leader-completeness recovery step
gating activation when `QuorumApplied` is in force". As built the word
*activation* is narrowed to the **activation of hosted service**. The engine's
leadership activation is **unchanged**.

A candidate that collects a voter majority activates exactly when M3 says it
does, publishes its `LeadState`, and `Group::leadership()` reports
`(epoch, Some(self))` the instant it does. What waits is `HostedWrites`: until
the recovery rule below is satisfied it refuses service — every write returns
`HostedError::Recovering` — even though the node is by every other measure the
host. Four reasons this is the right cut, each load-bearing:

* **Layering.** Committed state lives in *entries the engine does not
  interpret* — a feed ring, a watermark ledger, a consumer's own datum codec.
  `groupnet-core` cannot evaluate a completeness predicate over payloads whose
  framing belongs to the consistency crate and whose contents belong to the
  application. Gating in the engine means teaching the engine the write path's
  schema, which is the layering violation this design has refused everywhere
  else.
* **Liveness.** A group whose consumers never construct the write path must not
  sit hostless. Gating in the engine would make hostship conditional on
  machinery that may not exist: a consumer of a `Hosted` group that wants only
  the fence token, or only `leadership()` for routing, would never see a host at
  all. Activation stays a membership fact; service is a consumer-layer verdict.
* **Authority.** Leadership was never permission to serve — that is M3's own
  contract, stated there in the minority-freeze paragraph ("a consumer must not
  read a non-`None` host as permission to serve" until M4 gives the write path
  its own verdict). This milestone supplies the verdict; it does not retract the
  rule. `Recovering` is what *elected but not yet serving* looks like through
  the API.
* **DST-provability, equal or better.** The recovery rule is a pure function of
  gossiped readings (`CompletenessCore::step`), so the simulator drives the
  identical code the tokio shell does, in virtual time, with no runtime and no
  transport — the posture the lease tier's cores established. Nothing is lost by
  keeping it out of the engine, and S5 becomes a predicate over snapshots rather
  than an emergent behaviour of an actor.

**The error surface, mapped to what was promised.**
`HostedError::NotHost { host: None }` **is** the `NoLeader` the activation-policy
table promised for a minority side: this node is not the host and believes the
group has none. `NotHost { host: Some(peer) }` is the redirect.
`Deposed { epoch }` is a fence hit mid-write. `Recovering` is the gate above.
`Rejected` is the group actor's bounded inbox refusing the enqueue — a
backpressure signal, never a consistency verdict.

##### The commit ledger: epoch-stamped, and why literal ack reuse fails

The commit-levels sketch reads "the acks-tier machinery scoped to voters". As
built it is a **new, epoch-stamped ledger** — `~hosted:applied`
(`~hosted:applied:<name>` for a named write path) — carrying one leadership
epoch followed by the same watermark records the ack ledger uses:

```text
(lead_epoch: u64 LE) (records: u32 LE) (writer_len: u32, writer: utf-8, token_epoch: u64, token_seq: u64)*
```

`lead_epoch` is the leadership epoch the publisher had adopted when it
published. Nothing else changes: watermarks are monotone per writer exactly as
`AckLedger`'s are, and `CommitLedger::refresh` re-stamps without touching them.

The `records` count is the one addition to the ack ledger's shape, and it is a
safety field rather than a convenience. Without it a reading truncated on a
record boundary still decodes — as a **subset** of the watermarks the publisher
meant to send, which is a *lower* recovery target and a *smaller* set of writers
for the recovery rule to demand coverage of. Silently under-recovering is
exactly the failure this tier refuses everywhere else, so the count makes such a
truncation undecodable instead: `decode_ledger` checks it three ways — the
records must all be present, they must end exactly at the last byte, and they
must name that many *distinct* writers — and any failure is `None`, "this member
publishes nothing", which the rules already treat as a non-witness. A reading is
all-or-nothing; twelve bytes (a stamp and a zero count) remain a legitimate one.

The two tiers stay independent all the way out to their capabilities: `hosted`
does **not** imply `acks`, `CAP_HOSTED` and `CAP_ACKS` advertise participation in
separate ledgers under separate rules, and a node may run and advertise either,
both, or neither.

Two holes force the stamp. Neither is closable by scoping `applied_by_selected`
to the roster:

1. **Gossip staleness — recovery undershoots.** The recovery rule reads voters'
   watermarks out of gossip, and an *unstamped* reading carries no evidence of
   **when** it was written. A new host can therefore satisfy a majority out of
   pre-migration views and set its recovery target below a write the old host
   had already acked. The reading looks like a majority; it is not a *fresh*
   one, and nothing in the payload can tell them apart.
2. **The late-ack race — a committed write is lost.** `applied_by` keeps
   advancing for the **old** host's feed after a new epoch activates: a voter
   still draining the old host's ring publishes a higher watermark long
   afterwards. An ack round opened before the migration can therefore resolve
   *after* the new host finished recovering — so the old host answers its client
   "committed" for a write the new host never saw, and the write is lost with an
   acknowledgement behind it.

The stamp is the **view-stamp fence**, and it is one sentence: *a voter that has
adopted a higher epoch stops counting.* Its reading no longer satisfies the
commit predicate's `lead_epoch == token.epoch`, so once a majority has adopted
`e′` no round at `e < e′` can ever close again — and the same stamp read the
other way (`lead_epoch ≥ e′`) is precisely what makes a recovery reading
provably *later* than any reading a commit at `e` could have counted.

##### The two rules, and the intersection argument

Fix the static voter roster `R`, with `m = |R| / 2 + 1` (the same strict
majority M3's grant rounds use).

**Commit rule.** A write `W` authored by host `H` at epoch `e`, bearing token
`t` (so `t.epoch = e`), is *committed at `QuorumApplied`* iff

> there is a set `S_c ⊆ R` with `|S_c| ≥ m` such that every `v ∈ S_c` publishes
> a reading `(lead_epoch_v, wm_v)` with `lead_epoch_v = e` **and**
> `wm_v(H) ≥ t`.

Liveness plays no part: a voter's reading counts whether or not membership
believes it alive, which is exactly what makes a *static* roster the denominator
rather than a rumour-derived set. (`Commit::AllApplied` applies the same
per-member predicate to every selected, currently-`Alive` member instead of to a
majority of `R`; `Commit::Local` commits on the host's own apply.)

**Recovery rule.** A host `H′` activating at epoch `e′` may begin serving iff

> there is a set `S_r ⊆ R` with `|S_r| ≥ m` such that every `v ∈ S_r` publishes
> a reading `(lead_epoch_v, wm_v)` with `lead_epoch_v ≥ e′`; **and** for every
> writer `w` named by any `v ∈ S_r`, `H′`'s own applied watermark satisfies
> `own_wm(w) ≥ max_{v ∈ S_r} wm_v(w)`.

Until both halves hold, the write path answers `Recovering`.

**The argument.** Suppose `W` committed at epoch `e`, and `H′` completed
recovery at some `e′ > e`. `S_c` and `S_r` are majorities of the **same** roster
`R`, so they intersect: pick `v* ∈ S_c ∩ S_r`. Then:

* `v*`'s counted commit reading was stamped `e`; its counted recovery reading is
  stamped `≥ e′ > e`.
* **Stamps are monotone per publisher.** A voter stamps the leadership epoch it
  has currently adopted, and an adopted epoch never regresses (property S2), so
  the recovery reading is a strictly **later publication by `v*`** than the
  commit reading.
* **Watermarks are monotone per publisher, per writer.** `record` never lowers
  one and `refresh` never touches one. So `v*`'s recovery-reading watermark for
  `H` is at or above its commit-reading watermark for `H`, which was `≥ t`.

Therefore `max_{v ∈ S_r} wm_v(H) ≥ t`, and the recovery rule's second half
forces `own_wm(H) ≥ t`: **`H′`'s state contains `W`**. `W` was an arbitrary
write committed at any epoch below `e′`, so no write acknowledged at
`QuorumApplied` is ever lost across a migration — property **S5**.

**The grant set is not needed.** The intersection above is commit-majority ∩
recovery-majority, both over the same static roster. M3's grant majority carries
a *different* property (at most one unexpired lease per group) and does not
enter this argument at all: a deployment reasoning about S5 needs the roster and
the ledger, not the election's vote records.

##### The deployment contract: every voter runs the follower loop

Both rules are predicates over what voters *publish*. A voter that votes but
never publishes is invisible to both, so the contract is explicit:

> **Every voter must run the follower loop.** Subscribe to the host's feed
> (`HostedReads`), apply each event, and call `CommitLedger::record(&host,
> token)` **after** the apply — and on a `Migrated` event call
> `CommitLedger::refresh()`, so the stamp tracks the epoch the voter has
> adopted even while no new writes are arriving. **Bind the subscriber to the
> node's own write path** (`HostedWrites::bind`, one line before the loop takes
> the handle), so that a voter which becomes host cuts its predecessor's lineage
> at the instant it starts serving — see deviation 2b.

Voting-without-applying is not a safety hazard and is not silently tolerated
either — the tier fails **closed** around it. Commits at `QuorumApplied` that
need that voter's ack run out the caller's deadline
(`CommitOutcome::TimedOut { waiting_on }`, naming it), and a new host that needs
it in `S_r` stalls in `Recovering`. Both are loud, both are availability
failures, and neither is a lost write. The symmetry with the lease tier's
fail-slow reader is exact, and so is the remedy: the outcome names the node.

##### Two deviations from the sketch, reviewed and recorded as contract

Both were taken during implementation, both were reviewed, and both are now
binding — a consumer may rely on them and an alternative implementation must
reproduce them. The second carries a rider (**2b**) added in review: the cut a
node must take on its predecessor's lineage the moment it *serves*, which the
first-delivered-write rule can never take for it.

**1. The host counts itself in a `QuorumApplied` majority.** A successful
`HostedWrites::publish` records the write into *this node's own* `CommitLedger`
before returning, so `S_c` may include `H` itself. That is honest rather than
generous: `publish` is called **after** the caller's local durable write (the
`WriteFeed` contract), so by the time the record lands the host genuinely has
applied it, and its reading says nothing it cannot back. The cost model this
buys is the one the tier advertises: on a roster of three, a write commits
through **one follower plus self** — a single ack round to whichever follower is
fastest — instead of unanimity among both.

The alternative (a host that never counts itself) is also safe and strictly
stricter, and the intersection argument is indifferent to the choice: any two
majorities of `R` meet whether or not `H` is in one of them. What the choice
does change is availability — never counting itself would turn a three-voter
`QuorumApplied` into unanimity among the two followers, so one slow follower
would tax every write. This design takes the majority; nothing about S5 moves.

**2. The lineage cursor cuts at the first *delivered* write of the new lineage,
not at watch adoption.** `HostedReads` keeps two positions — the adopted
`(epoch, host)` pair and the cursor it has actually delivered from — and drops a
write only once the *new* lineage has spoken, not the moment the watch adopts a
higher epoch.

The tempting simplification (drop anything below the adopted epoch) strands a
recovering host, and strands it permanently. A host activating at `e′` whose
apply loop was behind must still **drain the predecessor's tail** — that tail is
precisely what the recovery rule is measuring it against — and a subscriber
blinded to epoch-`e` writes the instant it adopts `e′` could never reach the
target its peers report. The engine would say host; the write path would answer
`Recovering`; nothing would ever move.

Deferring the cut costs no *acked* write, because safety here is not the
subscriber's job. A late epoch-`e` write delivered in that window cannot be
counted toward a commit at `e`: this node has already stamped `≥ e′`, so its
reading fails the commit predicate's `lead_epoch == t.epoch` — the view-stamp
fence above, doing exactly the work it was introduced for. **Safety is carried
by the stamp; ordering is carried by the subscriber** — and the ordering half is
kept, because the instant the new lineage delivers its first write the old one is
dead to this subscriber for good.

*What the window does cost, stated so nobody infers less.* A follower still
draining may **apply a write that is doomed** — never committed, and therefore no
part of any successor's recovery. The divergence is real and it is not
self-healing: a voter outside the recovery majority can be *ahead* of the
successor, and nothing later re-delivers the difference. What reconciles it is
the `Gap` that opens the next lineage: it is **authoritative**, not advisory, and
the coarse remediation it asks for (flush, rebuild, refetch from the consumer's
own store) is exactly what discards the doomed tail. A cache-shaped consumer —
the shape this tier is written for — is therefore safe by construction. A
consumer that treats the stream as an exact replay log and skips the rebuild
keeps the divergence permanently, and that is its choice, not the tier's
promise.

**2b. A host cuts its predecessor's lineage when it begins to *serve*.** The
deferral above has one hole, and it is on the node that can least afford it.
`HostedReads` excludes this node's own feed, so a node that is itself the host
never sees a write of its own lineage — the "first delivered write of the new
lineage" never arrives, and the open lineage stays the **predecessor's** for as
long as the process lives. Its predecessor's un-replicated tail is gossiped
state that can land minutes later, when a partition heals; delivered then it
would be applied *behind* the writes this node has authored at `e′`, which is a
fenced epoch-`e` write reordered after the authority's own — silent stale state
on the authority itself.

The signal that closes it is **service**, not delivery: `HostedReads::cut_below`
is the mechanism (drop everything below an epoch, emit nothing, move the cursor
off the dead lineage), and `HostedWrites::bind` takes it automatically at the
instant the admission table admits this node to serve. That instant is the
earliest honest one in both directions — a host that is still `Recovering` needs
the predecessor's tail, because the recovery rule is measuring it against exactly
that, and a serving host must not have it. Leadership alone cannot tell those two
apart; admission can.

Binding is one builder step and it is part of the deployment contract: **a
serving host that neither binds nor calls `cut_below` keeps the divergence window
above open for as long as it hosts**, on the one node whose state everybody else
is about to be recovered from.

##### The recovery verdict is latched per epoch

`HostedWrites` evaluates both rules **on demand and synchronously**, off the
always-current gossip snapshots: no background task, nothing to spawn, no `Drop`.
The recovery verdict is then **latched per epoch** — once `Completeness::Complete`
answers for `e′`, this node serves `e′` for as long as it holds it, and a
laggard's reading arriving afterwards, even one that would raise the target,
never re-closes the gate.

That is contract rather than an implementation detail, and it is correct for the
same reason the intersection argument is: the argument needs **one** fresh
majority, read **once**. A majority stamped `≥ e′` at any instant is proof that
no commit round below `e′` can ever close again, and a watermark that arrives
after that instant describes an *uncommitted* tail — not a write the successor
owes anybody. Re-closing the gate on it would trade availability for nothing.
`HostedWrites::recovery()` is deliberately **non-latching** and observability-only:
reading a `Complete` there does not take it, so an operator's probe never changes
what the node will serve. Decisions go through `fence()` / `publish()`.

##### Durability, honestly

`QuorumApplied` promises that no acked write is lost **while a majority of the
applied copies survives**. It cannot outlive their simultaneous loss: the
ledger's watermarks are gossiped state, and a majority of voters crashing
amnesiac at once takes the evidence — and, for a memory-resident consumer, the
applied state itself — with them. That is the standard majority-durability
assumption; nothing here weakens it and nothing here strengthens it. An
application with durable storage reseeds its ledger on boot from what its own
store says it applied (`CommitLedger::with_recovered`), which keeps a restarted
voter counting toward the majority it belongs to instead of dropping out of it
for a full catch-up.

S5 additionally presumes the **`GrantStore` posture** of M3's property matrix —
epoch uniqueness across restarts. The commit predicate compares a stamp to
`t.epoch` by **equality**, so an epoch that has stopped being a unique name for
a hostship (the storage-free blackout posture's one gap) would let a reading
stamped by a *different* hostship of the same integer count toward a commit.
Storage-free Quorum keeps S4c; it does not keep S5. A deployment choosing
`Commit::QuorumApplied` should run a `GrantStore`.

##### The ring is the substrate, and it is bounded

Recovery is expressed as watermarks, and a watermark past the end of a peer's
visible ring is reached by machinery that already exists: the subscriber
surfaces `PeerWrite::Gap`, the consumer remediates coarsely per its own contract
(flush, rebuild, refetch), and the frontier advances into the target. Stated so
that nobody reads "recovery" as "replay":

* A recovering host whose target lies **beyond the ring** does not replay those
  writes. It is `Gap`-remediated exactly as any lagging subscriber is, and
  completeness is satisfied by the remediation — not by the individual writes.
* A consumer needing **exact replay** rather than coarse remediation must size
  the ring for the worst migration lag it accepts. That is a capacity decision
  and it is the consumer's.
* **State transfer is not in this milestone**, and it is not in this tier: `Gap`
  plus the consumer's remediation is the whole story here — which is what the
  strong-profile-versus-Raft table's "a laggard gaps and state-resyncs" row
  means in practice. Milestone 6 has since delivered the *optional* way past the
  bound for consumers whose state **is** the groupnet-carried state (feature
  `handoff`, module `hosted::handoff`): a covering snapshot pulled from a donor
  over the data plane, verified at three points, seeded into this ledger. It
  changes nothing above — the rule still sees only watermarks that moved — and
  `hosted` is complete without it. See the M6 as-built subsection below.

#### External activation, as built (Milestone 5)

Status: **Delivered (pending review).** What shipped, by layer:

* **core** — `Activation::External { steal_margin_ms }`; the sans-IO anchor
  vocabulary in `groupnet-core/src/anchor.rs` (`AnchorRecord`, `stealable`,
  `ClaimPlan`, `plan_claim`, `renewal_record`, `ambiguous_applied`);
  `Effect::AnchorClaimDue`, `Command::AnchorActivated` /
  `Command::AnchorObserved`; the `X`-rows in `engine/election/external.rs`.
* **runtime** — the `Anchor` trait (`load` + conditional `store`), `AnchorToken`
  / `AnchorWriteIf` / `AnchorCas`, `GroupProfile::with_anchor`, and the per-group
  `anchor_task` in `groupnet-runtime/src/anchor.rs`.
* **sim** — a deterministic CAS register plus the driver state around it
  (`groupnet-sim/src/anchor.rs`), with fault knobs for anchor reachability,
  per-node wall-clock skew and ambiguous writes in **both** readings (applied
  but unreported, and never applied and unreported), all orthogonal to fabric
  partitions.
* **tests** — `groupnet-core/tests/election_external.rs`, the three
  `groupnet-sim/tests/election_external*.rs` suites,
  `groupnet-runtime/tests/external.rs` and `external_faults.rs`, and a runnable
  example (`cargo run -p groupnet-consistency --example anchored_ownership
  --features hosted`).

Everything below is the contract of record for what is built.

##### The anchor is a raw CAS store, not a lock service

The trait a deployment implements is deliberately the *smallest* shape a real
object store already offers — a read that returns a version marker, and a write
conditional on one:

```text
load()                  -> Option<(AnchorRecord, AnchorToken)>   // GET
store(Absent,  record)  -> Stored | Mismatch | Unknown           // PUT If-None-Match: *
store(Matches, record)  -> Stored | Mismatch | Unknown           // PUT If-Match: <etag>
```

As built there is **one method for both writes**, taking the precondition
`plan_claim` already decided (`AnchorWriteIf::Absent` / `Matches(token)`), so a
driver never infers which one to use. An implementation **closes over its object
key** — nothing in the trait names the group, because the `GroupProfile` a group
is joined under is what pairs them. An `Err` from `store` is read as `Unknown`,
so an implementation that cannot distinguish "never left" from "may have
applied" is still correct; an `Err` from `load` decides nothing at all.

That is S3/R2/GCS reality: `GET` plus `PUT` with `If-None-Match: *` or
`If-Match: <etag>`, and the third outcome is not optional — a timed-out or
interrupted conditional `PUT` is genuinely *ambiguous*, and the only honest
resolution is a read-back (shardstore's `renew` does exactly this, and
`anchor::ambiguous_applied` is that read-back rule as a pure predicate).

**The read-back compares the whole record, not the `(epoch, host)` pair.**
Reviewed and changed during M5. For a *claim* the pair would do — an attempted
claim bids strictly above everything standing, so finding it means our own write
put it there. A **renewal** breaks that: it keeps the epoch and the host and
only moves the expiry, so what it attempted and what is already standing are the
same pair, and "my renewal applied" is indistinguishable from "my old record is
still there". The reading that matters is not a rare timeout but a *standing*
one — a store whose `PUT`s fail while its `GET`s work (write throttle,
read-only window, expired write credentials) reports `Unknown` for **every**
renewal — and a pair-only verdict would call each of them a win: the engine
lease extends indefinitely off a record nobody is refreshing, until a rival
steals at `expires + steal_margin` and two nodes host **with perfect clocks**.
`expires_at_wall_ms` is exactly the discriminator, because the driver's pacing
floor puts renewal rounds at least half a lease apart, so a renewal always
stamps a strictly later expiry than the one it replaces. Both DST and the
runtime suite carry the fault (`X-ambiguity-b`, and the write-throttled fixture
in `groupnet-runtime/tests/external_faults.rs`).
Anything richer — a lock service with sessions, a lease API, a compare-and-swap
with a callback — would be a second protocol to write per backend. This one is
written **once**, and a consumer with etcd or ZooKeeper adapts *down* to it
rather than groupnet adapting up to each.

##### The decision rules live in core, so the sim and the driver run one copy

`groupnet-core/src/anchor.rs` is pure and clock-free: `AnchorRecord`,
`AnchorRecord::stealable`, `ClaimPlan`, `plan_claim`, `renewal_record`,
`ambiguous_applied`. It performs no I/O and reads no clock — `now_wall_ms`
arrives as an argument, exactly as shardstore's epoch module takes it — so every
branch (absent record, held by self, held by a live other, stealable at the exact
boundary, an epoch hint that dominates) is a table, not a scenario. The driver
supplies bytes and etags; the *verdicts* are this module's, which is what lets
the deterministic simulator drive the identical code an S3-backed driver does.
Same posture the lease tier's cores established, for the same reason.

##### Two clocks, and which one each number answers to

The tier touches two unrelated time bases, and conflating them is the mistake
this table exists to prevent:

| | Unit | Written by | Judged by | What it decides |
|---|---|---|---|---|
| `AnchorRecord::expires_at_wall_ms` | wall-clock ms, absolute | the holder, into the anchor record | **every claimant's own wall clock**, plus `steal_margin_ms` | when a *successor* may steal — the succession rule, subject to the skew assumption in the §3 footnote |
| `Command::AnchorActivated { lease_until }` | the engine's logical `Time` | the driver, from the engine's own time base | the engine's `on_tick` | when *this* node stops believing itself host (row 6's step-down) |

The engine never sees a wall-clock millisecond and the anchor record never sees
a `Time`. `lease_until` is fed **in** by the driver precisely so the sans-IO
rule holds; the engine does not derive it, because deriving it would mean
knowing how long the CAS took.

**`lease_until` is anchored at round *initiation*** — as built, both clocks are
sampled in `Claimant::round` *before* the load is issued, not after the CAS
returns, and an ambiguous write resolved by read-back takes its lease from the
same `t0` (the round trip was longer, not the authority). This is Quorum's
send-instant attribution argument in a different dress and it is conservative
for the same reason: the record's `expires_at_wall_ms` was computed from a
`now_wall_ms` sampled at or after that instant, so an engine lease anchored at
initiation always expires no later than the record it was earned from. Anchoring
after the round would hand the host an overhang past its own anchor record —
exactly the window a successor's steal is entitled to use.

**Where the anchor-latency term sits differs by layer, and both are
conservative.** The simulator stamps the record when the round *fires* at the
store (`started_at + latency`) while taking the engine lease from `started_at`,
so a sim record outlives the lease it granted by exactly one anchor latency —
the cushion is on the holder's side, and it is why the skew suite's arithmetic
reads `skew(successor) − skew(holder) > margin + anchor_latency`. The runtime
stamps both from the same pre-load instant, so its record and lease expire
together and the cushion is the **successor's** own round trip instead: a rival
cannot even observe the expiry until one `load` after it has passed. Same
inequality, the latency term simply belongs to a different party in each layer;
neither ever hands a holder time past its own record.

**The skew assumption is pairwise.** What enters `stealable` is the
disagreement between *these two* clocks — the holder stamped
`expires_at_wall_ms` from its own, the claimant judges it against its own — so
the condition is `|skew(claimant) − skew(holder)| ≤ steal_margin_ms` (plus the
anchor round trip). "Every node within `steal_margin_ms` of true time" is
strictly weaker and does **not** imply it: two nodes at opposite ends of that
band disagree by two margins. The DST's within-margin family draws per-node
offsets inside ±`margin/2` for exactly this reason, and its arithmetic note
states the boundary an overlap needs to cross as
`skew(successor) − skew(holder) > margin + anchor_latency`.

##### The command/effect surface

* **`Effect::AnchorClaimDue { epoch_hint }`** — the engine *prompting* the
  driver, never performing. Emitted only under `External`, only on the
  anti-entropy cadence, and only when **row 1's ordinary `ClaimGuard` opens**
  (not leaving, past the boot guard, top-ranked, not already the adopted host).
  It is a repeated prompt rather than a one-shot, because a lost prompt must
  self-heal; the driver **debounces** it against its own in-flight round. With
  no anchor configured a driver simply drops it, and the group never hosts —
  fail-safe, and the same shape as an empty voter roster.
* **`Command::AnchorActivated { epoch, lease_until }`** — the driver reporting a
  CAS it *won*. Row **X2** activates on it (reusing row 4's `activate`
  verbatim), row **X3** treats the same epoch while already `Host` as a lease
  extension (`lease_until = max`).
* **`Command::AnchorObserved { epoch, host }`** — the driver reporting a record
  it *read* and did not win. Row **X4** adopts it when it outranks the adopted
  pair in the existing `(epoch, host)` fencing order; row **X5** is row 12b
  verbatim when it names *this* node at a strictly higher epoch.
* **Row X6** is the gate on both: silently dropped outside `External`, and
  dropped below the monotone bars (`AnchorActivated` under `highest_seen`,
  `AnchorObserved` that does not outrank what is held). A command is driver
  input, so it is exactly where a stale, duplicated or misrouted report has to
  die. Two narrowings landed in M5's review, both on `AnchorActivated`:
  * **Leaving drops it.** Row 15 demotes *before* the leave disseminates,
    precisely so a node never serves an epoch it has announced it is gone from;
    an anchor round already in flight when `Command::Leave` landed would
    otherwise come back and re-activate it at the very epoch it just gave up.
    Row X1's guard already refuses to *prompt* while leaving; this is the same
    rule applied to a prompt issued earlier.
  * **The `>=` bar admits equality only where the adopted host at that epoch is
    hostless or ourselves** — the shapes row X5 (`(epoch, None)`) and row 6's
    lapse leave behind, and row X3's renewal. An equal epoch adopted for
    *another* node came through row X4, and activating over it would serve an
    epoch the anchor awarded elsewhere. One anchor cannot produce that report;
    a misrouted one can, which is what this row is for.
* **Row X7** is the host's renewal prompt: the same `AnchorClaimDue` on the same
  cadence, hinting the epoch it *already* holds rather than one above it,
  reached only past row 6's lapse check — and **rank-gated**, see the next
  subsection.

**`Role::Claimant` is never entered.** Under `External` there is no in-fabric
bid to stand: row **X1** replaces row 1's claim with a prompt, and no
`LeadClaim` or `LeadGrant` frame is ever built. The only election frame an
`External` group emits is `LeadState` — the activation broadcast and row 7's
repair beacon. That is **X-purity**, and it is pinned by a test asserting zero
claim/grant/persist effects across a whole run.

##### Renewal is rank-gated under every activation (row X7)

**Reviewed and changed during M5** (owner-approved): row X7's prompt is gated on
`is_coordinator()`, exactly as row Q7's renewal round is, and row 5's re-rank
renewal already was. The three rows are then one design — every activation's
renewal is rank-gated, and they differ only in the *evidence* that extends the
lease (this node's own view; a fresh majority of the roster; a fresh anchor
round), never in who is entitled to go looking for it.

What the ungated version did, and why it was wrong to ship: an incumbent that a
returning higher-ranked peer had outranked kept renewing indefinitely, while the
new rendezvous top prompted, read a live record, and yielded — **one store round
trip per anti-entropy interval, per pinned candidate, for as long as the
mismatch lasted**. Safe (the anchor allocates the epoch, and every yielded round
is fenced by it), and permanently wasteful. With the gate, the outranked
incumbent stops being prompted, its record ages out, its engine lease lapses on
row 6, and the top-ranked node supersedes it at a strictly higher epoch — so the
design's own sentence, *the host in the common case lands where the coordinator
ranking points*, survives churn instead of holding only until the first restart.

This is a **liveness and cost** rule, in the same class as row 6, and the DST
says so: the hand-back is a `Steal` like any other, cross-epoch and store-fenced,
and the sim measures it at a handful of yielded rounds rather than an unbounded
stream (`X-handback`). It is also what lets **L1-external** assert the settled
host is the rendezvous owner of the live set, and not merely whoever last held
the record.

**What it costs, stated plainly:** because renewal is rank-gated and candidacy
is too, an incumbent that is anchor-connected and serving perfectly well lapses
the moment a higher-ranked node returns — *even when that returning node has no
anchor access at all*, so nobody replaces it and the group is left **hostless
despite having a working, willing host**. That is the compound price of the two
gates together (`X-rank` × row X7), it is the CP posture being honest rather
than a bug, and it is pinned by `X-rank-compound` in
`election_external_failover.rs`, which asserts both halves: the incumbent
lapses, and the group stays hostless past the instant its record became
stealable until the top-ranked node's anchor heals.

There is still **no cooperative handoff** — `AnchorRecord` carries no successor
hint, and a record is superseded on its expiry or not at all. The lapse costs one
TTL plus the steal margin. Milestone 6 examined shortening it with a successor
hint and **dropped** the idea; the rank gates make it inert and the voluntary
path is already served by release-on-leave (see M6 as-built, "The two Milestone 6
conditionals, discharged").

##### The driver, as built: hold, leadership, and the third posture

The runtime half is one task per group (`anchor_task`), and three of its rules
are contract rather than implementation detail:

* **What a prompted round may do is decided by the hold and the published
  leadership *together*.** The **hold** (the epoch + etag this node's last
  winning write returned) is what blocks the claim path: a round ends by feeding
  `AnchorActivated` into a bounded inbox the group actor has not necessarily
  drained, so the leadership watch is routinely one hop behind the task, and a
  holder that fell through to `plan_claim` there would dutifully supersede its
  *own* record and burn an epoch per round. **Leadership** is what licenses the
  renewal: a node the engine has demoted must not renew, and above all must not
  re-report an activation, which would hand back a group it has just given up.
* **So there is a third posture, `Wait`.** A hold the engine is not currently
  showing does nothing at all this round — the right answer for both readings of
  it (the actor is one hop behind, and the next prompt resolves it; or a release
  edge is on its way, and claiming would undo it). **The wait is bounded by the
  record itself**: if the activation never reached the engine (a `try_send`
  dropped under load), the hold lapses when its record does and this node
  re-claims from scratch at a strictly higher epoch. One lease is the worst a
  lost report costs, and it costs it without a special case.
* **A renewal has a pacing floor of half a lease.** The engine's prompt is the
  *ceiling* on how often a renewal may happen; `expires_at_wall_ms − now >
  lease_ms / 2` is the floor on how late it may be left. That leaves a full half
  lease spare for a slow round trip, a dropped report or a retry, and stops a
  brisk gossip interval turning into a store write every few milliseconds.
* **The command channel back into the actor is a `WeakSender`, on purpose.** The
  group actor stops when its inbox closes — i.e. when every sender is dropped —
  and the actor itself owns this task's prompt channel. A strong sender here
  would make the pair mutually immortal: a killed node would keep gossiping *and
  keep renewing its anchor record*, which is the one leak this tier cannot
  afford. The task ends when the prompt sender drops, which happens when the
  actor returns; `feed` upgrades the weak sender per report and gives up quietly
  if the actor has already stopped.
* **`AnchorClaimDue` must never block the group actor**, which is the sharp
  inversion of `PersistGrant` (which blocks it *by contract* — a grant must not
  outrun its own durability). Here there is nothing to be ahead of: until the
  anchor answers, this node is not host, so making the actor wait would buy
  nothing and stall gossip. The **capacity-one prompt channel plus `try_send`
  *is* the debounce** the level-signal prompt requires of every driver.

##### Fail-closed, and why partitions stop being the axis

Anchor connectivity **is** the availability axis, and it replaces reachability
of peers:

* **Partition-irrelevance.** A host cut off from the entire fabric but still
  able to reach the anchor keeps renewing and keeps hosting — correctly, because
  the anchor is the only thing that can award the epoch, and nobody else can take
  it. Under `Quorum` that same node would lapse. Under `External` a partition is
  not a leadership event at all.
* **Fail-closed on anchor loss.** A node that cannot reach the anchor cannot
  renew, so its engine lease lapses on row 6 and it demotes. It does not keep
  serving on rank, which is exactly why row 5's renewal is gated to `Settle`
  (see the row-5 note below).
* **The rank-pinned-hostless shape.** Candidacy is rank-gated — row X1's guard
  is row 1's guard — so if the *top-ranked* node is the one that has lost the
  anchor, the group stays hostless even though a second-ranked node could reach
  it perfectly well. This is deliberate and it mirrors Quorum's stalled-candidate
  behaviour: a rank-driven candidate set is what keeps the election free of
  duelling timeouts, and the price is that a single node's connectivity can pin
  the group. An operator's signal is the anchor error at the driver, not
  anything in `NetStats`.

  **Compounded with the rank-gated renewal (row X7), it can also *take the
  group away* rather than merely fail to hand it over**: an outranked but
  anchor-connected incumbent stops being prompted and lapses even when the
  returning rendezvous top has no anchor access — so the group goes hostless
  while a perfectly working, willing host sits in it. Both gates are defensible
  alone and the composition is the actual price paid; `X-rank-compound` asserts
  it, so a change that widens the candidate set to buy it back has to come and
  edit that test.

##### Restart is re-winning, never resuming

There is no resume path and there will not be one. A restarted node comes back
`Follower` at epoch 0 with no memory of hostship; if it is still entitled it
prompts, and the driver **re-wins through the anchor** at a strictly higher
epoch. The old epoch is never picked back up, even though the record may still
name this node — the record naming us is *evidence of an epoch*, not a grant of
hostship, and it is consumed by row X5 exactly as row 12b consumes a
self-naming `LeadState`: the pair is taken with its hostship stripped off, as
`(epoch, None)`, and the node re-earns the group above it. Epoch monotonicity
across restarts is therefore free here — the anchor remembers, so the node
does not have to.

##### A leave releases; everything else lapses

`Group::leave` demotes on row 15 *before* the leave disseminates, and the
driver's leadership watch turns that host→not-host edge into a **release**: the
record is re-stamped already-expired, at the **same epoch** (a release decides
nothing, so it allocates nothing) and still conditional on this node's own etag,
so a successor that already superseded it cannot be clobbered by a late release.
A successor then claims after `steal_margin_ms` instead of waiting out a whole
TTL — pinned in the runtime suite against a budget it could not possibly meet
otherwise. The release is **best-effort and its failures are ignored on
purpose**: a node that cannot reach the anchor to release (usually *why* it
demoted) leaves the record to lapse, which is the same outcome one TTL later.
The edge also discards a prompt issued while this node was still host, since
replaying it would take the claim path and supersede the record the release just
gave up.

Everything else — a crash, an anchor outage, a lost rank, a deposing pair — is a
**lapse**, not a release, and costs the full TTL plus the margin. The
deterministic simulator deliberately models only the lapse path: it has no
release, so every succession there is the slow one and no property in the DST is
allowed to depend on a courtesy.

##### The X properties, as they landed

| Where | What it pins |
|---|---|
| `groupnet-core/src/anchor.rs` (unit) | the decision rules as tables: absent/held-by-self/live-other/stealable at the exact millisecond, the hint as a floor, saturating arithmetic, the ambiguous-write truth table — including the renewal that did **not** apply and must not be mistaken for the record it meant to replace |
| `groupnet-core/tests/election_external.rs` | the `X`-rows against a real engine, including the rank-gated X7, the fail-closed step-down, and the leave that no in-flight anchor round may undo; **X-purity** asserted over each run's *whole* effect stream |
| `groupnet-sim/tests/election_external.rs` | **X-S1** over 128 chaos seeds (crashes, amnesiac restarts, partitions, anchor outages, loss, reorder, arbitrary skew) — unconditional, with no storage anywhere; X-S2/S4b sampled after every round; **L1-external** (one host, it is the register's holder *and* the rendezvous owner of the live set) |
| `groupnet-sim/tests/election_external_failover.rs` | the shaped scenarios: **X-part** (partition-irrelevance, with the `Quorum` inversion on the identical schedule), **X-closed** (no anchor, no host), **X-rank** (the rank-pinned-hostless cost), **X-rank-compound** (that cost composed with the rank-gated renewal: a working host lapses and nobody replaces it), **X-handback**, and **X-budget** — 32 seeds against an itemized virtual-time budget with no fudge term |
| `groupnet-sim/tests/election_external_skew.rs` | **X-skew-a** (96 seeds, `hosts() ≤ 1` after *every scheduled event*), **X-skew-b** (64 seeds, each producing a real overlap: bounded by the excess, always cross-epoch, always resolved), **X-ambiguity-a** (64 seeds with a fifth to a half of writes applying and reporting `Unknown`), **X-ambiguity-b** (32 seeds of the store that swallows every write and still says `Unknown`: the lease lapses at exactly the instant the last landed round bought it, with perfect clocks and no overlap) |
| `groupnet-runtime/tests/external.rs` | the driver half over the async runtime and a real `Anchor`: elect, steal, the two inert postures, release-on-leave |
| `groupnet-runtime/tests/external_faults.rs` | the same fixture with the store broken: unreachable-anchor availability, the incumbent-only cut, ambiguous-write read-back, and the write-throttled store whose failed renewals must lapse the lease instead of extending it |

Every seeded family prints its own floors on success (round tallies, steal
counts, slack), so a schedule that has drifted *towards* its floor reports it
before it drifts past it.

##### Non-goals for this milestone

* **No handoff.** `AnchorRecord` carries no `handoff_to` successor hint, unlike
  shardstore's `WriterRecord`. Adding the field early would put an unexercised
  branch in the steal rule. *(Resolved in Milestone 6: the hint is **dropped**,
  not deferred — rank gates make a successor hint unable to change who claims,
  release-on-leave already shortens the voluntary path, and crash paths cannot
  cooperate. See "The two Milestone 6 conditionals, discharged" below.)*
* **No `GrantStore` analogue, and none is coming.** The anchor *is* the ledger.
  There is no `Effect::Persist*` under `External`, no recovery constructor, and
  no boot blackout — those exist under `Quorum` to stand in for a durable
  allocator, and here there is a real one.
* **Two hot-path lines touched, both of them rank conditions.** Row 5's
  condition moves from `is_coordinator() && !is_quorum()` to
  `is_coordinator() && is_settle()` — value-identical for `Settle` and `Quorum`,
  and what stops an `External` host renewing its engine lease off its own rank
  instead of off the anchor. Row X7's prompt gained the same
  `is_coordinator()` gate row Q7 already had (see above). The regression bar for
  both is that every pre-existing suite passes byte-unmodified.

#### Handoff, as built (Milestone 6)

Status: **Delivered (pending review).** What shipped, by layer:

* **`groupnet-transport-mem`**, feature `bulk` — `MemBulkNet` /
  `MemBulkTransport`: an in-process **data plane** of `tokio::io::duplex` pipes,
  the sibling of the control plane's `Network` one plane down. Connection-
  oriented, so connecting to an unknown id is an error rather than the silent
  drop the datagram plane owes. Without it nothing could exercise both planes in
  one process, and this milestone is the first thing that needs to.
* **`groupnet-consistency`**, feature `handoff` (facade
  **`consistency-handoff`**), module `hosted::handoff` — the sans-IO
  `HandoffCore` (three verdicts) and `HandoffPhase` (the order they must be
  taken in), the `GNHO/1` codec, and the `Handoff` shell: `offer`, `fetch` /
  `fetch_on`, `donors`, `seed`. The consumer supplies both ends of the *data*
  via `SnapshotSource` and `SnapshotSink`; this module never interprets a chunk.
* **Tests** — `handoff.rs` (the ordered exchange over a plain group),
  `handoff_fence.rs` (the two staleness re-verifications and `donors()` against
  real epochs), `handoff_resync.rs` (a late joiner beyond the ring),
  `handoff_migration.rs` (a recovering host completing through a transfer), the
  facade smoke `groupnet/tests/consistency_handoff.rs`, and the runnable example
  `crates/groupnet-consistency/examples/hosted_handoff.rs`.

The tier is **optional and additive**: `hosted` is complete without it, the
`handoff` feature is off in every build that does not ask for it, and it is the
only consistency feature whose dependency graph reaches the data plane.

##### It remediates a `Gap`, and it adds no cursor

The one API question worth stating plainly: a handoff introduces **no new read
position, no seek, no replay-from**. `HostedReads` already positioned the
subscriber when it emitted the `Gap` — the cursor sits at the first write the
ring still holds, and everything below it is what the `Gap` names. What the
laggard is missing is *state*, not *position*, and the transfer supplies exactly
that. Adding a cursor API would be adding a second way to be wrong about where a
subscriber is.

Two consequences fall straight out of that, and both are load-bearing:

* **The live stream resumes contiguously.** After the transfer, the next write
  the subscriber is handed is the one after its cursor. There is no second
  `Gap`, because nothing was ever missed a second time. `handoff_resync.rs`
  asserts the arriving tokens are contiguous *and* that the **total** gap count
  over the whole run is still exactly one — the arrival gap — rather than
  comparing against a snapshot taken after the transfer, which would say nothing
  about the transfer window itself.
* **Overlap is free.** A snapshot is a covering image, not a delta, so it
  routinely re-delivers state the requester already applied — and on a retried
  handoff that is the common case. It is safe because this crate's standing
  contract already requires an apply to be **idempotent**; the handoff adds no
  exception to it, and a sink that is not idempotent was broken for the session
  tier already.

The recovery rule is likewise untouched. `CompletenessCore::step` never learns a
handoff happened; it sees watermarks that moved. `Handoff::seed` is the whole
join: fold the receipt's `covers` into the `CommitLedger` and the `Frontier`,
`refresh` the ledger's stamp, and re-ask. `Completeness::Recovering { needed }`
is literally the `need` map a fetch is given — one names a debt, the other pays
it, with no translation step between them.

##### The stream protocol: `GNHO/1`, and why the terminator is in-band

One request/response exchange on one bulk stream, five frame kinds:

```text
requester                                  donor
  │── Request { group, name, need } ─────────▶│
  │◀────────── Offer { fence, covers } ───────│   or Refuse { code, have }
  │  (verify: staleness, then coverage)       │
  │◀────────── Chunk × n ─────────────────────│
  │◀────────── Done { chunks, bytes, fence } ─│
  │  (verify: counts, then the re-read fence) │
  │  sink.finish()                            │
```

Every frame opens `b"GNHO"`, version `1`, kind. The `(writer, token)` record map
inside `Request`, `Offer` and a `NotCovered` refusal is the **commit ledger's
map, byte for byte and by the same code** (`encode_records` / `decode_records`,
factored out of `encode_ledger` for exactly this): a watermark map that parsed
differently in two places would be a silent divergence between what a donor
claims to cover and what a ledger says a voter applied, and the whole design
turns on those being the same kind of number.

**`Done` is in-band because a clean EOF at a frame boundary is
indistinguishable from completion.** The framing layer reports a peer that died
mid-snapshot as `Ok(None)` — the same answer it gives for a peer that finished.
Nothing below the protocol can tell those apart, so the protocol carries its own
terminator and **silence is never success**: a stream that ends without a `Done`
is `HandoffError::Truncated`, and the sink is dropped. The counts in that frame
are checked for **equality**, not for a floor: a short count is the truncation
the name describes, and a long one is a re-framing or a duplicated chunk, which
is the same mistake pointed the other way.

Version and kind bytes are refused loudly rather than folded into a plausible
neighbour. `RefusalCode::Version` is the one code this version never *sends* —
answering an unreadable version in-band would mean encoding a reply in the
version the peer has just demonstrated it cannot read — and it exists so a future
version has a word for it and this one can decode it.

##### Three verification points, and what they prove

| Point | Check | Refusing means |
|---|---|---|
| the `Offer` arrives | `HandoffCore::staleness` on the donor's fence stamp, then `HandoffCore::coverage` of its `covers` against `need` | this donor's view is superseded, or it is behind what we need |
| the `Done` arrives | `HandoffCore::done_consistent` on the counts | what landed is not what was sent |
| immediately before `finish` | `staleness` again, on the **final** stamp in that `Done`, against a **freshly re-read** `leadership()` | the donor was deposed while it was still sending |

The third point is the one the design turns on and the reason there are two
fence checks rather than one. A snapshot takes real time to stream, and a donor
that was the serving host when it opened the image can be deposed while it is
still sending it. Both halves move — the donor stamps what *it* now believes into
the terminator, and the requester compares against what *it* now believes — so a
migration either side has learned about lands as `HandoffError::StaleDonor` with
nothing installed. Verifying only at the offer would adopt exactly the state a
surviving host has already ruled out.

**What all three prove is staleness, never freshness.** A `Stale` verdict is a
proof that this donor's state belongs to a superseded view. An `Ok` verdict is
the *absence* of such a proof, which is strictly weaker: two nodes inside one
stale partition agree perfectly and neither can tell. That is stated in the
module's honesty box and it is contract, not a caveat.

The residue is named, and it is **not new**. It is M4's drain-window divergence
verbatim: state applied under a view no surviving host will hold. Nothing
acknowledged is at risk — the commit ledger's view-stamp fence means such a write
was never committed — and the reconciliation is the standing one: the
authoritative `Gap` that opens the next lineage, and the consumer's own
remediation behind it. A handoff can move that divergence faster than gossip
would have; it cannot make it permanent. A consumer that treats the `Gap` as
advisory keeps the divergence with or without this module.

One failure this design **cannot** detect is the source over-claiming `covers`.
A torn image with an honest-looking `covers` map is indistinguishable from a good
one at this layer, because the chunks are opaque bytes. Take the `covers` reading
at or before the instant the image is fixed — one lock across both, a
copy-on-write handle, a store snapshot — and take it *after* nothing. That is the
`SnapshotSource` contract and the whole weight of the trait sits on it.

##### The hostless donor: the epoch is the fence, the name is weaker

A fence stamp is `(epoch, Option<host>)`, and the `None` is real: a node can have
adopted epoch `e` while still believing the group hostless at it — a follower
that saw the epoch bump before the leadership entry naming its winner, which is
one gossip round wide and entirely ordinary.

**A hostless-stamped donor at our own epoch answers `Staleness::Ok`.** The epoch
*is* the fence; the host name is strictly weaker information about the same
hostship, and lagging on the name is not lagging on the fence. Under the posture
S5 already presumes — an epoch is a unique name for one hostship — nothing the
donor applied at `e` can belong to a *later* hostship, because a later hostship
carries a higher epoch, which the epoch-major test catches on its own. The
dangerous direction is accepting a donor that is genuinely behind, and that
requires a *lower* epoch. Refusing the hostless donor would buy no safety and
would cost exactly the availability this module exists to restore: it would
refuse the most likely donor in the seconds after a migration, which is precisely
when a handoff is wanted. The mirror case — a donor naming a host at an epoch we
believe hostless — is the better-informed party, and is answered the same way.

The one same-epoch refusal is **two different named hosts at one epoch**. That is
not a lag, it is a contradiction — an epoch names one hostship — so one of the two
beliefs is already dead, and nothing is adopted.

The engine is **not** stuck on that case, and the handoff's refusal is stricter
than it has to be, deliberately. The fencing order is total over `(epoch, host)`
pairs: epoch-major, and at equal epochs the `placement::owner` of the group id
among the two hosts wins — a tiebreak that reads nothing but the group id and the
two host ids, so every node on either side of a heal picks the same survivor
without exchanging anything (`groupnet-core`'s `engine/election/mod.rs`). A
handoff could apply the same rule and accept a donor whose host is the one that
will win. It does not, because it is fail-closed and its refusals are transient:
the requester is about to adopt a whole image on the strength of this one verdict,
and "our two stamps contradict each other" is as much a statement about the
requester's view being unhealed as about the donor's. Waiting costs a retry — the
engine's own merge closes the disagreement, after which the two stamps agree and
the same donor is accepted — while adopting across it rests the check on a
tiebreak the requester has not yet seen its own engine apply. Taking it here is an
available *availability* win, not a correctness requirement, and it is not taken.

The window is narrow and **`Settle`-only** in any case: two same-epoch hostships
arise because each side of a partition counted the members it could see and
derived the same `highest_seen + 1`. A `Quorum` epoch is granted by a majority of
a static roster and an `External` one is a compare-and-swap on a real allocator;
neither can mint the pair.

##### The sink is taken by value, and `finish` consumes it

`fetch` / `fetch_on` take the `SnapshotSink` **by value**, and
`SnapshotSink::finish` takes `self`. That is not an ergonomics choice: the
contract is that **a dropped, unfinished sink discards**, and no caller still
holding a reference can be held to it. Every failure path — a refusal, either
staleness verdict, short coverage, a count mismatch, an out-of-order frame, an
I/O error, a sink error mid-stream — returns without finishing, and the driver
owns the sink, so returning drops it. A failed handoff therefore leaves the node
exactly as it was: still refusing service, still able to try another donor,
nothing half-adopted. Consumers stage to a side location and make `finish` the
swap; they do not stream into live state.

##### `donors()`: host first, and the follower-donor honesty

`Handoff::donors(need)` answers the members whose **published** commit ledgers
already cover `need`, best first: the group's **serving host**, then everyone
else in rendezvous order over the group id — the same deterministic ranking the
engine's own candidate order uses, so every requester agrees on who to ask second
without agreeing on anything. The coverage filter runs last, so a host that does
not cover is merely *absent* rather than promoted-then-dropped, and **the caller
is excluded by construction** — `Handoff::new` / `named` take this node's id
alongside the group (the `HostedWrites` / `HostedReads` convention) and `donors()`
drops it in the same `retain` as the short members.

That exclusion is load-bearing, not tidy. A caller left on its own list would
`connect` to its own endpoint, which no transport refuses: in-process the stream
lands on that node's own accept queue, and over TCP it is an ordinary loopback
connection. The failure is therefore not an error but a **silent hang** — the
fetch waits for an offer only this node's own accept loop could write, which is at
best a node streaming its state to itself and at worst (a serving loop that is
busy, absent, or draining an endpoint that has since been replaced) a wait with
nothing under it. "A node whose own reading covered `need` would not be asking" is
true of the ordinary case and is not a mechanism; the filter is.

It is a snapshot, stale the instant it is taken, which is why the answer is a list
and why every refusal names the next thing to try.

Host-first is the honesty box made operational. **A donor need not be the host** —
the host is often the busiest node in the group and often the very node that is
recovering — but a follower's applied state can include the previous host's
un-replicated tail, which is precisely the state no surviving host will hold. The
host's state is the one state definitionally survivable; a follower's is a
best-effort second choice, taken when the host cannot serve, and it inherits the
drain-window paragraph above rather than escaping it. `handoff_migration.rs` is
the case where the host *is* the requester, so the caught-up voter is next — a
follower donor, chosen knowingly.

##### Sizing: the transfer must outrun the writers

The requester's `need` is computed from a target that is itself moving: while a
snapshot streams, the group keeps writing. If the writers advance past what the
snapshot covers before it lands, the requester finishes, re-asks the recovery
rule, and is short again — and if that is *durably* true the handoff repeats
forever, transferring state and never catching up.

There is no clever fix and this module does not pretend to one. The transfer must
be able to outrun the write rate, which makes it a capacity decision — snapshot
size, link bandwidth, ring depth — of exactly the same kind as the ring-sizing
decision it replaces. **Size the ring for the worst migration lag you accept, or
size the snapshot to land inside it.** A caller that wants the failure to be loud
bounds its own retries and surfaces the stall.

##### The two Milestone 6 conditionals, discharged

Two conditionals pointed at this milestone, from two different places. Section 6's
Milestone 6 line carried **one** "if" clause of its own — *plus host-scoped
registers if fence tokens prove insufficient for docres locks*. The other was left
here by **M5's as-built non-goals**, which deferred the anchor's `handoff_to`
successor hint on the grounds that cooperative handoff was Milestone 6's business.
Both are now answered, and neither is being built.

* **Host-scoped registers: DEFERRED.** The clause was "*plus host-scoped
  registers if fence tokens prove insufficient for docres locks*". They are
  sufficient. The two runnable examples — `fenced_ownership` (Quorum) and
  `anchored_ownership` (External) — **are** the docres lock shape end to end: an
  ownership record committed through the host's serialized feed, then written to
  the store under the fence that authorized it, with the store refusing the
  deposed writer. Nothing in that path wants a register: the record lives where
  truth already lives (the consumer's store), and a groupnet-side register would
  be a second copy of it under weaker durability. No consumer has pulled for one
  — docres/shardstore's stated needs (Section 2) are membership, TTL'd entries,
  placement and the fence, and s3cache does not use Hosted mode at all. Deferred
  rather than rejected: **revisit only against a concrete consumer** with a
  requirement the fence token demonstrably cannot carry.
* **The M5 anchor's `handoff_to` successor hint: DROPPED.** The External
  as-built subsection flagged cooperative handoff as "Milestone 6's business"
  and left `AnchorRecord` without a successor hint. It stays without one, for
  three reasons that compound:
  * **A hint cannot change who claims.** Renewal and claiming are *rank-gated*
    under every activation (the M5 as-built row X7 / Q7 change): the top-ranked
    live member is the one that bids. A departing host naming a successor could
    only either agree with the rank order — in which case the hint is
    decoration — or disagree with it, in which case the hint is ignored and the
    field is an unexercised branch in the steal rule. That was the original
    reason for leaving it out and it did not weaken.
  * **The voluntary path is already short.** A voluntary leave **releases** the
    anchor record rather than letting it lapse, so the cooperative case — the
    one a hint would serve — already costs a claim round instead of a TTL plus
    the steal margin. The remaining lapse cost is paid only by endings that are
    *not* cooperative.
  * **Crash paths cannot cooperate by construction.** A host that dies does not
    write a hint, and those are exactly the endings where succession latency
    hurts. A mechanism that helps only the case already handled, and cannot help
    the case that is not, is not worth a wire field.

  What Milestone 6 *did* deliver against the same underlying complaint is the
  half that generalizes: state transfer, so a successor's slowness is a capacity
  question rather than an unbounded wait on a ring.

#### Consumer mapping

* **docres document ownership/locking** = Hosted mode per shard group with
  `Activation::External`: a "lock" is an ownership record written through the
  host's serialized feed, carried with the fence token. This is the
  *guaranteed-and-fast* quadrant of the dial — fencing amortizes onto
  storage I/O docres already pays — and a lift of the pattern shardstore's
  `caslog/epoch.rs` already implements by hand; the ambition is that
  shardstore could eventually shed that bespoke code.
* **s3cache** does not use Hosted mode at all; it is served by T2 plus the
  API gaps in Section 7.
* **p2p-game-style consumers** use `Settle` + `Commit::Local` — the lobby
  semantics the mode was named for. A small session that must never lose
  acked state (or a small cache cluster wanting real strong) steps up to
  `Quorum` + `Commit::QuorumApplied`.

### M4 — the general replicated-log machine: out of scope

First, the honest concession (owner asked directly: "isn't our leader
election just Raft?"): **the strong profile is consensus.** Epochs are
terms; one-grant-per-epoch-per-voter with majority activation *is* Raft's
vote rule; the safety of both rests on the same theorem (two majorities
intersect). Once a group runs `Quorum` × `Commit::QuorumApplied`, groupnet
contains in-fabric consensus, and no wording should pretend otherwise — the
README's identity line softens to "leaderless **by default**", with
consensus opt-in per group.

What is *not* Raft, and why the distinction is substance rather than
branding:

| | Strong profile | Raft |
|---|---|---|
| Terms/epochs | epochs, monotone | terms — same role |
| Candidate | deterministic (rendezvous top-ranked live) | any server, randomized timeouts |
| Votes | ≤ 1 grant / epoch / voter, majority | same rule |
| Vote durability | persisted grant when the driver has storage; restart-blackout fallback | persisted `votedFor`, mandatory |
| Leader completeness | **recovery after winning**: fetch newest committed state from the heard majority (Viewstamped-Replication-style view change) | **election restriction**: stale candidates are refused votes |
| Replication substrate | the existing session feed (bounded ring); a laggard gaps and state-resyncs | append-entries log, log matching, backtracking repair |
| Compaction | none needed — state is the artifact, there is no unbounded log | snapshots + InstallSnapshot |
| Membership change | none — static voter roster, changed by redeploy | joint consensus |
| Scope | fixed single-digit rosters | general |

So the accurate boundary is not "no Raft" but: **consensus comes in
(opt-in, small static rosters, VR-style view change over the existing feed);
the general replicated-log machine stays out** — log repair, compaction,
snapshot install, dynamic reconfiguration, client sessions. Those are the
second product this library refuses to become. Neither consumer needs it
(docres: ownership/serialization via M3-External; s3cache: coherence via
T2/T3), and the fence token keeps an external CP store composable for
anyone who does. Revisit only against a concrete consumer.

No other speculative modes: causal broadcast and multi-writer CRDT registers
were considered and dropped for lack of consumer pull.

## 4. Architecture and placement

**Decided: election lives inside the engine** — a new
`groupnet-core/src/engine/election.rs`, sibling to `liveness.rs`/
`anti_entropy.rs`/`merge.rs`, active only when the group's mode is Hosted.
Rationale: the sim drives engines only, so anything outside the engine is
invisible to DST — and DST must own election correctness; fencing lives in
the merge path, which is engine-internal; the election consumes SWIM state
and rendezvous ranking already resident in the engine; zero new dependencies.

| Layer | Change |
|---|---|
| `groupnet-core` | `engine/election.rs`; `Config.mode: GroupMode` (`Eventual` default / `Hosted(HostedConfig)`); wire kinds `KIND_LEAD_CLAIM=8`, `KIND_LEAD_GRANT=9`, `KIND_LEAD_STATE=10` inside `FRAME_VERSION 3`; `Effect::LeadershipChanged { epoch, host }`; epoch-major merge rule for host-scoped state |
| `groupnet-sim` | dispatch the new effect (a `leadership_log` mirroring `coordinator_log`), accessors, and a deterministic in-sim CAS register modeling the external anchor |
| `groupnet-runtime` | `GroupEvent::LeadershipChanged`, `Group::leadership()`, `Node::join_group_with(name, GroupProfile)`; effect plumbing in `driver.rs` |
| `groupnet-consistency` | feature `hosted` (following the `acks` pattern): `HostedWrites` — a `WriteFeed` whose epoch *is* the leadership epoch; fence surfacing; commit levels composing with T2 |
| `groupnet` facade | feature `hosted` → `consistency` layering, mirroring `consistency-acks` |

**Wire compatibility:** new frame kinds only — an unknown kind decodes to
`None` and is dropped, so v3 stays v3. A mixed cluster degrades to "no host
electable until enough nodes upgrade": Quorum mode fails safe automatically
(no majority of grants); Settle mode simply never settles cluster-wide until
the upgrade completes (documented). Digest bodies are not touched (that would
force v4).

**API sketch** (consumer's view):

```rust
// Mode selection, per group:
let group = node.join_group_with("docs-shard-7", GroupProfile::hosted(
    HostedConfig {
        activation: Activation::Quorum { voters: roster },
        lease_ms: 2_000,
    },
));

// Observing leadership (watch-shaped, like coordinator):
let lead = group.leadership();   // Leadership { epoch, host: Option<NodeId>, role }

// Fenced write path (groupnet-consistency, feature "hosted"):
let hosted = HostedWrites::new(group.clone(), codec);
match hosted.publish(&op).await {
    Ok(token)                 => { /* WriteToken { epoch = leadership epoch, seq } */ }
    Err(NotHost { host, .. }) => { /* redirect to `host` */ }
    Err(Deposed { epoch })    => { /* fenced out mid-write */ }
}
let fence: Fence = hosted.fence()?;  // stamp data-plane ops / external CAS
```

Followers consume the host's feed through the existing
`PeerWrites`/`Frontier`/`AckLedger` machinery unchanged.

## 5. Testing strategy

The deterministic simulator is the backbone. DST suites in the existing style
(seeded `SplitMix64`, hundreds of seeds, scripted crash/restart/partition/
heal schedules) assert the safety and liveness properties:

* **S1** — no two nodes ever activate as host of the same *fencing pair*
  (true by construction — a pair names its activator; the DST-falsifiable
  content is the interval fold: no node re-activates a pair it has left);
  strict same-*epoch* uniqueness holds on partition-free runs, and returns
  globally under Quorum activation (M3) **given voter durability** — a voter
  that restarts amnesiac may grant one epoch twice at disjoint times, which
  costs S1-strict and not S4c (see the M3 as-built property matrix) — see the
  fencing-pair finding in the election design above.
* **S2** — a node's observed epoch never regresses, and its adopted pair
  never regresses in the fencing order (modulo its own self-demotion step,
  which is `(e, Some(self)) → (e, None)` by design).
* **S3** (Quorum) — a side without a voter majority never activates; voter
  crash-restart seeds prove the grant blackout suffices for S4c in the
  storage-free posture, and the persisted-ledger seeds prove the durable one.
  As built, what makes S4c *global* is the voter's grant promise (refuse new
  claimants for `lease_ms` after any grant) together with send-instant lease
  attribution, not the lease-vs-detection sizing — see M3 as-built.
* **S4** — lease disjointness in virtual time, decomposed honestly for
  Settle mode: **S4a** per fencing pair, only its named host ever holds a
  lease, over at most one contiguous activation interval; **S4b** no node
  is ever `Host` on an expired lease; **S4c** at most one unexpired lease
  per group at any instant on partition-free runs (across a partition, two
  unexpired leases may coexist only under *distinct* fencing pairs — the
  documented Settle split-brain, fenced at heal). Quorum (M3) strengthens
  S4c to global — including the storage-free posture, since an amnesiac
  voter's boot blackout covers exactly the promise it forgot.
* **S5** (with `Commit::QuorumApplied`) — no write acknowledged at the
  quorum-applied level is ever lost, across every crash/partition/migration
  schedule: after any activation, the new host's state contains every such
  write (leader completeness).
* **L1** — after heal + settle, exactly one host; all agree on
  `(host, epoch)`; it is the rendezvous top-ranked live member.
* **X** (External, M5) — the anchor tier's own properties. All four are DST
  properties now; see the M5 as-built subsection for which suite pins each.
  * **X-S1** — *absolute epoch uniqueness*: no two nodes ever hold the same
    epoch, at any instant or at disjoint times, across any partition, and with
    **no node-local storage**. Strictly stronger than S1-strict under Quorum,
    which is storage-conditional — the anchor allocates the epoch, so there is
    nothing for a restart to forget.
  * **X-purity** — an `External` group builds **zero** `LeadClaim` and
    `LeadGrant` frames and emits **zero** `PersistGrant` effects over any
    schedule, and `Role::Claimant` is never entered. Its only election frame is
    `LeadState`. Pinned at the core layer by asserting on the whole effect
    stream of a run, not on a sampled tick.
  * **X-part** — *partition-irrelevance*: a host that retains anchor access
    keeps hosting however the fabric is cut, and a node that loses anchor access
    demotes on lease lapse however well-connected its peers are. Anchor
    connectivity replaces peer reachability as the availability axis.
  * **X-skew**, the honesty pair. **X-skew-a**: while every *pair* of clocks
    disagrees by at most `steal_margin_ms`, no two hosts' leases overlap at any
    instant (S4c under this tier) — and this is the *only* External property
    that consults a clock at all. **X-skew-b**: when that assumption is
    violated, the overlap is
    bounded by the excess, is **always cross-epoch** (a successor's epoch is
    strictly higher by construction), and is **always fenced** at the store —
    so a broken clock costs succession timing and never X-S1.

Plus: codec round-trip tests for the new frames (testkit `frames` fixtures),
mem-transport end-to-end (elect → kill host → observe migration as a `Gap`),
and mixed-version compat tests (old node drops the new kinds).

**Handoff (M6)** carries no DST family of its own, and deliberately: its three
verdicts and its phase table are pure functions unit-tested exhaustively beside
the code (the 6×6 table is asserted cell by cell), and everything above them is
I/O over two planes, which is an integration concern rather than a scheduling
one. Four suites split by harness family, the house pattern:

| Suite | What it earns |
|---|---|
| `groupnet-consistency/tests/handoff.rs` | the ordered exchange over a plain (hostless) group: a covering snapshot crossing whole and seeding the receiver, an early refusal that reads no byte of the donor's image, a mid-stream death installing nothing, and `is_request` demuxing a shared plane |
| `groupnet-consistency/tests/handoff_fence.rs` | the fence-sensitive half, against real epochs: refusal at the **offer** for a donor behind us, refusal at the **terminator** for one deposed mid-stream, refusal at the terminator again for a donor whose *own* leadership moved while it streamed (the re-stamp, seen from the donor's side), and `donors()` over real gossiped ledgers — host first, short members out, and the caller out even when its own reading covers |
| `groupnet-consistency/tests/handoff_resync.rs` | the laggard, over one steady hostship: a late joiner past the ring gets one `Gap`, fetches from the host `donors()` names, seeds, and **resumes contiguously with no second `Gap`** |
| `groupnet-consistency/tests/handoff_migration.rs` | the ring-bound recovery, walked out of: an heir beyond the ring sits in `Recovering`, fetches from the caught-up voter (`donors()` excludes the requester itself), seeds, and the latch fires — then a `QuorumApplied` round resolves behind it |

`groupnet/tests/consistency_handoff.rs` is the facade smoke: the tier's
vocabulary reachable through `groupnet::consistency::hosted::handoff`, and one
real transfer over `groupnet::transport::mem`'s **both** planes — which is what
fails if the manifest's `groupnet-transport-mem?/bulk` edge is ever removed.

## 6. Milestones

Build order of record (resolved 2026-08-05, decision D-order): prove each
safety rung under DST before the next stands on it, and serve shipping
consumers earliest. Settle is built first not because it is the priority but
because it is the smallest activation that exercises the entire
epoch/fencing/lease skeleton; Quorum and External land on the proven result.

* **Milestone 0 — consumer-pulled API (starts immediately).** The four
  items of Section 7: detector-timing introspection, capability
  advertisement, fencing-verdict roster, externally-typed sequence floors.
  Small, independently valuable to shipping consumers, no election
  dependency. Item 4 is the largest; if design shows it needs its own
  slice, it splits out rather than delaying the other three.
* **Milestone 1 — election skeleton with Settle activation.**
  `election.rs`: epochs, `LeadClaim`/`LeadGrant`/`LeadState` frames + codec
  round-trip tests, the epoch-major fencing merge rule, leases +
  self-demotion, `Effect::LeadershipChanged`, `Config.mode`, `Settle`
  activation; sim dispatch + DST (S1, S2, S4, L1, fenced-split-brain heal
  seeds); runtime surfacing (`Group::leadership()`, `join_group_with`,
  `GroupEvent::LeadershipChanged`). Excluded: voting, write path, anchors.
* **Milestone 2 — coherence-lease tier (T3).** Reader serve-leases over the
  freshly DST-proven lease machinery; writer invalidation blocks on
  responsive lease-holders or lease lapse; the successor to s3cache's
  unanimity-ack strong mode. Milestone 0's items 1–2 are part of its
  contract.
* **Milestone 3 — Quorum activation. Delivered (pending review).** Static
  voter roster, one grant per epoch per voter, the grant promise, send-instant
  lease attribution, persisted grants over a runtime `GrantStore` (with the
  restart-blackout fallback retained as the storage-free posture); DST S3
  including voter crash-restart seeds. The minority freeze is *structural*
  here — a minority side simply never activates — but has no `NoLeader` error
  to report until M4 gives the hosted write path one; see the M3 as-built
  subsection for what it looks like through `leadership()` today.
* **Milestone 4 — hosted write path + commit levels. Delivered (pending
  review).** `HostedWrites` in `groupnet-consistency` behind feature `hosted`
  (`consistency-hosted` on the facade); fence surfacing;
  `Local` / `QuorumApplied` / `AllApplied` with the leader-completeness
  recovery step gating activation when `QuorumApplied` is in force (DST
  property S5); `NotHost`/`Deposed`; a runnable fenced-ownership example
  (the docres shape:
  `cargo run -p groupnet-consistency --example fenced_ownership --features
  hosted`). **The strong profile is complete at the end of this
  milestone.** As built, "gating activation" means gating *hosted service*
  and the commit ledger is epoch-stamped rather than a literal reuse of the
  ack tier — see the M4 as-built subsection above for the two rules, the
  intersection argument, the deployment contract they impose (including the
  serving host's own lineage cut), the latched recovery verdict, and the two
  reviewed deviations that are now contract.
* **Milestone 5 — external-CAS anchor. Delivered (pending review).**
  `Activation::External` with a driver-side `Anchor` trait (runtime layer, never
  core); the engine consumes anchor outcomes as commands; the sim models the
  anchor as a deterministic CAS register with orthogonal knobs for store
  reachability, per-node wall-clock skew and ambiguous writes. The
  shardstore-pattern lift — docres's guaranteed-and-fast quadrant. A runnable
  anchored-ownership example (the docres shape with the election replaced:
  `cargo run -p groupnet-consistency --example anchored_ownership --features
  hosted`). As built, renewal is rank-gated under *every* activation (row X7
  gained row Q7's `is_coordinator()` gate, reviewed and owner-approved during
  the milestone), the driver decides claim-versus-renew from the hold and the
  published leadership together with a bounded `Wait` for the third case, and a
  voluntary leave *releases* the record while every other ending lapses — see
  the M5 as-built subsection above for those and for the shape the `X`
  properties landed in.
* **Milestone 6 (optional) — snapshot handoff. Delivered (pending review).**
  The handoff helper over `BulkTransport`: `hosted::handoff` behind feature
  `handoff` (`consistency-handoff` on the facade) — three sans-IO verdicts and
  the phase table that orders them, the `GNHO/1` stream protocol with its own
  in-band terminator, `donors()` / `fetch()` / `offer()` / `seed()`, and the
  `SnapshotSource` / `SnapshotSink` pair through which the consumer supplies
  both ends of the data. Plus the in-process data plane it is exercised over
  (`groupnet-transport-mem`'s `bulk` feature) and a runnable late-joiner example
  (`cargo run -p groupnet-consistency --example hosted_handoff --features
  handoff`). It is a **`Gap` remediator and adds no cursor API**: the `Gap`
  already positioned the subscriber, the transfer supplies the state behind it,
  and idempotent re-apply absorbs the overlap. Both conditionals aimed at this
  milestone are discharged in the M6 as-built subsection — this line's own "if"
  clause, **host-scoped registers DEFERRED** (fence tokens are sufficient; the two
  ownership examples are the docres lock shape, and no consumer has pulled for a
  register — revisit only against a concrete one), and the one M5's as-built
  non-goals left here, **the anchor's `handoff_to` hint DROPPED** (rank
  gates make a successor hint unable to change who claims, release-on-leave
  already shortens the voluntary path, and crash paths cannot cooperate).

## 7. Consumer-pulled adjacent work (independent of Hosted mode)

Cheap, concrete, and directly "retains docres/s3cache support". **This is
Milestone 0** (decision D-api):

1. **Detector-timing introspection** — expose the effective probe/suspect
   timings on the built `Node`/`Group` so s3cache stops reading
   `Config::default()` and its 404-trust window stays honest under
   configuration drift.
2. **Capability advertisement in membership** — let a node advertise (e.g.)
   ack participation so strong-mode writers exclude non-acking peers instead
   of eating timeouts in mixed deployments.
3. **Fencing-verdict roster** — formalize the "gossip-dead for N ⇒ fence
   verdict; gossip-healthy for N ⇒ unfence verdict" contract shardstore
   hand-rolls over `members()` (the durable act stays with the consumer's
   CAS log; groupnet supplies the verdict, liveness-only).
4. **Externally-typed sequence numbers** — let the session tier carry
   consumer-typed sequence floors (shard LSNs) with TTL'd dissemination and
   a fallback-when-unknown posture, so shardstore's hand-rolled hot-set
   could migrate onto `groupnet-consistency`.

## 8. Decisions

All resolved (owner, 2026-08-05). This ledger is final; reopen an entry only
with a new owner decision.

* **D-place:** election implemented as an engine module in `groupnet-core`.
* **D-process:** this document was reviewed and accepted before code; it is
  the contract of record for the work.
* **D-dial:** the product shape is the dial in Section 3 — derived
  coordinator always present and never authoritative; elected host opt-in
  per group; guarantee level = activation × commit level; costs as tabled
  ("you pay for a guarantee where the guarantee lives"), with docres in the
  guaranteed-and-fast External quadrant and s3cache on the coherence tiers,
  leaderless.
* **D-name** (formerly O4): the elected role is **Hosted / host**
  (`GroupMode::Hosted`, `Group::leadership()`, `HostedConfig`,
  `Activation`). `Leader`/`Primary` rejected to keep distance from the
  untouched derived coordinator.
* **D-boundary** (formerly O3): consensus is **opt-in per group**; the
  general replicated-log machine (log repair, compaction, dynamic
  reconfiguration, client sessions) is **never** in scope; the README
  identity line softens to "leaderless by default".
* **D-order** (formerly O1; supersedes the sequencing half of the original
  D-strong): the owner directed "the most proper and correct approach" —
  **skeleton-first**. Epochs/fencing/leases are proven under DST with the
  smallest activation (Settle, Milestone 1) before consensus stands on them
  (Quorum, Milestone 3) and before the anchor (External, Milestone 5). The
  commitment to Quorum itself is unchanged.
* **D-lease** (formerly O5): the coherence-lease tier T3 is **adopted**,
  sequenced as Milestone 2 — the most consumer-pulled piece of the design.
* **D-api** (formerly O2): the four consumer-pulled items land first, as
  **Milestone 0, starting immediately**.
* **D-strong** (historical): Quorum activation is committed; its original
  "day one" sequencing is superseded by D-order.
