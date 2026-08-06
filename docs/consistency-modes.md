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
| `External` (CAS-anchored lease) | whoever wins the external conditional write is host; partition sides are irrelevant | CP; consensus outsourced to the anchor | none, absolutely (the anchor is linearizable) |
| `Settle { claim_settle_ms }` (lobby-style) | each side elects its own host after the settle window | AP + serialization per side | yes — bounded and **fenced**: at heal exactly one epoch survives; the loser surfaces as a `Gap` + demotion event for the app to reconcile |

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
  migration as the epoch `Gap`. A later milestone can add snapshot handoff
  over the existing `BulkTransport` data plane.

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
* **State transfer is not in this milestone.** Snapshot handoff over the
  `BulkTransport` data plane is Section 6's Milestone 6; until it exists, `Gap`
  plus the consumer's remediation is the whole story — which is what the
  strong-profile-versus-Raft table's "a laggard gaps and state-resyncs" row
  means in practice.

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

Plus: codec round-trip tests for the new frames (testkit `frames` fixtures),
mem-transport end-to-end (elect → kill host → observe migration as a `Gap`),
and mixed-version compat tests (old node drops the new kinds).

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
* **Milestone 5 — external-CAS anchor.** `Activation::External` with a
  driver-side `Anchor` trait (runtime layer, never core); the engine
  consumes anchor outcomes as commands; the sim models the anchor as a
  deterministic CAS register. The shardstore-pattern lift — docres's
  guaranteed-and-fast quadrant.
* **Milestone 6 (optional) — handoff helper** over `BulkTransport`, plus
  host-scoped registers if fence tokens prove insufficient for docres locks.

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
