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

Status: **adopted** (decision D-lease) — sequenced as Milestone 2,
immediately after the election skeleton proves the lease machinery under
DST.

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
| `Quorum { voters }` | only a side holding a majority of the **static voter roster** activates a host; the minority side fails hosted writes fast (`NoLeader`) while base-fabric gossip continues | CP for the hosted domain | none outside the lease window (lease expiry + bounded clock-*rate* skew) |
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
  `LeadGrant` (Quorum: at most one grant per epoch per voter; Settle: grant
  if claimant is the peer's top-ranked live candidate and the epoch is
  higher). Current `(epoch, host)` rides a small `LeadState` frame on the
  anti-entropy cadence for repair.
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
  on nothing at all).
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

* **S1** — no two nodes ever activate as host of the same epoch.
* **S2** — a node's observed epoch never regresses.
* **S3** (Quorum) — a side without a voter majority never activates; voter
  crash-restart seeds prove the grant blackout suffices.
* **S4** — lease disjointness in virtual time: no instant with two unexpired
  leases for one group.
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
* **Milestone 3 — Quorum activation.** Static voter roster, one grant per
  epoch per voter, persisted grants (restart-blackout fallback), minority
  freeze (`NoLeader`); DST S3 including voter crash-restart seeds.
* **Milestone 4 — hosted write path + commit levels.** `HostedWrites` in
  `groupnet-consistency` behind feature `hosted`; fence surfacing;
  `Local` / `QuorumApplied` / `AllApplied` with the leader-completeness
  recovery step gating activation when `QuorumApplied` is in force (DST
  property S5); `NotHost`/`Deposed`; a runnable fenced-ownership example
  (the docres shape). **The strong profile is complete at the end of this
  milestone.**
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
