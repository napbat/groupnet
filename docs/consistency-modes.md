# Consistency modes

Status: **proposal — awaiting owner sign-off.** Nothing here is implemented.
Decisions taken so far and decisions still open are listed at the end.

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
This tier **is** the s3cache invalidation-coherence story; no new mode needed
there.

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
  persistent-vote problem). The core stays storage-free; mitigation is a
  **post-restart grant blackout** — a freshly booted voter refuses to grant
  for ≥ `lease_ms`, converting durability into a timing rule DST can prove
  sufficient in logical time. Drivers that do have durable storage may use an
  optional recovered-state constructor instead.
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

#### Consumer mapping

* **docres document ownership/locking** = Hosted mode per shard group with
  `Activation::External`: a "lock" is an ownership record written through the
  host's serialized feed, carried with the fence token. This is a lift of the
  pattern shardstore's `caslog/epoch.rs` already implements by hand — the
  ambition is that shardstore could eventually shed that bespoke code.
* **s3cache** does not use Hosted mode at all; it is served by T2 plus the
  API gaps in Section 7.
* **p2p-game-style consumers** use `Settle` — the lobby semantics the mode
  was named for.

### M4 — Quorum-replicated log (Raft-shaped): out of scope

Explicitly not on the roadmap. (a) It is a second product — log compaction,
snapshot install, joint consensus, client sessions — inside a thin library
whose identity is "without Raft, Paxos, or global consensus". (b) Neither
consumer needs a replicated state machine: docres needs ownership/
serialization (M3-External), s3cache needs coherence (T2). (c) The fence
token deliberately makes an external CP store composable when a consumer
truly needs one. One README sentence will draw this boundary. Revisit only
against a concrete consumer.

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
* **L1** — after heal + settle, exactly one host; all agree on
  `(host, epoch)`; it is the rendezvous top-ranked live member.

Plus: codec round-trip tests for the new frames (testkit `frames` fixtures),
mem-transport end-to-end (elect → kill host → observe migration as a `Gap`),
and mixed-version compat tests (old node drops the new kinds).

## 6. Milestones

Per the owner's decision, Quorum activation ships in the first slice — which
pulls leases and the grant blackout forward (they are what makes Quorum mean
anything):

* **Milestone 1 — epoch-fenced election with Quorum activation.**
  `election.rs` (epochs, claims/grants, `LeadState` repair), leases +
  self-demotion, post-restart grant blackout, `Effect::LeadershipChanged`,
  `Config.mode`, wire kinds + codec tests, sim dispatch + DST suite
  (S1–S4, L1), runtime surfacing (`leadership()`, `join_group_with`,
  `GroupEvent`). Excluded: write path, Settle, External, handoff.
* **Milestone 2 — Settle activation** (the lobby policy) + fenced-split-brain
  DST seeds (heal ⇒ one surviving epoch, loser surfaced).
* **Milestone 3 — hosted write path.** `HostedWrites` in
  `groupnet-consistency` behind `hosted`; fence surfacing; commit levels
  (`Local` / `Applied` via T2); `NotHost`/`Deposed`; integration tests +
  a runnable fenced-ownership example (the docres shape).
* **Milestone 4 — external-CAS anchor.** `Activation::External` with an
  `Anchor` trait (driver-side, async — runtime layer, never core); the
  engine consumes anchor outcomes as commands; sim models the anchor as a
  deterministic CAS register. This is the shardstore-pattern lift.
* **Milestone 5 (optional) — handoff helper** over `BulkTransport`, plus
  host-scoped registers if fence tokens prove insufficient for docres locks.

## 7. Consumer-pulled adjacent work (independent of Hosted mode)

Cheap, concrete, and directly "retains docres/s3cache support" — can land
before or alongside Milestone 1:

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

Taken (owner, 2026-08-05):

* **D-strong:** Quorum activation from day one (Milestone 1 includes
  `Activation::Quorum`, leases, blackout).
* **D-place:** election implemented as an engine module in `groupnet-core`.
* **D-process:** this document is reviewed and signed off before any code.

Open — for the owner:

* **O1 — CP-anchor emphasis.** The consumer review found neither docres nor
  s3cache needs in-fabric quorum: docres's strong path is external-CAS
  fencing (shardstore's shipped design), s3cache needs the acks tier. Quorum
  mode therefore serves the *generic base-library* ambition (and p2p-game
  consumers wanting CP without an external store), not the current
  consumers. Keep Quorum in Milestone 1 as decided, or resequence
  (External first, Quorum second)? **Recommendation: keep the decided order
  only if the generic-library goal outweighs time-to-value for docres;
  otherwise swap Milestones — the skeleton is identical either way, so
  nothing is thrown away.**
* **O2 — Section 7 scope.** Land the four consumer-pulled items as a
  pre-milestone (small, immediately useful to shipping consumers), fold them
  into Milestone 1, or defer?
* **O3 — Raft boundary.** Confirm M4-Raft stays explicitly out of scope
  (one README sentence drawing the line).
* **O4 — naming.** `Hosted` / `host` / `GroupProfile` / `Activation` — happy
  with these names, or prefer e.g. `Leader`/`leader`?
