# Groupnet

A **deterministic, leaderless-by-default coordination fabric** for distributed
systems that partition state into shard groups. Groupnet is an *engine*, not a
database: it gives you group-local membership, an implicit (derived,
non-elected) coordinator, inter-group routing awareness, and shard-scoped
operations. Consensus is **opt-in per group** — the Hosted mode's Quorum
profile, epoch-fenced and majority-committed — while the general replicated-log
machine (log repair, compaction, reconfiguration) is deliberately absent. You
bring the storage and the wire.

That opt-in is a dial, set per group: eventual metadata for free at one end, an
elected epoch-fenced host paying a majority round-trip per write at the other,
and the session and coherence tiers in between. Each rung, its price, and what
it deliberately does *not* promise are in
[`docs/consistency-modes.md`](docs/consistency-modes.md).

> Status: early scaffold. The architecture and public API are in place with a
> working gossip/coordinator core; several protocol pieces are stubbed and
> marked `TODO` (see [Roadmap](#roadmap)).

## Design in one breath

The coordination protocol is a **sans-IO state machine**: it never touches the
network or the clock. It consumes events and returns effects.

```text
  on_message(from, wire) ─┐
  on_tick(now)           ─┼──▶  GroupEngine  ──▶  Vec<Effect>
  apply(command)         ─┘        (pure)         (Send / ArmTimer / ...)
```

Because the core is pure, *how* it runs is a driver's choice — and determinism
is **structural**, not conventional: there is no clock to read and no socket to
touch, so a driver cannot accidentally make it non-deterministic.

* **Sync core, async I/O.** The engine is synchronous (nanosecond state
  transitions); only the I/O layer is `async`. This is the `quinn`/`rustls`
  split.
* **Single-writer per group, not single-threaded.** Each group is an
  independent actor. A node hosting N groups runs N engines across every core
  with no lock on the hot path — the shard-per-core model.
* **Best-effort, message-oriented transport.** Gossip tolerates loss, reorder,
  and duplication, which is what makes UDP / IPC / shared-memory bindable.
* **Thin.** The core and the transport trait have **zero** dependencies; only
  the async runtime layer pulls in `tokio` (a narrow feature slice).

## Workspace layout

Groupnet separates two traffic classes — a **control plane** (small best-effort
datagrams: gossip, membership, coordinator, routing) and an opt-in **data plane**
(reliable byte streams: replication, bulk state transfer). They have opposite
requirements, so they're separate traits bound to separate physical connections.

| Crate | Plane | Deps | Role |
|-------|-------|------|------|
| [`groupnet-core`](crates/groupnet-core) | — | none | sans-IO state machine: engine, ids, wire codec, coordinator selection |
| [`groupnet-transport`](crates/groupnet-transport) | both | core *(+bulk feature: futures-io, bytes, zerocopy)* | the transport **traits**: `Transport` (datagram, always, dep-free) + `bulk::BulkTransport` (stream, feature `bulk`) |
| [`groupnet-transport-mem`](crates/groupnet-transport-mem) | control | transport, core, tokio(sync) | in-process binding (tests, examples, single-process) |
| [`groupnet-transport-udp`](crates/groupnet-transport-udp) | control | transport, core, tokio(net) | UDP binding over real sockets |
| [`groupnet-transport-tcp`](crates/groupnet-transport-tcp) | data | transport(bulk), core, tokio(net) | TCP stream binding |
| [`groupnet-runtime`](crates/groupnet-runtime) | — | core, transport, tokio | **transport-agnostic** async `Node`/`Group` driver + routing table |
| [`groupnet-consistency`](crates/groupnet-consistency) | — | core, runtime, tokio(sync) | session-consistency layer: per-writer sequenced write feeds (loss & restarts surface as explicit gaps) + read-your-writes frontiers |
| [`groupnet-sim`](crates/groupnet-sim) | — | core | deterministic simulator (virtual clock + lossy/partitioned net) |
| [`groupnet`](crates/groupnet) | — | facade | umbrella re-export; `runtime`+`mem` default, `udp`/`tcp`/`sim` opt-in |
| [`groupnet-testkit`](crates/groupnet-testkit) | — | core *(+cluster feature: runtime, transport-mem, tokio)* | shared test support: sans-IO frame fixtures + an async multi-node harness. Internal, `publish = false`, dev-dependency only |

The runtime is generic over `T: Transport` and never depends on a concrete
binding — you pick a transport crate (or write your own impl) and the driver is
none the wiser. Every concrete transport lives in its own `groupnet-transport-*`
crate so each pulls only the I/O deps it needs, and the control plane stays
dependency-free.

The same core runs under both drivers: `groupnet-runtime` across threads in
production, `groupnet-sim` in a single-threaded, reproducible event loop for
tests.

Most consumers pull the single `groupnet` facade, which mirrors each layer as a
module — `groupnet::core`, `groupnet::transport` (with the `mem` / `udp` / `tcp` /
`bulk` bindings nested under it), `groupnet::runtime`, and `groupnet::sim` — so
you write `groupnet::transport::Transport`, never the underlying crate name.

## Example

Runnable examples live in [`crates/groupnet/examples`](crates/groupnet/examples):

```bash
cargo run --example placement   # weighted HA-hash placement (sync, no I/O)
cargo run --example cluster     # 3-node convergence, derived coordinator, metadata
cargo run --example routing     # resolve a resource to its owner from any node
```

Four more live with the layers they exercise:

```bash
cargo run -p groupnet-sim --example partition              # partition -> detect -> heal -> rejoin, bit-for-bit reproducible
cargo run -p groupnet-consistency --example read_your_writes   # write feed + applied-frontier barrier, and a gap surfacing
cargo run -p groupnet-consistency --example fenced_ownership --features hosted   # elected host, majority-committed claims, and a store refusing a fenced-out writer
cargo run -p groupnet-consistency --example anchored_ownership --features hosted   # the same claims, with the epoch won by one conditional PUT against a CAS object
```

```rust
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::mem::Network;

let net = Network::new(); // any Transport impl works here
let node = Node::builder(NodeId::new("node-a"), net.endpoint(NodeId::new("node-a")))
    .seed(NodeId::new("node-b"))
    .spawn();

let group = node.join_group("shard-42");

if group.is_coordinator() {
    group.sync(|ctx| ctx.update_metadata("routing", "v3"));
}
```

Swap the in-memory transport for real UDP sockets without touching anything else
— just bind a different `Transport`:

```rust
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::udp::UdpTransport; // enable feature "udp"

let transport = UdpTransport::bind(NodeId::new("node-a"), "0.0.0.0:7000").await?;
transport.register_peer(NodeId::new("node-b"), "10.0.0.2:7000".parse()?);
let node = Node::builder(NodeId::new("node-a"), transport).seed(NodeId::new("node-b")).spawn();
```

Binding your own transport is one trait — implement it and the runtime works
unchanged:

```rust
pub trait Transport: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&self) -> Result<Inbound, Self::Error>;
}
```

## Data plane (streams)

For app-level payloads — replicating a write, streaming a shard snapshot to a
fresh replica — the control-plane datagram API is the wrong shape. Bind a
**data-plane** transport (`BulkTransport`) and move `Bytes` over reliable,
ordered, backpressured streams. Framing is length-delimited with a
[`zerocopy`](https://crates.io/crates/zerocopy)-parsed header (typed, no copy, no
`unsafe` — the whole workspace is `#![forbid(unsafe_code)]`), and payloads stay
as [`Bytes`](https://crates.io/crates/bytes) slices end-to-end:

```rust
use groupnet::core::NodeId;
use groupnet::transport::tcp::TcpTransport; // feature "tcp"
use groupnet::transport::bulk::DataPlane;
use bytes::Bytes;

let tcp = TcpTransport::bind(NodeId::new("node-a"), "0.0.0.0:8000").await?;
tcp.register_peer(NodeId::new("node-b"), "10.0.0.2:8000".parse()?);
let data = DataPlane::new(tcp);

// sender: pick the peer via the control plane's routing, then stream to it
let mut s = data.connect(&NodeId::new("node-b")).await?;
s.send(Bytes::from(snapshot)).await?;   // multi-MB, zero payload copies

// receiver:
let (from, mut s) = data.accept().await?;
while let Some(frame) = s.recv().await? { /* apply */ }
```

The data plane is a separate handle from `Node`, bound to its own socket — so you
gossip over UDP and replicate over TCP, independently. The control-plane
coordination core is untouched by any of it.

## Inter-group routing

Any node can resolve a resource to the node that owns it, without global
consensus. Each group's coordinator identity and each key-range's owning group
are gossiped as an eventually-consistent, cluster-wide table (itself just LWW
metadata in a reserved system group every node joins):

```rust
use groupnet::core::{GroupId, NodeId};

// The coordinator of the group that owns "users" claims the range:
node.routing().claim("users", &GroupId::new("shard-1"));

// From *any* node in the cluster:
let owner: Option<GroupId> = node.routing().owner("users");        // -> shard-1
let target: Option<NodeId> = node.routing().route("users");        // -> shard-1's coordinator
```

## Coordinator selection

The coordinator is *derived*, never elected: every node scores each **live**
member (Alive or Suspect — never Dead) with rendezvous (highest-random-weight)
hashing over `hash(group ‖ node)` and the highest score wins. This spreads
coordinator load evenly across groups and stays stable under churn; when a node
dies or leaves it drops out of candidacy and the coordinator moves
deterministically. The hash is a fixed FNV-1a with a splitmix64 finalizer —
integer-only, no floats — so all nodes agree byte-for-byte on every platform
(`std`'s `DefaultHasher` is deliberately *not* stable and must never be used for
cross-node agreement).

The coordinator is **non-authoritative** — no write-ahead log, no quorum, no
commit. During a partition two nodes may briefly compute different coordinators;
because a coordinator can't do anything binding, that's harmless.

## Scaling envelope

Groupnet is a **full-membership** fabric: every node knows every member, which
is exactly what weighted placement and routing rely on. That contract sets the
envelope — know where you are in it:

| Members per fabric | Verdict |
|---|---|
| ≤ ~1,000 | Comfortable at default cadences. |
| ~1,000 – ~10,000 | Works; per-peer **delta digests** (default) keep steady-state rounds proportional to churn, not membership. Tune `full_digest_every` up and cadences down as you grow. |
| beyond ~10,000 | Don't grow the fabric — **shard it into cells** and put the cell directory in an external store with conditional writes (CAS). Partial-view membership would break the full-membership contract placement depends on, so it is deliberately not on the table. |

The load-bearing facts:

* **Digests are the O(N) term.** A *full* digest lists every member (~40
  bytes each). Per-peer delta digests (`full_digest_every`, default 4) list
  only members whose summary changed since the last digest built for that
  peer, so a quiet cluster's rounds cost near zero and a busy one's cost
  tracks churn. The periodic full digest is the repair bound for anything a
  dropped frame or TTL drift left divergent.
* **Watch it, don't guess.** `Group::net_stats()` exposes digests built,
  full digests, summaries listed, delta/request frames, and anti-entropy
  bytes. If `digest_summaries_listed / digests_built` tracks your membership
  size instead of your churn, the fabric has outgrown its configuration.
* **Metadata registers ride every digest.** Keep the register set (routing
  table, coordinator keys) small; per-node keyed *entries* are the scalable
  bulk channel, registers are not.
* **Probing is O(1) per node** — failure detection is never the wall.
* Groups within a fabric are cheap and independent: the intended shape for a
  big system is many small shard groups (replica sets) plus one membership
  fabric per cell, with an authoritative store owning cross-cell placement.
  Gossip carries liveness and coherence signals; stores own truth.

If a single fabric ever genuinely needs to go past this envelope, the known
escalation is Merkle-style state comparison for the full-sync path — an open
roadmap item, deliberately unbuilt until a real deployment demands it.

## Build & test

```bash
cargo test --workspace        # unit + deterministic sim + async e2e + real UDP
cargo clippy --workspace --all-targets -- -D warnings
cargo bench -p groupnet-core  # wire codec + placement, at 5/50/500 members
cargo build -p groupnet --no-default-features   # core + transport trait only, no tokio
```

## Roadmap

What's done, and what's honestly still stubbed:

- ~~**Metadata dissemination.**~~ *Done.* Per-key last-writer-wins register
  (`(version, writer)` tiebreak), so `sync`/`update_metadata` converges
  cluster-wide. Reads: lock-free snapshot via `Group::metadata`.
- ~~**Real membership.**~~ *Done.* SWIM-style: per-node incarnation numbers, an
  `Alive`/`Suspect`/`Dead` state machine, a suspicion window with
  self-refutation, and a real `leave` that sticks. Read via `Group::members()`.
- ~~**Indirect probes (`ping-req`).**~~ *Done.* A direct-probe miss enlists *k*
  indirect probers before suspecting, so a dropped packet or one-way link no
  longer falsely kills a healthy node.
- ~~**Dead-node reaping.**~~ *Done.* Tombstones are gossiped for `dead_timeout`,
  then stop being re-advertised, then reaped at `2×` — so removal converges
  without peers re-teaching each other.
- ~~**Inter-group routing map.**~~ *Done.* Cluster-wide `resource → owning group`
  and `group → coordinator`, resolvable from any node via `Node::routing()`.
- ~~**Real transports.**~~ *Done.* Concrete bindings are their own
  `groupnet-transport-*` crates; control-plane `-mem`/`-udp` and data-plane
  `-tcp` ship. IPC/shmem/QUIC are the same one-trait exercise.
- ~~**Bulk / data-plane transport.**~~ *Done.* A separate stream-shaped
  `BulkTransport` (its own crate, `futures-io` + `bytes` + `zerocopy`), off the
  datagram hot path, for replication and bulk state transfer. Verified streaming
  multi-MB payloads over real TCP.
- **Data-plane maturity.** The stream transport moves opaque `Bytes`; a store on
  top still needs replication protocol, snapshot/anti-entropy over it, and its
  own `zerocopy` record layouts.
- **Dynamic address discovery.** Both socket bindings use a static
  `NodeId → addr` book; production would gossip or resolve addresses.

## License

MIT OR Apache-2.0
