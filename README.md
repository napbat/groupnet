# Groupnet

A **deterministic, leaderless coordination fabric** for distributed systems that
partition state into shard groups. Groupnet is an *engine*, not a database: it
gives you group-local membership, an implicit (derived, non-elected)
coordinator, inter-group routing awareness, and shard-scoped operations —
without Raft, Paxos, or global consensus. You bring the storage and the wire.

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

| Crate | Deps | Role |
|-------|------|------|
| [`groupnet-core`](crates/groupnet-core) | none | sans-IO state machine: engine, ids, wire codec, coordinator selection |
| [`groupnet-transport`](crates/groupnet-transport) | core | the `Transport` trait you bind (TCP/UDP/IPC/shmem) |
| [`groupnet-runtime`](crates/groupnet-runtime) | core, transport, tokio | async, group-per-task `Node`/`Group` driver + in-memory transport |
| [`groupnet-sim`](crates/groupnet-sim) | core | deterministic single-threaded simulator (virtual clock + lossy in-memory net) |
| [`groupnet`](crates/groupnet) | facade | umbrella re-export; `runtime` on by default, `sim` opt-in |

The same core runs under both drivers: `groupnet-runtime` across threads in
production, `groupnet-sim` in a single-threaded, reproducible event loop for
tests.

## Example

```rust
use groupnet::{Node, NodeId};
use groupnet::mem::Network;

let net = Network::new(); // any Transport impl works here
let node = Node::builder(NodeId::new("node-a"), net.endpoint(NodeId::new("node-a")))
    .seed(NodeId::new("node-b"))
    .spawn();

let group = node.join_group("shard-42");

if group.is_coordinator() {
    group.sync(|ctx| ctx.update_metadata("routing", "v3"));
}
```

Binding your own transport is one trait:

```rust
pub trait Transport: Send + Sync + 'static {
    type Error: std::error::Error + Send + Sync + 'static;
    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error>;
    async fn recv(&self) -> Result<Inbound, Self::Error>;
}
```

## Coordinator selection

The coordinator is *derived*, never elected: every node scores each **live**
member (Alive or Suspect — never Dead) with rendezvous (highest-random-weight)
hashing over `hash(group ‖ node)` and the highest score wins. This spreads
coordinator load evenly across groups and stays stable under churn; when a node
dies or leaves it drops out of candidacy and the coordinator moves
deterministically. The hash is a fixed FNV-1a so all nodes agree on every
platform (`std`'s `DefaultHasher` is deliberately *not* stable and must never be
used for cross-node agreement).

The coordinator is **non-authoritative** — no write-ahead log, no quorum, no
commit. During a partition two nodes may briefly compute different coordinators;
because a coordinator can't do anything binding, that's harmless.

## Build & test

```bash
cargo test --workspace        # unit + deterministic sim + async end-to-end
cargo clippy --workspace --all-targets -- -D warnings
cargo build -p groupnet --no-default-features   # core + transport only, no tokio
```

## Roadmap

Scaffolded and honest about what's stubbed:

- ~~**Metadata dissemination.**~~ *Done.* Gossip carries `(key, version, writer,
  value)` deltas; every node merges them as a per-key last-writer-wins register
  (`(version, writer)` tiebreak), so `sync`/`update_metadata` converges
  cluster-wide. Reads are a lock-free snapshot via `Group::metadata`.
- ~~**Real membership.**~~ *Done.* SWIM-style membership: per-node incarnation
  numbers, an `Alive`/`Suspect`/`Dead` state machine, direct liveness probes
  (`Ping`/`Ack`), a suspicion window with self-refutation, and a real `leave`
  that sticks. Crashes and departures drop nodes from the live set and from
  coordinator candidacy. Read via `Group::members()`.
- **Indirect probes (`ping-req`).** Failure detection is direct-only today, so a
  lossy link can cause false positives (a wrongly-suspected node refutes, but
  it's churn). SWIM's k indirect probers before declaring suspicion are the next
  refinement.
- **Dead-node reaping.** `Dead` entries are kept as tombstones and never GC'd;
  production needs a bounded tombstone lifetime.
- **Inter-group routing map.** The cluster-wide "which group owns which
  key-range" table (read-mostly, snapshot-published) is designed but not yet
  built.
- **Bulk transport.** An opt-in `BulkTransport: Transport` capability for
  stream-shaped anti-entropy / state transfer, kept off the datagram hot path.
- **Real transports.** TCP/UDP/IPC bindings alongside the in-memory one.

## License

MIT OR Apache-2.0
