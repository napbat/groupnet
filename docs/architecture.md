# Architecture diagrams

[Documentation index](README.md) · [Next: registration and lifecycle](link-lifecycle.md)

These diagrams describe the implemented managed-network architecture. Optional
consistency modes have their own [contract](consistency-modes.md); the diagrams
below do not imply that ordinary coordination groups provide consensus or access
control.

## 1. From application to physical links

A non-generic `Node` owns coordination and its routed network. Applications
address logical `NodeId` values, not a particular TCP connection or IPC pipe.
The router selects an adjacent peer and link for each destination.

```mermaid
flowchart TB
    App["Application"]
    subgraph Managed["Managed Node — shared lifetime across node clones"]
        Groups["Coordination groups\nMembership and metadata"]
        Tunnels["Optional TLS tunnels\nBulkTransport streams"]
        Router["Router\nDiscovery, path selection, forwarding"]
        Links["Registered link workers\nBounded physical-frame I/O"]
        Groups -->|"Best-effort messages"| Router
        Tunnels -->|"Reliable tunnel packets"| Router
        Router --> Links
    end
    App -->|"join_group"| Groups
    App -->|"connect / accept"| Tunnels
    Links <-->|"One-hop frames"| IPC["IPC peer"]
    Links <-->|"One-hop frames"| TCP["TCP peer"]
    Links <-->|"One-hop frames"| UDP["UDP / punch peer"]
    Links <-->|"In-process frames"| Mem["Memory peer"]
```

Routing is intrinsic to every runtime node. Applications and tests use
`Node::builder(id).link(provider).start().await`; all protocol choices produce
the same `Node` type. `NetworkConfig::bind` starts a standalone routed `Network`
without the node's membership actors.

## 2. Crate dependency boundaries

Arrows mean **depends on**, not packet flow. This is the relevant production
subgraph, not an exhaustive workspace/dependency listing. Runtime always depends
on the network layer; disabling the facade's default features keeps the core-only surface.

```mermaid
flowchart TB
    Facade["groupnet\nFeature-selected facade"]
    Runtime["groupnet-runtime\nNode and Group actors"]
    Network["groupnet-network\nRouting and TLS tunnels"]
    Protocols["Protocol implementation crates\nTCP, UDP, memory, IPC, punch"]
    Shared["groupnet-transport\nTransport + optional bulk and link contracts"]
    Core["groupnet-core\nSans-IO coordination"]
    Sim["groupnet-sim\nDeterministic simulation"]
    Facade --> Runtime
    Facade --> Network
    Facade --> Protocols
    Runtime --> Network
    Runtime --> Shared
    Runtime --> Core
    Network --> Shared
    Network --> Core
    Protocols --> Shared
    Protocols --> Core
    Shared --> Core
    Sim --> Core
```

There is deliberately **no network-to-protocol dependency**. A protocol implements
`LinkProvider` in its own crate; an application registers it without editing a
router enum or match. IPC and punching are independent implementation crates,
not modules owned by the network layer. `groupnet-core` never depends on Tokio,
a clock, or a transport implementation.

## 3. One pure engine, two execution environments

Both drivers supply inputs to the same synchronous engine. The real runtime
executes effects with timers and I/O; the simulator executes them against its
virtual clock and simulated network. Neither driver moves sockets into the core.

```mermaid
flowchart LR
    subgraph Inputs["Inputs supplied by a driver"]
        Commands["Application commands"]
        Messages["Incoming messages"]
        Ticks["Explicit time / ticks"]
    end
    Engine["GroupEngine\nPure state transitions"]
    Effects["Effects\nMessages, timers, notifications"]
    Runtime["Tokio runtime driver\nReal timers and transports"]
    Simulator["Deterministic simulator\nVirtual time, loss, partitions"]
    Commands --> Engine
    Messages --> Engine
    Ticks --> Engine
    Engine --> Effects
    Effects -->|"Production execution"| Runtime
    Effects -->|"Simulation execution"| Simulator
    Runtime -.->|"Next inputs"| Inputs
    Simulator -.->|"Next inputs"| Inputs
```

A derived coordinator is a convergent coordination choice, not authority to
commit writes. Epoch-fenced authority belongs to the explicitly selected Hosted
mode described in the consistency contract.

## Coordination model

A group is a logical shard/partition coordination scope, not a security group.
Each node can join multiple groups; each group actor owns its own engine and
serializes its local state transitions. The base Eventual mode combines:

- SWIM-style direct and indirect probes, suspicion/refutation, and dead-member
  tombstone reaping. Membership is observer-local during failure and partition.
- Digest/delta anti-entropy for membership, TTL'd per-node keyed entries, and
  last-writer-wins metadata registers.
- A derived coordinator selected with stable rendezvous hashing over the group
  and live member IDs (Alive or Suspect, not Dead). Equal converged views compute
  the same coordinator; different partition views can compute different ones.

The derived coordinator neither serializes nor commits application writes.
Session feeds, applied acknowledgements, coherence leases, and Hosted
epoch-fenced authority are explicitly selected
[consistency tiers](consistency-modes.md), not implicit properties of gossip.
Groupnet does not provide a general replicated log or database.

### Metadata routing versus packet routing

Two APIs use the word routing:

| API | What it resolves | Source of the local view |
|---|---|---|
| `Node::routing()` | Resource to owning group, group to coordinator | LWW metadata in the reserved coordination group |
| `node.router().route_to(&peer)` | Logical peer to physical next hop and link | Adjacent-peer path advertisements |

Neither lookup is an authorization decision or a globally authoritative
ownership transaction. An application can use the metadata map to choose a peer,
then send through the physical router. Both views can be incomplete or stale
while converging; protected ownership still needs the appropriate authority.

### Local operation batching is not a barrier

`Group::sync` runs a closure that stages commands and enqueues them into the
bounded group inbox. It is fire-and-forget and best-effort under backpressure;
it does not await cluster-wide application, make a distributed transaction, or
establish a read-your-writes barrier. Use the relevant consistency tier when
those stronger guarantees are required.

## Implementation references

- [Workspace architecture and feature selection](../README.md#workspace-layout)
- [Managed node initialization](../crates/groupnet-runtime/src/node/builder.rs)
- [Shared registration contracts](../crates/groupnet-transport/src/link.rs)
- [Network ownership and standalone binding](../crates/groupnet-network/src/config.rs)
- [Group engine membership and anti-entropy](../crates/groupnet-core/src/engine/state.rs)
- [Metadata-based inter-group routing](../crates/groupnet-runtime/src/routing.rs)
- [Group operation batching](../crates/groupnet-runtime/src/group.rs)
