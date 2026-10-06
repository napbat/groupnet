# Routing diagrams

[Documentation index](README.md) · [Previous: registration and lifecycle](link-lifecycle.md) · [Next: tunnels and security](tunnels-and-security.md)

The router builds a network from adjacent-peer links. A destination is a logical
`NodeId`; a selected route contains its next hop, link, cost, and path. Route
discovery is asynchronous and separate from group membership convergence.

## 1. Bridging heterogeneous links

A node needs only its own links and explicitly configured neighbors. It does not
need the physical address of every destination. A bridge can forward between
different protocols or between neighbors on the same adapter.

```mermaid
flowchart LR
    subgraph Local["Local machine"]
        A["Node A\nIPC only"]
        B["Node B\nIPC + TCP bridge"]
        A <-->|"IPC frames"| B
    end
    subgraph Remote["Another machine"]
        C["Node C\nTCP + memory bridge"]
        D["Node D\nMemory only"]
        C <-->|"In-process frames"| D
    end
    B <-->|"TCP frames"| C
    A -.->|"Logical destination D; routed through B and C"| D
```

Forwarding is enabled by default; there is no separate bridge mode to enable.
`RouterConfig { forwarding: false, .. }` makes an endpoint stop advertising
learned transit routes and stop forwarding transit packets. It can still send
and receive its own traffic. This flag is **not** an application access policy.

## 2. Learning a path

For an advertisement, the link must admit the sending neighbor. The path must
start with that neighbor, exclude the local node, and fit the hop bound. The
router adds the link cost and retains a timestamped candidate within its capacity
limits. The example shows eventual propagation, not one synchronous transaction.

```mermaid
sequenceDiagram
    participant A as Router A
    participant B as Router B
    participant C as Router C
    C->>B: Advertise path [C], cost 0
    B->>B: Validate peer and path, add B-to-C link cost
    B->>B: Retain candidate path [B, C]
    Note over A,C: A later announcement tick exports selected transit routes
    B->>A: Advertise path [B, C] and accumulated cost
    A->>A: Reject loops or invalid bounds, add A-to-B link cost
    A->>A: Retain candidate [A, B, C], next hop B
    A->>B: Packet addressed to C
    B->>C: Forward with decremented hop budget
    Note over A,C: Link removal or route expiry invalidates affected candidates
```

Advertisements are not proof of cryptographic endpoint identity. Route inspection
through `route_to` reports local routing state, not a guarantee that the next
packet will be delivered. A path can disappear immediately afterward.

## 3. Receiving and forwarding a packet

The diagram combines the worker's physical-frame check with the routing actor's
processing. Incomplete fragments wait for more data within bounded reassembly;
invalid frames are dropped. Advertisements and data then take separate paths.

```mermaid
flowchart TD
    Receive["Receive physical frame"] --> MTU{"Within link MTU?"}
    MTU -->|"No"| Drop["Drop"]
    MTU -->|"Yes"| Peer{"Source admitted on this link?"}
    Peer -->|"No"| Drop
    Peer -->|"Yes"| Reassemble["Bounded fragment reassembly"]
    Reassemble --> Decode{"Complete valid routed frame?"}
    Decode -->|"Invalid"| Drop
    Decode -->|"Incomplete"| Pending["Wait for remaining fragments"]
    Decode -->|"Yes"| Kind{"Frame kind"}
    Kind -->|"Advertisement"| Learn["Validate path and cost; update route candidates"]
    Kind -->|"Data"| Replay{"Duplicate source and packet ID?"}
    Replay -->|"Yes"| Drop
    Replay -->|"No"| Local{"Destination is local?"}
    Local -->|"Yes"| Plane{"Payload kind"}
    Plane -->|"Message"| Messages["Bounded control-message queue"]
    Plane -->|"Tunnel"| Tunnels["Bounded tunnel-packet queue"]
    Local -->|"No"| Transit{"Forwarding enabled and hop budget remains?"}
    Transit -->|"No"| Drop
    Transit -->|"Yes"| Route{"Live route available?"}
    Route -->|"No"| Drop
    Route -->|"Yes"| Queue["Decrement hop budget; queue selected next hop"]
```

Routing remains best-effort: queue saturation, a missing route, or a transport
failure can discard a frame. A successful `Transport::send` is not an
acknowledgement of remote delivery. The outgoing scheduler handles announcements,
link MTUs, fragmentation, and deadlines. Reliability for application streams is
provided by the separate [tunnel layer](tunnels-and-security.md), not by turning
all gossip into a reliable stream.

## Implementation references

- [Routes, link registration, and configuration](../crates/groupnet-network/src/router.rs)
- [Learning, replay suppression, and forwarding](../crates/groupnet-network/src/router/routing.rs)
- [Outgoing scheduling and fragmentation](../crates/groupnet-network/src/router/adapters.rs)
- [Incoming worker bounds](../crates/groupnet-transport/src/link/worker.rs)
