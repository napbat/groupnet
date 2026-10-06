# **Groupnet: Technical Overview**

Groupnet is a **deterministic, leaderless coordination fabric** designed for distributed systems that partition state into shard groups. It provides **group‑local membership**, **implicit coordinator selection**, **inter‑group awareness**, and **synchronized shard‑scoped operations** without relying on consensus protocols such as Raft or Paxos.

Groupnet enables distributed databases, search engines, vector stores, and time‑series systems to implement shard‑level orchestration without embedding bespoke coordination logic or maintaining global consensus.

Source-backed replay and native snapshots are opt-in through the
`consistency-replication` feature. Its `with_event_complete` extension protects
a stable named subscriber's native suffix at the source, applies sink effects
durably before advancing the source acknowledgement, and uses a persistent
per-name fence across process replacement. Local detach preserves source
retention; explicit unsubscribe, finite source expiry, and `ResetAt` use a
durable terminal tombstone. The source and sink supply their native durability
and conditional transactions; Groupnet supplies the shared sans-IO session
schedule. See [the subscription contract](replication-subscriptions.md) for
the guarantees and implementation status.

---

## **1. System Model**

### **1.1 Groups**
A *group* is a logical set of nodes responsible for a shard or partition.  
Each group maintains:

- a convergent membership view  
- a deterministic coordinator  
- shard‑local metadata  
- versioning state  
- routing information  
- **knowledge of other groups for cross‑shard routing**  

Groups operate independently for coordination but maintain **global awareness** for routing.

### **1.2 Inter‑Group Awareness**
Groups maintain a lightweight, cluster‑wide map describing:

- which group owns which resource or key‑range  
- coordinator identities for each group  
- routing metadata required to forward requests to the correct owner  

This enables any node to route a request to the correct group without global consensus.

### **1.3 Nodes**
Nodes participate in one or more groups.  
Each node maintains:

- local membership state  
- local metadata cache  
- coordinator selection logic  
- inter‑group routing tables  
- transport bindings (RPC)  

Nodes do not maintain logs or term histories.

---

## **2. Coordinator Model**

Groupnet uses **implicit coordinator selection**, not leader election.

### **2.1 Deterministic Selection**
The coordinator is chosen using a deterministic rule, such as:

- lowest node ID  
- highest priority  
- stable hash ordering  

All nodes converge on the same coordinator without voting.

### **2.2 Non‑authoritative Coordinator**
The coordinator:

- does **not** own a write‑ahead log  
- does **not** enforce global ordering  
- does **not** commit entries  
- does **not** require quorum agreement  

It acts as a **lightweight orchestrator** for shard‑local operations.

---

## **3. Membership Management**

Groupnet maintains group membership using **gossip‑based dissemination**.

### **3.1 Convergence**
Nodes exchange membership deltas until all replicas converge on:

- the same membership set  
- the same coordinator  
- the same shard metadata  
- the same inter‑group routing map  

### **3.2 Failure Handling**
Failures are detected via:

- heartbeat timeouts  
- gossip suspicion  
- transport‑level disconnects  

Membership changes propagate deterministically.

---

## **4. Synchronization Model**

Groupnet provides **synchronized shard‑local operations** without consensus.

### **4.1 Operation Context**
Operations run inside a *synchronization context*:

```rust
group.sync(|ctx| {
    ctx.update_metadata("routing", "v3");
});
```

## **5. Multi-transport routing and tunnels**

Managed nodes always route over their registered links. `groupnet-transport-router`
has no production dependency on a concrete protocol implementation. A peer may
attach only IPC, another only network transports, and a bridge both. Adapters
establish one-hop communication; routing learns bounded path-vector routes and
forwards packets across those links.
Forwarding is enabled by default between configured peers, across adapters and
within one adapter. Set `RouterConfig::forwarding = false` for an endpoint-only
node: it neither advertises learned transit routes nor forwards transit packets.
Hop limits, loop rejection, route expiry, bounded queues, replay suppression,
and bounded fragment reassembly constrain failures. Neighbor configuration
grants link-level trust; gossip is not authorization.

The router preserves the control-plane `Transport` contract. Its separate tunnel
endpoint implements `BulkTransport`: bounded reliable packet streams run above
routing, with end-to-end mutual TLS and pinned peer certificates. Bridges can
observe routing metadata and deny service but cannot decrypt tunnel contents.
Application bytes are not replayed when retransmissions take another route.
Admission and cryptographic identity are separate from advertised reachability.

Independent `groupnet-transport-ipc` and `groupnet-transport-punch` crates supply
local IPC and authenticated UDP discovery, simultaneous hole-punch probes, and
self-hosted relay fallback. The UDP fabric is provisioned
with a random shared network key: participants are mutually trusted for routing,
not Byzantine-safe. End-to-end tunnel certificates enforce endpoint identity
even across a trusted forwarding fabric. No public third-party relay is needed.
Not every NAT supports a direct path; relay fallback remains part of the design.

USB permissions and exclusive device ownership remain application policy.
Membership/route availability never authorizes attachment to a security key.

### Initialization and ownership

The facade enables routing by default; low-level runtime consumers can select
the `router` feature independently. `Node::network_builder(id).link(provider)`
or `.links(providers)` collects implementations and `.start().await` binds them,
starts routing, and seeds membership from explicit neighbors. The result is an
ordinary `Node<Router>`. The equivalent prebuilt configuration path is
`Node::network(id, NetworkConfig)`. `start_with` and `Node::network_with` accept the
existing typed `NodeBuilder<Router>` closure for membership settings.
Every ordinary node clone retains the managed network. Ownership lives on the
public handles, not in the receive loop's shared state: a background task must
not keep its own network alive forever. A raw router clone or group handle alone
does not extend that lifetime. Dropping the last node owner initiates cancellation;
closing any clone drains network tasks and closes connections for all clones.

`groupnet-transport::link` defines the shared object-safe `LinkProvider` contract
below both routing and protocol crates. Providers receive a local identity and
return a `BoundLink`, never a concrete router. Each implementation owns typed
addresses/configuration and binding. `NetworkConfig` stores
`Vec<Box<dyn LinkProvider>>`; there is no closed protocol enum or custom escape hatch.
`TcpLink`, `UdpLink`, `MemLink`, `IpcLink`, and `PunchLink` use that same interface.
All providers start in insertion order within the router's admission/capacity bounds.

`BoundLink` carries a `LinkConfig` and single-use `LinkDriver`. The driver erases
one worker lifetime while keeping the concrete `Transport` send/receive futures
monomorphized. `LinkIo` supplies an outgoing stream and backpressured incoming sink
over the router's existing queues; registration adds no extra packet queue or
per-packet boxed future. Router scheduling still owns announcements, fragmentation,
MTU enforcement, and per-frame deadlines. Shared frames and owned fragments cross
the interface without copying their payload allocations.

Protocols with independently spawned tasks supply `LinkLifecycle`. Dropping its
owner initiates cancellation; worker completion and explicit close drain protocol
tasks. TCP owns listener/readers/writers; native IPC and punching retain their
own supervisors. Socket/channel-only implementations release resources when their
typed workers drop. Registration failure drains rejected endpoints, and partial
startup closes previously registered links. Router close synchronizes with link
registration before waiting for workers, preventing late registrations from escaping.

Initialization failure and explicit close share one shutdown path, including
draining a native listener when its peer registration fails. Dropping an
initialization future initiates cleanup through the same ownership guard.
Initialization starts route discovery; it does not wait for remote peers or
route convergence.

Both a managed `Node<Router>` and the standalone `Network` returned by
`NetworkConfig::bind` implement `BulkTransport`. Missing tunnel configuration
returns `Unsupported`, never plaintext. `tunnels()?` exposes per-peer admission:
`admit_peer` installs/replaces a certificate pin and `revoke_peer` invalidates
its active and queued streams. This is bidirectional admission, not directional
permission-group policy or application-resource authorization.

The shared registration and worker machinery lives in `groupnet-transport/src/link`.
The router separates network configuration/lifetime (`config.rs`), public routing
state (`router.rs`), scheduling (`router/adapters.rs`), path learning/forwarding
(`router/routing.rs`), and bounded codecs (`wire.rs`). Protocol configuration,
binding, and native OS workers live in their respective implementation crates.
Adding a protocol does not require modifying routing. Targets follow
[Cargo's project layout](https://doc.rust-lang.org/cargo/guide/project-layout.html);
the multi-file tunnel suite is `tests/tunnels/main.rs` with suite-local fixtures.

### Reliable streams and security boundaries

The tunnel layer uses bounded ordered segments, cumulative acknowledgements,
receive credit, RTT-based retransmission deadlines, duplicate-ACK gap repair,
and additive-increase/multiplicative-decrease congestion control. Retries use
fresh router packet IDs but retain tunnel sequence numbers, so router replay
suppression cannot discard legitimate retransmissions. FIN closes one direction;
the opposite direction remains usable until independently closed.

The UDP rendezvous is a separate explicitly started `Rendezvous`, not a
third-party service. Registrations require an authenticated challenge response
from the observed source address. Allowlists, leases, replay checks, packet
bounds, and per-identity rate limits constrain discovery and relay traffic.
The shared network key authorizes a trusted routing fabric: its holders can
impersonate routing aliases. Raw membership messages are not end-to-end private.
Only pinned mutual-TLS tunnels independently authenticate endpoints and conceal
application bytes from transit peers. IP addresses and traffic metadata remain
visible. Revoking a tunnel peer closes its sessions; it does not revoke a shared
network key or revoke application-level USB ownership.

Coordination groups are not security groups. Private-peer discovery controls,
permission-group policies, and stateful connection ACLs are not implemented.
Such policies must bind trusted grants to authenticated identities rather than
to self-advertised group membership or shared-key routing aliases. Transit
eligibility, permission to initiate a connection, and application resource
authorization are separate decisions.

No QUIC/P2P stack is wrapped. Tokio supplies I/O; rustls and ring supply standard
TLS and cryptographic primitives. Public-Internet NAT combinations, Unix runtime
behavior, and physical USB drivers still require deployment-specific validation.

Windows loopback verification includes real named-pipe/TCP bridging, typed
node-owned membership and device-metadata propagation, native UDP direct and
relay-only paths carrying pinned TLS with half-close, and a live TLS stream
which survives direct-link removal and continues through a TCP bridge. An
impaired-router regression drops, reorders, and duplicates packets during a
1 MiB transfer; duplicate-ACK gap repair avoids waiting for an RTO on every loss.
