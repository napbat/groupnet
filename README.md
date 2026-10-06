# Groupnet

A **deterministic, leaderless-by-default coordination fabric** for distributed
systems that partition state into shard groups. Groupnet is an *engine*, not a
database: it gives you group-local membership, an implicit (derived,
non-elected) coordinator, inter-group routing awareness, and shard-scoped
operations. Consensus is **opt-in per group** — the Hosted mode's Quorum
profile, epoch-fenced and majority-committed — while the general replicated-log
machine (log repair, compaction, reconfiguration) is deliberately absent. You
bring the storage and the wire.

Start with the [architecture diagram guides](docs/README.md) for Mermaid maps of
the crate boundaries, link registration, routing, lifecycle, and security model.

That opt-in is a dial, set per group: eventual metadata for free at one end, an
elected epoch-fenced host paying a majority round-trip per write at the other,
and the session and coherence tiers in between. Each rung, its price, and what
it deliberately does *not* promise are in
[`docs/consistency-modes.md`](docs/consistency-modes.md).
The opt-in `consistency-replication` feature adds source-backed replay, native
cursor checkpoint resume, read-your-writes floor waits, source-certified named
acknowledgement waits, and opt-in native snapshot recovery. Typed source and
application adapters supply storage and state; Groupnet does not impose a
second commit log. Snapshot recovery requires a source
retention hold and a guarded application install; replay-only adapters keep
their existing API. Named waits pin a bounded source-certified roster and
return a target-bound invalidation or materialization outcome; they do not
register durable subscribers. Optional idle backoff reduces source checks on
quiet scopes while preserving the original proof-freshness gate. The optional
`with_event_complete` capability registers source-protected named subscribers
through `StartAt` or `ResumeExisting`; sink effects and cursors become durable
before the source ack advances. Local `cancel` or `close_named_if` only detaches
the worker. `unsubscribe` needs a durable source tombstone to release retention,
and `ResetAt` requires that exact receipt and a strictly newer per-name fence.
For example, an adapter-backed manager can call
`replication.with_event_complete(sink, SubscriptionLimits::default())?`, then
`open_named(&scope, subscriber, incarnation, SubscriptionStart::StartAt { position, policy, request_id })?`.
Production source adapters and consumer migrations remain in development.
Contracts and implementation status are in
[`docs/replication.md`](docs/replication.md).

> Status: the coordination core, routed-network stack, and opt-in consistency
> layers are implemented. Production source adapters and consumer migrations
> have separate status notes in the [documentation index](docs/README.md).
> Loopback networking verification is not public-Internet or USB qualification.

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
| [`groupnet-transport`](crates/groupnet-transport) | both | core *(+bulk: futures-io, bytes, zerocopy; +link: tokio, tokio-util, futures-util)* | dependency-free `Transport`, optional `BulkTransport`, and shared object-safe link registration/lifecycle |
| [`groupnet-transport-mem`](crates/groupnet-transport-mem) | both | transport, core, tokio(sync) *(+bulk feature: transport(bulk), tokio(io-util), tokio-util(compat))* | in-process bindings (tests, examples, single-process): datagrams always, `MemBulkNet` byte streams under feature `bulk` |
| [`groupnet-transport-udp`](crates/groupnet-transport-udp) | control | transport, core, tokio(net) | UDP binding over real sockets |
| [`groupnet-transport-tcp`](crates/groupnet-transport-tcp) | both | transport, core, tokio(net) | persistent TCP messages, TCP streams, and `TcpLink` registration |
| [`groupnet-transport-ipc`](crates/groupnet-transport-ipc) | control | transport(link), core, tokio | native named pipes/Unix sockets and `IpcLink` registration |
| [`groupnet-transport-punch`](crates/groupnet-transport-punch) | connectivity | transport admission/session primitives, core, tokio, ring, socket2, if-addrs | native adjacent-node UDP/TCP connections, candidate checks, rendezvous and relay; no transport or router link |
| [`groupnet-network`](crates/groupnet-network) | both | transport(bulk, link), tokio, rustls, ring | protocol-independent group-network routing and pinned end-to-end TLS streams |
| [`groupnet-messaging`](crates/groupnet-messaging) | application packets | core, network, bytes, ring, tokio, tokio-util | static `MessageProtocol`, generic receipt contexts, `Messaging` endpoint, bounded retry/deduplication |
| [`groupnet-streams`](crates/groupnet-streams) | sessions | core, network, transport(bulk), bytes, ring, futures-util, tokio, tokio-util | static `SessionProtocol`, shared protocol engines, authenticated session policies |
| [`groupnet-runtime`](crates/groupnet-runtime) | both | core, transport(bulk, link), messaging, streams, network, tokio | non-generic managed `Node`, unsealed `PeerImplementation`, generic `Endpoint<P>` / `Peer<P>`, group actors, receive callbacks/fanout reports, and `FileGrantStore` |
| [`groupnet-rpc`](crates/groupnet-rpc) | data | core, transport(bulk), bytes, futures-util(io), tokio(rt, sync, time, macros) | request/response RPC over the data plane: concurrent calls multiplexed onto one stream per peer, deadlines, bounded frames, per-connection handler limits |
| [`groupnet-consistency`](crates/groupnet-consistency) | — *(data, under `handoff`)* | core, runtime, tokio(sync) *(+handoff feature: transport(bulk), bytes, futures-util)* | session-consistency layer: per-writer sequenced write feeds (loss & restarts surface as explicit gaps) + read-your-writes frontiers; the opt-in `handoff` tier is the one piece that reaches the data plane, to pull a covering snapshot a gap cannot replay |
| [`groupnet-sim`](crates/groupnet-sim) | — | core | deterministic simulator (virtual clock + lossy/partitioned net) |
| [`groupnet`](crates/groupnet) | — | facade | `runtime`+`mem` default; routing is intrinsic to runtime; socket protocols, RPC, and simulator selectable by feature |
| [`groupnet-testkit`](crates/groupnet-testkit) | — | core *(+cluster feature: runtime, transport-mem, tokio)* | shared test support: sans-IO frame fixtures + an async multi-node harness. Internal, `publish = false`, dev-dependency only |

Every runtime node owns a routed network. Use
`Node::builder(id).link(provider).start().await` for memory, sockets, or a mixture:
protocol choices do not change the `Node` type or its coordination API. Links and
membership settings share one builder; startup binds all links before spawning
coordination. `groupnet-transport` without features and `groupnet` with
`default-features = false` retain their dependency-free core surface. Protocol
implementations never enter `groupnet-core`.

`groupnet-network` implements routed networks independently of the transport
contracts and protocol implementations. The facade exposes it as
`groupnet::network`; `groupnet::transport` keeps the shared contracts and protocol
bindings.

Opaque application packets follow `runtime -> messaging -> network -> transport`.
`groupnet-messaging` owns the application codec and `BestEffort` / `Delivered` /
`Applied` acknowledgement state. The router carries opaque application packets
without decoding their payloads or interpreting receipts. Runtime retains node
and group receive ownership, callbacks, membership-snapshot fanout, and reports.
The facade preserves these ergonomic APIs under `groupnet::messaging`; standalone
routed networks can use `groupnet_messaging::Messaging` directly.

The same core runs under both drivers: `groupnet-runtime` across threads in
production, `groupnet-sim` in a single-threaded, reproducible event loop for
tests.

Most consumers pull the single `groupnet` facade, which mirrors each layer as a
module — `groupnet::core`, `groupnet::transport` (with the `mem` / `udp` / `tcp` /
`bulk` bindings nested under it), `groupnet::network`, `groupnet::messaging`,
`groupnet::runtime`, `groupnet::rpc`, and `groupnet::sim` — so you write
`groupnet::transport::Transport`, never the underlying crate name.

## Example

Runnable examples live in [`crates/groupnet/examples`](crates/groupnet/examples):

```bash
cargo run --example placement   # weighted HA-hash placement (sync, no I/O)
cargo run --example cluster     # 3-node convergence, derived coordinator, metadata
cargo run --example routing     # resolve a resource to its owner from any node
cargo run --example dynamic-admission --features tcp-msg  # unknown clients, open admission
cargo run --example dynamic-admission --features tcp-msg -- --invite  # custom credential policy
cargo run --example dynamic-relay --features udp,connectivity  # discovery through a keyless relay
cargo run --example dynamic-relay --features udp,connectivity -- --direct
cargo run --example native-traversal --features udp,tcp-msg,connectivity
cargo run --example native-traversal --features udp,tcp-msg,connectivity -- --tcp
cargo run --example native-traversal --features udp,tcp-msg,connectivity -- --tcp --relay-only
cargo run --example native-traversal --features udp,tcp-msg,connectivity -- --ipv6
cargo run --example connectivity-bridge --features tcp-msg,connectivity  # memory A -> TCP edge B -> remote C
cargo run --example connectivity-bridge --features tcp-msg,connectivity -- --relay-only
cargo run --example connectivity-bridge --features tcp-msg,connectivity -- --upgrade
```

Five more live with the layers they exercise:

```bash
cargo run -p groupnet-sim --example partition              # partition -> detect -> heal -> rejoin, bit-for-bit reproducible
cargo run -p groupnet-consistency --example read_your_writes   # write feed + applied-frontier barrier, and a gap surfacing
cargo run -p groupnet-consistency --example fenced_ownership --features hosted   # elected host, majority-committed claims, and a store refusing a fenced-out writer
cargo run -p groupnet-consistency --example anchored_ownership --features hosted   # the same claims, with the epoch won by one conditional PUT against a CAS object
cargo run -p groupnet-consistency --example hosted_handoff --features handoff   # a late joiner past the ring: the honest gap, a covering snapshot over the data plane, and a contiguous resume
```

```rust
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemLink, Network};

let net = Network::new();
let id = NodeId::new("node-a");
let node = Node::builder(id.clone())
    .link(MemLink::new(net.endpoint(id), vec![NodeId::new("node-b")]))
    .start()
    .await?;

let group = node.join_group("shard-42");

if group.is_coordinator() {
    group.sync(|ctx| ctx.update_metadata("routing", "v3"));
}
```

Use the same node and group APIs with UDP sockets — select feature `udp` and
register a `UdpLink` instead:

```rust
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::{link::PeerEndpoint, udp::UdpLink};

let node = Node::builder(NodeId::new("node-a"))
    .link(UdpLink::new(
        "0.0.0.0:7000".parse()?,
        vec![PeerEndpoint::new(NodeId::new("node-b"), "10.0.0.2:7000".parse()?)],
    ))
    .start()
    .await?;
```

### Application buffers, frames, and callbacks

`Node` and `Group` expose matching buffer and frame APIs:

| Operation | Buffer API | Frame API |
|---|---|---|
| Send | `send(..., bytes)` | `send_frame(..., Bytes, SendOptions)` |
| Manual receive | `recv() -> (MessageContext, Bytes)` | `recv_frame() -> Frame` |
| Async callback | `on_recv(|context, bytes| async { ... })` | `on_frame(|frame| async { ... })` |

Send/receive operations are awaited and return `io::Result`; callback registration
returns `io::Result<ReceiveHandle>`. Callbacks return `io::Result<()>`.
The node send methods take a destination `&NodeId`; group sends resolve recipients.

Applications address a node or group, never an intermediate bridge:

```rust
use groupnet::messaging::{Bytes, Delivery, SendOptions};
use std::time::Duration;

// On the receiving node: keep the handle alive while receiving.
let incoming = receiver.join_group("cache-invalidations");
let handler = incoming.on_recv(|context, bytes| async move {
    apply_invalidation(&context.from, &bytes).await?;
    Ok(()) // Applied is acknowledged only after this returns successfully.
})?;

// On the sending node:
let outgoing = sender.join_group("cache-invalidations");
// Wait for both membership views to converge before sending.
let report = outgoing.send_frame(
    Bytes::from_static(b"opaque application bytes"),
    SendOptions {
        delivery: Delivery::Applied,
        timeout: Duration::from_secs(5),
    },
).await?;
for outcome in report.outcomes {
    outcome.result?; // Every selected recipient has its own result.
}
handler.close().await?; // Cancels/drains the callback and releases its inbox.
```

`apply_invalidation` is application code; Groupnet does not interpret the buffer.
Manual buffer receives retain the complete message context and receipt:

```rust
let (context, bytes) = incoming.recv().await?;
apply_invalidation(&context.from, &bytes).await?;
drop(bytes); // Moving or dropping the payload does not discard its receipt.
context.applied()?; // Explicitly acknowledge successful processing.
```

Use that manual receive after closing the callback handle above. Alternatively,
receive a full `Frame` with `recv_frame()`, process `frame.payload`, then call
`frame.applied()?`. These are equivalent lossless representations:
`frame.into_parts()` moves every field into `(MessageContext, Bytes)` without
cloning metadata, cloning the receipt, or copying the payload. The context
retains `id`, original `from` (A, not forwarding bridge B), optional `group`, and
the receipt; `context.delivery()` reports the requested delivery boundary.
`context.receipt()` returns a cheap, independent receipt handle, while
`context.reject(error_kind)?` or `frame.receipt().reject(error_kind)?` explicitly
rejects processing.

For frame-aware callbacks, use `group.on_frame(|frame| async move { ... })`.
`Node` provides the same receive and callback variants for node-addressed messages
(`group` is `None`). Borrowed sends copy into owned storage; `send_frame` takes
existing `Bytes`, shared across group recipients. Buffer receives/callbacks move
the existing context and payload without copying them.

Neither manual receive variant automatically acknowledges `Applied`; call
`context.applied()` or `frame.applied()` after successful processing. Both callback
variants retain a receipt internally and automatically acknowledge successful
completion, even if the callback moves or drops the context/frame and payload.

| Delivery | Successful send means |
|---|---|
| `BestEffort` (default for `send`) | Locally accepted for routing, not remotely acknowledged |
| `Delivered` | Destination reserved its bounded application inbox |
| `Applied` | Destination explicitly acknowledged processing, or its callback returned `Ok(())` |

Groupnet freezes its local live-membership snapshot, excludes self, routes to
each member, and reports every selected recipient's outcome. Departures do not
erase failures; later joins do not add recipients or replay prior messages.
An empty recipient snapshot succeeds with an empty report. Receiver-local
membership can lag: unjoined groups and unknown/dead group senders are rejected.
A nonmember bridge forwards without receiving or acknowledging application work.

Node and group inboxes are separate from each other, metadata, coordination,
and TLS streams. Buffer/frame variants share the same inbox; they do not duplicate
delivery. Each inbox has one receive owner: `recv`, `recv_frame`, `on_recv`, or
`on_frame`. Competing owners fail with `WouldBlock`. Callback errors reject the
frame and stop the worker; observe them through `handler.wait().await`.
Dropping the handle cancels its worker, including an in-flight callback.
Cancellation cannot undo side effects and does not acknowledge unfinished work.
Use `close().await` to drain before switching back to manual receives.
Network shutdown cancels blocked receives and callbacks.

Bounds: 60,000 payload bytes, 64 queued frames per runtime inbox, at most 32
concurrent sends per group fanout, and 256 outstanding acknowledged sends per
node. Acknowledged deadlines must be nonzero and at most 30 seconds. Retries
retain message identity and use bounded duplicate suppression; timeout means
**unknown outcome**, not proof of nonexecution. There is no ordering, durability,
crash recovery, or exactly-once guarantee.

These are **trusted-fabric messages, not encrypted/authenticated application
channels**. Sender attribution and membership checks trust transit peers.
Use the existing pinned TLS stream API for confidential/authenticated traffic.
Metadata still converges as replicated state; Hosted/quorum commits, feeds,
frontiers, and coherence leases retain their existing separate contracts.
Sending to a Hosted group does not turn a frame into a quorum commit.
The routed wire guard is now `GNR3`, and the authenticated tunnel preamble is
`GN-TUNNEL-2`; upgrade communicating nodes together.

Executable memory A → TCP bridge B → TCP C demonstration:

```bash
cargo run -p groupnet --example application-messages --features tcp-msg,connectivity
cargo run -p groupnet --example application-messages --features tcp-msg,connectivity -- --relay-only
```

The example checks original A attribution across B, group destinations and
receipt delivery modes for both buffer receive variants. Manual node and group
buffers acknowledge `Applied` through their contexts after the payload is dropped;
node and group callbacks demonstrate automatic success acknowledgements.

### Typed message and session protocols

`node.endpoint(implementation)?` returns a generic `Endpoint<P>`;
`node.peer(id, implementation)?` returns a destination-bound `Peer<P>`.
`Messages`, `Ordered`, and `Unordered` select the concrete protocol and its
options once; callers do not retrieve and shuttle raw protocol engines into
peers. Both constructors resolve the node's shared protocol state internally
and create no connection. The handles use static dispatch and retain the
managed node's lifetime. Routes, physical connections, and protocol workers
remain shared underneath them. There is one public endpoint handle family,
not separate message/ordered/unordered endpoint types.

| Descriptor | Send/connect contract | Receive surface |
|---|---|---|
| `Messages::best_effort()` | Send once, without an application receipt wait | Existing node/group lossless contexts, frames and callbacks |
| `Messages::delivered(timeout)` / `Messages::applied(timeout)` | Wait for inbox acceptance / application acknowledgement | Existing node/group lossless contexts, frames and callbacks |
| `Ordered::new()` | Reliable ordered bytes through pinned TLS tunnels | `AsyncRead` / `AsyncWrite` |
| `Unordered::reliable()` | Independently retry each message until inbox acceptance or a finite deadline | Whole messages; no waiting for an earlier missing message |
| `Unordered::unreliable()` | Send once, without data ACKs or session-layer retries | Whole messages; loss and reordering permitted |

```rust,ignore
use groupnet::{Messages, Ordered, Unordered};
use groupnet::messaging::Bytes;
use std::time::Duration;

let messages = node.peer(remote.clone(), Messages::best_effort())?;
messages.send(Bytes::from_static(b"update")).await?;

let applied = node.peer(remote.clone(), Messages::applied(Duration::from_secs(5)))?;
applied.send(Bytes::from_static(b"process this")).await?;

let ordered = node.peer(remote.clone(), Ordered::new())?;
let stream = ordered.connect().await?;

let reliable = node.peer(remote.clone(), Unordered::reliable())?;
let reliable_session = reliable.connect().await?;

let peer = node.peer(remote, Unordered::unreliable())?;
let session = peer.connect().await?;
session.send(Bytes::from_static(b"current state")).await?;
let response = session.recv().await?;
session.close().await?;
```

The receiver creates an endpoint for the desired policy and calls
`node.endpoint(Unordered::reliable())?.accept()` (or `Unordered::unreliable()`).
It receives the authenticated original `NodeId` and a session exposing
`delivery()` and `session_id()`. Reliable and unreliable accepts consume
separate policy queues, so both can run concurrently without stealing each
other's sessions. Configure node-wide capacity, timers, and
`allow_reliable` / `allow_unreliable` with `node.configure_unordered(config)?`
before the protocol is first resolved. Disallowed policies are rejected; there
is no silent downgrade. Ordered streams arrive through `node.accept()` or
`node.endpoint(Ordered::new())?.accept()`. The two session protocols do not
compete for setup traffic.

Both session protocols require configured TLS identity and peer pins. Unordered
setup derives directional AEAD keys from the pinned TLS exporter; payloads travel
as authenticated encrypted routed datagrams, **not over the ordered control
stream**. Reliable unordered success means bounded receiver-inbox acceptance,
not application processing or durability. A timeout leaves acceptance unknown.
Replay/deduplication windows are bounded; neither mode promises exactly-once
application execution or successful delivery across arbitrary outages.

Default unordered limits: 48 KiB per message, 32 queued messages and 32 pending
sends per session, 64 sessions and eight per peer (also subject to tunnel limits).
Setup and queued accepts count against the same session limits. Admitted setups
run concurrently with individual deadlines; a slow peer does not serialize
every other peer's handshake. Saturated setup capacity fails closed.
Reliable sends have a ten-second deadline, 100 ms retry interval and 100-attempt
cap. Healthy held sessions exchange heartbeats; five seconds without fresh
authenticated traffic expires them. Endpoint limits and timers are configurable.
Reliable sends return `WouldBlock` if a new logical ID would overtake the oldest
unresolved send beyond the bounded deduplication horizon, even when a concurrent
send slot is free. This protects pending retries without imposing receive order.
TCP paths retain TCP's own ordering/retransmission even for unreliable sessions:
this policy does not constrain route selection to UDP.

`Node` owns and caches the distinct underlying protocol engines; `Endpoint<P>`
and `Peer<P>` are handles over that shared state, not new workers. Node-wide
`UnorderedConfig` is separate from the per-handle delivery/setup options selected
by `Unordered`; repeated identical configuration is allowed, but conflicting
protocol configuration returns `InvalidInput`. `Messages::new(SendOptions)` and
`Unordered::new(UnorderedOptions)` bind explicit per-handle options when the
convenience constructors are insufficient. `peer.send(bytes)` and
`peer.connect()` take no options: every operation reuses the bound policy.

For node-addressed messaging without a fixed destination, use
`node.endpoint(Messages::applied(timeout))?.send(&remote, bytes).await?`.
Message endpoints expose `recv`, `recv_frame`, `on_recv`, and `on_frame` through
the same single-owner runtime inbox as `Node`: a competing receive returns
`WouldBlock`, not a second copy or a frame stolen from the messaging worker.
The group receive surface remains `Group`'s existing inbox.

Dropping a temporary peer does not cancel a stream retained by the application
while the node remains alive. Node close, admission revocation, protocol shutdown,
and last session-handle drop terminate affected sessions. Dropping an endpoint
handle alone does not close the node; cached protocol state remains owned by it.
`endpoint.protocol()` exposes the concrete lower-level implementation for
explicit protocol lifecycle management (`shutdown` / `closed`) or custom
protocol operations; ordinary sends, receives, connects and accepts use the
generic handles.

Custom descriptors implement the public, unsealed `PeerImplementation` trait
exported by `groupnet-runtime` and the `groupnet` facade. Its associated
`Protocol` and cloneable `Options` types identify the concrete protocol engine
and bound options; `bind(self, node: &Node)` returns that protocol/options pair as
an `io::Result`. Custom implementations can resolve shared node resources and
return their own protocol without changing `Node` or using dynamic dispatch.
The protocol engine implements the existing unsealed `MessageProtocol` or
`SessionProtocol` contract; no per-call future boxing is required.
`MessageProtocol` associates send options and receipt type; `Frame<R>` and
`MessageContext<R>` retain that receipt without requiring `Clone`.
`SessionProtocol` associates connect options and the concrete session type;
there is no byte-IO requirement on message-oriented sessions.
Register custom routed protocols through `Router::bind_protocol(id)`, obtaining
bounded `ProtocolIo`. IDs 1 and 2 belong to messaging and unordered datagrams;
unknown destination namespaces are rejected/dropped, never reinterpreted.
The router demultiplexes IDs, not application bodies or receipts.

The managed messaging receiver remains owned by the runtime dispatcher: use
message endpoints, `node.recv()` / `group.recv()`, and callback variants, not a
competing raw `Messaging::recv()`. Creating a custom peer does not install its
protocol on the remote node. Application timers decide continuous traffic versus
periodic polling; neither requires rebuilding a peer or endpoint between sends.


### Node-owned heterogeneous connections

Select the protocol features you need (`tcp-msg` for this example; also `udp`,
`ipc`; enable `connectivity` for native TCP/UDP paths). Add links to bridge protocols:

```rust
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::{link::PeerEndpoint, tcp::TcpLink};

let node = Node::builder(NodeId::new("node-a"))
    .link(TcpLink::new(
        "127.0.0.1:7000".parse()?,
        vec![PeerEndpoint::new(
            NodeId::new("bridge"),
            "127.0.0.1:7001".parse()?,
        )],
    ))
    .gossip_interval_ms(100)
    .start()
    .await?;
let devices = node.join_group("devices");
// Use devices and node.router(); close drains owned network I/O tasks.
node.close().await;
```

`TcpLink`, `UdpLink`, `MemLink`, and `IpcLink` belong to their
implementation crates. All implement `groupnet::transport::link::LinkProvider`;
there is no protocol enum or special custom-adapter path. To add a protocol,
implement that shared contract outside the router. Binding returns a `BoundLink`
with adjacent-peer admission, cost/MTU, and an owned worker/lifecycle handle.
Provider `with_cost` methods override the default route cost of one.

Already configured providers can be registered as a heterogeneous collection:

```rust
use groupnet::transport::link::LinkProvider;

let links: Vec<Box<dyn LinkProvider>> = vec![Box::new(tcp), Box::new(ipc)];
let node = Node::builder(local_id).links(links).start().await?;
```

`NetworkConfig::with_link` / `with_links` configure a standalone routed network
through `config.bind(id)` when no coordination groups are needed. Nodes always
use `Node::builder`. The router depends only on the shared link contract, not
TCP, UDP, IPC, or punching implementations. Registration erases provider and
worker lifetimes once; packet send/receive futures remain statically dispatched.
Existing bounded scheduling queues are reused.

Initialization returns a non-generic `Node`. Every node clone retains network
ownership; a raw router clone or group handle does not. Closing any node clone
closes connections for all of them; dropping the last node clone initiates
shutdown. Configure membership with `.config(...)`, `.seed(...)`,
`.named_seeds(...)`, and the timing setters directly on the same builder.
Initialization binds listeners and starts discovery; route convergence remains
asynchronous and can be inspected with `node.router().route_to(&peer)`.

Configured adjacent identities are initial membership seeds. Dynamic-capable
links can instead admit previously unknown identities through an application
policy. Additional membership seeds, DNS answers, and gossiped addresses do
**not** grant link admission; only configured or successfully admitted neighbors
can supply router traffic.

For an open TCP listener, select feature `tcp-msg` and explicitly choose the
unauthenticated policy:

```rust
use std::sync::Arc;
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::{admission::OpenAdmission, tcp::TcpLink};

let node = Node::builder(NodeId::new("game-server"))
    .link(
        TcpLink::new("0.0.0.0:7000".parse()?, Vec::new())
            .with_admission(Arc::new(OpenAdmission)),
    )
    .start()
    .await?;
let room = node.join_group("game-room");
```

Clients configure the server as a bootstrap `PeerEndpoint` and use their own
`NodeId`, such as a generated UUID string. The server need not know those IDs
before they connect. Membership converges asynchronously after admission.
The [dynamic admission example](crates/groupnet/examples/dynamic-admission.rs)
starts an ephemeral listener and two initially unknown clients; its `--invite`
mode implements a custom `Admission` policy and passes credentials through
`TcpLink::with_credentials`.

`Admission::admit` receives a claimed identity, bounded opaque credential bytes,
and the remote address when available. Return `AcceptedPeer` or an error.
The accepted identity must match the client's configured routing identity:
account policies should reject mismatched claims, not silently rename a live
node. Open admission proves no ownership of the name. Duplicate active sessions
are rejected; after disconnection, the same unauthenticated name can be claimed
again. Admission is independent of TLS and resource authorization.

Forwarding between configured peers is automatic, including between neighbors
on the same adapter. No special bridge flag is required:

```text
IPC-only node ── IPC ── bridge ── TCP or authenticated UDP ── network-only node
```

Use `.routing(RouterConfig { forwarding: false, ..RouterConfig::default() })`
for an endpoint-only node which must not advertise or forward transit routes.
This is a routing-role setting, not an access-control policy for peers or groups.

Plain TCP/UDP does not provide endpoint authentication or encryption. Static
links assume trusted peers; explicitly open admission accepts untrusted identity
claims and does not make their routing advertisements trustworthy.
Keyed `PunchConfig` supplies a self-hosted rendezvous address, provisioned
`NetworkKey`, explicit peers, and `PathPolicy`. Direct-preferred mode probes
peers simultaneously; relay-only mode never sends direct probes. Not every NAT
supports direct connectivity.

For dynamic keyless discovery, start `Rendezvous::bind_open(address)` and give
each node `UdpLink::connectivity(PunchConfig::open(local_id, rendezvous_address))`.
No participant list, public/private key pair, or pre-shared transport key is
required. Here **keyless** means `PunchConfig.key == None`, not an absence of
application admission or all cryptography. `PunchConfig::open` conservatively
defaults to `RelayOnly`; select direct punching explicitly:

```rust
use groupnet::connectivity::{PathPolicy, PunchConfig};
use groupnet::transport::udp::UdpLink;

let mut config = PunchConfig::open(local_id, rendezvous_address);
config.policy = PathPolicy::DirectPreferred;
// Register with Node::builder(...).link(UdpLink::connectivity(config)).start().await?
```

Admission, transport authentication, and path policy are independent choices:

| Choice | Configuration |
|---|---|
| Accept a claimed `NodeId` without ownership proof | Explicit `OpenAdmission` / `Rendezvous::bind_open` |
| Apply application-defined admission rules | `Rendezvous::bind_with_admission`; supply credentials through `PunchConfig::dynamic` |
| Authenticate datagrams as members of a trusted fabric | Configure the same `NetworkKey`; keyed endpoints never downgrade |
| Prefer verified direct UDP, falling back to relay | `PathPolicy::DirectPreferred`, with or without a configured key |
| Use relay only; do not exchange peer socket addresses or send direct probes | `PathPolicy::RelayOnly`; applies to a pair if either endpoint selects it |
| Authenticate and encrypt application streams | Separately configure pinned TLS tunnels |

These choices are workload-neutral: database engines and general applications
use the same APIs. Open admission is opt-in, not a recommendation for a database
trust boundary. Coordination groups do not authorize access to application data.

The rendezvous checks source-address reachability, applies admission, and leases
registrations; it is not itself a coordination-group member. Fresh private session
proofs and direct-path challenges bind packets to current sessions; a public
`NodeId` or advertised session ID alone cannot authorize direct data. These
mechanisms do not provide identity ownership or confidentiality. Never send
reusable credentials over an unencrypted admission exchange.
See the runnable [dynamic relay/direct example](crates/groupnet/examples/dynamic-relay.rs)
and [security boundaries](docs/tunnels-and-security.md#3-native-discovery-direct-paths-and-relay).

Native UDP uses `GNP4`; native TCP frames use version 2. Upgrade the rendezvous
and its endpoints together; earlier formats are rejected without negotiation.

### Native UDP and TCP candidate traversal

The `connectivity` feature exposes the custom native connection library through
`groupnet::connectivity` and enables it internally for selected `udp` / `tcp-msg`
bindings. It is not a transport or a separately registered router link.

| Actual transport | Connectivity configuration / transport-owned link | Rendezvous |
|---|---|---|
| UDP | `PunchConfig` / `UdpLink::connectivity(config)` | `Rendezvous` |
| TCP | `TcpPunchConfig` / `TcpLink::connectivity(config)` | `TcpRendezvous` |

The library owns live `UdpConnection` / `TcpConnection` resources: discovery,
bounded checks, mapping refresh, path recovery, and relay fallback. The adapters
own transport traits, link registration, admitted-session integration, and
shutdown. A connection is not just an address to reopen later: preserving the
actual checked socket or established stream preserves its NAT mapping.

The router remains responsible for logical multi-hop paths. With memory-only A,
edge B owning memory and TCP, and TCP-only C, A addresses C by `NodeId`; ordinary
routing forwards A → B → C without application forwarding code on B. If A gains
compatible admitted TCP connectivity to C, its cheaper one-hop route can replace
the bridge route; loss of that adjacency permits fallback through B. Membership
or route advertisements alone never authorize that new adjacent connection.

Applications such as docstore, docres, and s3cache build on these domain-neutral
APIs. They choose groups, admission, security, and application behavior—not NAT
checks or bridge forwarding. `RelayOnly` controls the physical adjacent path,
not whether the router may forward through other nodes.

Both configurations support `candidate_binds` (up to three additional owned
sockets), `advertised_candidates` (up to eight explicit address hints), and
`gather_interfaces` (enabled by default). Wildcard binds gather compatible local
interface addresses; binding one family does not automatically create sockets
for the other. Add a bind in the other family when both IPv4 and IPv6 paths are
required. Link-local/scoped, multicast, broadcast, unspecified, and zero-port
remote candidate addresses are not usable advertised destinations.

Checks are bounded and session-authenticated. A dead candidate does not block
other candidates or working relay traffic. UDP preserves a healthy selected path
and falls back to another validated path or the relay when it expires. Authenticated
peer-reflexive checks can discover source mappings not in the original candidate
list; arbitrary source-address changes are not accepted as proof.

TCP uses its own TCP rendezvous and framed relay, with no UDP dependency. It
coordinates active opens from reusable source ports and accepts validated passive
connections. Direct success depends on OS socket-reuse and NAT behavior; failure
retains TCP relay operation. This is native TCP, not HTTPS proxy traversal or a
TLS-wrapped control protocol.

```rust
use groupnet::runtime::Node;
use groupnet::connectivity::{PathPolicy, TcpPunchConfig};
use groupnet::transport::tcp::TcpLink;

let mut config = TcpPunchConfig::open(local_id.clone(), tcp_rendezvous_address);
config.policy = PathPolicy::DirectPreferred;
config.candidate_binds.push("[::]:0".parse()?);
let node = Node::builder(local_id)
    .link(TcpLink::connectivity(config))
    .start()
    .await?;
```

Protocol adapters `UdpTransport::bind_connectivity(config)` and
`TcpMsgTransport::bind_connectivity(config)` expose `local_addrs()`,
`local_candidates()`, `observed_addr()`, `direct_addr_to(&peer)`, and
`path_to(&peer)` for inspecting actual selected paths. Their `into_bound_link(cost)`
transfers the live session registry and shutdown lifecycle together.
The [native traversal example](crates/groupnet/examples/native-traversal.rs)
exercises bidirectional bytes, managed membership, and departure cleanup.
Its `--tcp`, `--ipv6`, and `--relay-only` flags may be combined.

UDP dynamic sessions retain rendezvous/discovery lease requirements. TCP keeps
already-established direct sessions after rendezvous loss; when no usable direct
sessions remain, the endpoint terminates and callers must rebind. TCP does not
silently reconnect its control session. A path change never carries queued traffic
into a replacement admission generation.

Neither implementation is ICE/STUN/TURN or guarantees traversal through arbitrary
NATs. Open admission proves no identity ownership; transport MACs are not payload
encryption. Use application-defined admission and pinned tunnels where required.

The [connectivity bridge example](crates/groupnet/examples/connectivity-bridge.rs)
demonstrates memory-only A talking bidirectionally to TCP-only C through B,
group convergence, and route withdrawal without breaking the memory adjacency.
With `--upgrade`, A explicitly gains a TCP adapter; routing promotes its admitted
A-C connection to a one-hop route, then restores transit through B after TCP loss.

The example provisions disposable pinned TLS identities and exercises messaging,
ordered byte streams and both unordered policies over the same bridge.
The managed node owns `router().recv()` for group coordination; applications
must not compete for that inbox.

For confidential streams, call
`.tunnels(TunnelConfig::new(identity, [peer_pin]))` on the node builder with a `TlsIdentity` and
explicit `PeerIdentity` certificate pins. Import
`groupnet::transport::bulk::BulkTransport`, then use `node.connect(&peer).await?`
to initiate a stream or `node.accept().await?` to accept one. A node without
configured tunnels returns `Unsupported`; it never falls back to plaintext.
`NetworkConfig::bind` also returns a standalone `Network` implementing the same
stream trait when membership is not needed.

`node.tunnels()?` exposes `admit_peer` and `revoke_peer` for bidirectional tunnel
admission. Replacing a pin or revoking a peer invalidates its existing and queued
streams; re-admitting the same pin preserves its current sessions.
TLS 1.3 remains end to end across bridge nodes; route changes do not restart the
application stream. Certificates must include the `groupnet.peer` DNS SAN and
client/server authentication usages.

Coordination groups are not permission groups. Private-peer route visibility
and group-based connection policies are not implemented. Open admission accepts
claimed identities without proving who owns them. A shared network key
establishes a trusted fabric but does not securely distinguish its members.
Use pinned tunnel identities and application authorization for protected resources.

Windows IPC addresses are local `\\.\pipe\name` paths. Unix IPC requires a
caller-owned private directory with no group/other permissions. No adapters
install drivers, change firewall rules, or authorize USB access.

An external Windows smoke consumer registered its own provider without router
implementation knowledge. Four nodes carried membership, metadata, and an exact
1 MiB pinned-TLS stream across IPC, TCP, and memory links, with a half-close reply.
Separate runs exercised UDP, punching/relay configurations, revocation/readmission,
and failed-initialization cleanup. This is not public-Internet NAT, Unix runtime, or
physical USB/YubiKey qualification. See the [routing guide](docs/routing.md) and
[tunnel security boundaries](docs/tunnels-and-security.md).

### Named seeds

In an orchestrated deployment a seed's address moves (a `StatefulSet` peer's DNS
record appears after its pod starts; a rolling restart hands it a new IP). Name
the seed instead and let the node keep it current: it is resolved off the
startup path, retried until it first resolves, re-resolved for the life of the
node, and every new address reaches links that already admit that identity through
`Transport::learn_peer`. Feature `dns` supplies the operating-system resolver;
any `SeedResolver` works. Address learning never widens the admitted peer set:

```rust
use groupnet::core::NodeId;
use groupnet::runtime::{NamedSeeds, Node, SystemResolver}; // feature "dns"
use groupnet::transport::{link::{BoundLink, LinkConfig}, udp::UdpTransport}; // feature "udp"

let id = NodeId::new("node-a");
let peer = NodeId::new("node-b");
let udp = UdpTransport::bind(id.clone(), "0.0.0.0:7000").await?;
let node = Node::builder(id)
    .link(BoundLink::new(udp, LinkConfig::new(vec![peer.clone()])))
    .named_seeds(NamedSeeds::new(SystemResolver).seed(peer, "node-b.peers:7000"))
    .start()
    .await?;
```

Custom protocols implement `Transport` for concrete packet I/O and `LinkProvider`
for configuration/binding. Already-bound transports register through
`BoundLink::new(transport, LinkConfig::new(peers))`, which itself implements
`LinkProvider`. Supply `with_lifecycle` when the endpoint owns independent tasks
that must be cancelled and drained; no separate node construction path is needed:

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

### Request/response RPC (feature `rpc`)

For calls rather than streams — "read this key from its owner", "apply this
write on that replica" — `groupnet::rpc` multiplexes any number of concurrent
calls onto **one** data-plane stream per peer, opened lazily, reused, and
replaced after it breaks:

```rust
use groupnet::rpc::{RpcClient, RpcConfig, RpcError, RpcServer, RpcStatus};

// Serving node: the server owns this plane's `accept`.
let server = RpcServer::spawn(rpc_plane, |from: NodeId, request: Bytes| async move {
    lookup(&request).ok_or_else(|| RpcStatus::new(404, "no such key"))
});

// Calling node: one cloneable client; every call carries its own deadline.
let client = RpcClient::new(client_plane, RpcConfig::default());
match client.call(&owner, Bytes::from(key), Duration::from_millis(200)).await {
    Ok(value) => { /* ... */ }
    Err(RpcError::ConnectionLost | RpcError::Timeout) => { /* outcome unknown */ }
    Err(other) => { /* not sent (Unreachable, TooLarge), or Remote(status) */ }
}
```

The deadline travels with the request and the server drops the work once it
passes; each connection runs a bounded number of handlers and answers through a
single writer. The server takes over `accept` of its plane, so give RPC its own
bulk transport (its own TCP port) unless nothing else accepts streams, and
register peer addresses on the transports (`DataPlane::transport`) yourself.

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

Implemented capabilities and their contracts:

| Area | Current behavior | Details |
|---|---|---|
| Coordination | SWIM membership, indirect probes, refutation, tombstone reaping, and digest/delta anti-entropy | [Coordination model](docs/architecture.md#coordination-model) |
| Metadata | LWW registers, TTL'd node entries, and eventually consistent resource/group/coordinator lookup | [Metadata versus packet routing](docs/architecture.md#metadata-routing-versus-packet-routing) |
| Networking | Independent protocol providers, intrinsic managed routing, direct/relay discovery, pinned TLS streams | [Diagram guides](docs/README.md#architecture-diagram-guides) |
| Consistency | Session, acknowledgement, lease, and Hosted tiers selected explicitly | [Consistency contract](docs/consistency-modes.md) |
| Source-backed replication | Replay, checkpoints, native snapshots, and named subscriptions through application adapters | [Implementation status](docs/replication.md#9-implementation-status) |

Remaining work and deployment boundaries:

- Production source adapters and downstream consumer migrations have their own
  status in the replication contracts; a byte-stream transport does not supply
  durable application history or storage semantics.
- Named DNS seeds and native rendezvous discovery are implemented. Plain
  TCP/UDP addressing still requires a trusted deployment; discovery is not an
  authorization or private-peer visibility policy.
- Permission-group connection policies and VPN interfaces are not implemented.
- Public-Internet NAT, Unix runtime behavior, and USB/YubiKey hardware require
  deployment-specific qualification beyond the Windows loopback scenarios.
- Merkle-style full-state comparison remains an unbuilt scaling option; current
  full-membership fabrics should stay within the envelope described above.

## License

MIT OR Apache-2.0
