# Clients of one node

Status: contract of record for napbat/groupnet#15. Section 7 is the build
order and records what has landed.

Groupnet is the network substrate for distributed applications: clusters that
shard state, services that many clients connect to, and later VPN-style
overlays. This document covers the second shape — a **hub** that serves and
**clients** that connect to it — without weakening the hard invariants in
`AGENTS.md`.

## 1. Terms

- **Member**: a managed node that takes part in membership, gossip and
  routing. Every node today is a member.
- **Hub**: a node or a standalone endpoint that admits clients it was not
  configured with. A hub can be a member of a cluster at the same time.
- **Client**: an endpoint that dials one or more hubs, never listens, never
  forwards, and joins no membership. It is admitted by the hub's policy, not
  by a configured peer list.
- **Seed**: a configured peer address. Seeds bootstrap a link; they never
  bound who may join it.

## 2. Peer lists grow at run time

A link's peer list is not static. The link contract already says so
(`BoundLink::with_sessions`: "configured peers are bootstrap seeds only"); this
section makes it a rule every link implementation follows.

1. A link admits peers through an `Admission` policy and records them in a
   `SessionRegistry`. Configured peers are seeds: the link dials them on its
   own and keeps dialing while they are absent.
2. Without an explicit policy, a link admits exactly its configured peers
   (an allowlist). That is a policy choice, not a property of the link.
   `OpenAdmission` or an application policy admits peers the link was never
   told about.
3. The router routes only through live sessions. A frame produced by a
   session carries its `SessionId`; a send names the `SessionId` the router
   selected and is dropped if that generation is no longer live.
4. Revoking a session (`SessionRegistry::revoke`) ends its connection; the
   peer can be admitted again later under a new generation.

TCP implements this today (`TcpLink`). IPC implements it with this work.

### 2.1 IPC admission

`IpcLink` is always admitted. `IpcLink::with_admission` selects the policy and
`IpcLink::with_credentials` the bytes presented to the remote policy.
`IpcLink::dial_only` builds a link with no listener: a client.
`IpcTransport::bind_admitted` and `IpcTransport::dial_only_admitted` are the
transport-level forms, with `into_bound_link` producing the registered link.
`IpcTransport::bind`/`bind_with` stay the raw static transport.

The IPC identity rules do not change: the operating system boundary is the
trust boundary (owner-only Unix directories, local-only named pipes), and a
credential is an application claim, not a cryptographic identity.

Wire, in order (every field is prefixed by the shared big-endian
`LengthHeader`; ids are at most 64 bytes, credentials at most
`MAX_CREDENTIAL_BYTES`):

1. Dialer → acceptor: `GNIJ`, id, credential.
2. The acceptor checks the claim (not its own id), runs its policy, reserves
   the identity (`try_admit`) and answers with the same hello for itself,
   followed by the accepted dialer id (length-prefixed).
3. The dialer checks that the acceptor is the peer it dialed and that the
   echoed id is its own, runs its own policy on the acceptor, reserves the
   identity and acknowledges with the acceptor's id.
4. The acceptor checks the acknowledgement. Frames then flow in both
   directions as in the static transport.

Each side bounds the exchange by `IpcConfig::setup_timeout`. A static endpoint
refuses `GNIJ` and an admitted endpoint refuses the static `GNI1`
introduction. When both sides dial at once, the outbound connection of the
lexically lower identity wins at both ends, the same rule as TCP.

## 3. A data plane for clients

Clients call hubs over the data plane (`BulkTransport`), which is reliable and
ordered, through `groupnet-rpc`.

- **IPC** (`groupnet-transport-ipc`, feature `bulk`): `IpcBulkTransport`
  implements `BulkTransport` over local IPC. `bind`/`bind_with` listen;
  `dial_only` makes a client that registers hub addresses and never listens.
  Its handshake is versioned (`GNIB`, version `1`): the dialer sends its id
  and an opaque credential; the acceptor runs an optional `Admission` policy
  and answers with one status byte and its own id, which the dialer checks
  against the peer it dialed. Without a policy every local peer is accepted,
  matching the static IPC trust model. Handshakes are bounded in time and
  number, like `TcpBulkTransport`.
- **TCP**: `TcpBulkTransport::dial_only` and `dial_only_with` already exist.
  Admission of dynamic clients over a network (per-session keys pinned at
  admission) is slice 6.

## 4. Server push: subscriptions

An RPC call answers once. A subscription answers many times: the client
opens it with one request, the server sends items until it ends or either side
cancels. It travels on the same multiplexed connection as calls, so a client
that only dials can receive events.

Wire (codec version stays `1`; peers upgrade together):

| Kind | Direction | Layout after `version`, `kind` |
|---|---|---|
| `3` Subscribe | client → server | `id: u64`, `window: u32` (nonzero), payload |
| `4` Item | server → client | `id: u64`, payload |
| `5` End | server → client | `id: u64` |
| `6` Credit | client → server | `id: u64`, `grant: u32` (nonzero) |
| `7` Cancel | client → server | `id: u64` |

A subscription that fails ends with the existing `2` Error frame. Ids share
the connection's id space with calls.

Flow control is per subscription: the server may send at most `window` items
before it receives Credit, and each Credit adds `grant`. The client grants
credit as the application consumes items, so one slow subscription never
blocks the connection's other calls or subscriptions. An item over the frame
limit ends the subscription with `RESPONSE_TOO_LARGE`.

API shape: `RpcServer::with_subscriptions(call_handler, subscribe_handler)`
where the subscribe handler gets the caller, the payload and a
`SubscriptionSink` whose `send` waits for credit and fails once the client
cancels or disconnects; returning `Ok(())` sends End, `Err(status)` sends
Error. `RpcClient::subscribe(peer, payload, timeout)` returns a
`Subscription` whose `next` yields items, then `None` after End, or an
`RpcError` (`Remote`, `ConnectionLost`, `Shutdown`). Dropping it sends Cancel.
Live subscriptions keep a connection from being idle on both sides, and both
sides bound them (`max_subscriptions_per_peer`,
`max_subscriptions_per_connection`).

## 5. Versions

Clients follow the same rule as members: every endpoint of one deployment runs
the same groupnet build, and groupnet negotiates nothing with another version.
Each handshake and codec keeps its magic and version byte only as a guard: a
mis-deployed client is refused with a typed I/O error (`InvalidData`) instead
of being misread. An application that ships clients separately versions its own
payloads.

## 6. Later slices

### 6.1 Dynamic clients over TCP (slice 6)

`TcpBulkTransport` authenticates connection setup with a shared
`ClusterSecret` or not at all, and its stream bytes carry no integrity
protection. A client over a network needs its own identity: the client makes a
key pair for the session, admission binds that key to the client's credential,
and the hub pins it for the length of the session, so the stream runs over
mutually authenticated TLS. A network link never falls back to unauthenticated
traffic.

### 6.2 Runtime client role (slice 7)

A client of the managed network keeps the `Endpoint<P>` and `Peer<P>` handle
family (`Messages`, `Ordered`, `Unordered`) but has no group API: it joins no
group, not even the reserved routing group, gossips nothing and forwards
nothing. Because a setting that cannot apply in a mode must not exist in that
mode, it is its own type (a `Client` built by the node builder's client form)
that shares the protocol engines with `Node`, not a `Node` whose
`join_group` fails. Its links are dial-only and admitted (section 2), and
`RouterConfig::forwarding` is `false`.

### 6.3 Not planned here

VPN interfaces, permission-group connection policies, a WebSocket link and
public-Internet NAT qualification (#6).

## 7. Build order

| Slice | Content | Status |
|---|---|---|
| 1 | This contract; charter in `README.md` and `AGENTS.md` | done |
| 2 | IPC admission: dynamic peers, seeds, dial-only links, revocation | done |
| 3 | `IpcBulkTransport` with admission | in progress |
| 4 | RPC subscriptions | in progress |
| 5 | Hub example over IPC (RPC calls and a subscription from dynamic clients) | open |
| 6 | Dynamic clients over TCP with per-session keys pinned at admission | open |
| 7 | Runtime client role | open |
