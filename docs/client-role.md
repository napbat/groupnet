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
  matching the static IPC trust model. The acceptor answers `0` and its id,
  or `1` alone and closes. Handshakes are bounded in time and number, like
  `TcpBulkTransport`, and `IpcBulkConfig::max_connections` bounds the accepted
  streams alive at once (the native pipe instances on Windows). A dial-only
  endpoint takes `IpcBulkDialConfig`: only the settings a dialer uses.
- **TCP** (`groupnet-transport-tcp`): `TcpBulkTransport` has two modes, a
  type parameter. `TcpBulkTransport<TcpPlain>` (the default) is the cleartext
  cluster data plane: `bind`/`bind_with`, `dial_only`, `dial_only_with` (see
  `docs/tunnels-and-security.md` for its cluster-secret handshake).
  `TcpBulkTransport<TcpTls>` (feature `tls`) is the data plane for clients
  over a network, section 3.1.

### 3.1 TCP clients over TLS

API (feature `tls`; the facade's `tls` with `tcp`):

- `TcpTlsKey`: a private key. `generate()` makes an Ed25519 key;
  `from_pkcs8_der(Vec<u8>)` loads any PKCS#8 key the ring provider signs with;
  `secret_pkcs8_der()` returns it for storage; `public_key()` returns its
  `TcpTlsPublicKey`.
- `TcpTlsPublicKey`: a DER `SubjectPublicKeyInfo` (`from_der`, `as_der`,
  `fingerprint`), compared byte for byte; at most `MAX_DER_BYTES` (1024).
- `TcpTlsConfig::new(key, admission)`: the hub's long-lived key and its
  mandatory policy, plus public fields `credential`, `handshake_timeout` (5s),
  `max_handshakes` (64), `accept_queue` (64) and `max_connections` (1024);
  `validate()` returns `TcpTlsConfigError`.
- `TcpBulkTransport::bind_tls(local, addr, TcpTlsConfig)`: a hub. It also
  dials, as a client does.
- `TcpBulkTransport::dial_only_tls(local, TcpTlsDialConfig)`: a client with
  `credential` and `handshake_timeout`; it never listens and needs no runtime
  to be made.
- `register_peer(node, addr, key)` and `register_peer_host(node, host, key)`:
  an address and the pinned key together. A peer without a pin cannot be
  registered.
- The stream is `Compat<TcpTlsStream>`; `TcpTlsStream::peer_key()` is the
  hub's pinned key on a dialed stream and the client's per-connection key on
  an accepted one. An accepted stream holds one of `max_connections` until it
  is dropped.

Wire, in order:

1. TLS 1.3 only (ring provider), ALPN `groupnet/tcp-bulk/1`, no resumption,
   no early data, no SNI. Both sides authenticate with RFC 7250 raw public
   keys. The client offers a key it generated for this connection and accepts
   only the hub key it pinned for the dialed `NodeId`; the hub accepts any
   client key (the TLS handshake proves possession).
2. Client → hub, inside TLS: `GNBT`, version `1`, id, credential (each field
   prefixed by the shared big-endian `LengthHeader`; ids 1 to 64 bytes,
   credentials at most `MAX_CREDENTIAL_BYTES`, checked before allocation).
3. The hub refuses its own id, runs its policy with
   `JoinRequest { claimed, credential, remote: Some(addr) }` and requires the
   accepted id to be the claimed one. It answers `0` and its own id
   (length-prefixed), or `1` alone and closes.
4. The client checks the status and that the id is the hub it dialed. Stream
   bytes follow.

The admitted stream is that TLS connection, so the client's claim is bound to
its per-connection key for the stream's life. A TLS endpoint refuses a plain
`GNBH` hello and a plain endpoint refuses a TLS handshake. Every refusal or
timeout closes the connection and counts in `auth_failures`.

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
(and `with_subscriptions_config`, then `accept_on(plane)`) where the subscribe
handler gets the caller, the payload and a `SubscriptionSink` whose `send`
waits for credit; returning `Ok(())` sends End, `Err(status)` sends Error. A
Cancel or the end of the connection drops the handler's future, so a client
that stops reading cannot pin a subscription slot.
`RpcClient::subscribe(peer, payload, timeout)` returns once the Subscribe is
sent — the wire has no acknowledgement, so a refusal (no subscribe handler:
`SUBSCRIPTIONS_UNSUPPORTED`; the per-connection limit:
`TOO_MANY_SUBSCRIPTIONS`) is the first `next`. `next` yields items, then
`None` after End, or an `RpcError` (`Remote`, `ConnectionLost`, `Shutdown`).
Dropping the `Subscription` sends Cancel. End, Credit and Cancel frames have an
exact length, an item beyond the client's credit or credit beyond the window
is a protocol breach, and each drops the connection.
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

Done; section 3.1 is the contract. `TcpBulkTransport<TcpPlain>`
authenticates connection setup with a shared `ClusterSecret` or not at all,
and its stream bytes carry no integrity protection. A client over a network
uses `TcpBulkTransport<TcpTls>` instead: the client makes a key for each
connection, admission binds that connection to the client's claim and
credential, and the stream runs over mutually authenticated TLS. A network
endpoint never falls back to unauthenticated traffic.

### 6.2 Runtime client role (slice 7)

A client of the managed network keeps the `Endpoint<P>` and `Peer<P>` handle
family (`Messages`, `Ordered`, `Unordered`) but has no group API: it joins no
group, not even the reserved routing group, gossips nothing and forwards
nothing. Because a setting that cannot apply in a mode must not exist in that
mode, it is its own type, `groupnet_runtime::Client`, not a `Node` whose
`join_group` fails. Its links are normally dial-only and admitted (section 2),
and `RouterConfig::forwarding` is `false`.

API (`groupnet::runtime`):

- `Client::builder(id)` returns a `ClientBuilder`, the client form of
  `NodeBuilder`. It has only the settings that apply to a client: `link`,
  `links`, `routing`, `tunnels`, `messaging`, `ordered` and `unordered`. No
  seed, named seed, gossip, `Config`, group profile or address-advertisement
  setting exists on it. `start()` validates every policy once, before any link
  binds. The default routing configuration is `RouterConfig::default()` with
  `forwarding: false`; a `RouterConfig` with `forwarding: true` is refused as
  `ClientConfigError::Forwarding` (`InvalidInput`), never corrected.
- `Client` offers `id`, `router`, `tunnels`, `endpoint`, `peer`, the
  node-addressed `send`, `send_frame`, `recv`, `recv_frame`, `on_recv` and
  `on_frame`, `participant` and `close`, and implements `BulkTransport` over
  its default ordered endpoint, as `Node` does.
- `Participant` is the network side that a `Node` and a `Client` share: the
  identity, the owned network, the node-addressed inbox and its dispatcher, and
  the cached messaging, ordered and unordered engines. `node.participant()` and
  `client.participant()` return it, and every `Endpoint` and `Peer` retains it
  (`endpoint.participant()`). `PeerImplementation::bind` and the
  `ReceiveBinding` methods take `&Participant`, so one custom binding serves
  both roles. A `Participant` has no close of its own.
- A client starts no group actor, coordination receive loop, gossip, peer
  discovery or address sync. Coordination frames that reach it stay unread in
  the router's bounded queue. A group-addressed application frame sent to it
  is refused as a missing group (the sender sees `PermissionDenied`, as for a
  node that has not joined the group).
- `Client::close()` stops the protocol engines, closes the network (every
  link and its sessions), then waits for the messaging and session engines.
  It returns nothing: a client has no group grants to report.

A hub is an ordinary `Node`. It admits the client through its link admission
(for example `IpcLink::new(..).with_admission(..)`), and its discovery may list
the client as a bootstrap contact, but the client never answers group
protocol, so it never becomes a member of any group. Ordered and unordered
sessions between a client and its hub use the tunnel configuration
(`ClientBuilder::tunnels`) exactly as between two nodes.

Tests: `crates/groupnet-runtime/tests/client` (in-memory and IPC);
example: `crates/groupnet/examples/client.rs`.

### 6.3 Not planned here

VPN interfaces, permission-group connection policies, a WebSocket link and
public-Internet NAT qualification (#6).

## 7. Build order

| Slice | Content | Status |
|---|---|---|
| 1 | This contract; charter in `README.md` and `AGENTS.md` | done |
| 2 | IPC admission: dynamic peers, seeds, dial-only links, revocation | done |
| 3 | `IpcBulkTransport` with admission | done |
| 4 | RPC subscriptions | done |
| 5 | Hub example over IPC (RPC calls and a subscription from dynamic clients): `examples/hub.rs` | done |
| 6 | Dynamic clients over TCP with per-session keys pinned at admission: `TcpBulkTransport<TcpTls>` (section 3.1) | done |
| 7 | Runtime client role: `Client`, `ClientBuilder` and the shared `Participant` in `groupnet-runtime` | done |
