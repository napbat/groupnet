# Tunnels and security boundaries

[Documentation index](README.md) · [Previous: routing](routing.md)

Reachability, routing-fabric trust, tunnel identity, and application authorization
are different decisions. Coordination-group membership does not grant permission
to open a protected resource.

## 1. Authenticated streams across a bridge

Both endpoints configure a `TlsIdentity` and explicit `PeerIdentity` certificate
pins. `Node` and standalone `Network` implement `BulkTransport` through
this tunnel endpoint. Without tunnel configuration, `connect` and `accept` return
`Unsupported`; there is no plaintext fallback.

```mermaid
sequenceDiagram
    participant AppA as App A
    participant A as Endpoint A
    participant B as Transit bridge
    participant C as Endpoint C
    participant AppC as App C
    AppC->>C: accept()
    AppA->>A: connect(C)
    A->>A: Require admitted peer C
    A->>B: Reliable tunnel packets carrying TLS handshake
    B->>C: Forward routed packets
    C->>C: Require admitted peer A, authenticate certificate
    C->>B: TLS handshake response packets
    B->>A: Forward routed packets
    A->>A: Authenticate C, verify pin and protocol
    Note over A,C: Complete mutual TLS and authenticated preamble exchange
    A-->>AppA: TunneledStream
    C-->>AppC: Authenticated NodeId and TunneledStream
    AppA->>A: Application bytes
    A->>B: Encrypted application data in tunnel packets
    B->>C: Forward ciphertext
    C-->>AppC: Decrypted application bytes
    Note over B: Sees routing and traffic metadata, not application plaintext
```

TLS 1.3 authenticates the endpoints, not each intermediate bridge. Certificates
must include the `groupnet.peer` DNS SAN and client/server authentication usages.
The reliable tunnel transport supplies ordered segments, acknowledgements,
receive credit, retransmission, and congestion control below TLS. A changed route
does not itself create a new application stream or replay delivered application
bytes. This is not a guarantee of availability through arbitrary outages.

### Reliable stream contract

- Bounded ordered segments use cumulative acknowledgements and receive credit.
- RTT-based retransmission deadlines and duplicate-ACK gap repair handle loss;
  additive-increase/multiplicative-decrease controls congestion.
- A retry keeps its tunnel sequence number but uses a fresh router packet ID.
  Router replay suppression therefore does not discard legitimate retries.
- FIN closes one stream direction. The other remains usable until it closes
  independently; a half-close can still receive a response.
- Route changes select another next hop below the existing authenticated
  session. They do not terminate TLS or restart application delivery.


## 2. Peer admission and revocation

Admission is local and bidirectional: it permits this tunnel endpoint to connect
to or accept that peer. The other endpoint must independently admit the matching
identity. It is not a directional permission-group policy.

```mermaid
stateDiagram-v2
    [*] --> NotAdmitted
    NotAdmitted --> Admitted: admit_peer installs a pin
    Admitted --> Admitted: Same pin preserves current sessions
    Admitted --> Admitted: New pin invalidates old sessions and installs new admission
    Admitted --> NotAdmitted: revoke_peer invalidates active and queued streams
    Admitted --> Closed: close cancels and drains tunnel tasks
    NotAdmitted --> Closed: close
    Closed --> [*]
```

New sessions after pin replacement must authenticate against the new admission.
Revocation does not retract routes, rotate a shared network key, or revoke an
application's USB ownership. Applications still need their own resource policy.

## 3. Native discovery, direct paths, and relay

`UdpLink::connectivity` uses an explicitly started, self-hosted `Rendezvous`. In keyed mode,
the shared `NetworkKey` authenticates the routing fabric. Registration includes
a challenge response tied to the observed source address; leases, admission,
replay checks, and bounds constrain discovery and relay traffic.

Dynamic admission is distinct from tunnel certificate admission. An explicit
open policy permits previously unknown routing identities without a key pair.
Custom policies can require application credentials. A claimed identity and a
return-routability check do not authenticate a person or prove identity continuity.
Never send reusable secrets over an unencrypted admission exchange.

**Keyless** specifically means no provisioned `NetworkKey` (`key: None`); no
public/private key pair is required by the connection library either. It does
not forbid application credentials or separately authenticated TLS streams.
Fresh random protocol capabilities remain mandatory internal session mechanics,
not application-managed identity keys.

Path selection is independent of admission and configured-key authentication.
`PunchConfig::open` defaults to `RelayOnly`; setting its public `policy` field to
`DirectPreferred` enables punching with relay fallback. A pair stays relay-only
if either endpoint requests it: neither endpoint is offered the other's socket
address, and neither sends or accepts direct probes/data for that pair. The
rendezvous still knows both observed addresses; relay-only is not anonymity
against the operator or a network observer.

Registration establishes a private control proof that is never included in peer
discovery records. Established rendezvous requests and responses must match that
proof before mutating session, sequence, or lease state. Direct paths additionally
exchange fresh, session-bound challenges and responder capabilities; a discovery
record alone cannot authorize direct data. Pending/confirmed state is bounded,
unproven probes cannot replace confirmed receive capabilities or advance data
replay state, and expired direct paths fall back to a live relay registration.
Reconnection revokes the old generation and its capabilities.

In keyless mode these capabilities travel without encryption. They constrain
off-path spoofing; they do not resist an observer who captures them, authenticate
a claimed identity, or make routing participants trustworthy. The rendezvous is
trusted to introduce peers. Open admission does not make the fabric Byzantine-safe.
Database engines and other consumers choose their own trust and authorization
policies; no application-specific admission policy is imposed by Groupnet.

### Native multi-candidate traversal contract

UDP and TCP connection establishment remain distinct protocol implementations
in the custom connectivity library, consumed by the actual TCP/UDP transport
adapters. The library implements neither `Transport` nor router registration.
Native candidate exchange is not ICE/STUN/TURN. Candidate
gathering, bounded session-bound checks, and stable path selection have separate
responsibilities. Working relay paths remain usable while direct checks run.
Local, observed, and explicitly configured addresses are untrusted candidates;
neither advertising an address nor receiving an unsolicited packet proves a path.
IPv4 and IPv6 candidates must be checked using compatible owned sockets.

TCP traversal requires TCP rendezvous and relay connections, independent of UDP.
Direct attempts coordinate reusable source-port connections with passive accepts
where supported by the OS. Ordinary dialing from an unrelated ephemeral port is
not evidence of TCP hole punching. Simultaneous-open and NAT behavior vary by
platform; failed direct attempts must retain relay operation, not claim success.
Neither transport provides a universal NAT traversal guarantee.

Logical route bridging is separate from adjacent connection establishment. A
memory-only node reaches a remote TCP node through an edge with both adapters.
The router can prefer a cheaper compatible admitted adjacency when one becomes
available and retain transit routes otherwise. Group membership, addresses, and
route advertisements do not grant admission or create unsupported transports.

Admission, duplicate-identity rejection, producing-session generation, and
bounded queues apply to every candidate and path. Relay-only participants do not
disclose candidate addresses or initiate direct checks. Path replacement cannot
move old queued traffic into a replacement session, and an unverified candidate
cannot replace a working path. Application encryption remains separately chosen.

Both endpoint configurations expose up to three additional candidate binds and
eight explicit advertised addresses, plus optional interface gathering. UDP
retains the separately observed mapping alongside those advertisements and caps
active candidate/socket checks at 32, rotating unvalidated checks as necessary.
Authenticated peer-reflexive sources can replace an unvalidated slot, never a
healthy selected path. UDP final data and confirmations require pair-secret MACs:
forwarding a captured authentic probe and learning its returned capability is
not sufficient to inject data or advance replay state.

TCP direct and keyed control streams derive separate transmit/receive MAC keys
and authenticate monotonic frame sequences. Reflection and replay cannot convert
locally emitted traffic into traffic attributed to the remote peer. Direct
handshakes include fresh mutual challenges, including the two-active-open case.
At most sixteen candidate dials run concurrently, with at most sixteen new checks
per second; per-tuple bursts and cooldowns prevent indefinite aggressive probing.
Rendezvous established events take precedence over unauthenticated accepts.

TCP admission lasts for the live admitted session. Losing either peer's rendezvous
connection does not revoke a healthy direct session, but disables that peer's relay
fallback and further candidate checks. The generation is withdrawn when its direct
stream closes; a fresh rendezvous admission can supersede it with a new generation.
An endpoint with a lost local control connection terminates when no usable direct
sessions remain, rather than remaining inert; callers must explicitly rebind.
UDP retains its existing rendezvous/discovery lease coupling. Neither TCP control
nor its relay is automatically TLS-encrypted or an HTTP proxy protocol.

The following path-selection flow applies with or without a provisioned key:

```mermaid
flowchart TD
    Start["TCP/UDP link configured with connectivity, credentials, optional NetworkKey"]
    Register["Address-verified, admitted rendezvous session"]
    Policy{"PathPolicy"}
    Probe["DirectPreferred\nExchange discovery information and probe peers"]
    Direct["Usable direct UDP path"]
    Relay["Rendezvous relays peer packets"]
    Routed["Same registered link carries routed frames"]
    Start --> Register --> Policy
    Policy -->|"DirectPreferred"| Probe
    Policy -->|"RelayOnly: no direct probes"| Relay
    Probe -->|"Direct path available"| Direct
    Probe -->|"No usable direct path"| Relay
    Direct --> Routed
    Relay --> Routed
```

This is a path-selection overview, not the packet-level rendezvous state machine.
Not every NAT permits a direct path. The rendezvous must be reachable for the
configured operation; there is no implicit public relay service. Shared-key
routing authentication is not confidentiality: use pinned TLS tunnels for
protected application bytes. Shared-key holders are trusted and can impersonate
routing aliases; the fabric is not Byzantine-safe.

The native UDP format is `GNP4`; TCP frames use version 2. Each rendezvous and
its endpoints must upgrade together. Earlier formats and authentication-mode
mismatches are rejected without compatibility fallback.

## 4. What each security layer actually grants

These are separate checks, not one inherited permission chain. Applications must
bind their authorization decisions to authenticated identities, not to a claimed
routing alias or advertised group name.

```mermaid
flowchart TB
    Neighbor["Configured adjacent peers / NetworkKey"] --> Fabric["Routing-fabric participation"]
    Pins["Pinned certificates + tunnel admission"] --> Identity["Authenticated encrypted stream"]
    Policy["Application-owned resource policy"] --> Access["Permission to use a resource"]
    Group["Coordination-group membership"] --> Metadata["Membership and coordination metadata"]
    Fabric -.->|"Does not grant"| Access
    Identity -.->|"Does not by itself grant"| Access
    Metadata -.->|"Does not grant"| Access
```

| Boundary | Implemented guarantee or limitation |
|---|---|
| Plain TCP/UDP links | No inherent endpoint authentication or encryption; static configurations assume trusted peers. |
| Open dynamic admission | Accepts claimed identities without ownership proof; session binding is not account authentication. |
| Native punching fabric | Keyed mode authenticates fabric membership; explicit keyless mode does not. |
| Pinned TLS tunnels | End-to-end endpoint identity and application-byte confidentiality across transit peers. |
| Transit bridges | Can observe routing/traffic metadata and deny service. |
| Application resources | Authorization and exclusive ownership remain the application's responsibility. |
| Permission groups and private-peer discovery | Not implemented; ordinary coordination groups are not security groups. |

Public-Internet NAT combinations, Unix runtime behavior, and physical USB/YubiKey
operation require deployment-specific qualification. The existing Windows
loopback smoke coverage does not establish those guarantees.

## Implementation references

- [Tunnel establishment, admission, revocation, and close](../crates/groupnet-network/src/tunnel.rs)
- [Native punching implementation crate](../crates/groupnet-transport-punch)
- [Configuration and public stream API](../README.md#node-owned-heterogeneous-connections)
