# Groupnet documentation

## Architecture diagram guides

Start here for the managed-network design. These four guides contain 14 Mermaid
diagrams covering component/dependency maps, startup and routing sequences,
packet flow, ownership/cleanup, and security boundaries.

| Guide | Diagrams | Questions answered |
|---|---:|---|
| [Architecture](architecture.md) | 3 | How do applications, coordination, routing, and protocol crates fit together? Why is the core sans-IO? |
| [Link registration and lifecycle](link-lifecycle.md) | 4 | How do providers start? Where is dynamic dispatch? Who owns cleanup, rollback, and shutdown? |
| [Routing](routing.md) | 3 | How do heterogeneous peers communicate? How are paths learned and packets forwarded or dropped? |
| [Tunnels and security](tunnels-and-security.md) | 4 | How do TLS, admission, native discovery/relay, and application permissions differ? |

Read in the order above for an end-to-end walkthrough. The architecture guide
also explains [coordination](architecture.md#coordination-model), the difference
between metadata and packet routing, and why `Group::sync` is not a distributed
barrier. The guides replace the former technical overview; each topic now has
one home and links to its implementation sources.

### Viewing the diagrams

Open these Markdown files on GitHub, which renders fenced `mermaid` blocks,
or use a Markdown preview with Mermaid support. Diagram source lives directly
in each document; no generated images or project build dependencies are required.

Within a diagram, solid arrows show the relationship or flow described in its
caption. Dashed arrows represent a logical relationship, feedback, or an explicit
non-guarantee as labeled. Dependency arrows mean **depends on** only in the crate
dependency diagram; sequence arrows show message/call order, not physical timing.

## Consistency and source-backed replication

These are the detailed design contracts, not additional layers automatically
implied by membership. Their implementation-status sections distinguish library
support from application adapters and downstream consumer migrations.

| Contract | Scope |
|---|---|
| [Consistency modes](consistency-modes.md) | Eventual metadata, session/coherence tiers, Hosted authority, consumer obligations, and the pinned build order |
| [Source-backed replication](replication.md) | Native replay/checkpoints, scheduling, idle checks, source-ordered admission, limits, and implementation status |
| [Snapshot recovery](replication-snapshots.md) | Opt-in capture/retention holds, guarded install, tail attachment, cancellation, and cleanup |
| [Named subscriptions and acknowledgement waits](replication-subscriptions.md) | Durable registration, subscriber fencing, source retention, terminal/reset semantics, and fixed-roster wait runtime |

## Volatile origin-backed recovery

A volatile cache without a committed source cursor needs a different recovery
contract. Gossip heads, membership observations, and donor images do not turn an
S3 origin into a durable publication log or certify application authority.

| Contract | Scope |
|---|---|
| [Coherence recovery](replication-volatile-coherence.md) | Finite origin rebuild/read-gate recovery and optional rearm |
| [Peer bootstrap and image transfer](replication-volatile-bootstrap.md) | Builder claims, bounded donor journals, exact image/session identity, transfer, and attach/barrier rules |
| [Bootstrap runtime and native claim source](replication-volatile-transfer-runtime.md) | Worker ownership, capability interfaces, native TTL observations, deadlines, and runtime binding |
| [Bootstrap bulk transport and adapter](replication-volatile-bulk.md) | Bounded wire/client/listener behavior and integration into the existing recovery worker |
| [Bootstrap membership binding](replication-volatile-membership.md) | Participation lifetimes, source-ordered publication, withdrawal, and actor inspection |

Read each contract's status before assuming a capability is integrated into a
particular consumer. Permission-group enforcement, private-peer discovery, and
VPN interfaces are not implemented. Public-Internet NAT, Unix runtime behavior,
and physical USB/YubiKey operation require deployment-specific qualification.

For feature selection and public API examples, see the
[repository README](../README.md). For engineering and verification rules, see
[AGENTS.md](../AGENTS.md).
