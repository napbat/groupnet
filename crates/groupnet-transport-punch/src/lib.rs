//! Native UDP and TCP multi-candidate traversal with session-bound relay fallback.
//!
//! [`UdpPunchConfig::new`] takes the local identity, the rendezvous, an
//! optional [`NetworkKey`] and a [`PeerPolicy`]: a static allowlist or dynamic
//! discovery, each carrying only its own data. Keyed fabrics default to
//! [`PathPolicy::DirectPreferred`]; keyless ones conservatively default to
//! [`PathPolicy::RelayOnly`], and explicitly setting [`UdpPunchConfig::policy`]
//! to `DirectPreferred` enables keyless punching: no pre-shared transport key or
//! identity keypair is required.
//! Internal fresh random challenges and capabilities prove return-routability,
//! not cryptographic identity. Application admission may independently require
//! credentials through the [`Admission`] policy a rendezvous configuration
//! takes; keyed configurations retain strict HMAC authentication and never
//! downgrade.
//! UDP registration proves address return-routability before discovery or relay.
//! Direct data requires current session-bound path proofs and message authentication.
//! Relay-only pair discovery omits physical endpoint addresses and never probes.
//! The rendezvous sees identities, addresses, and raw transport messages; it is
//! not a participant in the routing group.
//!
//! Each protocol has one endpoint configuration ([`UdpPunchConfig`],
//! [`TcpPunchConfig`]) and one rendezvous configuration ([`UdpRendezvousConfig`],
//! [`TcpRendezvousConfig`]); each takes its identity and security inputs in
//! `new` and keeps its operational policy in public fields with documented
//! defaults. The endpoint and rendezvous configurations expose `validate`,
//! returning a typed error that names the violated field (keeping a rejected
//! identity's [`NodeIdError`] as its `source`); each binder calls it first and
//! surfaces a failure as `InvalidInput` wrapping that typed error. Their nested
//! limits are valid by type, except [`TcpRendezvousLimits`], whose one
//! cross-field rule has its own `validate`; [`UdpRendezvousLimits`] needs none.
//!
//! The shared transport types these signatures name — [`QueueCapacity`],
//! [`NodeIdError`], [`Admission`], [`SessionRegistry`], [`SessionId`],
//! [`Bytes`], [`Inbound`] and [`AdmittedInbound`] — are re-exported here.
//!
//! [`UdpConnection`] and [`TcpConnection`] establish and maintain adjacent packet
//! paths, with bounded best-effort I/O, admission generations, and explicit
//! cancellation/drain. They do not implement router transport or link traits.
//! TCP/UDP transport adapters own routing integration; the router alone performs
//! logical multi-hop forwarding. UDP does not fragment oversized payloads: a
//! message over [`MAX_UDP_MESSAGE`] or [`MAX_TCP_MESSAGE`] is refused as
//! `InvalidInput` carrying [`PunchSendError`].
//! Local bind addresses and candidates are cached at bind and retained after
//! shutdown; peer paths and observed addresses are live queries.
//!
//! [`TcpPunchConfig`], [`TcpConnection`], and [`TcpRendezvous`]
//! provide independent TCP-only discovery, coordinated reusable-source-port active
//! opens, passive acceptance, and framed relay traffic. TCP does not depend on UDP.
//! Both configurations support additional candidate binds, explicit advertised
//! hints, and interface gathering; candidates are never authorization by themselves.
//! Validated TCP streams survive rendezvous loss; when none remain the endpoint
//! terminates and callers must rebind. TCP control does not implicitly reconnect.
//! Neither native protocol implements ICE/STUN/TURN or encrypts application bytes.
//! Actual traversal depends on OS/NAT behavior; unsupported direct paths retain
//! relay operation where that relay remains reachable.

mod common;
mod pacing;
mod peers;
mod punch;
mod tcp;

pub use common::PunchSendError;
pub use groupnet_transport::admission::{Admission, SessionId, SessionRegistry};
pub use groupnet_transport::link::AdmittedInbound;
pub use groupnet_transport::{Bytes, Inbound, NodeIdError, QueueCapacity};
pub use pacing::RelayPacing;
pub use peers::PeerPolicy;
pub use punch::{
    MAX_UDP_MESSAGE, NetworkKey, PathPolicy, PeerPath, UdpConnection, UdpPunchConfig,
    UdpPunchConfigError, UdpRendezvous, UdpRendezvousConfig, UdpRendezvousConfigError,
    UdpRendezvousLimits,
};
pub use tcp::{
    ControlRateLimit, MAX_TCP_MESSAGE, TcpConnection, TcpPunchConfig, TcpPunchConfigError,
    TcpRendezvous, TcpRendezvousConfig, TcpRendezvousConfigError, TcpRendezvousLimits,
};
