//! Native UDP and TCP multi-candidate traversal with session-bound relay fallback.
//!
//! [`PunchConfig::new`] preserves explicitly keyed, allowlisted fabrics.
//! [`PunchConfig::open`] defaults to [`PathPolicy::RelayOnly`]. Explicitly setting
//! [`PunchConfig::policy`] to [`PathPolicy::DirectPreferred`] enables keyless
//! punching: no pre-shared transport key or identity keypair is required.
//! Internal fresh random challenges and capabilities prove return-routability,
//! not cryptographic identity. Application admission may independently require
//! credentials through [`Rendezvous::bind_with_admission`]; keyed configurations
//! retain strict HMAC authentication and never downgrade.
//! UDP registration proves address return-routability before discovery or relay.
//! Direct data requires current session-bound path proofs and message authentication.
//! Relay-only pair discovery omits physical endpoint addresses and never probes.
//! The rendezvous sees identities, addresses, and raw transport messages; it is
//! not a participant in the routing group.
//!
//! [`UdpConnection`] and [`TcpConnection`] establish and maintain adjacent packet
//! paths, with bounded best-effort I/O, admission generations, and explicit
//! cancellation/drain. They do not implement router transport or link traits.
//! TCP/UDP transport adapters own routing integration; the router alone performs
//! logical multi-hop forwarding. UDP does not fragment oversized payloads.
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

mod pacing;
mod punch;
mod tcp;

pub use pacing::RelayPacing;
pub use punch::{
    MAX_MESSAGE, NetworkKey, PathPolicy, PeerPath, PunchConfig, Rendezvous, RendezvousConfig,
    RendezvousLimits, UdpConnection,
};
pub use tcp::{
    ControlRateLimit, MAX_TCP_MESSAGE, TcpConnection, TcpPunchConfig, TcpRendezvous,
    TcpRendezvousConfig,
};
