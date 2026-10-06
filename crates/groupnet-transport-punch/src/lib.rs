//! UDP discovery, session-bound hole punching, and relay fallback.
//!
//! [`PunchConfig::new`] preserves explicitly keyed, allowlisted fabrics.
//! [`PunchConfig::open`] defaults to [`PathPolicy::RelayOnly`]. Explicitly setting
//! [`PunchConfig::policy`] to [`PathPolicy::DirectPreferred`] enables keyless
//! punching: no pre-shared transport key or identity keypair is required.
//! Internal fresh random challenges and capabilities prove return-routability,
//! not cryptographic identity. Application admission may independently require
//! credentials through [`Rendezvous::bind_with_admission`]; keyed configurations
//! retain strict HMAC authentication and never downgrade.
//! Every registration proves UDP address return-routability before discovery or
//! relay. Direct data additionally requires a current session-bound capability.
//! Relay-only pair discovery omits physical endpoint addresses and never probes.
//! The rendezvous sees identities, addresses, and raw transport messages; it is
//! not a participant in the routing group.
//!
//! [`PunchTransport`] preserves the standalone transport API. [`PunchLink`]
//! implements [`groupnet_transport::link::LinkProvider`] without depending on a
//! concrete router, validates the bound node identity, and advertises
//! [`MAX_MESSAGE`] as its MTU. Its lifecycle cancels and drains owned UDP I/O on
//! registration rollback and router shutdown. Messages remain best-effort
//! datagrams; this transport does not fragment oversized payloads.

mod link;
mod punch;

pub use link::PunchLink;
pub use punch::{
    MAX_MESSAGE, NetworkKey, PathPolicy, PeerPath, PunchConfig, PunchTransport, Rendezvous,
};
