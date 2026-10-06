//! UDP discovery, simultaneous keyed hole punching, and relay fallback.
//!
//! [`PunchConfig::new`] preserves explicitly keyed, allowlisted fabrics.
//! [`PunchConfig::open`] and [`Rendezvous::bind_open`] explicitly opt into
//! keyless dynamic relay with no cryptographic identity assurance. Keyless
//! direct punching is unsupported and rejected, not silently downgraded.
//! Application policies use [`Rendezvous::bind_with_admission`]. Every
//! registration proves UDP address return-routability before discovery or relay.
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
