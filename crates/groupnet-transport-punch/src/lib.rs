//! Authenticated UDP discovery, simultaneous hole punching and relay fallback.
//!
//! Provision a [`NetworkKey`] and explicit peer allowlists out of band. The key
//! authenticates trusted-fabric membership, not Byzantine endpoint identity.
//! [`Rendezvous`] sees identities, addresses and raw transport messages.
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
