//! # groupnet-transport-udp
//!
//! A real [`Transport`] over UDP datagrams — a concrete binding of Groupnet's
//! transport-agnostic trait.
//!
//! UDP fits the best-effort, message-oriented contract: one frame per datagram,
//! loss and reorder tolerated. The opt-in `link` feature exposes `UdpLink` for
//! router registration with explicit peer identities and addresses. Raw mode owns
//! its bound socket without independently spawned tasks.
//!
//! The optional `connectivity` feature lets the same `UdpTransport` and `UdpLink`
//! own a native UDP connection from `groupnet-transport-punch`: admission,
//! candidate checks, maintained direct paths and relay fallback all use that
//! connection's actual leased sockets. Use `UdpTransport::bind_connectivity` or
//! `UdpLink::connectivity`; transfer an already-bound endpoint with
//! `UdpTransport::into_bound_link` to retain sessions, native MTU and task cleanup.
//! Connected mode never admits peers from `register_peer`, advertisements or raw
//! datagram self-attribution.
//!
//! ## Raw datagram behavior
//!
//! * **Seeded address book.** The engine speaks only in [`NodeId`]s, so this
//!   transport maps them to socket addresses via a book: seeds are registered
//!   up front ([`register_peer`](UdpTransport::register_peer)) or named
//!   (`host:port`) and kept current by the runtime's `NamedSeeds`, and the
//!   rest arrives from gossiped `advertise_addr` values. Both of those reach
//!   the book through `Transport::learn_peer` (the runtime feeds them
//!   automatically). Inbound datagrams carry their sender's id, so a peer the
//!   book has never heard of is attributed and learned from its first frame.
//! * **One frame per datagram.** A raw frame must fit in a single UDP packet.
//!   Managed connected links advertise the native protocol's smaller message MTU
//!   so routing can fragment frames before sending.
//!
//! [`Transport`]: groupnet_transport::Transport

//! [`NodeId`]: groupnet_core::NodeId

mod udp;

pub use udp::UdpTransport;

#[cfg(feature = "link")]
mod link;
#[cfg(feature = "link")]
pub use link::UdpLink;
