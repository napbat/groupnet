//! # groupnet-transport-udp
//!
//! A real [`Transport`] over UDP datagrams — a concrete binding of Groupnet's
//! transport-agnostic trait.
//!
//! UDP is the natural fit for the best-effort, message-oriented contract: one
//! frame per datagram, loss and reorder tolerated, no connection state.
//!
//! ## Scaffold simplifications
//!
//! * **Seeded address book.** The engine speaks only in [`NodeId`]s, so this
//!   transport maps them to socket addresses via a book: seeds are registered
//!   up front ([`register_peer`](UdpTransport::register_peer)) or named
//!   (`host:port`) and kept current by the runtime's `NamedSeeds`, and the
//!   rest arrives from gossiped `advertise_addr` values. Both of those reach
//!   the book through `Transport::learn_peer` (the runtime feeds them
//!   automatically). Inbound datagrams carry their sender's id, so a peer the
//!   book has never heard of is attributed and learned from its first frame.
//! * **One frame per datagram.** A frame must fit in a single UDP packet; very
//!   large clusters could exceed the MTU. Fragmentation / a stream fallback is
//!   future work.
//!
//! [`Transport`]: groupnet_transport::Transport

//! [`NodeId`]: groupnet_core::NodeId

mod udp;

pub use udp::UdpTransport;
