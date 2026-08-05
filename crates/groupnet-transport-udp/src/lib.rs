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
//!   up front ([`register_peer`](UdpTransport::register_peer)), and the rest
//!   arrives from gossiped `advertise_addr` values via `Transport::learn_peer`
//!   (the runtime feeds them automatically). Inbound datagrams are attributed
//!   by matching their source address, so a peer must be in the book before
//!   its frames are accepted.
//! * **One frame per datagram.** A frame must fit in a single UDP packet; very
//!   large clusters could exceed the MTU. Fragmentation / a stream fallback is
//!   future work.
//!
//! [`Transport`]: groupnet_transport::Transport

//! [`NodeId`]: groupnet_core::NodeId

mod udp;

pub use udp::UdpTransport;
