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
//! `UdpLink::connectivity` (a `UdpLink<UdpConnectivity>`, distinct from the
//! direct `UdpLink<UdpDirect>` built by `UdpLink::new`); transfer an
//! already-bound endpoint with `UdpTransport::into_bound_link` to retain
//! sessions, native MTU and task cleanup.
//! Connected mode never admits peers from advertisements or raw datagram
//! self-attribution, and its `register_peer` returns
//! `UdpRegisterPeerError::Connectivity` instead of recording an address. The
//! configuration, connection and path types these signatures name
//! (`UdpPunchConfig`, `UdpPunchConfigError`, `PeerPolicy`, `UdpConnection`,
//! `NetworkKey`, `PathPolicy`, `PeerPath`) and the `PunchSendError` a
//! connected send carries are re-exported here, so standalone users need no
//! direct `groupnet-transport-punch` dependency; only a self-hosted rendezvous
//! (`UdpRendezvous`) comes from that crate. With `link`, so are the shared
//! link types `UdpLink` and `into_bound_link` name (`BoundLink`,
//! `PeerEndpoint`, `SessionRegistry`).
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
//! * **One frame per datagram.** A raw frame must fit in a single UDP packet;
//!   a larger one is refused as `InvalidInput` carrying
//!   `UdpSendError::PayloadTooLarge`.
//!   Managed connected links advertise the native protocol's smaller message MTU
//!   so routing can fragment frames before sending.
//!
//! [`Transport`]: groupnet_transport::Transport

//! [`NodeId`]: groupnet_core::NodeId
#![cfg_attr(docsrs, feature(doc_cfg))]

mod udp;

pub use udp::{UdpRegisterPeerError, UdpSendError, UdpTransport};

#[cfg(feature = "link")]
pub use groupnet_transport::{
    admission::SessionRegistry,
    link::{BoundLink, PeerEndpoint},
};
#[cfg(feature = "connectivity")]
pub use groupnet_transport_punch::{
    NetworkKey, PathPolicy, PeerPath, PeerPolicy, PunchSendError, UdpConnection, UdpPunchConfig,
    UdpPunchConfigError,
};

#[cfg(feature = "link")]
mod link;
#[cfg(feature = "connectivity")]
pub use link::UdpConnectivity;
#[cfg(feature = "link")]
pub use link::{UdpDirect, UdpLink};
