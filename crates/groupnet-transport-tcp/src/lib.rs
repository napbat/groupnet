//! # groupnet-transport-tcp
//!
//! Groupnet's TCP bindings — one crate, both planes, each behind its own
//! default-on feature:
//!
//! * **`msg` — control plane.** `TcpMsgTransport` implements the best-effort,
//!   message-oriented [`Transport`] over a bounded pool of **persistent**
//!   connections: dialed lazily on first send, reused, closed when idle,
//!   oldest-evicted at the cap. The constant-connection alternative to the
//!   UDP binding for deployments that want it — see its docs for the exact
//!   pooling behaviour.
//! * **`bulk` — data plane.** `TcpBulkTransport` implements `BulkTransport`:
//!   one reliable, ordered byte stream per `connect`, for replication and
//!   bulk transfer. Both ends set `TCP_NODELAY`; inbound identity handshakes
//!   run concurrently under a deadline (`TcpBulkConfig`), so a stalled client
//!   never delays other accepts. With a `ClusterSecret`
//!   (`TcpBulkConfig::secret`, `TcpBulkTransport::dial_only_with`) the
//!   versioned handshake is a mutual challenge: both sides prove they hold the
//!   cluster's secret before a stream byte flows, and every refusal counts in
//!   `TcpBulkTransport::auth_failures`. Without one, any party that reaches
//!   the listener can claim any node id: run such endpoints only on a private
//!   network.
//! * **`tls` — data plane across a trust boundary** (opt-in, implies `bulk`):
//!   `TcpBulkTransport<TcpTls>` runs every stream over mutually authenticated
//!   TLS 1.3 with RFC 7250 raw public keys. `TcpBulkTransport::bind_tls` makes
//!   a hub with a long-lived `TcpTlsKey` that admits clients it was never
//!   configured with through an `Admission` policy (`TcpTlsConfig`);
//!   `TcpBulkTransport::dial_only_tls` makes a client (`TcpTlsDialConfig`)
//!   that pins each hub's `TcpTlsPublicKey`, generates a fresh key for every
//!   connection and presents an opaque credential inside TLS. The admitted
//!   stream (`TcpTlsStream`) is that TLS connection, so the client's claim
//!   stays bound to its key. The plain mode (`TcpPlain`, the default type
//!   parameter) and the TLS mode refuse each other; nothing falls back to
//!   cleartext.
//! * **`link` — router registration** (opt-in): `TcpLink` admits peers through
//!   an explicit application policy (a configured-peer allowlist by default).
//!   Managed sessions are full-duplex, bounded, and retained while connected;
//!   only live admitted sessions become routing neighbors. Its admission
//!   setters exist only on a direct link (`TcpLink<TcpDirect>`); a native link
//!   (`TcpLink<TcpConnectivity>`) takes its admission from `TcpPunchConfig`, so
//!   settings it would ignore cannot be written. `TcpMsgTransport::bind_admitted`
//!   takes the same policy, credential and bounds bundled as a `TcpAdmission`.
//! * **`connectivity` — native TCP paths** (opt-in): the existing message transport
//!   and `TcpLink` can own a `TcpConnection` from `groupnet-transport-punch`,
//!   with rendezvous admission, candidate traversal, and maintained relay fallback.
//!   Use `TcpMsgTransport::bind_connectivity` or `TcpLink::connectivity`; no separate
//!   transport or link type is registered. Native paths retain their protocol MTU
//!   and live admission generations across conversion into a managed link.
//!   `TcpMsgTransport::path_changes` observes direct/relay path changes.
//!   The configuration and path types these signatures name (`TcpPunchConfig`,
//!   `TcpPunchConfigError`, `PeerPolicy`, `NetworkKey`, `PathPolicy`,
//!   `PeerPath`) and the `PunchSendError` a connected send carries are
//!   re-exported here, so standalone users need no direct
//!   `groupnet-transport-punch` dependency; only a self-hosted rendezvous
//!   (`TcpRendezvous`) comes from that crate.
//!
//! `TcpMsgTransport::closed` resolves, stickily, once an endpoint shuts down.
//! `TcpMsgTransport::register_peer` records a direct endpoint's dial address
//! and rejects a connectivity endpoint with `TcpRegisterPeerError`;
//! `TcpMsgTransport::connect_peer` refuses a direct endpoint bound without
//! admission with `TcpConnectPeerError`. `TcpBulkTransport::register_peer_host`
//! names a malformed `host:port` endpoint's violated rule with
//! `TcpHostEndpointError`.
//!
//! The low-level message and bulk APIs retain trusted-topology identity
//! attribution unless the bulk plane holds a cluster secret. Frames a router
//! carries over this crate's message plane are authenticated by the router's
//! own cluster secret (`groupnet_network::RouterConfig::cluster_secret`), not
//! here. Managed message admission uses its own bounded wire exchange, with
//! no downgrade to that raw handshake. Open admission is unauthenticated.
//! Every handshake introduces a node id of 1 to
//! `groupnet_transport::MAX_NODE_ID_BYTES` bytes, the bound session admission
//! enforces.
//!
//! The shared transport types these signatures name are re-exported at this
//! crate's root, each with the feature that exposes it: `QueueCapacity`
//! (`msg` or `bulk`), `NodeIdError`, `ClusterSecret` and `ClusterSecretError`
//! (`bulk`), `Admission` (`link` or `tls`), `BoundLink`, `PeerEndpoint` and
//! `SessionRegistry` (`link`), and the `rustls` crate, whose error a
//! `TcpTlsKeyError` carries (`tls`).
//!
//! [`Transport`]: groupnet_transport::Transport
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(any(feature = "bulk", feature = "msg"))]
mod handshake;

#[cfg(feature = "bulk")]
mod bulk;
#[cfg(feature = "bulk")]
pub use bulk::{
    TcpBulkConfig, TcpBulkConfigError, TcpBulkTransport, TcpHostEndpointError, TcpPlain,
};
#[cfg(feature = "tls")]
pub use bulk::{
    TcpTls, TcpTlsConfig, TcpTlsConfigError, TcpTlsDialConfig, TcpTlsKey, TcpTlsKeyError,
    TcpTlsPublicKey, TcpTlsPublicKeyError, TcpTlsStream,
};
#[cfg(feature = "bulk")]
pub use groupnet_transport::NodeIdError;
#[cfg(any(feature = "bulk", feature = "msg"))]
pub use groupnet_transport::QueueCapacity;
#[cfg(any(feature = "link", feature = "tls"))]
pub use groupnet_transport::admission::Admission;
#[cfg(feature = "bulk")]
pub use groupnet_transport::auth::{ClusterSecret, ClusterSecretError};
#[cfg(feature = "link")]
pub use groupnet_transport::{
    admission::SessionRegistry,
    link::{BoundLink, PeerEndpoint},
};
#[cfg(feature = "tls")]
pub use rustls;

#[cfg(feature = "msg")]
mod msg;
#[cfg(feature = "connectivity")]
pub use groupnet_transport_punch::{
    NetworkKey, PathPolicy, PeerPath, PeerPolicy, PunchSendError, TcpPunchConfig,
    TcpPunchConfigError,
};
#[cfg(feature = "connectivity")]
pub use msg::TcpPathChanges;
#[cfg(feature = "link")]
pub use msg::{TcpAdmission, TcpAdmissionConfig, TcpAdmissionError, TcpConnectPeerError};
#[cfg(feature = "msg")]
pub use msg::{TcpMsgConfig, TcpMsgConfigError, TcpMsgTransport, TcpRegisterPeerError};

#[cfg(feature = "link")]
mod link;
#[cfg(feature = "msg")]
mod tasks;
#[cfg(feature = "connectivity")]
pub use link::TcpConnectivity;
#[cfg(feature = "link")]
pub use link::{TcpDirect, TcpLink};
