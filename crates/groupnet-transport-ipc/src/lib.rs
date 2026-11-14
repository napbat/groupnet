//! Persistent, bounded native local IPC for Groupnet.
//!
//! Two planes over one platform layer (Unix sockets, Windows named pipes):
//!
//! * **Control plane** — [`IpcTransport`] exposes the standalone message
//!   transport. [`IpcLink`] binds it through the router-independent
//!   [`groupnet_transport::link::LinkProvider`] contract, configuring peers
//!   and the framing MTU automatically. It preflights its configuration and
//!   every configured peer before opening the listener; its lifecycle cancels
//!   and drains the listener and sessions on rollback or router shutdown.
//! * **Data plane** (feature `bulk`) — `IpcBulkTransport` implements the
//!   reliable, ordered `BulkTransport` stream contract. A hub binds it and
//!   admits clients it was never configured with through an optional
//!   admission policy; a client made with `IpcBulkTransport::dial_only`
//!   registers hub addresses and never listens. Its handshake (`GNIB`,
//!   version 1) carries the dialer's id and an opaque credential, bounded in
//!   time and number.
//!
//! Unix callers create a private owner-only directory before binding. Windows
//! listeners reserve local-only named pipes. Identities are trusted within the
//! operating system's security boundary, not cryptographically authenticated.
//!
//! Binding is synchronous (`IpcTransport::bind` / `bind_with`, and likewise
//! `IpcBulkTransport::bind` / `bind_with`): a local listener needs no name
//! resolution, but it must run inside a Tokio runtime that hosts its workers.
//! The `groupnet-transport` crate docs compare the construction contract of
//! every binding.
//!
//! The shared transport types these signatures name — [`QueueCapacity`]
//! (in [`IpcConfig`]), [`PeerEndpoint`] (in [`IpcLink::new`]),
//! [`NodeIdError`] (in [`IpcPeerError`]) and, with `bulk`, `Admission` (in
//! `IpcBulkConfig`) — are re-exported here.
#![cfg_attr(docsrs, feature(doc_cfg))]

#[cfg(feature = "bulk")]
mod bulk;
mod ipc;
mod link;

#[cfg(feature = "bulk")]
pub use bulk::{
    IpcBulkConfig, IpcBulkConfigError, IpcBulkDialConfig, IpcBulkStream, IpcBulkTransport,
};
#[cfg(feature = "bulk")]
pub use groupnet_transport::admission::Admission;
pub use groupnet_transport::link::PeerEndpoint;
pub use groupnet_transport::{NodeIdError, QueueCapacity};
pub use ipc::{
    IpcAddress, IpcAddressError, IpcConfig, IpcConfigError, IpcPeerError, IpcSendError,
    IpcTransport, MAX_FRAME,
};
pub use link::IpcLink;
