//! Persistent, bounded native local IPC for Groupnet.
//!
//! [`IpcTransport`] exposes the standalone message transport. [`IpcLink`] binds
//! it through the router-independent [`groupnet_transport::link::LinkProvider`]
//! contract, configuring peers and the framing MTU automatically. It preflights
//! its configuration and every configured peer before opening the listener;
//! its lifecycle cancels and drains the listener and sessions on rollback or
//! router shutdown.
//!
//! Unix callers create a private owner-only directory before binding. Windows
//! listeners reserve local-only named pipes. Identities are trusted within the
//! operating system's security boundary, not cryptographically authenticated.
//!
//! Binding is synchronous (`IpcTransport::bind` / `bind_with`): a local
//! listener needs no name resolution, but it must run inside a Tokio runtime
//! that hosts its workers. The `groupnet-transport` crate docs compare the
//! construction contract of every binding.
//!
//! The shared transport types these signatures name — [`QueueCapacity`]
//! (in [`IpcConfig`]), [`PeerEndpoint`] (in [`IpcLink::new`]) and
//! [`NodeIdError`] (in [`IpcPeerError`]) — are re-exported here.

mod ipc;
mod link;

pub use groupnet_transport::link::PeerEndpoint;
pub use groupnet_transport::{NodeIdError, QueueCapacity};
pub use ipc::{IpcAddress, IpcConfig, IpcConfigError, IpcPeerError, IpcTransport, MAX_FRAME};
pub use link::IpcLink;
