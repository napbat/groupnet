//! Persistent, bounded native local IPC for Groupnet.
//!
//! [`IpcTransport`] exposes the standalone message transport. [`IpcLink`] binds
//! it through the router-independent [`groupnet_transport::link::LinkProvider`]
//! contract, configuring peers and the framing MTU automatically. Its lifecycle
//! cancels and drains the listener and sessions on rollback or router shutdown.
//!
//! Unix callers create a private owner-only directory before binding. Windows
//! listeners reserve local-only named pipes. Identities are trusted within the
//! operating system's security boundary, not cryptographically authenticated.
//!
//! Binding is synchronous (`IpcTransport::bind` / `bind_with`): a local
//! listener needs no name resolution, but it must run inside a Tokio runtime
//! that hosts its workers. The `groupnet-transport` crate docs compare the
//! construction contract of every binding.

mod ipc;
mod link;

pub use ipc::{IpcAddress, IpcConfig, IpcConfigError, IpcTransport, MAX_FRAME};
pub use link::IpcLink;
