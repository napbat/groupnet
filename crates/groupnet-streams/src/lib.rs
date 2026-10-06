//! Static-dispatch session protocols over authenticated group-network peers.
//!
//! [`OrderedProtocol`] preserves byte order through the existing pinned TLS tunnels.
//! [`UnorderedProtocol`] preserves message boundaries and explicitly selects reliable
//! or unreliable delivery without imposing head-of-line ordering. Session
//! protocols need not implement byte-oriented IO, and callers can implement
//! [`SessionProtocol`] for their own concrete endpoints without boxing.

use std::{future::Future, io};

use groupnet_core::NodeId;

mod ordered;
pub mod unordered;

pub use groupnet_network::tunnel::TunneledStream;
pub use ordered::OrderedProtocol;
pub use unordered::{
    UnorderedConfig, UnorderedDelivery, UnorderedOptions, UnorderedProtocol, UnorderedSession,
};

/// A statically dispatched protocol establishing sessions with logical peers.
///
/// A session can be byte-oriented or message-oriented; this trait deliberately
/// imposes no `AsyncRead` or `AsyncWrite` bound. Implementations own negotiation,
/// admission and lifecycle semantics, while typed peer handles supply only the
/// logical destination.
pub trait SessionProtocol: Send + Sync {
    /// The concrete established session, including its protocol-specific API.
    type Session: Send;

    /// Explicit policy and connection settings supplied by the initiator.
    type ConnectOptions: Send;

    /// Establishes a session with an admitted logical destination.
    ///
    /// # Errors
    /// Returns the implementation's admission, setup, capacity or shutdown error.
    fn connect(
        &self,
        to: &NodeId,
        options: Self::ConnectOptions,
    ) -> impl Future<Output = io::Result<Self::Session>> + Send;

    /// Accepts a negotiated session and its authenticated original peer identity.
    ///
    /// # Errors
    /// Returns the implementation's setup, capacity or shutdown error.
    fn accept(&self) -> impl Future<Output = io::Result<(NodeId, Self::Session)>> + Send;

    /// Accepts a session matching configured policy. Protocols without distinct
    /// acceptance policies may use the default implementation.
    ///
    /// # Errors
    /// Returns the implementation's policy, setup, capacity or shutdown error.
    fn accept_with_options(
        &self,
        options: Self::ConnectOptions,
    ) -> impl Future<Output = io::Result<(NodeId, Self::Session)>> + Send {
        async move {
            drop(options);
            self.accept().await
        }
    }
}
