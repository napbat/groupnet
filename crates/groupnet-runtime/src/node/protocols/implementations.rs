//! Configurations that resolve the node's cached concrete protocols once.

use super::{Node, PeerImplementation};
use groupnet_messaging::{Delivery, Messaging, SendOptions};
use groupnet_streams::{OrderedProtocol, UnorderedDelivery, UnorderedOptions, UnorderedProtocol};
use std::{io, time::Duration};

/// Application messages with a delivery boundary fixed once at binding time.
#[derive(Clone, Copy, Debug, Default)]
pub struct Messages {
    options: SendOptions,
}

impl Messages {
    /// Selects explicit application-message options.
    #[must_use]
    pub const fn new(options: SendOptions) -> Self {
        Self { options }
    }

    /// Sends once with local best-effort enqueue semantics.
    #[must_use]
    pub fn best_effort() -> Self {
        Self::default()
    }

    /// Waits for receiver inbox acceptance within a finite deadline.
    #[must_use]
    pub const fn delivered(timeout: Duration) -> Self {
        Self::new(SendOptions {
            delivery: Delivery::Delivered,
            timeout,
        })
    }

    /// Waits for explicit receiver application completion within a finite deadline.
    /// A timeout leaves the outcome unknown, not known unapplied.
    #[must_use]
    pub const fn applied(timeout: Duration) -> Self {
        Self::new(SendOptions {
            delivery: Delivery::Applied,
            timeout,
        })
    }
}

impl PeerImplementation for Messages {
    type Protocol = Messaging;
    type Options = SendOptions;

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        self.options.validate(0)?;
        Ok((node.cached_messages(), self.options))
    }
}

/// Authenticated ordered TLS bytes on the node's existing tunnel plane.
#[derive(Clone, Copy, Debug, Default)]
pub struct Ordered;

impl Ordered {
    /// Selects ordered reliable byte-stream sessions.
    #[must_use]
    pub const fn new() -> Self {
        Self
    }
}

impl PeerImplementation for Ordered {
    type Protocol = OrderedProtocol;
    type Options = ();

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        Ok((node.cached_ordered()?, ()))
    }
}

/// Encrypted unordered messages with an explicit session delivery policy.
/// Node-wide admission and capacity are configured separately with
/// [`Node::configure_unordered`]. Neither policy guarantees ordered delivery.
#[derive(Clone, Debug)]
pub struct Unordered {
    options: UnorderedOptions,
}

impl Unordered {
    /// Selects explicit session policy and finite setup deadline.
    #[must_use]
    pub const fn new(options: UnorderedOptions) -> Self {
        Self { options }
    }

    /// Retries independent messages until inbox acceptance or a finite deadline.
    #[must_use]
    pub fn reliable() -> Self {
        Self::new(UnorderedOptions::default())
    }

    /// Sends each message once, without data acknowledgement or retry.
    #[must_use]
    pub fn unreliable() -> Self {
        Self::new(UnorderedOptions {
            delivery: UnorderedDelivery::Unreliable,
            ..UnorderedOptions::default()
        })
    }
}

impl PeerImplementation for Unordered {
    type Protocol = UnorderedProtocol;
    type Options = UnorderedOptions;

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        let protocol = node.cached_unordered()?;
        protocol.validate_options(&self.options)?;
        Ok((protocol, self.options))
    }
}
