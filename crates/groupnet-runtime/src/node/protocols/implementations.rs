//! Configurations that resolve the node's cached concrete protocols once.

use super::{ManagedProtocol, Node, PeerImplementation};
use groupnet_messaging::{Delivery, Messaging, SendOptions};
use groupnet_streams::{
    OrderedProtocol, StreamProtocolId, UnorderedConfig, UnorderedDelivery, UnorderedOptions,
    UnorderedProtocol,
};
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
    type Protocol = ManagedProtocol<Messaging>;
    type Options = SendOptions;

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        let protocol = node.cached_messages();
        protocol.config().validate_send(self.options, 0)?;
        Ok((protocol, self.options))
    }
}

/// Authenticated ordered TLS bytes on the node's existing tunnel plane, for
/// one application stream protocol.
///
/// Each [`StreamProtocolId`] has its own accept queue on every node, so
/// independent consumers — an RPC server beside a blob service, say — each run
/// their own accept loop and never receive each other's streams. The connector
/// names the protocol in the stream's authenticated setup; a peer with no
/// endpoint bound for it refuses the stream (`ConnectionRefused`).
///
/// ```no_run
/// use groupnet_runtime::{Node, Ordered, StreamProtocolId};
///
/// const RPC: StreamProtocolId = StreamProtocolId::new(1);
///
/// # async fn demo(node: Node) -> std::io::Result<()> {
/// // Accepts only streams connected with `Ordered::protocol(RPC)`.
/// let rpc = node.endpoint(Ordered::protocol(RPC))?;
/// let (origin, stream) = rpc.accept().await?;
/// # let _ = (origin, stream);
/// # Ok(())
/// # }
/// ```
#[derive(Clone, Copy, Debug, Default)]
pub struct Ordered {
    protocol: StreamProtocolId,
}

impl Ordered {
    /// Selects ordered reliable byte-stream sessions of the default
    /// [`StreamProtocolId::DEFAULT`] protocol.
    #[must_use]
    pub const fn new() -> Self {
        Self::protocol(StreamProtocolId::DEFAULT)
    }

    /// Selects ordered reliable byte-stream sessions of `protocol`, with
    /// their own accept queue on every node.
    #[must_use]
    pub const fn protocol(protocol: StreamProtocolId) -> Self {
        Self { protocol }
    }
}

impl PeerImplementation for Ordered {
    type Protocol = ManagedProtocol<OrderedProtocol>;
    type Options = ();

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        let protocol = node.cached_ordered()?.scoped(self.protocol);
        Ok((ManagedProtocol::new(protocol), ()))
    }
}

/// Encrypted unordered messages with an explicit session delivery policy.
/// Node-wide admission and capacity are fixed when the node starts, with
/// [`NodeBuilder::unordered`](crate::NodeBuilder::unordered). Neither policy
/// guarantees ordered delivery.
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
    type Protocol = ManagedProtocol<UnorderedProtocol>;
    type Options = UnorderedOptions;

    fn bind(self, node: &Node) -> io::Result<(Self::Protocol, Self::Options)> {
        // Checked against the node-wide policy before the shared engine is
        // created or cached, so an invalid descriptor starts nothing.
        validate_unordered(&self.options, &node.inner.unordered_config)?;
        let protocol = node.cached_unordered()?;
        Ok((ManagedProtocol::new(protocol), self.options))
    }
}

/// Descriptor options against the node's fixed unordered policy, without an
/// engine: a positive setup deadline and a delivery policy the node allows.
fn validate_unordered(options: &UnorderedOptions, policy: &UnorderedConfig) -> io::Result<()> {
    let allowed = match options.delivery {
        UnorderedDelivery::Reliable => policy.allow_reliable,
        UnorderedDelivery::Unreliable => policy.allow_unreliable,
    };
    if options.timeout.is_zero() || !allowed {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "unordered policy or setup timeout disallowed locally",
        ));
    }
    Ok(())
}
