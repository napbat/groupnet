//! Typed rejections of caller inputs outside the router's configured bounds.

use std::{error, fmt, io};

use groupnet_transport::NodeIdError;

/// A link whose admission or framing this router's
/// [`RouterConfig`](crate::RouterConfig) cannot carry. Checked after the
/// link's own [`LinkConfig::validate_for`](crate::LinkConfig::validate_for)
/// and before the router stores or starts anything.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LinkRegistrationError {
    /// The link admits more `peers` than the router's `max_routes`.
    PeersAboveMaxRoutes {
        /// Number of peers the link admits.
        peers: usize,
        /// The router's `max_routes`.
        max_routes: usize,
    },
    /// The link's `mtu` cannot carry a fragment header and one byte, or
    /// exceeds the router's `max_frame`.
    MtuOutOfRange {
        /// The link's `mtu`.
        mtu: usize,
        /// Smallest accepted `mtu`: the fragment header plus one byte.
        min: usize,
        /// Largest accepted `mtu`: the router's `max_frame`.
        max: usize,
    },
    /// A `max_frame` frame needs more fragments at this `mtu` than the
    /// router's `reassembly.max_fragments` retains.
    FragmentsAboveReassembly {
        /// Fragments one `max_frame` frame needs at this `mtu`.
        fragments: usize,
        /// The router's `reassembly.max_fragments`.
        max_fragments: usize,
    },
}

impl fmt::Display for LinkRegistrationError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::PeersAboveMaxRoutes { peers, max_routes } => write!(
                f,
                "link admits {peers} peers, above the router's max_routes {max_routes}"
            ),
            Self::MtuOutOfRange { mtu, min, max } => {
                write!(f, "link mtu {mtu} is outside the router's {min}..={max}")
            }
            Self::FragmentsAboveReassembly {
                fragments,
                max_fragments,
            } => write!(
                f,
                "link mtu splits a max_frame frame into {fragments} fragments, above \
                 reassembly.max_fragments {max_fragments}"
            ),
        }
    }
}

impl error::Error for LinkRegistrationError {}

impl From<LinkRegistrationError> for io::Error {
    fn from(error: LinkRegistrationError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// A routed send or packet buffer the router refuses before routing it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RoutedSendError {
    /// The destination is not an identity the routing wire can carry.
    Destination(NodeIdError),
    /// The payload exceeds the room the router's `max_frame` leaves after
    /// this destination's routing header.
    PayloadTooLarge {
        /// The rejected payload length, or requested buffer capacity.
        len: usize,
        /// The largest payload this destination's routing header leaves room for.
        max: usize,
    },
    /// A [`PacketBuffer`](crate::PacketBuffer) was sent toward a destination
    /// or namespace other than the one it was allocated for.
    PacketMismatch,
}

impl fmt::Display for RoutedSendError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Destination(_) => f.write_str("invalid routed destination"),
            Self::PayloadTooLarge { len, max } => write!(
                f,
                "routed payload of {len} bytes exceeds the {max} bytes max_frame leaves"
            ),
            Self::PacketMismatch => {
                f.write_str("packet buffer was allocated for another destination or namespace")
            }
        }
    }
}

impl error::Error for RoutedSendError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Destination(error) => Some(error),
            Self::PayloadTooLarge { .. } | Self::PacketMismatch => None,
        }
    }
}

impl From<RoutedSendError> for io::Error {
    fn from(error: RoutedSendError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}
