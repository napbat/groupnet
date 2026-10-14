//! The routing-identity bound every adapter enforces.

use std::{error, fmt, io};

use groupnet_core::NodeId;

/// Largest routing identity ([`NodeId`]) any adapter admits or introduces,
/// in bytes. An empty identity is never valid either. Adapters whose wire
/// encoding is narrower pass their own smaller bound to [`validate_node_id`].
pub const MAX_NODE_ID_BYTES: usize = 255;

/// Why a routing identity was rejected by [`validate_node_id`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeIdError {
    /// The identity has no bytes.
    Empty,
    /// The identity is longer than the bound in force.
    TooLong {
        /// The identity's length in bytes.
        len: usize,
        /// The largest length the checking adapter accepts.
        max: usize,
    },
}

impl fmt::Display for NodeIdError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("node id is empty"),
            Self::TooLong { len, max } => {
                write!(f, "node id is {len} bytes, above the {max}-byte bound")
            }
        }
    }
}

impl error::Error for NodeIdError {}

impl From<NodeIdError> for io::Error {
    fn from(error: NodeIdError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Checks that `id` is nonempty and at most `max_bytes` long.
///
/// Pass [`MAX_NODE_ID_BYTES`] unless the adapter's wire encoding carries a
/// narrower identity field.
///
/// # Errors
/// Returns [`NodeIdError::Empty`] or [`NodeIdError::TooLong`].
pub fn validate_node_id(id: &NodeId, max_bytes: usize) -> Result<(), NodeIdError> {
    let len = id.as_str().len();
    if len == 0 {
        Err(NodeIdError::Empty)
    } else if len > max_bytes {
        Err(NodeIdError::TooLong {
            len,
            max: max_bytes,
        })
    } else {
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identity_bound_names_the_violation() {
        assert_eq!(
            validate_node_id(&NodeId::new(""), MAX_NODE_ID_BYTES),
            Err(NodeIdError::Empty)
        );
        assert_eq!(
            validate_node_id(&NodeId::new("abc"), 2),
            Err(NodeIdError::TooLong { len: 3, max: 2 })
        );
        assert_eq!(validate_node_id(&NodeId::new("ab"), 2), Ok(()));
        let error = io::Error::from(NodeIdError::Empty);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<NodeIdError>()),
            Some(&NodeIdError::Empty)
        );
    }
}
