use std::fmt;
use std::sync::Arc;

/// Stable identity of a node, cheap to clone (`Arc<str>` inside).
///
/// IDs must be stable across restarts: the coordinator rule (see
/// [`crate::wire`] / the engine) is a deterministic function of the member set,
/// so an id that changes on reboot would reshuffle coordinators.
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(Arc<str>);

/// Identity of a group (shard / partition).
#[derive(Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct GroupId(Arc<str>);

macro_rules! string_id {
    ($ty:ident) => {
        impl $ty {
            /// Creates an id from anything convertible into `Arc<str>`
            /// (`&str`, `String`, `Arc<str>`).
            pub fn new(s: impl Into<Arc<str>>) -> Self {
                Self(s.into())
            }

            /// Borrows the id as a string slice.
            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }

        impl fmt::Display for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.0)
            }
        }

        impl fmt::Debug for $ty {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({:?})", stringify!($ty), &*self.0)
            }
        }

        impl From<&str> for $ty {
            fn from(s: &str) -> Self {
                Self::new(s)
            }
        }

        impl From<String> for $ty {
            fn from(s: String) -> Self {
                Self::new(s)
            }
        }
    };
}

string_id!(NodeId);
string_id!(GroupId);
