//! Native OS handles, with no networking fallbacks.

#[cfg(unix)]
#[path = "unix.rs"]
mod native;
#[cfg(windows)]
#[path = "windows.rs"]
mod native;

#[cfg(windows)]
pub(crate) use native::MAX_SESSIONS;
pub(crate) use native::{Listener, Stream, connect, validate_address};
