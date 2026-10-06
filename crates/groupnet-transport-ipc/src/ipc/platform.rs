//! Native OS handles, with no networking fallbacks.

#[cfg(unix)]
#[path = "unix.rs"]
mod native;
#[cfg(windows)]
#[path = "windows.rs"]
mod native;

pub(super) use native::{Listener, Stream, connect, validate_address};
