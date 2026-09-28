//! Optional bounded peer-bootstrap resources driven by the recovery worker.

pub mod admission;
#[cfg(feature = "volatile-bootstrap-bulk")]
pub mod bulk_wire;
pub mod driver;
pub mod inbox;
pub mod native_claims;
pub mod ports;
pub mod session;
