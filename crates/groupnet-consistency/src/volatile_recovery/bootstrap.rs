//! Optional bounded peer-bootstrap resources driven by the recovery worker.
//!
//! The core claim, journal, and transfer domain types the bootstrap ports and
//! errors name are re-exported here. The sans-IO claim and transfer engines,
//! with their events and effects, stay in `groupnet_core::volatile_bootstrap`.

pub mod admission;
#[cfg(feature = "volatile-bootstrap-bulk")]
pub mod bulk_adapter;
#[cfg(feature = "volatile-bootstrap-bulk")]
pub mod bulk_wire;
pub mod driver;
pub mod inbox;
pub mod native_claims;
pub mod ports;
pub mod report;
pub mod session;

pub use groupnet_core::volatile_bootstrap::journal::{
    AttachToken, BarrierReceipt, CaptureId, DeltaIdentity, DonorJournal, Invalidation,
    JournalBatch, JournalConfig, JournalCursor, JournalDelta, JournalError, JournalState,
    NativeCut, ReservationId,
};
pub use groupnet_core::volatile_bootstrap::transfer::{
    NativeHandoffReceipt, TransferConfig, TransferError, TransferOffer,
};
pub use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapError, BootstrapMember,
    BootstrapMemberIdentity, BootstrapOperation, BootstrapPresence, BootstrapScope,
    ClaimCodecError, ClaimIdentity, ClaimPhase, PresenceIdentity, ReleaseReason,
};
