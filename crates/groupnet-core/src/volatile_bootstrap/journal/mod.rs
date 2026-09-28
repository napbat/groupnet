//! Bounded private donor-local index delta capture. This local suffix is
//! neither a durable source log nor serving authority.

mod core;
mod types;

pub use core::DonorJournal;
pub use types::{
    AttachToken, BarrierReceipt, CaptureCharge, CaptureId, DeltaIdentity, Invalidation,
    JournalBatch, JournalConfig, JournalCursor, JournalDelta, JournalError, JournalState,
    NativeCut, ReservationId, ReservationStage,
};

#[cfg(test)]
mod tests;
