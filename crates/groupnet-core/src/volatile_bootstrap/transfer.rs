//! Bounded private transfer decisions for a volatile donor index image.

mod engine;
mod types;

pub(crate) use types::native_cuts_cover;

pub use engine::TransferSession;
pub use types::{
    NativeCoverageReceipt, NativeHandoffReceipt, TransferBinding, TransferConfig, TransferEffect,
    TransferError, TransferEvent, TransferOffer, TransferStage, TransferStep,
};

#[cfg(test)]
mod tests;
