//! Sans-IO source-ordered reader admission and pre-mutation writer fencing.

mod reader;
mod timing;
mod types;
mod writer;

pub use reader::{AdmissionAppend, ReaderCore, ReaderError};
pub use timing::ExpiryTiming;
pub use types::{
    AdmissionId, AdmissionPolicy, AdmissionRef, PolicyError, RecordBinding, SourceReceipt,
};
pub use writer::{AckReceipt, AppendIntent, RosterEntry, RosterReceipt, WriterCore, WriterError};

#[cfg(test)]
mod tests;
