//! Provisional builder choice for volatile peer index bootstrap.
//!
//! Claims coordinate origin work but never authorize serving. Image capture,
//! transfer and replay are bounded child decisions. The final read gate is
//! always supplied by the application recovery and lease authority.
//!
//! [`BootstrapConfig`], [`journal::JournalConfig`] and
//! [`transfer::TransferConfig`] carry nonzero counts and byte caps as
//! `NonZero*` fields; each `validate` checks the remaining millisecond and
//! cross-field rules and returns a typed error whose variant names the field.

pub mod journal;
pub mod transfer;

mod claim_codec;
mod engine;
mod types;

pub use claim_codec::{
    ClaimCodecError, ObservedEntry, claim_entry_key, decode_claim_value, decode_presence_value,
    encode_claim_value, encode_presence_value, encoded_claim_len, encoded_presence_len,
    presence_entry_key,
};
pub use engine::ClaimEngine;
pub use types::{
    BootId, BootstrapClaim, BootstrapConfig, BootstrapConfigError, BootstrapEffect, BootstrapError,
    BootstrapEvent, BootstrapMember, BootstrapMemberIdentity, BootstrapOperation,
    BootstrapParticipant, BootstrapPresence, BootstrapScope, BootstrapStage, BootstrapStep,
    ClaimIdentity, ClaimPhase, ParticipationCut, PresenceIdentity, ReleaseReason, same_membership,
};

#[cfg(test)]
mod tests;

#[cfg(test)]
mod claim_transfer_tests;

#[cfg(test)]
mod release_tests;
