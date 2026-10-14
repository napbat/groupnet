//! The core configuration values this tier's builders and options start from.
//!
//! They restate `groupnet_core`'s own operational defaults for
//! `RecoveryConfig`, `BootstrapConfig` and `TransferConfig` field by field, so
//! every default the tier hands out is one the core would choose too.

use std::num::{NonZeroU32, NonZeroUsize};

use groupnet_core::volatile_bootstrap::BootstrapConfig;
use groupnet_core::volatile_bootstrap::transfer::TransferConfig;
use groupnet_core::volatile_recovery::RecoveryConfig;

const KIB: usize = 1024;
const MIB: usize = 1024 * KIB;

const fn nonzero(value: usize) -> NonZeroUsize {
    NonZeroUsize::new(value).expect("nonzero default")
}

/// Recovery limits for an episode of up to a minute over a 1024-member roster.
pub(super) const fn recovery() -> RecoveryConfig {
    RecoveryConfig {
        max_members: nonzero(1024),
        max_member_bytes: nonzero(256),
        max_barrier_rounds: NonZeroU32::new(8).expect("nonzero default"),
        total_ms: 60_000,
        attempt_ms: 10_000,
        settle_ms: 1_000,
        poll_ms: 200,
    }
}

/// Advisory claim limits for a 1024-member roster.
pub(super) const fn claim() -> BootstrapConfig {
    BootstrapConfig {
        max_members: nonzero(1024),
        max_member_bytes: nonzero(256),
        max_scope_bytes: nonzero(512),
        settle_ms: 2_000,
        renew_ms: 1_000,
        claim_ttl_ms: 5_000,
        observe_ms: 5_000,
        donor_wait_ms: 30_000,
        total_ms: 120_000,
    }
}

/// Private transfer limits for an image of up to 64 MiB in schema
/// `expected_schema`.
pub(super) const fn transfer(expected_schema: NonZeroU32) -> TransferConfig {
    TransferConfig {
        expected_schema,
        max_metadata_bytes: nonzero(MIB),
        max_encoded_bytes: nonzero(64 * MIB),
        max_decoded_bytes: nonzero(64 * MIB),
        max_chunk_bytes: nonzero(MIB),
        max_chunks: nonzero(1024),
        max_batch_bytes: nonzero(MIB),
        max_batch_events: nonzero(256),
        max_replay_events: nonzero(4096),
        max_native_buffer_bytes: nonzero(16 * MIB),
        max_members: nonzero(1024),
        max_cuts: nonzero(256),
        coverage_poll_ms: 100,
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroU32;

    #[test]
    fn every_default_satisfies_its_core_rules() {
        assert_eq!(super::recovery().validate(), Ok(()));
        assert_eq!(super::claim().validate(), Ok(()));
        assert_eq!(super::transfer(NonZeroU32::MIN).validate(), Ok(()));
    }
}
