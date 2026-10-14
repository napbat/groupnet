//! Opening a bootstrap child names the rule it violated.

use super::*;
use groupnet_consistency::volatile_recovery::bootstrap::admission::AdmissionError;
use groupnet_consistency::volatile_recovery::bootstrap::session::{
    BootstrapRuntimeConfigError, BootstrapSessionError,
};
use groupnet_consistency::volatile_recovery::bootstrap::{
    BootstrapConfigError, TransferConfigError,
};
use std::error::Error as _;
use std::num::NonZeroU32;

fn open(
    config: BootstrapRuntimeConfig,
    admission: ByteAdmission,
) -> Result<(), BootstrapSessionError> {
    BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::new(Claims::default()),
            donor: Arc::new(OriginDonor::default()),
            admission,
        },
        config,
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        PresenceIdentity {
            node: NodeId::from("me"),
            boot: BootId(1),
            session: 1,
        },
    )
    .map(drop)
}

#[test]
fn the_schema_constructor_yields_a_valid_config() {
    let config = BootstrapRuntimeConfig::new(NonZeroU32::new(7).unwrap());
    assert_eq!(config.validate(), Ok(()));
    assert_eq!(config.transfer.expected_schema.get(), 7);
    assert!(!config.require_participation);
}

#[test]
fn config_rules_are_typed_and_checked_before_any_reservation() {
    let invalid_claim = BootstrapRuntimeConfig {
        claim: BootstrapConfig {
            settle_ms: 0,
            ..bootstrap_config().claim
        },
        ..bootstrap_config()
    };
    assert_eq!(
        invalid_claim.validate(),
        Err(BootstrapRuntimeConfigError::Claim(
            BootstrapConfigError::ZeroSettle
        ))
    );
    let invalid_transfer = BootstrapRuntimeConfig {
        transfer: TransferConfig {
            coverage_poll_ms: 0,
            ..bootstrap_config().transfer
        },
        ..bootstrap_config()
    };
    assert_eq!(
        invalid_transfer.validate(),
        Err(BootstrapRuntimeConfigError::Transfer(
            TransferConfigError::ZeroCoveragePoll
        ))
    );
    let overflowing = BootstrapRuntimeConfig {
        max_claim_metadata_bytes: std::num::NonZeroUsize::MAX,
        ..bootstrap_config()
    };
    assert_eq!(
        overflowing.validate(),
        Err(BootstrapRuntimeConfigError::MetadataOverflow)
    );

    // Each rejection reaches the session as its config error, with the rule
    // as its source, and leaves the shared pool exactly as it found it.
    for config in [invalid_claim, invalid_transfer, overflowing] {
        let pool = admission();
        let rejected = open(config, pool.clone()).unwrap_err();
        assert_eq!(
            rejected,
            BootstrapSessionError::Config(config.validate().unwrap_err())
        );
        assert!(rejected.source().is_some(), "{rejected:?}");
        assert_eq!(pool.usage(), (0, [0; 5], 0), "{rejected:?}");
    }
}

#[test]
fn rejected_identity_and_admission_name_their_cause() {
    let rejected = BootstrapSession::new(
        BootstrapCapabilities {
            claims: Arc::new(Claims::default()),
            donor: Arc::new(OriginDonor::default()),
            admission: admission(),
        },
        bootstrap_config(),
        BootstrapScope {
            domain: "o".into(),
            partition: "b".into(),
        },
        PresenceIdentity {
            node: NodeId::from("me"),
            boot: BootId(0),
            session: 1,
        },
    )
    .map(drop)
    .unwrap_err();
    assert!(matches!(rejected, BootstrapSessionError::Claim(_)));
    assert!(
        rejected.source().is_some(),
        "the claim core's rejection is the source"
    );

    let tiny = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: std::num::NonZeroUsize::new(1).unwrap(),
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 1,
        max_reservations: std::num::NonZeroUsize::new(1).unwrap(),
    })
    .unwrap();
    let refused = open(bootstrap_config(), tiny).unwrap_err();
    assert_eq!(
        refused,
        BootstrapSessionError::Admission(AdmissionError::InvalidCharge)
    );
    assert_eq!(
        refused.source().map(ToString::to_string),
        Some(AdmissionError::InvalidCharge.to_string())
    );
}
