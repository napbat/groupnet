//! Opening a bootstrap child names the rule it violated.

use super::*;
use groupnet_consistency::volatile_recovery::bootstrap::admission::AdmissionError;
use groupnet_consistency::volatile_recovery::bootstrap::session::BootstrapSessionError;
use std::error::Error as _;

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
        NodeId::from("me"),
        BootId(1),
        1,
    )
    .map(drop)
}

#[test]
fn rejected_open_names_its_rule_and_keeps_the_cause() {
    let zero_metadata = BootstrapRuntimeConfig {
        max_claim_metadata_bytes: 0,
        ..bootstrap_config()
    };
    assert_eq!(
        open(zero_metadata, admission()),
        Err(BootstrapSessionError::ZeroClaimMetadata)
    );

    let invalid_claim = BootstrapRuntimeConfig {
        claim: BootstrapConfig {
            settle_ms: 0,
            ..bootstrap_config().claim
        },
        ..bootstrap_config()
    };
    let rejected = open(invalid_claim, admission()).unwrap_err();
    assert!(matches!(rejected, BootstrapSessionError::Claim(_)));
    assert!(
        rejected.source().is_some(),
        "the claim core's rejection is the source"
    );

    let tiny = ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 1,
        max_encoded_bytes: 0,
        max_decoded_bytes: 0,
        max_suffix_bytes: 0,
        max_native_overlap_bytes: 0,
        max_inflight_bytes: 1,
        max_reservations: 1,
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
