//! Every UDP endpoint rule surfaces as its own field-specific variant.

use super::*;
use crate::UdpConnection;

fn config() -> UdpPunchConfig {
    UdpPunchConfig::new(
        "local".into(),
        SocketAddr::from(([127, 0, 0, 1], 9000)),
        NetworkKey::from_bytes([1; 32]),
        vec!["peer".into()],
    )
}

fn rejected(change: impl FnOnce(&mut UdpPunchConfig)) -> UdpPunchConfigError {
    let mut config = config();
    change(&mut config);
    config.validate().unwrap_err()
}

#[test]
fn defaults_validate() {
    config().validate().unwrap();
    UdpPunchConfig::open("local".into(), SocketAddr::from(([127, 0, 0, 1], 9000)))
        .validate()
        .unwrap();
}

#[test]
fn identity_rules_name_their_field() {
    assert_eq!(
        rejected(|config| config.local = "".into()),
        UdpPunchConfigError::Local(NodeIdError::Empty)
    );
    assert_eq!(
        rejected(|config| config.local = "x".repeat(65).into()),
        UdpPunchConfigError::Local(NodeIdError::TooLong { len: 65, max: 64 })
    );
    assert_eq!(
        rejected(|config| config.peers = vec!["a".into(), "".into()]),
        UdpPunchConfigError::Peer {
            index: 1,
            error: NodeIdError::Empty
        }
    );
    assert_eq!(
        rejected(|config| config.peers = vec!["a".into(), "b".into(), "a".into()]),
        UdpPunchConfigError::DuplicatePeer { index: 2 }
    );
    assert_eq!(
        rejected(|config| config.peers.push("local".into())),
        UdpPunchConfigError::LocalListedAsPeer
    );
    assert_eq!(
        rejected(|config| {
            config.peers.push("other".into());
            config.max_peers = NonZeroUsize::MIN;
        }),
        UdpPunchConfigError::PeersExceedMaxPeers {
            peers: 2,
            max_peers: NonZeroUsize::MIN
        }
    );
}

#[test]
fn credential_and_address_rules_name_their_field() {
    assert_eq!(
        rejected(|config| config.credential = vec![0; MAX_CREDENTIAL_BYTES + 1]),
        UdpPunchConfigError::CredentialTooLarge {
            len: MAX_CREDENTIAL_BYTES + 1
        }
    );
    assert_eq!(
        rejected(|config| config.rendezvous = SocketAddr::from(([127, 0, 0, 1], 0))),
        UdpPunchConfigError::InvalidRendezvous
    );
    assert_eq!(
        rejected(|config| config.bind = "[::]:0".parse().unwrap()),
        UdpPunchConfigError::BindFamilyMismatch
    );
    assert_eq!(
        rejected(|config| config.bind = SocketAddr::from(([224, 0, 0, 1], 0))),
        UdpPunchConfigError::UnsupportedBind
    );
}

#[test]
fn candidate_rules_name_their_field() {
    let loopback = SocketAddr::from(([127, 0, 0, 1], 0));
    assert_eq!(
        rejected(|config| config.candidate_binds = vec![loopback; MAX_SOCKETS]),
        UdpPunchConfigError::TooManyCandidateBinds { len: MAX_SOCKETS }
    );
    assert_eq!(
        rejected(|config| {
            config.candidate_binds = vec![loopback, SocketAddr::from(([169, 254, 1, 1], 0))];
        }),
        UdpPunchConfigError::UnsupportedCandidateBind { index: 1 }
    );
    let advertised = SocketAddr::from(([127, 0, 0, 1], 7000));
    assert_eq!(
        rejected(|config| config.advertised_candidates = vec![advertised; MAX_CANDIDATES + 1]),
        UdpPunchConfigError::TooManyAdvertisedCandidates {
            len: MAX_CANDIDATES + 1
        }
    );
    assert_eq!(
        rejected(|config| config.advertised_candidates = vec![advertised, loopback]),
        UdpPunchConfigError::InvalidAdvertisedCandidate { index: 1 }
    );
}

#[test]
fn display_names_the_field() {
    assert!(
        UdpPunchConfigError::TooManyCandidateBinds { len: 4 }
            .to_string()
            .starts_with("candidate_binds")
    );
    assert!(
        UdpPunchConfigError::Local(NodeIdError::Empty)
            .to_string()
            .starts_with("local")
    );
}

#[tokio::test]
async fn bind_surfaces_the_typed_error_as_invalid_input() {
    let mut config = config();
    config.credential = vec![0; MAX_CREDENTIAL_BYTES + 1];
    let error = UdpConnection::bind(config).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<UdpPunchConfigError>()),
        Some(&UdpPunchConfigError::CredentialTooLarge {
            len: MAX_CREDENTIAL_BYTES + 1
        })
    );
}
