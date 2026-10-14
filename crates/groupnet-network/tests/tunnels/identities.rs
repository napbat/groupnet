//! Credential, pin and standalone tunnel admission failures are typed and keep
//! their rustls/parser causes.

use std::{error::Error, io, num::NonZeroUsize};

use groupnet_core::NodeId;
use groupnet_network::{
    Router, RouterConfig, TunnelConfig, TunnelConfigError,
    tunnel::{PeerIdentity, PeerIdentityError, TlsIdentity, TlsIdentityError, TunnelTransport},
};
use rcgen::{CertificateParams, KeyPair};

use super::credentials;

fn self_signed() -> (Vec<u8>, KeyPair) {
    let key = KeyPair::generate().unwrap();
    let params = CertificateParams::new(vec!["groupnet.peer".to_owned()]).unwrap();
    (params.self_signed(&key).unwrap().der().to_vec(), key)
}

#[test]
fn malformed_credentials_name_the_rule_and_keep_their_cause() {
    let (leaf, key) = self_signed();
    let der = || key.serialize_der();
    let identity = |roots: Vec<Vec<u8>>, chain: Vec<Vec<u8>>, key: Vec<u8>| {
        TlsIdentity::from_der(roots, chain, key).unwrap_err()
    };
    assert!(matches!(
        identity(Vec::new(), vec![leaf.clone()], der()),
        TlsIdentityError::RootCount
    ));
    assert!(matches!(
        identity(
            vec![leaf.clone(); TlsIdentity::MAX_ROOTS + 1],
            vec![leaf.clone()],
            der()
        ),
        TlsIdentityError::RootCount
    ));
    assert!(matches!(
        identity(vec![leaf.clone()], Vec::new(), der()),
        TlsIdentityError::ChainCount
    ));
    let root = identity(
        vec![b"not a certificate".to_vec()],
        vec![leaf.clone()],
        der(),
    );
    assert!(matches!(root, TlsIdentityError::Root(_)));
    assert!(root.source().is_some(), "the rustls cause is retained");
    let key_error = identity(
        vec![leaf.clone()],
        vec![leaf.clone()],
        b"not a key".to_vec(),
    );
    assert!(matches!(key_error, TlsIdentityError::Key { .. }));
    let (_, other) = self_signed();
    let mismatched = identity(vec![leaf.clone()], vec![leaf], other.serialize_der());
    assert!(matches!(mismatched, TlsIdentityError::Credentials(_)));
    assert!(
        mismatched.source().is_some(),
        "the rustls cause is retained"
    );
    let error = io::Error::from(mismatched);
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert!(
        error
            .into_inner()
            .and_then(|inner| inner.downcast::<TlsIdentityError>().ok())
            .is_some()
    );
}

#[test]
fn certificate_pins_reject_empty_and_oversized_encodings() {
    let node = || NodeId::new("peer");
    assert_eq!(
        PeerIdentity::new(node(), &[]).unwrap_err(),
        PeerIdentityError::EmptyCertificate
    );
    assert_eq!(
        PeerIdentity::new(node(), &vec![1; PeerIdentity::MAX_CERTIFICATE + 1]).unwrap_err(),
        PeerIdentityError::CertificateTooLarge
    );
    assert!(PeerIdentity::new(node(), &vec![1; PeerIdentity::MAX_CERTIFICATE]).is_ok());
}

#[tokio::test]
async fn standalone_tunnels_reject_admission_the_router_envelope_cannot_carry() {
    let router = Router::with_config(
        NodeId::new("local"),
        RouterConfig {
            max_frame: NonZeroUsize::new(4096).unwrap(),
            ..RouterConfig::default()
        },
    )
    .unwrap();
    let (local, peer, _) = credentials();
    let pin = PeerIdentity::new(NodeId::new("peer"), &peer.leaf).unwrap();
    let typed = |error: io::Error| {
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        error
            .into_inner()
            .and_then(|inner| inner.downcast::<TunnelConfigError>().ok())
            .map(|error| *error)
    };
    let oversized = TunnelTransport::with_config(
        router.clone(),
        TunnelConfig::new(local.identity.clone(), [pin.clone()]),
    )
    .unwrap_err();
    let expected = TunnelConfigError::PayloadAboveRouterFrame {
        node: NodeId::new("peer"),
    };
    assert_eq!(typed(oversized), Some(expected.clone()));
    // The rejected configuration never claimed the router's tunnel plane.
    let tunnels = TunnelTransport::new(router.clone(), local.identity).unwrap();
    assert_eq!(typed(tunnels.admit_peer(pin).unwrap_err()), Some(expected));
    tunnels.close().await;
    router.close().await;
}
