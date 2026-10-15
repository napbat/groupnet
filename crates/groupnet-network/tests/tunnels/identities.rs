//! Credential, pin and standalone tunnel admission failures are typed; rustls
//! causes stay as `source()`, the key parser's reason in its variant.

use std::{error::Error, io, num::NonZeroUsize};

use groupnet_core::NodeId;
use groupnet_network::{
    Router, RouterConfig, TunnelConfig, TunnelConfigError,
    tunnel::{
        PeerIdentity, PeerIdentityError, TlsIdentity, TlsIdentityError, TunnelTransport, rustls,
    },
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
    let TlsIdentityError::Key { reason } = key_error else {
        panic!("a malformed key is a Key error, got {key_error:?}");
    };
    assert!(key_error.to_string().contains(reason));
    assert!(
        key_error.source().is_none(),
        "the key parser has no error value"
    );
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

/// Every rustls type a [`TlsIdentityError`] variant carries is nameable through
/// the canonical `groupnet_network::tunnel::rustls` re-export.
#[test]
fn tls_identity_error_vocabulary_is_nameable_through_the_tunnel_namespace() {
    fn cause(error: &TlsIdentityError) -> Option<&dyn Error> {
        match error {
            TlsIdentityError::Root(cause)
            | TlsIdentityError::Protocol(cause)
            | TlsIdentityError::Credentials(cause) => {
                let cause: &rustls::Error = cause;
                Some(cause)
            }
            TlsIdentityError::Verifier(cause) => {
                let cause: &rustls::server::VerifierBuilderError = cause;
                Some(cause)
            }
            TlsIdentityError::RootCount
            | TlsIdentityError::ChainCount
            | TlsIdentityError::Key { .. } => None,
        }
    }

    let (leaf, key) = self_signed();
    let error =
        TlsIdentity::from_der(vec![b"bad".to_vec()], vec![leaf], key.serialize_der()).unwrap_err();
    assert!(cause(&error).is_some());
    assert_eq!(
        cause(&TlsIdentityError::Credentials(rustls::Error::General(
            "unusable".to_owned()
        )))
        .map(ToString::to_string),
        Some(rustls::Error::General("unusable".to_owned()).to_string())
    );
    assert!(
        cause(&TlsIdentityError::Verifier(
            rustls::server::VerifierBuilderError::NoRootAnchors
        ))
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
