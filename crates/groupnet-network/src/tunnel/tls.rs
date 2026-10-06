//! TLS configuration and independently provisioned routing-alias pins.

use std::{fmt, io, sync::Arc};

use groupnet_core::NodeId;
use ring::digest::{SHA256, digest};
use rustls::{
    ClientConfig, RootCertStore, ServerConfig,
    pki_types::{CertificateDer, PrivateKeyDer},
};

const ALPN: &[u8] = b"groupnet/router-tunnel/1";

/// A certificate chain and private key for TLS 1.3 with mandatory mutual authentication.
///
/// Trust roots validate certificates; a separate [`PeerIdentity`] binds each leaf
/// certificate to its routing alias. Certificates must cover `groupnet.peer`.
#[derive(Clone)]
pub struct TlsIdentity {
    pub(super) client: Arc<ClientConfig>,
    pub(super) server: Arc<ServerConfig>,
}

impl fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsIdentity").finish_non_exhaustive()
    }
}

impl TlsIdentity {
    /// Builds ring-backed TLS 1.3 configurations from DER credentials.
    /// Session resumption and early data are disabled in both directions.
    ///
    /// # Errors
    /// Rejects empty roots/chains, malformed certificates/keys or mismatched keys.
    pub fn from_der(roots: Vec<Vec<u8>>, chain: Vec<Vec<u8>>, key: Vec<u8>) -> io::Result<Self> {
        if roots.is_empty() || chain.is_empty() || roots.len() > 32 || chain.len() > 8 {
            return Err(invalid("invalid TLS certificate count"));
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store.add(CertificateDer::from(root)).map_err(invalid)?;
        }
        let chain: Vec<_> = chain.into_iter().map(CertificateDer::from).collect();
        let key = PrivateKeyDer::try_from(key).map_err(invalid)?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(store.clone()),
            provider.clone(),
        )
        .build()
        .map_err(invalid)?;
        let mut server = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(invalid)?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        server.send_tls13_tickets = 0;
        server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        server.max_early_data_size = 0;
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(invalid)?
            .with_root_certificates(store)
            .with_client_auth_cert(chain, key)
            .map_err(invalid)?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        client.enable_early_data = false;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

/// An explicitly provisioned TLS leaf-certificate pin for one routing alias.
#[derive(Clone, Debug)]
pub struct PeerIdentity {
    pub(super) node: NodeId,
    pub(super) pin: [u8; 32],
}

impl PeerIdentity {
    /// Pins the exact DER leaf certificate for `node` using SHA-256.
    ///
    /// # Errors
    /// Rejects empty or oversized certificate encodings.
    pub fn new(node: NodeId, certificate: &[u8]) -> io::Result<Self> {
        if certificate.is_empty() || certificate.len() > 65_536 {
            return Err(invalid("invalid peer certificate length"));
        }
        Ok(Self {
            node,
            pin: fingerprint(certificate),
        })
    }
}

fn fingerprint(bytes: &[u8]) -> [u8; 32] {
    let mut pin = [0; 32];
    pin.copy_from_slice(digest(&SHA256, bytes).as_ref());
    pin
}

pub(super) fn verify(
    peer: &PeerIdentity,
    certificates: Option<&[CertificateDer<'_>]>,
    alpn: Option<&[u8]>,
) -> io::Result<()> {
    let leaf = certificates
        .and_then(|chain| chain.first())
        .ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::PermissionDenied,
                "missing TLS peer certificate",
            )
        })?;
    if alpn != Some(ALPN) || fingerprint(leaf.as_ref()) != peer.pin {
        return Err(io::Error::new(
            io::ErrorKind::PermissionDenied,
            "TLS routing identity mismatch",
        ));
    }
    Ok(())
}

fn invalid(error: impl fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, error.to_string())
}
