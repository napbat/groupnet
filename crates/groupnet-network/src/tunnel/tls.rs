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
    /// Most trust roots one identity accepts.
    pub const MAX_ROOTS: usize = 32;

    /// Longest certificate chain one identity presents.
    pub const MAX_CHAIN: usize = 8;

    /// Builds ring-backed TLS 1.3 configurations from DER credentials.
    /// Session resumption and early data are disabled in both directions.
    ///
    /// # Errors
    /// Returns the [`TlsIdentityError`] naming an empty or oversized root set or
    /// chain, a malformed root or key, or a key the chain does not match; rustls
    /// and parser failures stay available as the error's `source()`.
    pub fn from_der(
        roots: Vec<Vec<u8>>,
        chain: Vec<Vec<u8>>,
        key: Vec<u8>,
    ) -> Result<Self, TlsIdentityError> {
        if roots.is_empty() || roots.len() > Self::MAX_ROOTS {
            return Err(TlsIdentityError::RootCount);
        }
        if chain.is_empty() || chain.len() > Self::MAX_CHAIN {
            return Err(TlsIdentityError::ChainCount);
        }
        let mut store = RootCertStore::empty();
        for root in roots {
            store
                .add(CertificateDer::from(root))
                .map_err(TlsIdentityError::Root)?;
        }
        let chain: Vec<_> = chain.into_iter().map(CertificateDer::from).collect();
        let key =
            PrivateKeyDer::try_from(key).map_err(|reason| TlsIdentityError::Key { reason })?;
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let verifier = rustls::server::WebPkiClientVerifier::builder_with_provider(
            Arc::new(store.clone()),
            provider.clone(),
        )
        .build()
        .map_err(TlsIdentityError::Verifier)?;
        let mut server = ServerConfig::builder_with_provider(provider.clone())
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(TlsIdentityError::Protocol)?
            .with_client_cert_verifier(verifier)
            .with_single_cert(chain.clone(), key.clone_key())
            .map_err(TlsIdentityError::Credentials)?;
        server.alpn_protocols = vec![ALPN.to_vec()];
        server.send_tls13_tickets = 0;
        server.session_storage = Arc::new(rustls::server::NoServerSessionStorage {});
        server.max_early_data_size = 0;
        let mut client = ClientConfig::builder_with_provider(provider)
            .with_protocol_versions(&[&rustls::version::TLS13])
            .map_err(TlsIdentityError::Protocol)?
            .with_root_certificates(store)
            .with_client_auth_cert(chain, key)
            .map_err(TlsIdentityError::Credentials)?;
        client.alpn_protocols = vec![ALPN.to_vec()];
        client.resumption = rustls::client::Resumption::disabled();
        client.enable_early_data = false;
        Ok(Self {
            client: Arc::new(client),
            server: Arc::new(server),
        })
    }
}

/// Why DER credentials cannot form a [`TlsIdentity`].
#[derive(Debug)]
pub enum TlsIdentityError {
    /// `roots` is empty or holds more than [`TlsIdentity::MAX_ROOTS`] certificates.
    RootCount,
    /// `chain` is empty or holds more than [`TlsIdentity::MAX_CHAIN`] certificates.
    ChainCount,
    /// A trust root is not a parseable certificate.
    Root(rustls::Error),
    /// `key` is not a recognized DER private key.
    Key {
        /// The parser's reason.
        reason: &'static str,
    },
    /// The client-certificate verifier rejected the trust roots.
    Verifier(rustls::server::VerifierBuilderError),
    /// The crypto provider does not support TLS 1.3.
    Protocol(rustls::Error),
    /// The chain and key do not form a usable credential (for example, a key
    /// that does not match the leaf certificate).
    Credentials(rustls::Error),
}

impl fmt::Display for TlsIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::RootCount => write!(
                f,
                "TLS identity needs 1..={} trust roots",
                TlsIdentity::MAX_ROOTS
            ),
            Self::ChainCount => write!(
                f,
                "TLS identity needs 1..={} chain certificates",
                TlsIdentity::MAX_CHAIN
            ),
            Self::Root(_) => f.write_str("TLS trust root is malformed"),
            Self::Key { reason } => write!(f, "TLS private key is malformed: {reason}"),
            Self::Verifier(_) => f.write_str("TLS trust roots cannot verify client certificates"),
            Self::Protocol(_) => f.write_str("TLS 1.3 is unavailable from the crypto provider"),
            Self::Credentials(_) => f.write_str("TLS certificate chain and key are unusable"),
        }
    }
}

impl std::error::Error for TlsIdentityError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Root(error) | Self::Protocol(error) | Self::Credentials(error) => Some(error),
            Self::Verifier(error) => Some(error),
            Self::RootCount | Self::ChainCount | Self::Key { .. } => None,
        }
    }
}

impl From<TlsIdentityError> for io::Error {
    fn from(error: TlsIdentityError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// An explicitly provisioned TLS leaf-certificate pin for one routing alias.
#[derive(Clone, Debug)]
pub struct PeerIdentity {
    pub(crate) node: NodeId,
    pub(super) pin: [u8; 32],
}

impl PeerIdentity {
    /// Largest DER leaf certificate a pin accepts.
    pub const MAX_CERTIFICATE: usize = 65_536;

    /// Pins the exact DER leaf certificate for `node` using SHA-256.
    ///
    /// # Errors
    /// Returns the [`PeerIdentityError`] for an empty or oversized certificate.
    pub fn new(node: NodeId, certificate: &[u8]) -> Result<Self, PeerIdentityError> {
        if certificate.is_empty() {
            return Err(PeerIdentityError::EmptyCertificate);
        }
        if certificate.len() > Self::MAX_CERTIFICATE {
            return Err(PeerIdentityError::CertificateTooLarge);
        }
        Ok(Self {
            node,
            pin: fingerprint(certificate),
        })
    }
}

/// Why a certificate cannot be pinned as a [`PeerIdentity`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PeerIdentityError {
    /// The certificate encoding is empty.
    EmptyCertificate,
    /// The certificate encoding exceeds [`PeerIdentity::MAX_CERTIFICATE`] bytes.
    CertificateTooLarge,
}

impl fmt::Display for PeerIdentityError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyCertificate => f.write_str("peer certificate is empty"),
            Self::CertificateTooLarge => write!(
                f,
                "peer certificate exceeds {} bytes",
                PeerIdentity::MAX_CERTIFICATE
            ),
        }
    }
}

impl std::error::Error for PeerIdentityError {}

impl From<PeerIdentityError> for io::Error {
    fn from(error: PeerIdentityError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
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
