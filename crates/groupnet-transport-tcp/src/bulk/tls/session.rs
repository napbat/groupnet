//! The TLS layer of the TLS data plane: TLS 1.3 only, ring only, RFC 7250 raw
//! public keys in both directions, ALPN [`ALPN`], no session resumption, no
//! early data, no SNI.
//!
//! The hub presents its long-lived key and requires a client key, but accepts
//! any client key: the TLS handshake proves that the client holds the key it
//! presents, and the admission exchange that follows binds that connection to
//! the client's claim. The client accepts exactly the hub key it pinned and
//! refuses any other, before it sends its claim or credential.

use std::io;
use std::net::IpAddr;
use std::sync::Arc;

use rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
use rustls::client::{AlwaysResolvesClientRawPublicKeys, Resumption};
use rustls::crypto::{
    CryptoProvider, WebPkiSupportedAlgorithms, verify_tls13_signature_with_raw_key,
};
use rustls::pki_types::{CertificateDer, ServerName, SubjectPublicKeyInfoDer, UnixTime};
use rustls::server::danger::{ClientCertVerified, ClientCertVerifier};
use rustls::server::{AlwaysResolvesServerRawPublicKeys, NoServerSessionStorage};
use rustls::{
    CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, Error, ServerConfig,
    SignatureScheme,
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_rustls::{TlsAcceptor, TlsConnector, client, server};

use super::{TcpTlsKey, TcpTlsPublicKey};

/// The application protocol both sides require.
pub(super) const ALPN: &[u8] = b"groupnet/tcp-bulk/1";

/// Neither side offers TLS 1.2, so its signature check never runs.
const TLS12_REFUSED: &str = "TLS 1.2 is not offered";

/// The ring provider every configuration of one endpoint shares.
pub(super) fn provider() -> Arc<CryptoProvider> {
    Arc::new(rustls::crypto::ring::default_provider())
}

/// The hub's TLS configuration: its key, any client key, TLS 1.3, no
/// resumption or early data.
pub(super) fn acceptor(provider: &Arc<CryptoProvider>, key: &TcpTlsKey) -> io::Result<TlsAcceptor> {
    let verifier = AnyClientKey {
        algorithms: provider.signature_verification_algorithms,
    };
    let mut config = ServerConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .with_client_cert_verifier(Arc::new(verifier))
        .with_cert_resolver(Arc::new(AlwaysResolvesServerRawPublicKeys::new(
            key.certified(),
        )));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.send_tls13_tickets = 0;
    config.session_storage = Arc::new(NoServerSessionStorage {});
    config.max_early_data_size = 0;
    Ok(TlsAcceptor::from(Arc::new(config)))
}

/// A client's TLS configuration for one connection: its session `key`, the
/// hub key it pinned, TLS 1.3, no resumption, early data or SNI.
fn connector(
    provider: &Arc<CryptoProvider>,
    key: &TcpTlsKey,
    pin: &TcpTlsPublicKey,
) -> io::Result<TlsConnector> {
    let verifier = PinnedHubKey {
        pin: pin.clone(),
        algorithms: provider.signature_verification_algorithms,
    };
    let mut config = ClientConfig::builder_with_provider(provider.clone())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .map_err(io::Error::other)?
        .dangerous()
        .with_custom_certificate_verifier(Arc::new(verifier))
        .with_client_cert_resolver(Arc::new(AlwaysResolvesClientRawPublicKeys::new(
            key.certified(),
        )));
    config.alpn_protocols = vec![ALPN.to_vec()];
    config.resumption = Resumption::disabled();
    config.enable_early_data = false;
    config.enable_sni = false;
    Ok(TlsConnector::from(Arc::new(config)))
}

/// Runs the client side of TLS toward the hub pinned as `pin`, presenting
/// `key`. `remote` only fills the server name rustls requires; the pin is the
/// whole check.
///
/// # Errors
/// `PermissionDenied` for a hub whose key is not `pin`; `InvalidData` for any
/// other TLS failure (a peer that is no TLS data-plane endpoint among them);
/// any I/O error.
pub(super) async fn connect<IO>(
    io: IO,
    provider: &Arc<CryptoProvider>,
    key: &TcpTlsKey,
    pin: &TcpTlsPublicKey,
    remote: IpAddr,
) -> io::Result<client::TlsStream<IO>>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let stream = connector(provider, key, pin)?
        .connect(ServerName::IpAddress(remote.into()), io)
        .await
        .map_err(refusal)?;
    check_alpn(stream.get_ref().1.alpn_protocol())?;
    Ok(stream)
}

/// Runs the hub side of TLS, returning the stream and the key the client
/// proved it holds.
///
/// # Errors
/// `InvalidData` for a TLS failure (a plain `GNBH` hello among them) or a
/// missing protocol; any I/O error.
pub(super) async fn accept<IO>(
    io: IO,
    acceptor: &TlsAcceptor,
) -> io::Result<(server::TlsStream<IO>, TcpTlsPublicKey)>
where
    IO: AsyncRead + AsyncWrite + Unpin,
{
    let stream = acceptor.accept(io).await?;
    let connection = stream.get_ref().1;
    check_alpn(connection.alpn_protocol())?;
    let key = connection
        .peer_certificates()
        .and_then(|keys| keys.first())
        .ok_or_else(|| io::Error::new(io::ErrorKind::PermissionDenied, "client sent no key"))?;
    let key = TcpTlsPublicKey::from_der(key.as_ref())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))?;
    Ok((stream, key))
}

fn check_alpn(alpn: Option<&[u8]>) -> io::Result<()> {
    if alpn != Some(ALPN) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "peer did not agree on the TLS data-plane protocol",
        ));
    }
    Ok(())
}

/// Reports a refused hub key as `PermissionDenied`, keeping the TLS error.
fn refusal(error: io::Error) -> io::Error {
    let pinned = matches!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<Error>()),
        Some(Error::InvalidCertificate(_))
    );
    if !pinned {
        return error;
    }
    match error.into_inner() {
        Some(inner) => io::Error::new(io::ErrorKind::PermissionDenied, inner),
        None => io::Error::from(io::ErrorKind::PermissionDenied),
    }
}

/// Verifies a TLS 1.3 handshake signature against a raw public key.
fn verify13(
    message: &[u8],
    key: &CertificateDer<'_>,
    dss: &DigitallySignedStruct,
    algorithms: &WebPkiSupportedAlgorithms,
) -> Result<HandshakeSignatureValid, Error> {
    verify_tls13_signature_with_raw_key(
        message,
        &SubjectPublicKeyInfoDer::from(key.as_ref()),
        dss,
        algorithms,
    )
}

/// The client's check of the hub: exactly the pinned key.
#[derive(Debug)]
struct PinnedHubKey {
    pin: TcpTlsPublicKey,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for PinnedHubKey {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        _now: UnixTime,
    ) -> Result<ServerCertVerified, Error> {
        if intermediates.is_empty() && end_entity.as_ref() == self.pin.as_der() {
            Ok(ServerCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificate(
                CertificateError::ApplicationVerificationFailure,
            ))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _key: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General(TLS12_REFUSED.into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        key: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify13(message, key, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}

/// The hub's check of a client: any raw public key whose holder signs the
/// handshake. Admission, not the TLS layer, decides who the client is.
#[derive(Debug)]
struct AnyClientKey {
    algorithms: WebPkiSupportedAlgorithms,
}

impl ClientCertVerifier for AnyClientKey {
    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        &[]
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _now: UnixTime,
    ) -> Result<ClientCertVerified, Error> {
        if intermediates.is_empty() && end_entity.len() <= TcpTlsPublicKey::MAX_DER_BYTES {
            Ok(ClientCertVerified::assertion())
        } else {
            Err(Error::InvalidCertificate(CertificateError::BadEncoding))
        }
    }

    fn verify_tls12_signature(
        &self,
        _message: &[u8],
        _key: &CertificateDer<'_>,
        _dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        Err(Error::General(TLS12_REFUSED.into()))
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        key: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, Error> {
        verify13(message, key, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }

    fn requires_raw_public_keys(&self) -> bool {
        true
    }
}
