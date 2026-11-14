//! The keys of the TLS data plane: a [`TcpTlsKey`] signs one side's TLS
//! handshakes, a [`TcpTlsPublicKey`] is what the other side pins. Keys travel
//! as RFC 7250 raw public keys (a DER `SubjectPublicKeyInfo`), never inside a
//! certificate, so no certificate authority, name or validity period is
//! involved: a key is trusted because it is pinned.

use std::sync::Arc;
use std::{error, fmt, io};

use ring::digest::{SHA256, digest};
use ring::rand::SystemRandom;
use ring::signature::Ed25519KeyPair;
use rustls::crypto::ring::sign::any_supported_type;
use rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
use rustls::sign::{CertifiedKey, SigningKey};

/// A private key that authenticates one side of a TLS data-plane connection:
/// a hub's long-lived identity, or a client's key for one connection.
///
/// [`generate`](Self::generate) makes an Ed25519 key;
/// [`from_pkcs8_der`](Self::from_pkcs8_der) loads any PKCS#8 key the ring
/// provider signs with (Ed25519, ECDSA P-256 or P-384, RSA). A hub persists
/// its key with [`secret_pkcs8_der`](Self::secret_pkcs8_der) so that its
/// [`public_key`](Self::public_key), the value its clients pin, survives a
/// restart.
///
/// Its `Debug` output names only the public key's fingerprint. Cloning it is
/// cheap. Like [`ClusterSecret`](crate::ClusterSecret), it does not wipe its
/// key material on drop.
#[derive(Clone)]
pub struct TcpTlsKey {
    pkcs8: Arc<PrivatePkcs8KeyDer<'static>>,
    certified: Arc<CertifiedKey>,
    public: TcpTlsPublicKey,
}

impl fmt::Debug for TcpTlsKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpTlsKey")
            .field("public", &self.public)
            .finish_non_exhaustive()
    }
}

impl TcpTlsKey {
    /// A fresh Ed25519 key from the operating system's random source.
    ///
    /// # Errors
    /// Returns [`TcpTlsKeyError::Generate`] when the random source fails.
    pub fn generate() -> Result<Self, TcpTlsKeyError> {
        let document = Ed25519KeyPair::generate_pkcs8(&SystemRandom::new())
            .map_err(|_| TcpTlsKeyError::Generate)?;
        Self::from_pkcs8_der(document.as_ref().to_vec())
    }

    /// Loads a DER PKCS#8 private key.
    ///
    /// # Errors
    /// Returns [`TcpTlsKeyError::Malformed`] for bytes that are no key the
    /// ring provider signs with, and [`TcpTlsKeyError::NoPublicKey`] for a key
    /// whose public half the provider cannot encode.
    pub fn from_pkcs8_der(der: Vec<u8>) -> Result<Self, TcpTlsKeyError> {
        let pkcs8 = PrivatePkcs8KeyDer::from(der);
        let signing = any_supported_type(&PrivateKeyDer::Pkcs8(pkcs8.clone_key()))
            .map_err(TcpTlsKeyError::Malformed)?;
        let public = public_of(signing.as_ref())?;
        let certified = CertifiedKey::new(vec![CertificateDer::from(public.der.to_vec())], signing);
        Ok(Self {
            pkcs8: Arc::new(pkcs8),
            certified: Arc::new(certified),
            public,
        })
    }

    /// The DER PKCS#8 encoding of this private key, for the owner to store
    /// as a secret. Never log it.
    #[must_use]
    pub fn secret_pkcs8_der(&self) -> &[u8] {
        self.pkcs8.secret_pkcs8_der()
    }

    /// The public key a peer pins for this key.
    #[must_use]
    pub fn public_key(&self) -> &TcpTlsPublicKey {
        &self.public
    }

    /// The key as rustls presents it: the raw public key and its signer.
    pub(super) fn certified(&self) -> Arc<CertifiedKey> {
        self.certified.clone()
    }
}

/// The public key of a [`TcpTlsKey`]: a DER `SubjectPublicKeyInfo`, compared
/// byte for byte with the key a peer presents.
///
/// A client pins each hub's key with `register_peer`; an accepted
/// [`TcpTlsStream`](super::TcpTlsStream) reports the key its client proved it
/// holds. Its `Debug` output is the SHA-256 fingerprint of the encoding.
#[derive(Clone, PartialEq, Eq, Hash)]
pub struct TcpTlsPublicKey {
    der: Arc<[u8]>,
}

impl fmt::Debug for TcpTlsPublicKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TcpTlsPublicKey(sha256:")?;
        for byte in self.fingerprint() {
            write!(f, "{byte:02x}")?;
        }
        f.write_str(")")
    }
}

impl TcpTlsPublicKey {
    /// Longest encoding accepted, in bytes: room for a 4096-bit RSA key.
    pub const MAX_DER_BYTES: usize = 1024;

    /// A public key from its DER `SubjectPublicKeyInfo` encoding, as
    /// [`as_der`](Self::as_der) returns it. The encoding is not parsed here:
    /// a key that is not well formed matches no peer's key.
    ///
    /// # Errors
    /// Returns the [`TcpTlsPublicKeyError`] for an empty or overlong encoding.
    pub fn from_der(der: &[u8]) -> Result<Self, TcpTlsPublicKeyError> {
        if der.is_empty() {
            return Err(TcpTlsPublicKeyError::Empty);
        }
        if der.len() > Self::MAX_DER_BYTES {
            return Err(TcpTlsPublicKeyError::TooLong { len: der.len() });
        }
        Ok(Self { der: der.into() })
    }

    /// The DER `SubjectPublicKeyInfo` encoding, for the owner to publish to
    /// the peers that pin it.
    #[must_use]
    pub fn as_der(&self) -> &[u8] {
        &self.der
    }

    /// The SHA-256 digest of [`as_der`](Self::as_der), for logs and
    /// out-of-band comparison.
    #[must_use]
    pub fn fingerprint(&self) -> [u8; 32] {
        let mut fingerprint = [0; 32];
        fingerprint.copy_from_slice(digest(&SHA256, &self.der).as_ref());
        fingerprint
    }
}

/// The public half of `signing`.
fn public_of(signing: &dyn SigningKey) -> Result<TcpTlsPublicKey, TcpTlsKeyError> {
    let spki = signing.public_key().ok_or(TcpTlsKeyError::NoPublicKey)?;
    Ok(TcpTlsPublicKey {
        der: spki.as_ref().into(),
    })
}

/// Why a [`TcpTlsKey`] cannot be made.
#[derive(Debug)]
pub enum TcpTlsKeyError {
    /// The operating system's random source failed.
    Generate,
    /// The bytes are no PKCS#8 key the ring provider signs with; the rustls
    /// cause is the `source()`.
    Malformed(rustls::Error),
    /// The provider cannot encode the key's public half.
    NoPublicKey,
}

impl fmt::Display for TcpTlsKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Generate => f.write_str("TLS key generation failed"),
            Self::Malformed(_) => f.write_str("TLS private key is not a supported PKCS#8 key"),
            Self::NoPublicKey => f.write_str("TLS private key has no encodable public key"),
        }
    }
}

impl error::Error for TcpTlsKeyError {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        match self {
            Self::Malformed(error) => Some(error),
            Self::Generate | Self::NoPublicKey => None,
        }
    }
}

impl From<TcpTlsKeyError> for io::Error {
    fn from(error: TcpTlsKeyError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Why bytes cannot form a [`TcpTlsPublicKey`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpTlsPublicKeyError {
    /// The encoding is empty.
    Empty,
    /// The encoding exceeds [`TcpTlsPublicKey::MAX_DER_BYTES`].
    TooLong {
        /// The encoding's length in bytes.
        len: usize,
    },
}

impl fmt::Display for TcpTlsPublicKeyError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Empty => f.write_str("TLS public key is empty"),
            Self::TooLong { len } => write!(
                f,
                "TLS public key is {len} bytes, above the {}-byte limit",
                TcpTlsPublicKey::MAX_DER_BYTES
            ),
        }
    }
}

impl error::Error for TcpTlsPublicKeyError {}

impl From<TcpTlsPublicKeyError> for io::Error {
    fn from(error: TcpTlsPublicKeyError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_generated_key_round_trips_through_pkcs8() {
        let key = TcpTlsKey::generate().expect("generate");
        let loaded = TcpTlsKey::from_pkcs8_der(key.secret_pkcs8_der().to_vec()).expect("load");
        assert_eq!(loaded.public_key(), key.public_key());
        assert_ne!(
            TcpTlsKey::generate().expect("generate").public_key(),
            key.public_key(),
            "every generated key is fresh"
        );
        let pinned = TcpTlsPublicKey::from_der(key.public_key().as_der()).expect("pin");
        assert_eq!(&pinned, key.public_key());
    }

    #[test]
    fn malformed_keys_and_pins_are_typed_errors() {
        assert!(matches!(
            TcpTlsKey::from_pkcs8_der(b"not a key".to_vec()),
            Err(TcpTlsKeyError::Malformed(_))
        ));
        assert_eq!(
            TcpTlsPublicKey::from_der(&[]),
            Err(TcpTlsPublicKeyError::Empty)
        );
        let long = vec![0; TcpTlsPublicKey::MAX_DER_BYTES + 1];
        assert_eq!(
            TcpTlsPublicKey::from_der(&long),
            Err(TcpTlsPublicKeyError::TooLong { len: long.len() })
        );
    }

    #[test]
    fn debug_prints_only_the_fingerprint() {
        let key = TcpTlsKey::generate().expect("generate");
        let printed = format!("{key:?}");
        assert!(printed.contains("sha256:"));
        assert!(!printed.contains(&format!("{:?}", key.secret_pkcs8_der())));
    }
}
