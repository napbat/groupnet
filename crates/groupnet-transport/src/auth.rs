//! Cluster authentication: one shared [`ClusterSecret`] that every node of a
//! cluster holds, and the keyed-hash primitives the bindings build on.
//!
//! A secret proves membership of the cluster, not a particular identity:
//! every holder can speak for every node id, so a node that holds it is
//! trusted. What it rules out is a party that reaches a node's cluster port
//! without it. The router seals each frame with it
//! (`groupnet_network::RouterConfig::cluster_secret`) and the TCP data plane
//! runs a mutual challenge with it (`groupnet_transport_tcp::TcpBulkConfig`).
//! A cluster that configures no secret authenticates nothing and must run on
//! a private network that only its nodes can reach.
//!
//! The primitive is HMAC-SHA256 (`ring`). [`ClusterSecret::tag`] hashes a
//! domain label and a list of parts, each prefixed with its big-endian `u64`
//! length, so no two different inputs share an encoding.
//! [`ClusterSecret::verify`] compares in constant time. [`fresh_nonce`] draws
//! from the operating system's random source.

use std::{error, fmt, io};

use ring::hmac;
use ring::rand::{SecureRandom, SystemRandom};

/// Bytes in a [`ClusterSecret::tag`].
pub const TAG_LEN: usize = 32;

/// Bytes in a [`fresh_nonce`].
pub const NONCE_LEN: usize = 32;

/// The shared secret of a cluster: an HMAC-SHA256 key of at least
/// [`MIN_LEN`](Self::MIN_LEN) bytes.
///
/// It keeps only the derived key, never the bytes it was built from, and its
/// `Debug` output names no part of it. Cloning it is cheap.
#[derive(Clone)]
pub struct ClusterSecret {
    key: hmac::Key,
}

impl fmt::Debug for ClusterSecret {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("ClusterSecret(..)")
    }
}

impl ClusterSecret {
    /// Fewest secret bytes accepted: the HMAC-SHA256 output size. Use
    /// uniformly random bytes, not a password.
    pub const MIN_LEN: usize = 32;

    /// Builds a secret from `bytes`.
    ///
    /// # Errors
    /// Returns [`ClusterSecretError::TooShort`] for fewer than
    /// [`MIN_LEN`](Self::MIN_LEN) bytes.
    pub fn new(bytes: impl AsRef<[u8]>) -> Result<Self, ClusterSecretError> {
        let bytes = bytes.as_ref();
        if bytes.len() < Self::MIN_LEN {
            return Err(ClusterSecretError::TooShort {
                len: bytes.len(),
                min: Self::MIN_LEN,
            });
        }
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, bytes),
        })
    }

    /// The keyed hash of `domain` and `parts`. Every use names its own
    /// `domain` (protocol, version and purpose), so a tag made for one use
    /// never verifies in another.
    #[must_use]
    pub fn tag(&self, domain: &[u8], parts: &[&[u8]]) -> [u8; TAG_LEN] {
        let mut context = hmac::Context::with_key(&self.key);
        for part in std::iter::once(domain).chain(parts.iter().copied()) {
            context.update(&(part.len() as u64).to_be_bytes());
            context.update(part);
        }
        let mut tag = [0; TAG_LEN];
        tag.copy_from_slice(context.sign().as_ref());
        tag
    }

    /// Whether `tag` is the [`tag`](Self::tag) of `domain` and `parts`. The
    /// comparison takes the same time wherever the bytes differ.
    #[must_use]
    pub fn verify(&self, domain: &[u8], parts: &[&[u8]], tag: &[u8]) -> bool {
        constant_time_eq(&self.tag(domain, parts), tag)
    }
}

/// Compares two byte strings without branching on their contents; only the
/// lengths, which are public, may end it early.
fn constant_time_eq(expected: &[u8], actual: &[u8]) -> bool {
    if expected.len() != actual.len() {
        return false;
    }
    let difference = expected
        .iter()
        .zip(actual)
        .fold(0u8, |difference, (a, b)| difference | (a ^ b));
    std::hint::black_box(difference) == 0
}

/// A fresh nonce from the operating system's random source.
///
/// # Errors
/// Returns an error when the operating system yields no randomness.
pub fn fresh_nonce() -> io::Result<[u8; NONCE_LEN]> {
    let mut nonce = [0; NONCE_LEN];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| io::Error::other("OS randomness unavailable"))?;
    Ok(nonce)
}

/// Why [`ClusterSecret::new`] refused its bytes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ClusterSecretError {
    /// The secret is shorter than [`ClusterSecret::MIN_LEN`].
    TooShort {
        /// The secret's length in bytes.
        len: usize,
        /// The fewest bytes accepted.
        min: usize,
    },
}

impl fmt::Display for ClusterSecretError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TooShort { len, min } => {
                write!(
                    f,
                    "cluster secret is {len} bytes, below the {min}-byte minimum"
                )
            }
        }
    }
}

impl error::Error for ClusterSecretError {}

impl From<ClusterSecretError> for io::Error {
    fn from(error: ClusterSecretError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn secret(byte: u8) -> ClusterSecret {
        ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
    }

    #[test]
    fn a_secret_below_the_minimum_is_refused() {
        let error = ClusterSecret::new([0; ClusterSecret::MIN_LEN - 1]).expect_err("too short");
        assert_eq!(
            error,
            ClusterSecretError::TooShort {
                len: ClusterSecret::MIN_LEN - 1,
                min: ClusterSecret::MIN_LEN,
            }
        );
        let error = io::Error::from(error);
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert!(ClusterSecret::new([0; 64]).is_ok());
    }

    #[test]
    fn debug_output_names_no_secret_byte() {
        assert_eq!(format!("{:?}", secret(0xab)), "ClusterSecret(..)");
    }

    #[test]
    fn a_tag_verifies_only_under_its_secret_domain_and_parts() {
        let key = secret(1);
        let tag = key.tag(b"domain", &[b"a", b"b"]);
        assert!(key.verify(b"domain", &[b"a", b"b"], &tag));
        assert!(!secret(2).verify(b"domain", &[b"a", b"b"], &tag));
        assert!(!key.verify(b"other", &[b"a", b"b"], &tag));
        assert!(!key.verify(b"domain", &[b"a", b"c"], &tag));
        assert!(!key.verify(b"domain", &[b"a", b"b"], &tag[..TAG_LEN - 1]));
        let mut flipped = tag;
        flipped[TAG_LEN - 1] ^= 1;
        assert!(!key.verify(b"domain", &[b"a", b"b"], &flipped));
    }

    /// Moving a byte across a part boundary changes the tag: the length
    /// prefixes make the encoding injective.
    #[test]
    fn part_boundaries_are_part_of_the_tag() {
        let key = secret(3);
        assert_ne!(key.tag(b"d", &[b"ab", b"c"]), key.tag(b"d", &[b"a", b"bc"]));
        assert_ne!(key.tag(b"da", &[b"b"]), key.tag(b"d", &[b"ab"]));
    }

    #[test]
    fn nonces_differ() {
        assert_ne!(fresh_nonce().expect("nonce"), fresh_nonce().expect("nonce"));
    }
}
