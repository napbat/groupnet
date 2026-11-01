//! Cluster-secret seals on router frames
//! ([`RouterConfig::cluster_secret`](super::RouterConfig::cluster_secret)).
//!
//! A sealing router seals every complete envelope it sends, adverts and data
//! alike, before fragmentation: it rewrites the envelope magic from the open
//! `GNR4` to the sealed `GNS2` and appends a [`TAG_LEN`]-byte HMAC over the
//! domain label, its own node id and the sealed envelope. A forwarding router
//! decrements the hop count and seals again under its own id. An envelope
//! larger than a link's MTU leaves as `GNT1` fragments, each with its own
//! tag over another domain label, the sending router's id, the fragment
//! header and the chunk.
//!
//! The receiving router verifies each fragment's tag against the sender its
//! link attributed it to before reassembly touches it, and the envelope's
//! seal before any decode, replay check, route learning, address
//! confirmation, delivery or forwarding; anything without a valid tag or seal
//! is dropped and counted in [`Router::auth_failures`](super::Router::auth_failures).
//!
//! The seal binds the sending hop, so a frame cannot be re-attributed to
//! another neighbor. It does not make a frame fresh: the router's replay
//! windows and advert freshness (see `routing`) refuse captured frames, within
//! the limits documented on [`RouterConfig::cluster_secret`](super::RouterConfig::cluster_secret).
//! Every holder of the secret can seal anything: the seal admits the
//! cluster, not one identity.

use bytes::{Bytes, BytesMut};
use groupnet_core::NodeId;
use groupnet_transport::auth::{ClusterSecret, TAG_LEN};

use crate::wire::{self, FragmentFormat};

/// The domain label of every router seal; the trailing number is its version.
const DOMAIN: &[u8] = b"groupnet router seal 2";

/// The domain label of every fragment tag; the trailing number is its version.
const FRAGMENT_DOMAIN: &[u8] = b"groupnet router fragment 1";

/// A router's sealing policy: open without a secret, sealed with one.
pub(super) struct Sealing {
    secret: Option<ClusterSecret>,
}

impl Sealing {
    pub(super) fn new(secret: Option<ClusterSecret>) -> Self {
        Self { secret }
    }

    /// The envelope magic this router sends and accepts.
    pub(super) fn magic(&self) -> [u8; 4] {
        if self.secret.is_some() {
            wire::SEALED
        } else {
            wire::OPEN
        }
    }

    /// Bytes a seal adds to an envelope.
    pub(super) fn overhead(&self) -> usize {
        if self.secret.is_some() { TAG_LEN } else { 0 }
    }

    /// How this router frames the fragments it sends.
    pub(super) fn fragments(&self) -> FragmentFormat {
        if self.secret.is_some() {
            FragmentFormat {
                magic: wire::SEALED_FRAGMENT,
                seal: TAG_LEN,
            }
        } else {
            FragmentFormat {
                magic: wire::OPEN_FRAGMENT,
                seal: 0,
            }
        }
    }

    /// Seals a complete envelope from `sender` in place.
    pub(super) fn seal(&self, sender: &NodeId, envelope: &mut Vec<u8>) {
        if let Some(secret) = &self.secret {
            envelope[..4].copy_from_slice(&wire::SEALED);
            let tag = secret.tag(DOMAIN, &[sender.as_str().as_bytes(), envelope]);
            envelope.extend_from_slice(&tag);
        }
    }

    /// Replaces the seal of a sealed frame whose envelope changed in place.
    pub(super) fn reseal(&self, sender: &NodeId, sealed: &mut [u8]) {
        if let Some(secret) = &self.secret {
            let (envelope, seal) = sealed.split_at_mut(sealed.len() - TAG_LEN);
            seal.copy_from_slice(&secret.tag(DOMAIN, &[sender.as_str().as_bytes(), envelope]));
        }
    }

    /// The envelope length of `frame` when it carries a valid seal from
    /// `sender`; the whole frame when this router does not seal.
    pub(super) fn open(&self, sender: &NodeId, frame: &[u8]) -> Option<usize> {
        let Some(secret) = &self.secret else {
            return Some(frame.len());
        };
        let length = frame.len().checked_sub(TAG_LEN)?;
        let (envelope, seal) = frame.split_at(length);
        secret
            .verify(DOMAIN, &[sender.as_str().as_bytes(), envelope], seal)
            .then_some(length)
    }

    /// Tags one fragment from `sender` in place; the fragment came from
    /// [`wire::fragment`] under [`fragments`](Self::fragments), with room for
    /// the tag.
    pub(super) fn seal_fragment(&self, sender: &NodeId, fragment: &mut BytesMut) {
        if let Some(secret) = &self.secret {
            let tag = secret.tag(FRAGMENT_DOMAIN, &[sender.as_str().as_bytes(), fragment]);
            fragment.extend_from_slice(&tag);
        }
    }

    /// Checks a sealing router's tagged fragment from `sender` before
    /// reassembly: `Some` fragment without its tag when the tag verifies,
    /// `None` when it does not. Anything else, and everything a router
    /// without a secret receives, passes unchanged.
    pub(super) fn open_fragment(&self, sender: &NodeId, packet: Bytes) -> Option<Bytes> {
        let Some(secret) = &self.secret else {
            return Some(packet);
        };
        if !packet.starts_with(&wire::SEALED_FRAGMENT) {
            return Some(packet);
        }
        let length = packet.len().checked_sub(TAG_LEN)?;
        let (fragment, tag) = packet.split_at(length);
        secret
            .verify(
                FRAGMENT_DOMAIN,
                &[sender.as_str().as_bytes(), fragment],
                tag,
            )
            .then(|| packet.slice(..length))
    }

    /// Whether this sealing router received an open envelope or an open
    /// fragment. Their magics keep them off reassembly's sealed path, so they
    /// are refused (and counted) before reassembly.
    pub(super) fn unsealed(&self, frame: &[u8]) -> bool {
        self.secret.is_some()
            && (frame.starts_with(&wire::OPEN) || frame.starts_with(&wire::OPEN_FRAGMENT))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Frame, decode_bounded, test_id};

    fn keyed(byte: u8) -> Sealing {
        Sealing::new(Some(
            ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("secret"),
        ))
    }

    fn sealed(sealing: &Sealing, sender: &str) -> Vec<u8> {
        let mut frame = wire::advert(test_id(1), 0, &[NodeId::new(sender)]);
        sealing.seal(&NodeId::new(sender), &mut frame);
        frame
    }

    #[test]
    fn a_seal_opens_only_for_its_sender_and_secret() {
        let sealing = keyed(1);
        let frame = sealed(&sealing, "a");
        assert_eq!(&frame[..4], b"GNS2");
        let length = sealing.open(&NodeId::new("a"), &frame).expect("valid seal");
        assert_eq!(length, frame.len() - TAG_LEN);
        let Ok(Frame::Advert { path, .. }) =
            decode_bounded(&frame[..length], wire::SEALED, wire::MAX_FRAME, 16)
        else {
            panic!("sealed advert decodes");
        };
        assert_eq!(path, [NodeId::new("a")]);

        // Another claimed sender, another secret, a flipped byte or a
        // missing seal never opens.
        assert!(sealing.open(&NodeId::new("b"), &frame).is_none());
        assert!(keyed(2).open(&NodeId::new("a"), &frame).is_none());
        let mut flipped = frame.clone();
        flipped[5] ^= 1;
        assert!(sealing.open(&NodeId::new("a"), &flipped).is_none());
        let open = wire::advert(test_id(1), 0, &[NodeId::new("a")]);
        assert!(sealing.open(&NodeId::new("a"), &open).is_none());
        assert!(
            sealing
                .open(&NodeId::new("a"), &frame[..TAG_LEN - 1])
                .is_none()
        );
    }

    #[test]
    fn a_resealed_frame_opens_for_the_new_sender_only() {
        let sealing = keyed(1);
        let mut frame = sealed(&sealing, "a");
        frame[6] ^= 1;
        assert!(sealing.open(&NodeId::new("a"), &frame).is_none());
        sealing.reseal(&NodeId::new("b"), &mut frame);
        assert!(sealing.open(&NodeId::new("b"), &frame).is_some());
        assert!(sealing.open(&NodeId::new("a"), &frame).is_none());
    }

    /// An open router neither seals nor accepts a sealed envelope.
    #[test]
    fn an_open_router_keeps_the_open_envelope() {
        let open = Sealing::new(None);
        let mut frame = wire::advert(test_id(1), 0, &[NodeId::new("a")]);
        let plain = frame.clone();
        open.seal(&NodeId::new("a"), &mut frame);
        assert_eq!(frame, plain);
        assert_eq!(open.open(&NodeId::new("a"), &frame), Some(frame.len()));
        let frame = sealed(&keyed(1), "a");
        assert!(decode_bounded(&frame, open.magic(), wire::MAX_FRAME, 16).is_err());
    }

    #[test]
    fn only_a_sealing_router_refuses_open_envelopes_and_fragments() {
        let open = wire::advert(test_id(1), 0, &[NodeId::new("a")]);
        let fragment = open_fragment(Bytes::from(open.clone()));
        assert!(keyed(1).unsealed(&open));
        assert!(keyed(1).unsealed(&fragment));
        assert!(!keyed(1).unsealed(&sealed(&keyed(1), "a")));
        assert!(!Sealing::new(None).unsealed(&open));
        assert!(!Sealing::new(None).unsealed(&fragment));
    }

    /// The first fragment of `envelope` at a 64-byte MTU, as `sealing` sends it.
    fn first_fragment(sealing: &Sealing, sender: &str, envelope: Bytes) -> Bytes {
        let mut fragment = wire::fragment(envelope, 64, test_id(2), sealing.fragments())
            .next()
            .expect("a fragment");
        sealing.seal_fragment(&NodeId::new(sender), &mut fragment);
        fragment.freeze()
    }

    fn open_fragment(envelope: Bytes) -> Bytes {
        first_fragment(&Sealing::new(None), "a", envelope)
    }

    #[test]
    fn a_fragment_tag_opens_only_for_its_sender_and_secret() {
        let sealing = keyed(1);
        let envelope = Bytes::from(sealed(&sealing, "a"));
        let fragment = first_fragment(&sealing, "a", envelope);
        assert_eq!(fragment.len(), 64, "the tag fits the MTU");
        assert_eq!(&fragment[..4], b"GNT1");
        let opened = sealing
            .open_fragment(&NodeId::new("a"), fragment.clone())
            .expect("valid tag");
        assert_eq!(opened, fragment.slice(..64 - TAG_LEN));

        assert!(
            sealing
                .open_fragment(&NodeId::new("b"), fragment.clone())
                .is_none()
        );
        assert!(
            keyed(2)
                .open_fragment(&NodeId::new("a"), fragment.clone())
                .is_none()
        );
        let mut flipped = fragment.to_vec();
        flipped[8] ^= 1;
        assert!(
            sealing
                .open_fragment(&NodeId::new("a"), flipped.into())
                .is_none()
        );
        // The same bytes tagged under the envelope seal's domain are not a
        // fragment tag.
        let body = fragment.slice(..64 - TAG_LEN);
        let secret = ClusterSecret::new([1; ClusterSecret::MIN_LEN]).expect("secret");
        let mut relabeled = body.to_vec();
        relabeled.extend_from_slice(&secret.tag(DOMAIN, &[b"a".as_slice(), &body[..]]));
        assert!(
            sealing
                .open_fragment(&NodeId::new("a"), relabeled.into())
                .is_none()
        );

        // An open router neither tags fragments nor checks them.
        let open = Sealing::new(None);
        let plain = open_fragment(Bytes::from(wire::advert(
            test_id(1),
            0,
            &[NodeId::new("a")],
        )));
        assert_eq!(&plain[..4], b"GNF1");
        assert_eq!(
            open.open_fragment(&NodeId::new("a"), plain.clone()),
            Some(plain)
        );
    }
}
