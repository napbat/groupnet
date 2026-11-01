//! Cluster-secret seals on router frames
//! ([`RouterConfig::cluster_secret`](super::RouterConfig::cluster_secret)).
//!
//! A sealing router seals every complete envelope it sends, adverts and data
//! alike, before fragmentation: it rewrites the envelope magic from the open
//! `GNR3` to the sealed `GNS1` and appends a [`TAG_LEN`]-byte HMAC over the
//! domain label, its own node id and the sealed envelope. A forwarding router
//! decrements the hop count and seals again under its own id. The receiving
//! router verifies the seal against the sender its link attributed the frame
//! to, after reassembly and before any decode, replay check, route learning,
//! delivery or forwarding; a frame without a valid seal is dropped and counted
//! in [`Router::auth_failures`](super::Router::auth_failures).
//!
//! The seal binds the sending hop, so a frame cannot be re-attributed to
//! another neighbor; a captured frame replayed by an outsider is a duplicate
//! that the router's replay windows drop when it is data, and an advert it has
//! already learned when it is an advert. Every holder of the secret can seal
//! anything: the seal admits the cluster, not one identity.

use groupnet_core::NodeId;
use groupnet_transport::auth::{ClusterSecret, TAG_LEN};

use crate::wire;

/// The domain label of every router seal; the trailing number is its version.
const DOMAIN: &[u8] = b"groupnet router seal 1";

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

    /// Whether this sealing router received a whole open envelope. Its magic
    /// keeps it off reassembly's sealed path, so it is refused (and counted)
    /// before reassembly; fragments of one fail [`open`](Self::open) after it.
    pub(super) fn unsealed(&self, frame: &[u8]) -> bool {
        self.secret.is_some() && frame.starts_with(&wire::OPEN)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::wire::{Frame, decode_bounded};

    fn keyed(byte: u8) -> Sealing {
        Sealing::new(Some(
            ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("secret"),
        ))
    }

    fn sealed(sealing: &Sealing, sender: &str) -> Vec<u8> {
        let mut frame = wire::advert(0, &[NodeId::new(sender)]);
        sealing.seal(&NodeId::new(sender), &mut frame);
        frame
    }

    #[test]
    fn a_seal_opens_only_for_its_sender_and_secret() {
        let sealing = keyed(1);
        let frame = sealed(&sealing, "a");
        assert_eq!(&frame[..4], b"GNS1");
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
        let open = wire::advert(0, &[NodeId::new("a")]);
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
        let mut frame = wire::advert(0, &[NodeId::new("a")]);
        let plain = frame.clone();
        open.seal(&NodeId::new("a"), &mut frame);
        assert_eq!(frame, plain);
        assert_eq!(open.open(&NodeId::new("a"), &frame), Some(frame.len()));
        let frame = sealed(&keyed(1), "a");
        assert!(decode_bounded(&frame, open.magic(), wire::MAX_FRAME, 16).is_err());
    }

    #[test]
    fn only_a_sealing_router_refuses_whole_open_envelopes() {
        let open = wire::advert(0, &[NodeId::new("a")]);
        assert!(keyed(1).unsealed(&open));
        assert!(!keyed(1).unsealed(&sealed(&keyed(1), "a")));
        assert!(!Sealing::new(None).unsealed(&open));
    }
}
