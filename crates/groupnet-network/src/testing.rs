//! Inspection of router envelopes for internal transport fault/count fixtures.

use crate::wire::{self, Frame, PayloadKind};

/// Borrows the control-message payload of an unfragmented routed data frame.
///
/// Returns `None` for malformed frames, route advertisements, tunnel traffic,
/// and physical fragments. Uses the router's decoder at wire representability
/// bounds; node-specific configured resource limits remain the router's policy.
#[must_use]
pub fn message_payload(frame: &[u8]) -> Option<&[u8]> {
    match wire::decode_bounded(
        frame,
        usize::try_from(u32::MAX).unwrap_or(usize::MAX),
        usize::from(u8::MAX),
    )
    .ok()?
    {
        Frame::Data {
            kind: PayloadKind::Message,
            payload,
            ..
        } => Some(payload),
        Frame::Advert { .. }
        | Frame::Data {
            kind: PayloadKind::Tunnel | PayloadKind::Application(_),
            ..
        } => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use groupnet_core::NodeId;

    #[test]
    fn only_valid_unfragmented_control_messages_expose_payload() {
        let from = NodeId::new("a");
        let to = NodeId::new("b");
        let payload = b"engine control frame";
        let frame = wire::data(PayloadKind::Message, 16, [7; 16], &from, &to, payload);
        assert_eq!(message_payload(&frame), Some(payload.as_slice()));
        assert!(message_payload(&wire::advert(0, std::slice::from_ref(&from))).is_none());
        assert!(
            message_payload(&wire::data(
                PayloadKind::Tunnel,
                16,
                [7; 16],
                &from,
                &to,
                payload
            ))
            .is_none()
        );
        for fragment in wire::fragment(frame.clone().into(), 40, [7; 16]) {
            assert!(message_payload(&fragment).is_none());
        }
        for length in 0..frame.len() - payload.len() {
            assert!(message_payload(&frame[..length]).is_none());
        }
        let mut invalid = frame;
        invalid[5] = 0;
        assert!(message_payload(&invalid).is_none());
    }
}
