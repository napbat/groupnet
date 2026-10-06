//! Session-bound return-routability; public offers alone never authorize data.

use super::super::PendingCapability;
use super::{
    AdmittedInbound, Body, Capability, Endpoint, Inbound, Instant, Packet, PathPolicy, Peer,
    Session, SocketAddr, lock, random,
};

impl Endpoint {
    pub(super) async fn receive_direct(&mut self, packet: Packet<'_>, address: SocketAddr) {
        if self.config.policy == PathPolicy::RelayOnly {
            return;
        }
        let now = Instant::now();
        let response = {
            let mut peers = lock(&self.peers);
            let Some(peer) = peers.get_mut(packet.sender) else {
                return;
            };
            if peer.relay_only
                || peer.address != Some(address)
                || peer.session != packet.session
                || peer.path(now).is_none()
                || !peer.lease.is_active()
            {
                return;
            }
            match packet.body {
                Body::Probe { target, nonce } if target == self.session => {
                    // Unproven probes cannot affect data replay state, an active
                    // capability, or an outstanding responder challenge.
                    if packet.sequence <= peer.confirmed_probe
                        || peer.pending.is_some_and(|pending| {
                            pending.capability.live(now)
                                || pending.nonce == nonce
                                || pending.sequence == packet.sequence
                        })
                    {
                        return;
                    }
                    let Ok(token) = random() else {
                        return;
                    };
                    peer.pending = Some(PendingCapability {
                        capability: Capability { token, issued: now },
                        nonce,
                        sequence: packet.sequence,
                    });
                    Some(Body::ProbeAck {
                        target: peer.session,
                        nonce,
                        capability: token,
                    })
                }
                Body::ProbeAck {
                    target,
                    nonce,
                    capability,
                } if target == self.session
                    && peer
                        .probe
                        .is_some_and(|probe| probe.token == nonce && probe.live(now)) =>
                {
                    peer.probe = None;
                    peer.direct = Some(Capability {
                        token: capability,
                        issued: now,
                    });
                    Some(Body::Confirm {
                        target: peer.session,
                        capability,
                    })
                }
                Body::Confirm { target, capability } if target == self.session => {
                    Self::confirm_capability(peer, capability, now);
                    None
                }
                Body::Direct {
                    peer: target_name,
                    target,
                    capability,
                    message,
                } if target_name == self.config.local.as_str()
                    && target == self.session
                    && packet.sequence > peer.sequence =>
                {
                    // First data may also complete the responder's round trip
                    // if the explicit confirmation was lost/reordered by UDP.
                    if !peer.confirmed.is_some_and(|confirmed| {
                        confirmed.token == capability && confirmed.live(now)
                    }) && !Self::confirm_capability(peer, capability, now)
                    {
                        return;
                    }
                    peer.sequence = packet.sequence;
                    if let Ok(permit) = self.incoming.try_reserve() {
                        permit.send(AdmittedInbound {
                            packet: Inbound {
                                from: peer.node.clone(),
                                msg: message.to_vec(),
                            },
                            session: Some(peer.lease.id()),
                        });
                    }
                    None
                }
                _ => None,
            }
        };
        if let Some(body) = response {
            self.transmit(address, body).await;
        }
    }

    fn confirm_capability(peer: &mut Peer, token: Session, now: Instant) -> bool {
        let Some(pending) = peer
            .pending
            .filter(|pending| pending.capability.token == token && pending.capability.live(now))
        else {
            return false;
        };
        peer.pending = None;
        peer.confirmed = Some(pending.capability);
        peer.confirmed_probe = pending.sequence;
        true
    }
}
