//! Session-authenticated candidate checks; public addresses never authorize data.

use bytes::Bytes;

use super::super::{PathCheck, Peer, PendingCapability, candidates::valid};
use super::{
    AdmittedInbound, Body, Capability, Endpoint, Inbound, Instant, Packet, PathPolicy, Session,
    SocketAddr, lock, random,
};

impl Endpoint {
    pub(super) fn receive_direct(
        &mut self,
        packet: Packet<'_>,
        address: SocketAddr,
        socket: usize,
    ) {
        if self.config.policy == PathPolicy::RelayOnly || !valid(address) {
            return;
        }
        let now = Instant::now();
        let response = {
            let mut peers = lock(&self.peers);
            let Some(peer) = peers.get_mut(packet.sender) else {
                return;
            };
            if peer.relay_only
                || peer.session != packet.session
                || peer.path(now).is_none()
                || !peer.lease.is_active()
                || !authenticated(peer, &packet)
            {
                return;
            }
            let Some(index) = check_index(peer, &packet, self.session, address, socket, now) else {
                return;
            };
            self.check_response(peer, index, &packet, now)
        };
        if let Some(body) = response {
            self.transmit_on(socket, address, body);
        }
    }

    fn check_response(
        &self,
        peer: &mut Peer,
        index: usize,
        packet: &Packet<'_>,
        now: Instant,
    ) -> Option<Body<'static>> {
        let check = &mut peer.checks[index];
        match packet.body {
            Body::Probe { target, nonce, .. } if target == self.session => challenge(
                check,
                packet.sequence,
                nonce,
                peer.session,
                peer.secret,
                now,
            ),
            Body::ProbeAck {
                target,
                nonce,
                capability,
                ..
            } if target == self.session
                && check
                    .probe
                    .is_some_and(|probe| probe.token == nonce && probe.live(now)) =>
            {
                check.probe = None;
                check.attempts = 0;
                check.direct = Some(Capability {
                    token: capability,
                    issued: now,
                });
                if peer.selected.is_none_or(|selected| {
                    peer.checks[selected]
                        .direct
                        .is_none_or(|cap| !cap.live(now))
                }) {
                    peer.selected = Some(index);
                }
                Some(Body::Confirm {
                    target: peer.session,
                    capability,
                    secret: peer.secret,
                })
            }
            Body::Confirm {
                target, capability, ..
            } if target == self.session => {
                confirm_capability(check, capability, now);
                None
            }
            Body::Direct { .. } => {
                self.deliver_direct(peer, index, packet, now);
                None
            }
            _ => None,
        }
    }

    fn deliver_direct(&self, peer: &mut Peer, index: usize, packet: &Packet<'_>, now: Instant) {
        let Body::Direct {
            peer: target_name,
            target,
            capability,
            message,
            ..
        } = packet.body
        else {
            return;
        };
        if target_name != self.config.local.as_str()
            || target != self.session
            || packet.sequence <= peer.sequence
        {
            return;
        }
        let check = &mut peer.checks[index];
        if !check
            .confirmed
            .is_some_and(|confirmed| confirmed.token == capability && confirmed.live(now))
            && !confirm_capability(check, capability, now)
        {
            return;
        }
        peer.sequence = packet.sequence;
        if let Ok(permit) = self.incoming.try_reserve() {
            permit.send(AdmittedInbound {
                packet: Inbound {
                    from: peer.node.clone(),
                    msg: Bytes::copy_from_slice(message),
                },
                session: Some(peer.lease.id()),
            });
        }
    }
}

fn authenticated(peer: &Peer, packet: &Packet<'_>) -> bool {
    let (Body::Probe { secret, .. }
    | Body::ProbeAck { secret, .. }
    | Body::Confirm { secret, .. }
    | Body::Direct { secret, .. }) = packet.body
    else {
        return false;
    };
    secret == super::wire::check_proof(peer.secret, *packet)
}

fn check_index(
    peer: &mut Peer,
    packet: &Packet<'_>,
    session: Session,
    address: SocketAddr,
    socket: usize,
    now: Instant,
) -> Option<usize> {
    if let Some(index) = peer
        .checks
        .iter()
        .position(|check| check.address == address && check.socket == socket)
    {
        return Some(index);
    }
    // Only an authenticated current-session probe can introduce a new tuple.
    if !matches!(packet.body, Body::Probe { target, .. } if target == session) {
        return None;
    }
    let check = PathCheck::new(address, socket);
    if peer.checks.len() < 32 {
        peer.checks.push(check);
        return Some(peer.checks.len() - 1);
    }
    let index = peer.checks.iter().position(|old| {
        old.direct.is_none_or(|cap| !cap.live(now))
            && old.confirmed.is_none_or(|cap| !cap.live(now))
            && old
                .pending
                .is_none_or(|pending| !pending.capability.live(now))
    })?;
    peer.checks[index] = check;
    if peer.selected == Some(index) {
        peer.selected = None;
    }
    Some(index)
}

fn challenge(
    check: &mut PathCheck,
    sequence: u64,
    nonce: Session,
    target: Session,
    secret: Session,
    now: Instant,
) -> Option<Body<'static>> {
    if sequence <= check.confirmed_probe
        || check.pending.is_some_and(|pending| {
            pending.capability.live(now) || pending.nonce == nonce || pending.sequence == sequence
        })
    {
        return None;
    }
    let token = random().ok()?;
    check.pending = Some(PendingCapability {
        capability: Capability { token, issued: now },
        nonce,
        sequence,
    });
    Some(Body::ProbeAck {
        target,
        nonce,
        capability: token,
        secret,
    })
}

fn confirm_capability(check: &mut PathCheck, token: Session, now: Instant) -> bool {
    let Some(pending) = check
        .pending
        .filter(|pending| pending.capability.token == token && pending.capability.live(now))
    else {
        return false;
    };
    check.pending = None;
    check.confirmed = Some(pending.capability);
    check.confirmed_probe = pending.sequence;
    true
}
