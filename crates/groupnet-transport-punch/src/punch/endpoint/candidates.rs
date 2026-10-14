//! Independent bounded socket polling and candidate-pair creation.

use std::io;
use std::net::SocketAddr;
use std::task::Poll;

use tokio::io::ReadBuf;
use tokio::net::UdpSocket;
use tokio::time::Instant;

use super::super::candidates::{CandidateSet, PeerCandidates};
use super::super::{MAX_CHECKS, PathCheck, Peer};
use super::Endpoint;

pub(super) async fn receive(
    primary: &UdpSocket,
    extras: &[UdpSocket],
    buffer: &mut [u8],
    first: usize,
) -> io::Result<(usize, SocketAddr, usize)> {
    std::future::poll_fn(|context| {
        for offset in 0..=extras.len() {
            let index = (first + offset) % (extras.len() + 1);
            let socket = if index == 0 {
                primary
            } else {
                &extras[index - 1]
            };
            let mut read = ReadBuf::new(buffer);
            match socket.poll_recv_from(context, &mut read) {
                Poll::Ready(Ok(address)) => {
                    return Poll::Ready(Ok((read.filled().len(), address, index)));
                }
                Poll::Ready(Err(error)) => return Poll::Ready(Err(error)),
                Poll::Pending => {}
            }
        }
        Poll::Pending
    })
    .await
}

impl Endpoint {
    pub(super) fn make_checks<const N: usize>(&self, addresses: CandidateSet<N>) -> Vec<PathCheck> {
        let mut checks = Vec::with_capacity(MAX_CHECKS);
        self.fill_checks(&mut checks, addresses);
        checks
    }

    /// Appends each new compatible address/socket tuple until the table is full.
    fn fill_checks<const N: usize>(&self, checks: &mut Vec<PathCheck>, addresses: CandidateSet<N>) {
        for address in addresses.iter() {
            for (index, socket) in std::iter::once(&self.socket)
                .chain(&self.extra_sockets)
                .enumerate()
            {
                if checks.len() >= MAX_CHECKS {
                    return;
                }
                if socket
                    .local_addr()
                    .is_ok_and(|bound| bound.is_ipv4() == address.is_ipv4())
                    && !checks.iter().any(|check| check.tuple() == (address, index))
                {
                    checks.push(PathCheck::new(address, index));
                }
            }
        }
    }

    /// Re-targets a same-session peer at `addresses` without losing proof state.
    pub(super) fn refresh_checks(&self, peer: &mut Peer, addresses: PeerCandidates, now: Instant) {
        let selected = peer
            .selected
            .and_then(|index| peer.checks.get(index))
            .map(PathCheck::tuple);
        // Advertisements add probe targets; they cannot revoke a validated NAT
        // tuple. Keep matching checks intact, including their in-flight proofs.
        peer.checks
            .retain(|check| check.validated(now) || addresses.contains(check.address));
        self.fill_checks(&mut peer.checks, addresses);
        peer.selected = selected.and_then(|selected| {
            peer.checks
                .iter()
                .position(|check| check.tuple() == selected)
        });
        peer.addresses = addresses;
    }

    pub(super) fn rotate_check(&self, peer: &mut Peer, now: Instant) {
        let sockets = self.extra_sockets.len() + 1;
        let total = peer.addresses.iter().count() * sockets;
        if total == 0 {
            return;
        }
        for _ in 0..total {
            let pair = peer.candidate_cursor % total;
            peer.candidate_cursor = (pair + 1) % total;
            let socket = pair % sockets;
            let Some(address) = peer.addresses.iter().nth(pair / sockets) else {
                return;
            };
            let ipv4 = if socket == 0 {
                self.config.bind.is_ipv4()
            } else {
                self.config
                    .candidate_binds
                    .get(socket - 1)
                    .map_or(self.config.bind.is_ipv4(), SocketAddr::is_ipv4)
            };
            if address.is_ipv4() != ipv4
                || peer
                    .checks
                    .iter()
                    .any(|check| check.tuple() == (address, socket))
            {
                continue;
            }
            let mut check = PathCheck::new(address, socket);
            check.next_probe = now;
            if peer.checks.len() < MAX_CHECKS {
                peer.checks.push(check);
            } else if let Some(index) = peer.checks.iter().position(|old| {
                !old.validated(now)
                    && old
                        .pending
                        .is_none_or(|pending| !pending.capability.live(now))
                    && old.probe.is_none_or(|cap| !cap.live(now))
                    && old.next_probe <= now
            }) {
                peer.checks[index] = check;
                if peer.selected == Some(index) {
                    peer.selected = None;
                }
            } else {
                peer.candidate_cursor = pair;
            }
            // One rotation per maintenance turn prevents newly inserted tuples
            // from displacing each other before their independent checks run.
            break;
        }
    }
}
