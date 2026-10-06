//! Independent bounded socket polling and candidate-pair creation.

use std::io;
use std::net::SocketAddr;
use std::task::Poll;

use tokio::io::ReadBuf;
use tokio::net::UdpSocket;

use super::super::{PathCheck, Peer};
use super::Endpoint;
use tokio::time::Instant;

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
    pub(super) fn make_checks<const N: usize>(
        &self,
        addresses: super::super::candidates::CandidateSet<N>,
    ) -> Vec<PathCheck> {
        let mut checks = Vec::with_capacity(32);
        for address in addresses.iter() {
            for (index, socket) in std::iter::once(&self.socket)
                .chain(&self.extra_sockets)
                .enumerate()
            {
                if checks.len() < 32
                    && socket
                        .local_addr()
                        .is_ok_and(|bound| bound.is_ipv4() == address.is_ipv4())
                {
                    checks.push(PathCheck::new(address, index));
                }
            }
        }
        checks
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
                    .any(|check| check.address == address && check.socket == socket)
            {
                continue;
            }
            let mut check = PathCheck::new(address, socket);
            check.next_probe = now;
            if peer.checks.len() < 32 {
                peer.checks.push(check);
            } else if let Some(index) = peer.checks.iter().position(|old| {
                old.direct.is_none_or(|cap| !cap.live(now))
                    && old.confirmed.is_none_or(|cap| !cap.live(now))
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
