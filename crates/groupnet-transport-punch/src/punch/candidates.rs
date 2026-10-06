//! Bounded socket leases and address gathering for native UDP traversal.

use std::io;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};
use tokio::net::UdpSocket;

use super::{PunchConfig, invalid};

pub(super) const MAX_CANDIDATES: usize = 8;
pub(super) const MAX_SOCKETS: usize = 4;
pub(super) type Candidates = CandidateSet<MAX_CANDIDATES>;
pub(super) type PeerCandidates = CandidateSet<9>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct CandidateSet<const N: usize> {
    ips: [[u8; 16]; N],
    ports: [u16; N],
    ipv4: u16,
    count: u8,
}

impl<const N: usize> Default for CandidateSet<N> {
    fn default() -> Self {
        Self {
            ips: [[0; 16]; N],
            ports: [0; N],
            ipv4: 0,
            count: 0,
        }
    }
}

impl<const N: usize> CandidateSet<N> {
    pub(super) fn insert(&mut self, address: SocketAddr) -> bool {
        if !valid(address) || self.iter().any(|existing| existing == address) {
            return false;
        }
        let index = usize::from(self.count);
        if index >= N {
            return false;
        }
        match address.ip() {
            IpAddr::V4(ip) => {
                self.ips[index][..4].copy_from_slice(&ip.octets());
                self.ipv4 |= 1 << index;
            }
            IpAddr::V6(ip) => self.ips[index] = ip.octets(),
        }
        self.ports[index] = address.port();
        self.count += 1;
        true
    }

    pub(super) fn iter(&self) -> impl Iterator<Item = SocketAddr> + '_ {
        (0..usize::from(self.count)).filter_map(|index| self.get(index))
    }

    pub(super) fn get(&self, index: usize) -> Option<SocketAddr> {
        if index >= usize::from(self.count) {
            return None;
        }
        let ip = if self.ipv4 & (1 << index) != 0 {
            IpAddr::V4(Ipv4Addr::new(
                self.ips[index][0],
                self.ips[index][1],
                self.ips[index][2],
                self.ips[index][3],
            ))
        } else {
            IpAddr::V6(Ipv6Addr::from(self.ips[index]))
        };
        Some(SocketAddr::new(ip, self.ports[index]))
    }

    pub(super) fn len(&self) -> usize {
        usize::from(self.count)
    }

    pub(super) fn is_empty(&self) -> bool {
        self.count == 0
    }
}

pub(super) fn valid(address: SocketAddr) -> bool {
    if address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast() {
        return false;
    }
    if matches!(address, SocketAddr::V6(address) if address.scope_id() != 0) {
        return false;
    }
    match address.ip() {
        IpAddr::V4(ip) => !ip.is_broadcast() && !ip.is_link_local(),
        // Scope identifiers are host-local and must never be copied from a peer.
        IpAddr::V6(ip) => !ip.is_unicast_link_local() && ip.to_ipv4_mapped().is_none(),
    }
}

pub(super) async fn bind(config: &PunchConfig) -> io::Result<(Vec<UdpSocket>, Candidates)> {
    if config.candidate_binds.len() >= MAX_SOCKETS
        || config.advertised_candidates.len() > MAX_CANDIDATES
        || config
            .advertised_candidates
            .iter()
            .any(|address| !valid(*address))
    {
        return Err(invalid("invalid UDP candidate count or address"));
    }
    let mut sockets = Vec::with_capacity(1 + config.candidate_binds.len());
    for address in std::iter::once(&config.bind).chain(&config.candidate_binds) {
        if address.ip().is_multicast()
            || matches!(address.ip(), IpAddr::V4(ip) if ip.is_link_local() || ip.is_broadcast())
            || matches!(address, SocketAddr::V6(address) if address.ip().is_unicast_link_local())
        {
            return Err(invalid(
                "multicast and link-local UDP binds are unsupported",
            ));
        }
        sockets.push(UdpSocket::bind(address).await?);
    }
    let mut candidates = Candidates::default();
    if config.policy == super::PathPolicy::RelayOnly {
        return Ok((sockets, candidates));
    }
    // Explicit operator candidates have deterministic priority. The server adds
    // its observed primary address separately, even if this bounded list fills.
    for address in &config.advertised_candidates {
        candidates.insert(*address);
    }
    let mut binds = [None; MAX_SOCKETS];
    for (slot, socket) in binds.iter_mut().zip(&sockets) {
        let bound = socket.local_addr()?;
        *slot = Some(bound);
        candidates.insert(bound);
    }
    if config.gather_interfaces {
        let interfaces = if_addrs::get_if_addrs()?;
        // Give each wildcard socket/family a local candidate before filling in
        // more interfaces; one multihomed socket cannot consume the whole list.
        for bound in binds
            .iter()
            .flatten()
            .filter(|bound| bound.ip().is_unspecified())
        {
            if let Some(ip) = interfaces.iter().map(if_addrs::Interface::ip).find(|ip| {
                ip.is_ipv4() == bound.is_ipv4() && valid(SocketAddr::new(*ip, bound.port()))
            }) {
                candidates.insert(SocketAddr::new(ip, bound.port()));
            }
        }
        for interface in &interfaces {
            let ip = interface.ip();
            for bound in binds
                .iter()
                .flatten()
                .filter(|bound| bound.ip().is_unspecified())
            {
                if ip.is_ipv4() == bound.is_ipv4() {
                    candidates.insert(SocketAddr::new(ip, bound.port()));
                }
            }
        }
    }
    Ok((sockets, candidates))
}
