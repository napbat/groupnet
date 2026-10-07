//! Safe pre-bind socket options, per-stream options, and retained candidate listeners.
//!
//! Every dialed and accepted stream passes through [`configure`], so latency-
//! sensitive small frames are never delayed by Nagle coalescing on any path.
use super::{MAX_CANDIDATES, TcpPunchConfig, invalid};
use socket2::{Domain, Protocol, Socket, Type};
use std::{
    io,
    net::{SocketAddr, TcpStream as StdStream},
};
use tokio::net::{TcpListener, TcpSocket, TcpStream};

pub(super) fn valid(address: SocketAddr) -> bool {
    if address.port() == 0 || address.ip().is_unspecified() || address.ip().is_multicast() {
        return false;
    }
    match address {
        SocketAddr::V4(address) => !address.ip().is_broadcast(),
        // Scope IDs are host-local, not meaningful across rendezvous clients.
        // Remote link-local candidates are therefore never probed.
        SocketAddr::V6(address) => !address.ip().is_unicast_link_local() && address.scope_id() == 0,
    }
}

fn socket(bind: SocketAddr) -> io::Result<Socket> {
    let socket = Socket::new(
        if bind.is_ipv4() {
            Domain::IPV4
        } else {
            Domain::IPV6
        },
        Type::STREAM,
        Some(Protocol::TCP),
    )?;
    socket.set_reuse_address(true)?;
    #[cfg(unix)]
    socket.set_reuse_port(true)?;
    if bind.is_ipv6() {
        socket.set_only_v6(true)?;
    }
    socket.set_nonblocking(true)?;
    socket.bind(&bind.into())?;
    Ok(socket)
}

#[cfg(test)]
pub(super) fn bound_socket(bind: SocketAddr) -> io::Result<Socket> {
    socket(bind)
}

pub(super) fn source(bind: SocketAddr) -> io::Result<TcpSocket> {
    let stream: StdStream = socket(bind)?.into();
    Ok(TcpSocket::from_std_stream(stream))
}

/// Applies the options every dialed and accepted TCP stream carries.
fn configure(stream: TcpStream) -> io::Result<TcpStream> {
    stream.set_nodelay(true)?;
    Ok(stream)
}

pub(super) async fn dial(bind: SocketAddr, target: SocketAddr) -> io::Result<TcpStream> {
    if bind.is_ipv4() != target.is_ipv4() || !valid(target) {
        return Err(invalid("invalid TCP dial candidate"));
    }
    configure(source(bind)?.connect(target).await?)
}

/// Connects from an ordinary, non-reusable source when reusable binding fails.
pub(super) async fn dial_exclusive(bind: SocketAddr, target: SocketAddr) -> io::Result<TcpStream> {
    let socket = if bind.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    socket.bind(bind)?;
    configure(socket.connect(target).await?)
}

pub(super) async fn accept(listener: &TcpListener) -> io::Result<(TcpStream, SocketAddr)> {
    let (stream, address) = listener.accept().await?;
    Ok((configure(stream)?, address))
}

pub(super) fn listen(bind: SocketAddr) -> io::Result<(TcpListener, SocketAddr)> {
    let socket = socket(bind)?;
    socket.listen(128)?;
    let listener = TcpListener::from_std(socket.into())?;
    let bound = listener.local_addr()?;
    Ok((listener, bound))
}

pub(super) fn gather(config: &TcpPunchConfig, binds: &[SocketAddr]) -> io::Result<Vec<SocketAddr>> {
    if config.policy == crate::PathPolicy::RelayOnly {
        return Ok(Vec::new());
    }
    let mut candidates = config.advertised_candidates.clone();
    let interfaces = if config.gather_interfaces {
        if_addrs::get_if_addrs()?
    } else {
        Vec::new()
    };
    for bind in binds {
        if valid(*bind) && !candidates.contains(bind) && candidates.len() < MAX_CANDIDATES {
            candidates.push(*bind);
        }
        if bind.ip().is_unspecified() {
            for interface in &interfaces {
                let ip = interface.ip();
                if ip.is_ipv4() != bind.is_ipv4() {
                    continue;
                }
                let address = SocketAddr::new(ip, bind.port());
                if valid(address)
                    && !candidates.contains(&address)
                    && candidates.len() < MAX_CANDIDATES
                {
                    candidates.push(address);
                }
            }
        }
    }
    Ok(candidates)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn dialed_and_accepted_streams_disable_nagle() {
        let (listener, address) = listen("127.0.0.1:0".parse().unwrap()).unwrap();
        let (dialed, accepted) = tokio::join!(
            dial("127.0.0.1:0".parse().unwrap(), address),
            accept(&listener)
        );
        assert!(dialed.unwrap().nodelay().unwrap());
        assert!(accepted.unwrap().0.nodelay().unwrap());
        let (exclusive, accepted) = tokio::join!(
            dial_exclusive("127.0.0.1:0".parse().unwrap(), address),
            accept(&listener)
        );
        assert!(exclusive.unwrap().nodelay().unwrap());
        assert!(accepted.unwrap().0.nodelay().unwrap());
    }
}
