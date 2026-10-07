//! Message body codec: identities, candidates, bounded blobs, and frame kinds.
use bytes::Bytes;
use groupnet_core::NodeId;
use std::{io, net::SocketAddr};
use zerocopy::FromBytes;
use zerocopy::byteorder::big_endian::U16;

use super::{Message, Token};
use crate::tcp::{MAX_CANDIDATES, MAX_PEERS, MAX_TCP_MESSAGE, invalid};

fn text(out: &mut Vec<u8>, value: &str) -> io::Result<()> {
    if value.is_empty() || value.len() > 64 {
        return Err(invalid("TCP identity must contain 1–64 UTF-8 bytes"));
    }
    out.push(u8::try_from(value.len()).map_err(|_| invalid("TCP identity length"))?);
    out.extend_from_slice(value.as_bytes());
    Ok(())
}

fn addresses(out: &mut Vec<u8>, values: &[SocketAddr]) -> io::Result<()> {
    if values.len() > MAX_CANDIDATES {
        return Err(invalid("too many TCP candidates"));
    }
    out.push(u8::try_from(values.len()).map_err(|_| invalid("TCP candidate count"))?);
    for value in values {
        match value {
            SocketAddr::V4(address) => {
                out.push(4);
                out.extend_from_slice(&address.ip().octets());
            }
            SocketAddr::V6(address) => {
                out.push(6);
                out.extend_from_slice(&address.ip().octets());
                out.extend_from_slice(&address.scope_id().to_be_bytes());
            }
        }
        out.extend_from_slice(&value.port().to_be_bytes());
    }
    Ok(())
}

fn blob(out: &mut Vec<u8>, bytes: &[u8], max: usize) -> io::Result<()> {
    if bytes.len() > max {
        return Err(invalid("TCP blob exceeds bound"));
    }
    out.extend_from_slice(
        &u16::try_from(bytes.len())
            .map_err(|_| invalid("TCP blob length"))?
            .to_be_bytes(),
    );
    out.extend_from_slice(bytes);
    Ok(())
}

pub(super) fn encode(message: &Message, out: &mut Vec<u8>) -> io::Result<()> {
    match message {
        Message::Challenge(token) => {
            out.push(0);
            out.extend_from_slice(token);
        }
        Message::Register {
            node,
            session,
            challenge,
            credential,
            peers,
            dynamic,
            relay_only,
            candidates,
        } => {
            out.push(1);
            text(out, node.as_str())?;
            out.extend_from_slice(session);
            out.extend_from_slice(challenge);
            blob(out, credential, 1024)?;
            if peers.len() > MAX_PEERS {
                return Err(invalid("too many TCP peers"));
            }
            out.push(u8::try_from(peers.len()).map_err(|_| invalid("TCP peer count"))?);
            for peer in peers {
                text(out, peer.as_str())?;
            }
            out.push(u8::from(*dynamic));
            out.push(u8::from(*relay_only));
            addresses(out, candidates)?;
        }
        Message::Welcome { observed } => {
            out.push(2);
            addresses(out, &[*observed])?;
        }
        Message::Denied => out.push(3),
        Message::Intro {
            node,
            session,
            secret,
            candidates,
        } => {
            out.push(4);
            text(out, node.as_str())?;
            out.extend_from_slice(session);
            out.extend_from_slice(secret);
            addresses(out, candidates)?;
        }
        Message::Gone { node, session } => {
            out.push(5);
            text(out, node.as_str())?;
            out.extend_from_slice(session);
        }
        Message::Relay {
            node,
            session,
            data,
        } => {
            out.push(6);
            text(out, node.as_str())?;
            out.extend_from_slice(session);
            blob(out, data, MAX_TCP_MESSAGE)?;
        }
        Message::Ping => out.push(7),
        Message::Hello {
            node,
            session,
            target,
            nonce,
            proof,
        } => {
            out.push(8);
            text(out, node.as_str())?;
            out.extend_from_slice(session);
            out.extend_from_slice(target);
            out.extend_from_slice(nonce);
            out.extend_from_slice(proof);
        }
        Message::Answer { nonce, proof } => {
            out.push(9);
            out.extend_from_slice(nonce);
            out.extend_from_slice(proof);
        }
        Message::Finish(proof) => {
            out.push(10);
            out.extend_from_slice(proof);
        }
        Message::Data(data) => {
            out.push(11);
            blob(out, data, MAX_TCP_MESSAGE)?;
        }
    }
    Ok(())
}

pub(super) struct Cursor<'a>(pub(super) &'a [u8]);

impl<'a> Cursor<'a> {
    fn take(&mut self, size: usize) -> io::Result<&'a [u8]> {
        let (value, rest) = self
            .0
            .split_at_checked(size)
            .ok_or_else(|| invalid("truncated TCP frame"))?;
        self.0 = rest;
        Ok(value)
    }

    fn byte(&mut self) -> io::Result<u8> {
        Ok(self.take(1)?[0])
    }

    fn boolean(&mut self) -> io::Result<bool> {
        match self.byte()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(invalid("invalid TCP boolean")),
        }
    }

    fn token(&mut self) -> io::Result<Token> {
        self.take(32)?
            .try_into()
            .map_err(|_| invalid("TCP token length"))
    }

    fn node(&mut self) -> io::Result<NodeId> {
        let size = usize::from(self.byte()?);
        if !(1..=64).contains(&size) {
            return Err(invalid("TCP identity length"));
        }
        let value =
            std::str::from_utf8(self.take(size)?).map_err(|_| invalid("TCP identity UTF-8"))?;
        Ok(NodeId::from(value))
    }

    fn blob(&mut self, max: usize) -> io::Result<Vec<u8>> {
        let size = usize::from(u16::from_be_bytes(
            self.take(2)?
                .try_into()
                .map_err(|_| invalid("TCP blob length"))?,
        ));
        if size > max {
            return Err(invalid("TCP blob exceeds bound"));
        }
        Ok(self.take(size)?.to_vec())
    }

    fn shared_blob(&mut self, max: usize, backing: &Bytes, end: usize) -> io::Result<Bytes> {
        let length = U16::ref_from_bytes(self.take(size_of::<U16>())?)
            .map_err(|_| invalid("TCP blob length"))?
            .get();
        let size = usize::from(length);
        if size > max {
            return Err(invalid("TCP blob exceeds bound"));
        }
        let offset = end - self.0.len();
        self.take(size)?;
        Ok(backing.slice(offset..offset + size))
    }

    fn addresses(&mut self) -> io::Result<Vec<SocketAddr>> {
        let count = usize::from(self.byte()?);
        if count > MAX_CANDIDATES {
            return Err(invalid("TCP candidate count"));
        }
        let mut addresses = Vec::with_capacity(count);
        for _ in 0..count {
            let (ip, scope) = match self.byte()? {
                4 => (
                    std::net::IpAddr::V4(std::net::Ipv4Addr::from(
                        <[u8; 4]>::try_from(self.take(4)?)
                            .map_err(|_| invalid("IPv4 TCP address"))?,
                    )),
                    0,
                ),
                6 => {
                    let ip = std::net::Ipv6Addr::from(
                        <[u8; 16]>::try_from(self.take(16)?)
                            .map_err(|_| invalid("IPv6 TCP address"))?,
                    );
                    let scope = u32::from_be_bytes(
                        self.take(4)?
                            .try_into()
                            .map_err(|_| invalid("IPv6 TCP scope"))?,
                    );
                    (std::net::IpAddr::V6(ip), scope)
                }
                _ => return Err(invalid("TCP address family")),
            };
            let port = u16::from_be_bytes(
                self.take(2)?
                    .try_into()
                    .map_err(|_| invalid("TCP address port"))?,
            );
            addresses.push(match ip {
                std::net::IpAddr::V4(ip) => SocketAddr::new(ip.into(), port),
                std::net::IpAddr::V6(ip) => std::net::SocketAddrV6::new(ip, port, 0, scope).into(),
            });
        }
        Ok(addresses)
    }
}

pub(super) fn decode(cursor: &mut Cursor<'_>, backing: &Bytes, end: usize) -> io::Result<Message> {
    Ok(match cursor.byte()? {
        0 => Message::Challenge(cursor.token()?),
        1 => {
            let node = cursor.node()?;
            let session = cursor.token()?;
            let challenge = cursor.token()?;
            let credential = cursor.blob(1024)?;
            let count = usize::from(cursor.byte()?);
            if count > MAX_PEERS {
                return Err(invalid("TCP peer count"));
            }
            let mut peers = Vec::with_capacity(count);
            for _ in 0..count {
                peers.push(cursor.node()?);
            }
            Message::Register {
                node,
                session,
                challenge,
                credential,
                peers,
                dynamic: cursor.boolean()?,
                relay_only: cursor.boolean()?,
                candidates: cursor.addresses()?,
            }
        }
        2 => {
            let addresses = cursor.addresses()?;
            if addresses.len() != 1 {
                return Err(invalid("TCP observed address"));
            }
            Message::Welcome {
                observed: addresses[0],
            }
        }
        3 => Message::Denied,
        4 => Message::Intro {
            node: cursor.node()?,
            session: cursor.token()?,
            secret: cursor.token()?,
            candidates: cursor.addresses()?,
        },
        5 => Message::Gone {
            node: cursor.node()?,
            session: cursor.token()?,
        },
        6 => Message::Relay {
            node: cursor.node()?,
            session: cursor.token()?,
            data: cursor.shared_blob(MAX_TCP_MESSAGE, backing, end)?,
        },
        7 => Message::Ping,
        8 => Message::Hello {
            node: cursor.node()?,
            session: cursor.token()?,
            target: cursor.token()?,
            nonce: cursor.token()?,
            proof: cursor.token()?,
        },
        9 => Message::Answer {
            nonce: cursor.token()?,
            proof: cursor.token()?,
        },
        10 => Message::Finish(cursor.token()?),
        11 => Message::Data(cursor.shared_blob(MAX_TCP_MESSAGE, backing, end)?),
        _ => return Err(invalid("unknown TCP frame kind")),
    })
}
