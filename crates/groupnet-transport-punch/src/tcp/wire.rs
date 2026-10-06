//! Independent bounded TCP framing and challenge-bound proofs.
use super::{MAX_CANDIDATES, MAX_PEERS, MAX_TCP_MESSAGE, invalid};
use groupnet_core::NodeId;
use ring::hmac;
use std::{
    io,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

pub(super) type Token = [u8; 32];
pub(super) type Auth = Option<Arc<Authentication>>;

pub(super) struct Authentication {
    key: hmac::Key,
    sequence: AtomicU64,
}

#[derive(Clone)]
pub(super) struct Duplex {
    pub(super) tx: Auth,
    pub(super) rx: Auth,
}

#[cfg(test)]
impl Duplex {
    pub(super) const fn plain() -> Self {
        Self { tx: None, rx: None }
    }
}

#[derive(Clone, Copy)]
pub(super) enum Role {
    Client,
    Server,
}

const MAX_FRAME: usize = MAX_TCP_MESSAGE + 4096;
const VERSION: u8 = 2;

pub(super) enum Message {
    Challenge(Token),
    Register {
        node: NodeId,
        session: Token,
        challenge: Token,
        credential: Vec<u8>,
        peers: Vec<NodeId>,
        dynamic: bool,
        relay_only: bool,
        candidates: Vec<SocketAddr>,
    },
    Welcome {
        observed: SocketAddr,
    },
    Denied,
    Intro {
        node: NodeId,
        session: Token,
        secret: Token,
        candidates: Vec<SocketAddr>,
    },
    Gone {
        node: NodeId,
        session: Token,
    },
    Relay {
        node: NodeId,
        session: Token,
        data: Vec<u8>,
    },
    Ping,
    Hello {
        node: NodeId,
        session: Token,
        target: Token,
        nonce: Token,
        proof: Token,
    },
    Answer {
        nonce: Token,
        proof: Token,
    },
    Finish(Token),
    Data(Vec<u8>),
}

pub(super) fn keyed(secret: &Token) -> Arc<Authentication> {
    Arc::new(Authentication {
        key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        sequence: AtomicU64::new(0),
    })
}

pub(super) fn auth(key: Option<&crate::NetworkKey>) -> Auth {
    key.map(|key| keyed(&key.to_bytes()))
}

pub(super) fn fresh(auth: &Auth) -> Auth {
    auth.as_ref().map(|auth| {
        Arc::new(Authentication {
            key: auth.key.clone(),
            sequence: AtomicU64::new(0),
        })
    })
}

pub(super) fn control_auth(auth: &Auth, challenge: &Token, session: &Token, role: Role) -> Duplex {
    let derive = |direction: &[u8]| {
        auth.as_ref().map(|auth| {
            let mut context = hmac::Context::with_key(&auth.key);
            context.update(b"groupnet TCP control session\0");
            context.update(challenge);
            context.update(session);
            context.update(direction);
            Arc::new(Authentication {
                key: hmac::Key::new(hmac::HMAC_SHA256, context.sign().as_ref()),
                sequence: AtomicU64::new(0),
            })
        })
    };
    let client = derive(b"client to server");
    let server = derive(b"server to client");
    match role {
        Role::Client => Duplex {
            tx: client,
            rx: server,
        },
        Role::Server => Duplex {
            tx: server,
            rx: client,
        },
    }
}

pub(super) fn proof(secret: &Token, domain: &[u8], parts: &[&[u8]]) -> Token {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(b"groupnet native TCP proof\0");
    context.update(domain);
    for part in parts {
        context.update(part);
    }
    let mut output = [0; 32];
    output.copy_from_slice(context.sign().as_ref());
    output
}

pub(super) fn matches(expected: &Token, actual: &Token) -> bool {
    // HMAC verification supplies a constant-time comparison without exposing
    // the introduction secret in the handshake.
    let key = hmac::Key::new(hmac::HMAC_SHA256, expected);
    hmac::verify(
        &key,
        b"TCP proof equality",
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, actual),
            b"TCP proof equality",
        )
        .as_ref(),
    )
    .is_ok()
}

pub(super) async fn write<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    message: &Message,
) -> io::Result<()> {
    let sequence = if let Some(auth) = auth {
        auth.sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |sequence| {
                sequence.checked_add(1)
            })
            .map_err(|_| invalid("TCP write sequence exhausted"))?
            + 1
    } else {
        0
    };
    let mut bytes = vec![VERSION, u8::from(auth.is_some())];
    bytes.extend_from_slice(&sequence.to_be_bytes());
    encode(message, &mut bytes)?;
    if let Some(auth) = auth {
        bytes.extend_from_slice(hmac::sign(&auth.key, &bytes).as_ref());
    }
    if bytes.len() > MAX_FRAME {
        return Err(invalid("TCP frame exceeds bound"));
    }
    let length = u32::try_from(bytes.len()).map_err(|_| invalid("TCP frame length"))?;
    stream.write_u32(length).await?;
    stream.write_all(&bytes).await
}

pub(super) async fn read<R: AsyncRead + Unpin>(stream: &mut R, auth: &Auth) -> io::Result<Message> {
    let length =
        usize::try_from(stream.read_u32().await?).map_err(|_| invalid("TCP frame length"))?;
    if !(11..=MAX_FRAME).contains(&length) {
        return Err(invalid("TCP frame length outside bound"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    if bytes[0] != VERSION || bytes[1] != u8::from(auth.is_some()) {
        return Err(invalid("TCP authentication mode/version mismatch"));
    }
    let sequence = u64::from_be_bytes(
        bytes[2..10]
            .try_into()
            .map_err(|_| invalid("TCP frame sequence"))?,
    );
    let end = if let Some(auth) = auth {
        let end = length
            .checked_sub(32)
            .filter(|end| *end >= 11)
            .ok_or_else(|| invalid("truncated keyed TCP frame"))?;
        hmac::verify(&auth.key, &bytes[..end], &bytes[end..])
            .map_err(|_| invalid("TCP authentication failed"))?;
        auth.sequence
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |previous| {
                (previous.checked_add(1) == Some(sequence)).then_some(sequence)
            })
            .map_err(|_| invalid("replayed or out-of-order TCP frame"))?;
        end
    } else {
        if sequence != 0 {
            return Err(invalid("unkeyed TCP sequence"));
        }
        length
    };
    let mut cursor = Cursor(&bytes[10..end]);
    let message = decode(&mut cursor)?;
    if !cursor.0.is_empty() {
        return Err(invalid("trailing TCP frame bytes"));
    }
    Ok(message)
}

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

fn encode(message: &Message, out: &mut Vec<u8>) -> io::Result<()> {
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

struct Cursor<'a>(&'a [u8]);

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

fn decode(cursor: &mut Cursor<'_>) -> io::Result<Message> {
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
            data: cursor.blob(MAX_TCP_MESSAGE)?,
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
        11 => Message::Data(cursor.blob(MAX_TCP_MESSAGE)?),
        _ => return Err(invalid("unknown TCP frame kind")),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn keyed_frames_do_not_downgrade() {
        let key = crate::NetworkKey::from_bytes([9; 32]);
        let (mut writer, mut reader) = tokio::io::duplex(256);
        write(&mut writer, &None, &Message::Ping).await.unwrap();
        assert!(read(&mut reader, &auth(Some(&key))).await.is_err());
    }

    #[test]
    fn proofs_bind_every_session_and_challenge() {
        let secret = [4; 32];
        let proof = proof(&secret, b"hello", &[&[1; 32], &[2; 32]]);
        assert!(!matches(
            &proof,
            &super::proof(&secret, b"hello", &[&[1; 32], &[3; 32]])
        ));
        assert!(!matches(
            &proof,
            &super::proof(&[5; 32], b"hello", &[&[1; 32], &[2; 32]])
        ));
    }

    #[tokio::test]
    async fn recorded_keyed_control_frames_cannot_cross_registration_sessions() {
        let master = auth(Some(&crate::NetworkKey::from_bytes([7; 32])));
        let old = control_auth(&master, &[1; 32], &[2; 32], Role::Server);
        let new = control_auth(&master, &[1; 32], &[3; 32], Role::Client);
        let (mut writer, mut reader) = tokio::io::duplex(256);
        write(
            &mut writer,
            &old.tx,
            &Message::Intro {
                node: NodeId::from("peer"),
                session: [4; 32],
                secret: [5; 32],
                candidates: Vec::new(),
            },
        )
        .await
        .unwrap();
        assert!(read(&mut reader, &new.rx).await.is_err());
    }

    #[test]
    fn every_message_round_trips_with_bounded_ipv4_ipv6_candidates() {
        let v4: SocketAddr = "127.0.0.1:1234".parse().unwrap();
        let v6: SocketAddr = "[::1]:4321".parse().unwrap();
        let messages = vec![
            Message::Challenge([1; 32]),
            Message::Register {
                node: NodeId::from("local"),
                session: [2; 32],
                challenge: [3; 32],
                credential: vec![4; 1024],
                peers: vec![NodeId::from("remote")],
                dynamic: true,
                relay_only: false,
                candidates: vec![v4, v6],
            },
            Message::Welcome { observed: v4 },
            Message::Denied,
            Message::Intro {
                node: NodeId::from("remote"),
                session: [5; 32],
                secret: [6; 32],
                candidates: vec![v6],
            },
            Message::Gone {
                node: NodeId::from("remote"),
                session: [5; 32],
            },
            Message::Relay {
                node: NodeId::from("remote"),
                session: [5; 32],
                data: vec![7; MAX_TCP_MESSAGE],
            },
            Message::Ping,
            Message::Hello {
                node: NodeId::from("remote"),
                session: [8; 32],
                target: [9; 32],
                nonce: [10; 32],
                proof: [11; 32],
            },
            Message::Answer {
                nonce: [12; 32],
                proof: [13; 32],
            },
            Message::Finish([14; 32]),
            Message::Data(Vec::new()),
        ];
        for message in messages {
            let mut bytes = Vec::new();
            encode(&message, &mut bytes).unwrap();
            let mut cursor = Cursor(&bytes);
            let decoded = decode(&mut cursor).unwrap();
            assert_eq!(cursor.0, []);
            let mut encoded = Vec::new();
            encode(&decoded, &mut encoded).unwrap();
            assert_eq!(bytes, encoded);
        }
        assert!(decode(&mut Cursor(&[255])).is_err());
        assert!(decode(&mut Cursor(&[0, 1])).is_err());
    }
}

#[cfg(test)]
pub(super) mod directional_tests {
    use super::*;

    async fn captured(auth: &Auth, message: &Message) -> Vec<u8> {
        let (mut writer, mut reader) = tokio::io::duplex(4096);
        write(&mut writer, auth, message).await.unwrap();
        drop(writer);
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).await.unwrap();
        bytes
    }

    pub(in crate::tcp) async fn rejects_reflection_and_replay(
        sender: &Duplex,
        recipient: &Duplex,
        message: &Message,
    ) {
        let bytes = captured(&sender.tx, message).await;
        let (mut writer, mut reader) = tokio::io::duplex(8192);
        writer.write_all(&bytes).await.unwrap();
        assert!(
            read(&mut reader, &sender.rx).await.is_err(),
            "reflected authentic outbound bytes"
        );
        let (mut writer, mut reader) = tokio::io::duplex(8192);
        writer.write_all(&bytes).await.unwrap();
        writer.write_all(&bytes).await.unwrap();
        assert!(read(&mut reader, &recipient.rx).await.is_ok());
        assert!(
            read(&mut reader, &recipient.rx).await.is_err(),
            "replayed authentic inbound bytes"
        );
    }

    #[tokio::test]
    async fn control_relay_bytes_are_direction_bound_and_monotonic() {
        let key = crate::NetworkKey::from_bytes([8; 32]);
        let master = auth(Some(&key));
        let client = control_auth(&master, &[1; 32], &[2; 32], Role::Client);
        let server = control_auth(&master, &[1; 32], &[2; 32], Role::Server);
        rejects_reflection_and_replay(
            &client,
            &server,
            &Message::Relay {
                node: NodeId::from("peer"),
                session: [3; 32],
                data: b"cannot reflect as peer traffic".to_vec(),
            },
        )
        .await;
    }
}
