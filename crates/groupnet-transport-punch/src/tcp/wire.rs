//! Independent bounded TCP framing and challenge-bound proofs.
use super::{MAX_CANDIDATES, MAX_PEERS, MAX_TCP_MESSAGE, invalid};
use bytes::Bytes;
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
use zerocopy::byteorder::big_endian::{U16, U32, U64};
use zerocopy::{FromBytes, Immutable, IntoBytes, KnownLayout, Unaligned};

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

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FrameLength {
    length: U32,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct FrameHeader {
    version: u8,
    authenticated: u8,
    sequence: U64,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct DataHeader {
    kind: u8,
    length: U16,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RelayHeader {
    kind: u8,
    identity_length: u8,
}

#[derive(FromBytes, IntoBytes, KnownLayout, Immutable, Unaligned)]
#[repr(C)]
struct RelayTarget {
    session: Token,
    length: U16,
}

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
        data: Bytes,
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
    Data(Bytes),
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
    write_buffered(stream, auth, message, &mut Vec::new()).await
}

pub(super) async fn write_buffered<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    message: &Message,
    scratch: &mut Vec<u8>,
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
    let header = FrameHeader {
        version: VERSION,
        authenticated: u8::from(auth.is_some()),
        sequence: U64::new(sequence),
    };
    match message {
        Message::Data(data) => {
            let length = payload_length(data)?;
            let prefix = DataHeader {
                kind: 11,
                length: U16::new(length),
            };
            write_parts(stream, auth, &[header.as_bytes(), prefix.as_bytes(), data]).await
        }
        Message::Relay {
            node,
            session,
            data,
        } => {
            let length = payload_length(data)?;
            let id = node.as_str().as_bytes();
            if id.is_empty() || id.len() > 64 {
                return Err(invalid("TCP identity length"));
            }
            let prefix = RelayHeader {
                kind: 6,
                identity_length: u8::try_from(id.len()).expect("bounded identity"),
            };
            let target = RelayTarget {
                session: *session,
                length: U16::new(length),
            };
            write_parts(
                stream,
                auth,
                &[
                    header.as_bytes(),
                    prefix.as_bytes(),
                    id,
                    target.as_bytes(),
                    data,
                ],
            )
            .await
        }
        _ => {
            scratch.clear();
            scratch.extend_from_slice(header.as_bytes());
            encode(message, scratch)?;
            write_parts(stream, auth, &[scratch]).await
        }
    }
}

fn payload_length(data: &[u8]) -> io::Result<u16> {
    if data.len() > MAX_TCP_MESSAGE {
        return Err(invalid("TCP payload exceeds bound"));
    }
    u16::try_from(data.len()).map_err(|_| invalid("TCP payload length"))
}

async fn write_parts<W: AsyncWrite + Unpin>(
    stream: &mut W,
    auth: &Auth,
    parts: &[&[u8]],
) -> io::Result<()> {
    let length =
        parts.iter().map(|part| part.len()).sum::<usize>() + if auth.is_some() { 32 } else { 0 };
    if length > MAX_FRAME {
        return Err(invalid("TCP frame exceeds bound"));
    }
    let length = FrameLength {
        length: U32::new(u32::try_from(length).map_err(|_| invalid("TCP frame length"))?),
    };
    let tag = auth.as_ref().map(|auth| {
        let mut context = hmac::Context::with_key(&auth.key);
        for part in parts {
            context.update(part);
        }
        context.sign()
    });
    if stream.is_write_vectored() {
        // At most length + five relay segments + MAC; every descriptor is
        // stack-owned and partial writes advance inside the original buffers.
        let mut slices: [io::IoSlice<'_>; 7] = std::array::from_fn(|_| io::IoSlice::new(&[]));
        slices[0] = io::IoSlice::new(length.as_bytes());
        for (index, part) in parts.iter().enumerate() {
            slices[index + 1] = io::IoSlice::new(part);
        }
        let mut count = 1 + parts.len();
        if let Some(tag) = &tag {
            slices[count] = io::IoSlice::new(tag.as_ref());
            count += 1;
        }
        let mut remaining = &mut slices[..count];
        while !remaining.is_empty() {
            let written = stream.write_vectored(remaining).await?;
            if written == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "TCP frame write stalled",
                ));
            }
            io::IoSlice::advance_slices(&mut remaining, written);
        }
    } else {
        stream.write_all(length.as_bytes()).await?;
        for part in parts {
            stream.write_all(part).await?;
        }
        if let Some(tag) = tag {
            stream.write_all(tag.as_ref()).await?;
        }
    }
    Ok(())
}

pub(super) async fn read<R: AsyncRead + Unpin>(stream: &mut R, auth: &Auth) -> io::Result<Message> {
    let mut length = FrameLength {
        length: U32::new(0),
    };
    stream.read_exact(length.as_mut_bytes()).await?;
    let length = usize::try_from(length.length.get()).map_err(|_| invalid("TCP frame length"))?;
    if !(11..=MAX_FRAME).contains(&length) {
        return Err(invalid("TCP frame length outside bound"));
    }
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes).await?;
    let header = FrameHeader::ref_from_bytes(&bytes[..core::mem::size_of::<FrameHeader>()])
        .map_err(|_| invalid("TCP frame header"))?;
    if header.version != VERSION || header.authenticated != u8::from(auth.is_some()) {
        return Err(invalid("TCP authentication mode/version mismatch"));
    }
    let sequence = header.sequence.get();
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
    let bytes = Bytes::from(bytes);
    let mut cursor = Cursor(&bytes[core::mem::size_of::<FrameHeader>()..end]);
    let message = decode(&mut cursor, &bytes, end)?;
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

    fn shared_blob(&mut self, max: usize, backing: &Bytes, end: usize) -> io::Result<Bytes> {
        let length = U16::ref_from_bytes(self.take(core::mem::size_of::<U16>())?)
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

fn decode(cursor: &mut Cursor<'_>, backing: &Bytes, end: usize) -> io::Result<Message> {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn typed_headers_preserve_network_layout_and_unaligned_views() {
        let length = FrameLength {
            length: U32::new(0x0102_0304),
        };
        assert_eq!(length.as_bytes(), &[1, 2, 3, 4]);
        let header = FrameHeader {
            version: VERSION,
            authenticated: 1,
            sequence: U64::new(0x0102_0304_0506_0708),
        };
        assert_eq!(header.as_bytes(), &[2, 1, 1, 2, 3, 4, 5, 6, 7, 8]);
        let mut unaligned = [0; 11];
        unaligned[1..].copy_from_slice(header.as_bytes());
        assert_eq!(
            FrameHeader::ref_from_bytes(&unaligned[1..])
                .unwrap()
                .sequence
                .get(),
            header.sequence.get()
        );
        let data = DataHeader {
            kind: 11,
            length: U16::new(0x0102),
        };
        assert_eq!(data.as_bytes(), &[11, 1, 2]);
        assert_eq!(core::mem::size_of::<RelayTarget>(), 34);
    }

    #[test]
    fn decoding_data_slices_the_original_owned_frame() {
        let frame = Bytes::from(vec![11, 0, 3, 4, 5, 6]);
        let pointer = frame[3..].as_ptr();
        let mut cursor = Cursor(&frame);
        let Message::Data(payload) = decode(&mut cursor, &frame, frame.len()).unwrap() else {
            panic!("data frame")
        };
        assert_eq!(payload.as_ptr(), pointer);
        assert_eq!(payload.as_ref(), &[4, 5, 6]);
        assert_eq!(cursor.0, b"");
        drop(frame);
        assert_eq!(payload.as_ref(), &[4, 5, 6]);
    }

    #[tokio::test]
    async fn split_data_and_relay_writes_match_the_existing_authenticated_layout() {
        for message in [
            Message::Data(Bytes::from_static(b"payload")),
            Message::Relay {
                node: NodeId::new("peer"),
                session: [4; 32],
                data: Bytes::from_static(b"payload"),
            },
        ] {
            let auth = Some(keyed(&[9; 32]));
            let mut expected = FrameHeader {
                version: VERSION,
                authenticated: 1,
                sequence: U64::new(1),
            }
            .as_bytes()
            .to_vec();
            encode(&message, &mut expected).unwrap();
            expected.extend_from_slice(hmac::sign(&auth.as_ref().unwrap().key, &expected).as_ref());
            let (mut writer, mut reader) = tokio::io::duplex(1);
            let mut actual = Vec::new();
            let mut scratch = Vec::new();
            let writing = async {
                write_buffered(&mut writer, &auth, &message, &mut scratch)
                    .await
                    .unwrap();
                drop(writer);
            };
            let reading = reader.read_to_end(&mut actual);
            let ((), read_result) = tokio::join!(writing, reading);
            read_result.unwrap();
            assert!(
                scratch.is_empty(),
                "hot data writes must not concatenate into scratch"
            );
            assert_eq!(
                actual[..4],
                FrameLength {
                    length: U32::new(u32::try_from(expected.len()).unwrap())
                }
                .as_bytes()[..]
            );
            assert_eq!(&actual[4..], expected.as_slice());
            let (mut writer, mut reader) = tokio::io::duplex(1);
            let receive_auth = Some(keyed(&[9; 32]));
            let reading = read(&mut reader, &receive_auth);
            let writing = async {
                writer.write_all(&actual).await.unwrap();
            };
            let (decoded, ()) = tokio::join!(reading, writing);
            let mut reencoded = Vec::new();
            encode(&decoded.unwrap(), &mut reencoded).unwrap();
            let mut original = Vec::new();
            encode(&message, &mut original).unwrap();
            assert_eq!(original, reencoded);
        }
    }

    #[tokio::test]
    async fn control_writer_reuses_its_scratch_storage() {
        let (mut writer, mut reader) = tokio::io::duplex(256);
        let mut scratch = Vec::new();
        write_buffered(&mut writer, &None, &Message::Ping, &mut scratch)
            .await
            .unwrap();
        let pointer = scratch.as_ptr();
        read(&mut reader, &None).await.unwrap();
        write_buffered(&mut writer, &None, &Message::Ping, &mut scratch)
            .await
            .unwrap();
        assert_eq!(scratch.as_ptr(), pointer);
        read(&mut reader, &None).await.unwrap();
    }

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
                data: vec![7; MAX_TCP_MESSAGE].into(),
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
            Message::Data(Bytes::new()),
        ];
        for message in messages {
            let mut bytes = Vec::new();
            encode(&message, &mut bytes).unwrap();
            let bytes = Bytes::from(bytes);
            let mut cursor = Cursor(&bytes);
            let decoded = decode(&mut cursor, &bytes, bytes.len()).unwrap();
            assert_eq!(cursor.0, []);
            let mut encoded = Vec::new();
            encode(&decoded, &mut encoded).unwrap();
            assert_eq!(bytes, encoded);
        }
        let unknown = Bytes::from_static(&[255]);
        let truncated = Bytes::from_static(&[0, 1]);
        assert!(decode(&mut Cursor(&unknown), &unknown, unknown.len()).is_err());
        assert!(decode(&mut Cursor(&truncated), &truncated, truncated.len()).is_err());
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
                data: Bytes::from_static(b"cannot reflect as peer traffic"),
            },
        )
        .await;
    }
}
