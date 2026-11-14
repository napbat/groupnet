//! The TLS mode of [`TcpBulkTransport`] (feature `tls`): a hub that admits
//! clients it was never configured with, across a trust boundary.
//!
//! Every connection is TLS 1.3 with mutual authentication by RFC 7250 raw
//! public keys (see `session`), then an admission exchange inside TLS (`GNBT`,
//! see `handshake`):
//!
//! 1. The client dials with a key it generated for this one connection and
//!    accepts only the hub key it pinned for the hub's [`NodeId`].
//! 2. The hub accepts any client key at the TLS layer; the TLS handshake
//!    proves the client holds it.
//! 3. Inside TLS the client claims its id and presents its credential; the
//!    hub's [`Admission`](groupnet_transport::admission::Admission) policy
//!    judges them with the client's socket address, the accepted id must be
//!    the claimed one, and the hub answers with its own id, which the client
//!    checks against the hub it dialed.
//!
//! The admitted stream is that TLS connection, so the claim stays bound to the
//! client's key for the connection's life: no other party can inject into or
//! read it. [`TcpTlsStream::peer_key`] reports the key on both sides.
//!
//! A TLS endpoint never falls back to cleartext: it refuses a plain `GNBH`
//! hello, and a plain endpoint refuses its TLS handshake.

mod config;
mod handshake;
mod key;
mod session;

use std::io;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use groupnet_core::NodeId;
use groupnet_transport::bulk::BulkTransport;
use rustls::crypto::CryptoProvider;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::{TcpListener, TcpStream, ToSocketAddrs};
use tokio::sync::OwnedSemaphorePermit;
use tokio_rustls::TlsStream;
use tokio_util::compat::{Compat, TokioAsyncReadCompatExt};

pub use config::{TcpTlsConfig, TcpTlsConfigError, TcpTlsDialConfig};
pub use key::{TcpTlsKey, TcpTlsKeyError, TcpTlsPublicKey, TcpTlsPublicKeyError};

use super::accept::{Bounds, Listener};
use super::endpoint::{PeerEndpoint, Peers};
use super::{Identity, TcpBulkTransport, TcpHostEndpointError};
use crate::handshake::check_id;

/// The TLS mode of a [`TcpBulkTransport`] (feature `tls`): mutually
/// authenticated TLS 1.3 with per-connection client keys pinned at admission.
/// Built by [`bind_tls`](TcpBulkTransport::bind_tls) (a hub) and
/// [`dial_only_tls`](TcpBulkTransport::dial_only_tls) (a client).
pub struct TcpTls {
    provider: Arc<CryptoProvider>,
    credential: Vec<u8>,
    listener: Option<Listener<TcpTlsStream>>,
    peers: Peers<Hub>,
}

impl std::fmt::Debug for TcpTls {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // The credential is never printed.
        f.debug_struct("TcpTls")
            .field("listener", &self.listener)
            .field("peers", &self.peers)
            .finish_non_exhaustive()
    }
}

/// Where a pinned peer listens, and the key it must present.
#[derive(Clone, Debug)]
struct Hub {
    endpoint: PeerEndpoint,
    key: TcpTlsPublicKey,
}

/// One established TLS data-plane connection, past its admission exchange.
///
/// The [`BulkTransport`] stream of a TLS [`TcpBulkTransport`] is this
/// connection behind `tokio_util::compat`; [`Compat::get_ref`] reaches it. An
/// accepted connection holds one of the hub's
/// [`TcpTlsConfig::max_connections`] until it is dropped.
#[derive(Debug)]
pub struct TcpTlsStream {
    // Declared first so the socket closes before the slot is released.
    stream: TlsStream<TcpStream>,
    peer_key: TcpTlsPublicKey,
    _slot: Option<OwnedSemaphorePermit>,
}

impl TcpTlsStream {
    /// The key the other side authenticated with: on a dialed stream the
    /// hub's pinned key, on an accepted stream the key the client generated
    /// for this connection.
    #[must_use]
    pub fn peer_key(&self) -> &TcpTlsPublicKey {
        &self.peer_key
    }

    /// The remote socket address of the connection.
    ///
    /// # Errors
    /// Any error the socket reports.
    pub fn peer_addr(&self) -> io::Result<SocketAddr> {
        self.stream.get_ref().0.peer_addr()
    }
}

impl AsyncRead for TcpTlsStream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_read(cx, buf)
    }
}

impl AsyncWrite for TcpTlsStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().stream).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().stream).poll_shutdown(cx)
    }
}

impl TcpBulkTransport<TcpTls> {
    /// Binds a TLS hub for `local` that admits clients through
    /// [`TcpTlsConfig::admission`] and authenticates with
    /// [`TcpTlsConfig::key`]. It dials too: register the peers it calls with
    /// [`register_peer`](Self::register_peer). Must be called within a Tokio
    /// runtime: the listener and its handshakes run as spawned tasks.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`TcpTlsConfigError`] or
    /// [`NodeIdError`](groupnet_transport::NodeIdError) before any socket
    /// exists, and propagates any socket bind error.
    pub async fn bind_tls(
        local: NodeId,
        addr: impl ToSocketAddrs,
        config: TcpTlsConfig,
    ) -> io::Result<Self> {
        config.validate()?;
        check_id(&local)?;
        let provider = session::provider();
        let acceptor = session::acceptor(&provider, &config.key)?;
        let listener = TcpListener::bind(addr).await?;
        let addr = listener.local_addr()?;
        let identity = Identity::new(local, config.handshake_timeout);
        let bounds = Bounds {
            max_handshakes: config.max_handshakes,
            accept_queue: config.accept_queue,
            max_connections: Some(config.max_connections),
        };
        let admission = config.admission;
        let accepting = identity.clone();
        let listener = Listener::spawn(
            listener,
            bounds,
            identity.clone(),
            move |sock, remote, slot| {
                let identity = accepting.clone();
                let acceptor = acceptor.clone();
                let admission = admission.clone();
                async move {
                    let (mut stream, peer_key) = session::accept(sock, &acceptor).await?;
                    let from = handshake::accept(
                        &mut stream,
                        &identity.local,
                        admission.as_ref(),
                        Some(remote),
                    )
                    .await?;
                    let stream = TcpTlsStream {
                        stream: TlsStream::Server(stream),
                        peer_key,
                        _slot: slot,
                    };
                    Ok((from, stream))
                }
            },
        );
        Ok(Self {
            identity,
            addr: Some(addr),
            mode: TcpTls {
                provider,
                credential: config.credential,
                listener: Some(listener),
                peers: Peers::new(),
            },
        })
    }

    /// A TLS client for `local` that never listens: it dials registered hubs
    /// presenting [`TcpTlsDialConfig::credential`], with a fresh key for every
    /// connection, and its [`accept`](BulkTransport::accept) never completes.
    /// It opens nothing until it dials and needs no Tokio runtime to be made.
    ///
    /// # Errors
    /// Returns `InvalidInput` carrying the [`TcpTlsConfigError`] or the
    /// [`NodeIdError`](groupnet_transport::NodeIdError), before any endpoint
    /// exists.
    pub fn dial_only_tls(local: NodeId, config: TcpTlsDialConfig) -> io::Result<Self> {
        config.validate()?;
        check_id(&local)?;
        Ok(Self {
            identity: Identity::new(local, config.handshake_timeout),
            addr: None,
            mode: TcpTls {
                provider: session::provider(),
                credential: config.credential,
                listener: None,
                peers: Peers::new(),
            },
        })
    }

    /// Teaches this endpoint that `node` listens at `addr` and authenticates
    /// with `key`, replacing any earlier entry. A connection to `node` refuses
    /// a peer that presents any other key. Connections already open are not
    /// affected.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr, key: TcpTlsPublicKey) {
        let endpoint = PeerEndpoint::Address(addr);
        self.mode.peers.insert(node, Hub { endpoint, key });
    }

    /// Registers a bounded host:port endpoint for `node`, resolved anew on
    /// every connect, and the key it must present. The pin, not the name, is
    /// the peer's identity.
    ///
    /// # Errors
    /// Returns the [`TcpHostEndpointError`] for an overlong endpoint, one with
    /// whitespace or a `/`, `\`, `@` or `#` byte, or one without a nonempty
    /// host and a port from 1 to 65535, and records nothing.
    ///
    /// # Panics
    /// If the address book was poisoned by a panic in another thread.
    pub fn register_peer_host(
        &self,
        node: NodeId,
        host: String,
        key: TcpTlsPublicKey,
    ) -> Result<(), TcpHostEndpointError> {
        let endpoint = PeerEndpoint::host(host)?;
        self.mode.peers.insert(node, Hub { endpoint, key });
        Ok(())
    }
}

impl BulkTransport for TcpBulkTransport<TcpTls> {
    type Error = io::Error;
    type Stream = Compat<TcpTlsStream>;

    async fn connect(&self, to: &NodeId) -> io::Result<Self::Stream> {
        let hub = self.mode.peers.get(to)?;
        let sock = hub.endpoint.open().await?;
        let remote = sock.peer_addr()?.ip();
        // A fresh key for every connection: no two sessions of this client
        // can be linked by their key.
        let key = TcpTlsKey::generate()?;
        let identity = &self.identity;
        let stream = identity
            .dialed(async {
                let mut stream =
                    session::connect(sock, &self.mode.provider, &key, &hub.key, remote).await?;
                handshake::dial(&mut stream, &identity.local, &self.mode.credential, to).await?;
                Ok(stream)
            })
            .await?;
        Ok(TcpTlsStream {
            stream: TlsStream::Client(stream),
            peer_key: hub.key,
            _slot: None,
        }
        .compat())
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Stream)> {
        let Some(listener) = &self.mode.listener else {
            // A dial-only endpoint has nothing to accept.
            return std::future::pending().await;
        };
        let (from, stream) = listener.accept().await?;
        Ok((from, stream.compat()))
    }
}

#[cfg(test)]
mod tests;
