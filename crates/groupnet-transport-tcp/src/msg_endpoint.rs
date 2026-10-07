//! Message endpoint diagnostics, lifecycle, and backend dispatch.

use super::{Backend, Inner, Outbound, TcpMsgTransport, write_loop};
use bytes::Bytes;
use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};
use std::{io, net::SocketAddr, sync::Arc};
use tokio::sync::mpsc;

impl TcpMsgTransport {
    #[cfg_attr(
        not(feature = "connectivity"),
        expect(
            clippy::unnecessary_wraps,
            reason = "backend access keeps one return type across optional connectivity builds"
        )
    )]
    pub(super) fn direct(&self) -> Option<&Arc<Inner>> {
        match &self.backend {
            Backend::Direct(inner) => Some(inner),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { .. } => None,
        }
    }

    /// This endpoint's local node id.
    #[must_use]
    pub fn local_id(&self) -> &NodeId {
        match &self.backend {
            Backend::Direct(inner) => &inner.local,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.local_id(),
        }
    }

    /// The listener's primary bind address, retained even after shutdown.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        match &self.backend {
            Backend::Direct(inner) => inner.local_addr,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { address, .. } => *address,
        }
    }

    /// Initiates cancellation of all owned listener and session tasks.
    /// All clones share this shutdown state; no new tasks can start afterward.
    ///
    /// # Panics
    /// If a task registry or connection pool lock was poisoned.
    pub fn shutdown(&self) {
        match &self.backend {
            Backend::Direct(inner) => {
                inner.tasks.shutdown();
                #[cfg(feature = "link")]
                if let Some(managed) = &inner.admission {
                    managed.shutdown();
                }
                let mut pool = inner.pool.lock().expect("pool lock poisoned");
                pool.conns.clear();
                pool.order.clear();
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.shutdown(),
        }
    }

    /// Cancels and drains every owned task, releasing listener/session sockets.
    /// Safe to call repeatedly or concurrently through different clones.
    ///
    /// # Panics
    /// If a task registry or connection pool lock was poisoned.
    pub async fn close(&self) {
        self.shutdown();
        match &self.backend {
            Backend::Direct(inner) => inner.tasks.close().await,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.close().await,
        }
    }

    /// Resolves once this endpoint has begun shutting down through any clone:
    /// [`shutdown`](Self::shutdown), [`close`](Self::close), or link shutdown.
    /// Sticky: awaiting it after shutdown resolves immediately. Native
    /// connectivity endpoints resolve from their session registry's closed
    /// state. Use [`close`](Self::close) to also wait for sockets to drain.
    pub async fn closed(&self) {
        match &self.backend {
            Backend::Direct(inner) => inner.tasks.stopping().await,
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.sessions().closed().await,
        }
    }

    #[cfg(feature = "link")]
    pub(crate) fn lifecycle(&self) -> Arc<dyn groupnet_transport::link::LinkLifecycle> {
        Arc::new(self.clone())
    }

    /// Teaches a direct endpoint that `node` listens at `addr`.
    /// An existing connection is left alone until it next closes.
    /// This has no effect in connectivity mode: rendezvous admission owns paths.
    ///
    /// # Panics
    /// If the direct address book lock was poisoned.
    pub fn register_peer(&self, node: NodeId, addr: SocketAddr) {
        if let Some(inner) = self.direct() {
            inner
                .peers
                .write()
                .expect("peers lock poisoned")
                .insert(node, addr);
        }
    }

    /// Returns a direct endpoint's dial address, or a validated native direct path.
    /// A relayed or unknown connectivity peer has no direct address.
    ///
    /// # Panics
    /// If the direct address book lock was poisoned.
    #[must_use]
    pub fn peer_addr(&self, node: &NodeId) -> Option<SocketAddr> {
        match &self.backend {
            Backend::Direct(inner) => inner
                .peers
                .read()
                .expect("peers lock poisoned")
                .get(node)
                .copied(),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.direct_addr_to(node),
        }
    }

    /// Outbound entries in the direct connection pool, including pending dials.
    /// Admitted endpoints count initiated full-duplex sessions, not accepted ones.
    /// Connectivity has no such pool and returns zero; use `known_peers` instead.
    ///
    /// # Panics
    /// If a direct connection pool lock was poisoned.
    #[must_use]
    pub fn outbound_connections(&self) -> usize {
        let Some(inner) = self.direct() else { return 0 };
        #[cfg(feature = "link")]
        if let Some(managed) = &inner.admission {
            return managed.outbound_connections();
        }
        inner.pool.lock().expect("pool lock poisoned").conns.len()
    }

    #[cfg(feature = "link")]
    /// Returns the shared live-session registry for admitted or native endpoints.
    /// Raw endpoints return `None`.
    #[must_use]
    pub fn sessions(&self) -> Option<groupnet_transport::admission::SessionRegistry> {
        match &self.backend {
            Backend::Direct(inner) => inner
                .admission
                .as_ref()
                .map(|managed| managed.sessions.clone()),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => Some(connection.sessions()),
        }
    }
}

impl Transport for TcpMsgTransport {
    type Error = io::Error;

    fn learn_peer(&self, node: &NodeId, addr: &str) {
        // Native admission owns its own candidates: hints must not bypass it.
        if self.direct().is_some()
            && let Ok(addr) = addr.parse::<SocketAddr>()
        {
            self.register_peer(node.clone(), addr);
        }
    }

    #[cfg_attr(
        not(feature = "connectivity"),
        expect(
            clippy::unused_async_trait_impl,
            reason = "the connectivity backend awaits I/O; direct sends only enqueue"
        )
    )]
    async fn send(&self, to: &NodeId, msg: &[u8]) -> io::Result<()> {
        match &self.backend {
            Backend::Direct(inner) => {
                if inner.tasks.stopped() {
                    return Err(shut_down());
                }
                if msg.len() > inner.config.max_frame_bytes {
                    return Ok(());
                }
                #[cfg(feature = "link")]
                if let Some(managed) = &inner.admission {
                    return managed.send(inner, to, Some(Bytes::copy_from_slice(msg)));
                }
                inner.send_raw(to, Bytes::copy_from_slice(msg))
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.send(to, msg).await,
        }
    }

    #[cfg(feature = "link")]
    async fn send_admitted(
        &self,
        to: &NodeId,
        msg: &[u8],
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        if let Some(inner) = self.direct() {
            if inner.tasks.stopped() {
                return Err(shut_down());
            }
            if msg.len() > inner.config.max_frame_bytes {
                return Ok(());
            }
        }
        self.send_owned_admitted(to, Bytes::copy_from_slice(msg), session)
            .await
    }

    #[cfg_attr(
        not(feature = "connectivity"),
        expect(
            clippy::unused_async_trait_impl,
            reason = "the connectivity backend awaits I/O; direct sends only enqueue"
        )
    )]
    #[cfg(feature = "link")]
    async fn send_owned_admitted(
        &self,
        to: &NodeId,
        msg: Bytes,
        session: Option<groupnet_transport::admission::SessionId>,
    ) -> io::Result<()> {
        match &self.backend {
            Backend::Direct(inner) => {
                if let Some(managed) = &inner.admission {
                    return managed.send_admitted(inner, to, msg, session);
                }
                inner.send_raw(to, msg)
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => {
                connection.send_owned_admitted(to, msg, session).await
            }
        }
    }

    async fn recv(&self) -> io::Result<Inbound> {
        match &self.backend {
            Backend::Direct(inner) => Ok(inner.recv_raw().await?.packet),
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.recv().await,
        }
    }

    #[cfg(feature = "link")]
    async fn recv_admitted(&self) -> io::Result<groupnet_transport::link::AdmittedInbound> {
        match &self.backend {
            Backend::Direct(inner) => {
                let queued = inner.recv_raw().await?;
                Ok(groupnet_transport::link::AdmittedInbound {
                    packet: queued.packet,
                    session: queued.session,
                })
            }
            #[cfg(feature = "connectivity")]
            Backend::Connectivity { connection, .. } => connection.recv_admitted().await,
        }
    }
}

impl Inner {
    async fn recv_raw(&self) -> io::Result<super::QueuedInbound> {
        if self.tasks.stopped() {
            return Err(shut_down());
        }
        // Single consumer holds the mutex across its receive await.
        self.inbox.lock().await.recv().await.ok_or_else(shut_down)
    }

    fn send_raw(self: &Arc<Self>, to: &NodeId, mut msg: Bytes) -> io::Result<()> {
        if self.tasks.stopped() {
            return Err(shut_down());
        }
        if msg.len() > self.config.max_frame_bytes {
            return Ok(());
        }
        {
            let mut pool = self.pool.lock().expect("pool lock poisoned");
            if let Some(conn) = pool.conns.get(to) {
                match conn.frames.try_send(msg) {
                    Ok(()) | Err(mpsc::error::TrySendError::Full(_)) => return Ok(()),
                    Err(mpsc::error::TrySendError::Closed(frame)) => {
                        msg = frame;
                        pool.remove(to);
                    }
                }
            }
        }
        let addr = self
            .peers
            .read()
            .expect("peers lock poisoned")
            .get(to)
            .copied();
        if let Some(addr) = addr {
            self.dial_raw(to, addr, msg);
        }
        Ok(())
    }

    fn dial_raw(self: &Arc<Self>, to: &NodeId, addr: SocketAddr, msg: Bytes) {
        let (tx, rx) = mpsc::channel(self.config.outbound_queue.get());
        tx.try_send(msg).expect("fresh queue has capacity");
        let generation;
        {
            let mut pool = self.pool.lock().expect("pool lock poisoned");
            // Closing the oldest writer keeps the pool bounded, not reliable.
            while pool.conns.len() >= self.config.max_outbound.get() {
                let Some((g, node)) = pool.order.pop_front() else {
                    break;
                };
                if pool.conns.get(&node).is_some_and(|c| c.generation == g) {
                    pool.conns.remove(&node);
                }
            }
            generation = pool.next_generation;
            pool.next_generation += 1;
            pool.conns.insert(
                to.clone(),
                super::Conn {
                    generation,
                    frames: tx,
                },
            );
            pool.order.push_back((generation, to.clone()));
        }
        // A concurrent insert may replace this entry; generation cleanup must
        // never remove the replacement and the unreferenced writer drains out.
        if !self.tasks.spawn(write_loop(Outbound {
            inner: Arc::downgrade(self),
            peer: to.clone(),
            generation,
            addr,
            frames: rx,
            idle: self.config.idle_timeout,
            local: self.local.clone(),
            intro: self.intro.clone(),
        })) {
            self.pool
                .lock()
                .expect("pool lock poisoned")
                .remove_generation(to, generation);
        }
    }
}

fn shut_down() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "tcp msg transport shut down")
}
