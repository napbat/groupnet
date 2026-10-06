//! Authenticated message-oriented sessions carried by routed datagrams.
//!
//! TLS is used only for pinned setup and lifetime binding. Reliable delivery
//! acknowledges bounded destination inbox acceptance, not processing or durability.
//! Unreliable delivery adds neither data acknowledgements nor retries; a routed
//! link may nevertheless be TCP. Neither policy promises ordered delivery.

mod endpoint;
mod session;
#[cfg(test)]
mod tests;
mod wire;

use std::{io, sync::Arc, time::Duration};

use groupnet_core::NodeId;
use groupnet_network::{Router, tunnel::TunnelTransport};

pub use session::UnorderedSession;

/// The explicitly negotiated session delivery policy.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnorderedDelivery {
    /// Retries independently until inbox acceptance or the finite send deadline.
    Reliable,
    /// Sends once without a data acknowledgement or delivery promise.
    Unreliable,
}

/// Endpoint bounds and policy admission. All buffers and retry lifetimes are finite.
/// Timers must fit the platform's monotonic clock; queue capacities must fit Tokio.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnorderedConfig {
    /// Whether to accept reliable sessions.
    pub allow_reliable: bool,
    /// Whether to accept unreliable sessions.
    pub allow_unreliable: bool,
    /// Maximum message size, negotiated down to the peer's bound.
    pub max_payload: usize,
    /// Maximum queued messages per session.
    pub inbox_capacity: usize,
    /// Maximum simultaneous reliable sends, at most the fixed 1024-ID dedup horizon.
    pub pending_sends: usize,
    /// Maximum sessions, including setup and queued accepts.
    pub max_sessions: usize,
    /// Maximum sessions per authenticated peer, including setup.
    pub sessions_per_peer: usize,
    /// Deadline for reliable send acceptance. Timeout leaves delivery unknown.
    pub send_timeout: Duration,
    /// Delay between independent reliable retransmissions.
    pub retry_interval: Duration,
    /// Maximum attempts per reliable message, including its initial send.
    pub max_attempts: usize,
    /// Interval between authenticated session heartbeats.
    pub heartbeat_interval: Duration,
    /// Expiry after no fresh authenticated packet from the peer.
    pub idle_timeout: Duration,
    /// Bound on authenticated setup, including admission queueing.
    pub setup_timeout: Duration,
}

impl Default for UnorderedConfig {
    fn default() -> Self {
        Self {
            allow_reliable: true,
            allow_unreliable: true,
            max_payload: 48 * 1024,
            inbox_capacity: 32,
            pending_sends: 32,
            max_sessions: 64,
            sessions_per_peer: 8,
            send_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(100),
            max_attempts: 100,
            heartbeat_interval: Duration::from_secs(1),
            idle_timeout: Duration::from_secs(5),
            setup_timeout: Duration::from_secs(10),
        }
    }
}

impl UnorderedConfig {
    /// Checks that every endpoint bound and timer is finite and admissible.
    ///
    /// # Errors
    /// Returns `InvalidInput` for unsupported sizes, capacities or timers.
    pub fn validate(&self) -> io::Result<()> {
        if (!self.allow_reliable && !self.allow_unreliable)
            || self.max_payload == 0
            || u32::try_from(self.max_payload).is_err()
            || self
                .max_payload
                .checked_add(wire::HEADER + wire::TAG)
                .is_none()
            || !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.inbox_capacity)
            || !(1..=wire::WINDOW).contains(&self.pending_sends)
            || !(1..=tokio::sync::Semaphore::MAX_PERMITS).contains(&self.max_sessions)
            || !(1..=self.max_sessions).contains(&self.sessions_per_peer)
            || self.max_attempts == 0
            || self.retry_interval.is_zero()
            || self.send_timeout < self.retry_interval
            || self.heartbeat_interval.is_zero()
            || self.idle_timeout <= self.heartbeat_interval
            || self.setup_timeout.is_zero()
            || [
                self.retry_interval,
                self.send_timeout,
                self.heartbeat_interval,
                self.idle_timeout,
                self.setup_timeout,
            ]
            .iter()
            .any(|timer| std::time::Instant::now().checked_add(*timer).is_none())
        {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "invalid unordered bounds",
            ));
        }
        Ok(())
    }

    fn allows(&self, delivery: UnorderedDelivery) -> bool {
        match delivery {
            UnorderedDelivery::Reliable => self.allow_reliable,
            UnorderedDelivery::Unreliable => self.allow_unreliable,
        }
    }
}

/// Explicit delivery policy and a finite setup deadline.
#[derive(Clone, Debug)]
pub struct UnorderedOptions {
    /// Requested policy; rejected explicitly if disallowed, never downgraded.
    pub delivery: UnorderedDelivery,
    /// Maximum time to establish the session.
    pub timeout: Duration,
}

impl Default for UnorderedOptions {
    fn default() -> Self {
        Self {
            delivery: UnorderedDelivery::Reliable,
            timeout: Duration::from_secs(10),
        }
    }
}

/// A shared, bounded authenticated unordered protocol engine.
#[derive(Clone, Debug)]
pub struct UnorderedProtocol {
    inner: Arc<endpoint::Inner>,
}

impl UnorderedProtocol {
    /// Binds routed protocol namespace 2 and the separate pinned TLS control plane.
    ///
    /// # Errors
    /// Rejects invalid bounds, a claimed protocol namespace, or a missing Tokio runtime.
    pub fn new(
        router: &Router,
        tunnels: TunnelTransport,
        config: UnorderedConfig,
    ) -> io::Result<Self> {
        config.validate()?;
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "Tokio executor required"))?;
        Ok(endpoint::new(router.bind_protocol(2)?, tunnels, config))
    }

    /// Validates a selected policy and setup deadline against node-wide settings.
    ///
    /// # Errors
    /// Returns `InvalidInput` for a disallowed policy or zero setup deadline.
    pub fn validate_options(&self, options: &UnorderedOptions) -> io::Result<()> {
        endpoint::validate_options(&self.inner, options)
    }

    /// Establishes a pinned, mutually authenticated session with an agreed policy.
    ///
    /// # Errors
    /// Returns admission, policy, capacity, cancellation or finite setup deadline errors.
    pub async fn connect(
        &self,
        to: &NodeId,
        options: UnorderedOptions,
    ) -> io::Result<UnorderedSession> {
        self.validate_options(&options)?;
        endpoint::connect(&self.inner, to, options).await
    }

    /// Receives either permitted policy and its authenticated routed peer identity.
    ///
    /// # Errors
    /// Returns an error when this endpoint or its network is closed.
    pub async fn accept(&self) -> io::Result<(NodeId, UnorderedSession)> {
        endpoint::accept(&self.inner).await
    }

    /// Receives only the selected delivery policy, sharing node-wide admission
    /// with accepts for the other policy but never consuming their sessions.
    ///
    /// # Errors
    /// Returns policy validation or endpoint/network shutdown errors.
    pub async fn accept_with_options(
        &self,
        options: UnorderedOptions,
    ) -> io::Result<(NodeId, UnorderedSession)> {
        self.validate_options(&options)?;
        endpoint::accept_policy(&self.inner, options.delivery).await
    }

    /// Cancels this endpoint without shutting down the shared router or tunnel transport.
    pub fn shutdown(&self) {
        self.inner.cancel.cancel();
    }

    /// Waits for endpoint shutdown, worker exit and queued-session cleanup.
    pub async fn closed(&self) {
        endpoint::closed(&self.inner).await;
    }
}

impl crate::SessionProtocol for UnorderedProtocol {
    type Session = UnorderedSession;
    type ConnectOptions = UnorderedOptions;

    async fn connect(
        &self,
        to: &NodeId,
        options: Self::ConnectOptions,
    ) -> io::Result<Self::Session> {
        Self::connect(self, to, options).await
    }

    async fn accept(&self) -> io::Result<(NodeId, Self::Session)> {
        Self::accept(self).await
    }

    async fn accept_with_options(
        &self,
        options: Self::ConnectOptions,
    ) -> io::Result<(NodeId, Self::Session)> {
        Self::accept_with_options(self, options).await
    }
}

fn aborted() -> io::Error {
    io::Error::new(io::ErrorKind::ConnectionAborted, "unordered session closed")
}
