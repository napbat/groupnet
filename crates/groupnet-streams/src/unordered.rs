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

use std::{fmt, io, num::NonZeroUsize, ops::Deref, sync::Arc, time::Duration};

use groupnet_core::NodeId;
use groupnet_network::{Router, tunnel::TunnelTransport};
use groupnet_transport::QueueCapacity;

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
/// Timers must fit the platform's monotonic clock; counts and queue capacities
/// are nonzero by construction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnorderedConfig {
    /// Whether to accept reliable sessions; at least one policy is allowed.
    pub allow_reliable: bool,
    /// Whether to accept unreliable sessions; at least one policy is allowed.
    pub allow_unreliable: bool,
    /// Maximum message size, negotiated down to the peer's bound; at most
    /// `u32::MAX` bytes.
    pub max_payload: NonZeroUsize,
    /// Maximum queued messages per session.
    pub inbox_capacity: QueueCapacity,
    /// Maximum simultaneous reliable sends, at most the fixed 1024-ID dedup horizon.
    pub pending_sends: QueueCapacity,
    /// Maximum sessions, including setup and queued accepts.
    pub max_sessions: QueueCapacity,
    /// Maximum sessions per authenticated peer, including setup; at most `max_sessions`.
    pub sessions_per_peer: QueueCapacity,
    /// Deadline for reliable send acceptance, at least `retry_interval`.
    /// Timeout leaves delivery unknown.
    pub send_timeout: Duration,
    /// Delay between independent reliable retransmissions; nonzero.
    pub retry_interval: Duration,
    /// Maximum attempts per reliable message, including its initial send.
    pub max_attempts: NonZeroUsize,
    /// Interval between authenticated session heartbeats; nonzero.
    pub heartbeat_interval: Duration,
    /// Expiry after no fresh authenticated packet from the peer; longer than
    /// `heartbeat_interval`.
    pub idle_timeout: Duration,
    /// Bound on authenticated setup, including admission queueing; nonzero.
    pub setup_timeout: Duration,
}

impl Default for UnorderedConfig {
    fn default() -> Self {
        Self {
            allow_reliable: true,
            allow_unreliable: true,
            max_payload: const { NonZeroUsize::new(48 * 1024).unwrap() },
            inbox_capacity: QueueCapacity::of(32),
            pending_sends: QueueCapacity::of(32),
            max_sessions: QueueCapacity::of(64),
            sessions_per_peer: QueueCapacity::of(8),
            send_timeout: Duration::from_secs(10),
            retry_interval: Duration::from_millis(100),
            max_attempts: const { NonZeroUsize::new(100).unwrap() },
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
    /// Returns the first violated [`UnorderedConfigError`] rule.
    pub fn validate(&self) -> Result<(), UnorderedConfigError> {
        if !self.allow_reliable && !self.allow_unreliable {
            return Err(UnorderedConfigError::NoPolicyAllowed);
        }
        if u32::try_from(self.max_payload.get()).is_err()
            || self
                .max_payload
                .get()
                .checked_add(wire::HEADER + wire::TAG)
                .is_none()
        {
            return Err(UnorderedConfigError::MaxPayloadAboveWire);
        }
        if self.pending_sends.get() > wire::WINDOW {
            return Err(UnorderedConfigError::PendingSendsAboveWindow);
        }
        if self.sessions_per_peer > self.max_sessions {
            return Err(UnorderedConfigError::SessionsPerPeerAboveMaxSessions);
        }
        if self.retry_interval.is_zero() {
            return Err(UnorderedConfigError::ZeroRetryInterval);
        }
        if self.send_timeout < self.retry_interval {
            return Err(UnorderedConfigError::SendTimeoutBelowRetryInterval);
        }
        if self.heartbeat_interval.is_zero() {
            return Err(UnorderedConfigError::ZeroHeartbeatInterval);
        }
        if self.idle_timeout <= self.heartbeat_interval {
            return Err(UnorderedConfigError::IdleTimeoutNotAboveHeartbeat);
        }
        if self.setup_timeout.is_zero() {
            return Err(UnorderedConfigError::ZeroSetupTimeout);
        }
        let now = std::time::Instant::now();
        for (field, timer) in [
            ("retry_interval", self.retry_interval),
            ("send_timeout", self.send_timeout),
            ("heartbeat_interval", self.heartbeat_interval),
            ("idle_timeout", self.idle_timeout),
            ("setup_timeout", self.setup_timeout),
        ] {
            if now.checked_add(timer).is_none() {
                return Err(UnorderedConfigError::UnrepresentableTimer { field });
            }
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

/// An [`UnorderedConfig`] that passed [`UnorderedConfig::validate`].
///
/// Constructible only through `TryFrom<UnorderedConfig>`, it lets a caller that
/// must reject invalid policy before other side effects (a node builder)
/// validate once and hand the proof to [`UnorderedProtocol::with_validated`],
/// which does not check again. Dereferences to the validated configuration.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ValidUnorderedConfig(UnorderedConfig);

impl TryFrom<UnorderedConfig> for ValidUnorderedConfig {
    type Error = UnorderedConfigError;

    /// # Errors
    /// Returns the first violated [`UnorderedConfigError`] rule.
    fn try_from(config: UnorderedConfig) -> Result<Self, Self::Error> {
        config.validate()?;
        Ok(Self(config))
    }
}

impl Deref for ValidUnorderedConfig {
    type Target = UnorderedConfig;

    fn deref(&self) -> &UnorderedConfig {
        &self.0
    }
}

/// A violated [`UnorderedConfig`] rule, naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnorderedConfigError {
    /// Both `allow_reliable` and `allow_unreliable` are false.
    NoPolicyAllowed,
    /// `max_payload` exceeds the `u32` wire bound.
    MaxPayloadAboveWire,
    /// `pending_sends` exceeds the fixed 1024-ID duplicate window.
    PendingSendsAboveWindow,
    /// `sessions_per_peer` exceeds `max_sessions`.
    SessionsPerPeerAboveMaxSessions,
    /// `retry_interval` is zero.
    ZeroRetryInterval,
    /// `send_timeout` is shorter than `retry_interval`.
    SendTimeoutBelowRetryInterval,
    /// `heartbeat_interval` is zero.
    ZeroHeartbeatInterval,
    /// `idle_timeout` does not exceed `heartbeat_interval`.
    IdleTimeoutNotAboveHeartbeat,
    /// `setup_timeout` is zero.
    ZeroSetupTimeout,
    /// A timer cannot extend the monotonic clock.
    UnrepresentableTimer {
        /// The offending field.
        field: &'static str,
    },
}

impl fmt::Display for UnorderedConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let message = match self {
            Self::NoPolicyAllowed => {
                "unordered allow_reliable and allow_unreliable cannot both be false"
            }
            Self::MaxPayloadAboveWire => "unordered max_payload exceeds the u32 wire bound",
            Self::PendingSendsAboveWindow => {
                "unordered pending_sends exceeds the 1024-ID duplicate window"
            }
            Self::SessionsPerPeerAboveMaxSessions => {
                "unordered sessions_per_peer must not exceed max_sessions"
            }
            Self::ZeroRetryInterval => "unordered retry_interval must be nonzero",
            Self::SendTimeoutBelowRetryInterval => {
                "unordered send_timeout must be at least retry_interval"
            }
            Self::ZeroHeartbeatInterval => "unordered heartbeat_interval must be nonzero",
            Self::IdleTimeoutNotAboveHeartbeat => {
                "unordered idle_timeout must exceed heartbeat_interval"
            }
            Self::ZeroSetupTimeout => "unordered setup_timeout must be nonzero",
            Self::UnrepresentableTimer { field } => {
                return write!(f, "unordered {field} exceeds the monotonic clock");
            }
        };
        f.write_str(message)
    }
}

impl std::error::Error for UnorderedConfigError {}

impl From<UnorderedConfigError> for io::Error {
    fn from(error: UnorderedConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// Explicit delivery policy and a finite setup deadline.
#[derive(Clone, Debug)]
pub struct UnorderedOptions {
    /// Requested policy; rejected explicitly if disallowed, never downgraded.
    pub delivery: UnorderedDelivery,
    /// Maximum time to establish the session; nonzero and at most the node
    /// policy's [`UnorderedConfig::setup_timeout`].
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

impl UnorderedOptions {
    /// Checks these options against a node-wide policy: the delivery must be
    /// allowed and the setup deadline nonzero and within `setup_timeout`.
    /// Nothing is downgraded or shortened.
    ///
    /// # Errors
    /// Returns the first violated [`UnorderedOptionsError`] rule.
    pub fn validate(&self, policy: &UnorderedConfig) -> Result<(), UnorderedOptionsError> {
        if !policy.allows(self.delivery) {
            return Err(UnorderedOptionsError::DeliveryNotAllowed(self.delivery));
        }
        if self.timeout.is_zero() {
            return Err(UnorderedOptionsError::ZeroTimeout);
        }
        if self.timeout > policy.setup_timeout {
            return Err(UnorderedOptionsError::TimeoutAboveSetupTimeout {
                timeout: self.timeout,
                setup_timeout: policy.setup_timeout,
            });
        }
        Ok(())
    }
}

/// [`UnorderedOptions`] a node-wide [`UnorderedConfig`] does not admit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnorderedOptionsError {
    /// The policy disallows this `delivery`.
    DeliveryNotAllowed(UnorderedDelivery),
    /// `timeout` is zero.
    ZeroTimeout,
    /// `timeout` exceeds the policy's `setup_timeout`.
    TimeoutAboveSetupTimeout {
        /// The rejected setup deadline.
        timeout: Duration,
        /// The policy's setup bound.
        setup_timeout: Duration,
    },
}

impl fmt::Display for UnorderedOptionsError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::DeliveryNotAllowed(delivery) => {
                write!(f, "unordered {delivery:?} delivery is disallowed locally")
            }
            Self::ZeroTimeout => f.write_str("unordered setup timeout must be nonzero"),
            Self::TimeoutAboveSetupTimeout {
                timeout,
                setup_timeout,
            } => write!(
                f,
                "unordered setup timeout {timeout:?} exceeds setup_timeout {setup_timeout:?}"
            ),
        }
    }
}

impl std::error::Error for UnorderedOptionsError {}

impl From<UnorderedOptionsError> for io::Error {
    fn from(error: UnorderedOptionsError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

/// A shared, bounded authenticated unordered protocol engine.
#[derive(Clone, Debug)]
pub struct UnorderedProtocol {
    inner: Arc<endpoint::Inner>,
}

impl UnorderedProtocol {
    /// Binds routed protocol namespace 2 and the separate pinned TLS control
    /// plane with the default [`UnorderedConfig`].
    ///
    /// # Errors
    /// Rejects a claimed protocol namespace or a missing Tokio runtime.
    pub fn new(router: &Router, tunnels: TunnelTransport) -> io::Result<Self> {
        Self::with_config(router, tunnels, UnorderedConfig::default())
    }

    /// Binds routed protocol namespace 2 and the separate pinned TLS control
    /// plane with explicit bounds and policy admission. Nothing binds when
    /// validation fails.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping an [`UnorderedConfigError`]; rejects a
    /// claimed protocol namespace or a missing Tokio runtime.
    pub fn with_config(
        router: &Router,
        tunnels: TunnelTransport,
        config: UnorderedConfig,
    ) -> io::Result<Self> {
        Self::with_validated(router, tunnels, ValidUnorderedConfig::try_from(config)?)
    }

    /// Binds with bounds and policy already proven valid, without checking them
    /// again.
    ///
    /// # Errors
    /// Rejects a claimed protocol namespace or a missing Tokio runtime.
    pub fn with_validated(
        router: &Router,
        tunnels: TunnelTransport,
        config: ValidUnorderedConfig,
    ) -> io::Result<Self> {
        tokio::runtime::Handle::try_current()
            .map_err(|_| io::Error::new(io::ErrorKind::NotConnected, "Tokio executor required"))?;
        Ok(endpoint::new(router.bind_protocol(2)?, tunnels, config.0))
    }

    /// Validates a selected policy and setup deadline against this engine's
    /// node-wide [`UnorderedConfig`] ([`UnorderedOptions::validate`]).
    ///
    /// # Errors
    /// Returns the violated [`UnorderedOptionsError`] rule.
    pub fn validate_options(
        &self,
        options: &UnorderedOptions,
    ) -> Result<(), UnorderedOptionsError> {
        options.validate(endpoint::config(&self.inner))
    }

    /// Establishes a pinned, mutually authenticated session with an agreed policy.
    ///
    /// # Errors
    /// Returns `InvalidInput` wrapping an [`UnorderedOptionsError`] before any
    /// setup, then admission, policy, capacity, cancellation or setup deadline errors.
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
    /// Returns `InvalidInput` wrapping an [`UnorderedOptionsError`], or
    /// endpoint/network shutdown errors.
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
