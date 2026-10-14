//! TCP rendezvous configuration, operational limits and the independent control budget.

use crate::common::{PeerListFault, check_peers};
use crate::{NetworkKey, RelayPacing};
use groupnet_core::NodeId;
use groupnet_transport::{
    NodeIdError, QueueCapacity,
    admission::{Admission, OpenAdmission},
};
use std::{error, fmt, io, net::SocketAddr, num::NonZeroU32, sync::Arc};

/// Rate and finite burst allowance for one admitted control connection.
#[derive(Clone, Copy, Debug)]
pub struct ControlRateLimit {
    /// Sustained control messages per second; relay data does not consume this budget.
    pub frames_per_second: NonZeroU32,
    /// Immediately available control messages after idle.
    pub burst_frames: NonZeroU32,
}

impl Default for ControlRateLimit {
    fn default() -> Self {
        Self {
            frames_per_second: NonZeroU32::new(32).expect("nonzero default"),
            burst_frames: NonZeroU32::new(32).expect("nonzero default"),
        }
    }
}

/// Operational limits for a TCP rendezvous.
/// Identity, candidate, credential and frame-length wire bounds are independent.
#[derive(Clone, Copy, Debug)]
pub struct TcpRendezvousLimits {
    /// Maximum concurrently admitted sessions.
    pub max_sessions: QueueCapacity,
    /// Maximum concurrent unauthenticated admission attempts.
    pub max_pending: QueueCapacity,
    /// Bounded shared admission/lifecycle event capacity.
    pub event_queue: QueueCapacity,
    /// Bounded per-session relay-data output capacity.
    pub session_queue: QueueCapacity,
    /// Independent bounded control output capacity; must hold one admission
    /// introduction per admitted session (at least `max_sessions`).
    pub control_queue: QueueCapacity,
    /// Independent abuse bound for control and rejected relay requests.
    pub control: ControlRateLimit,
}

impl Default for TcpRendezvousLimits {
    fn default() -> Self {
        Self {
            max_sessions: QueueCapacity::of(128),
            max_pending: QueueCapacity::of(32),
            event_queue: QueueCapacity::of(128),
            session_queue: QueueCapacity::of(128),
            control: ControlRateLimit::default(),
            control_queue: QueueCapacity::of(128),
        }
    }
}

impl TcpRendezvousLimits {
    /// Checks the cross-field limit rules.
    ///
    /// # Errors
    /// Returns [`TcpRendezvousConfigError::ControlQueueBelowMaxSessions`] when
    /// `control_queue` cannot hold one introduction per admitted session.
    pub fn validate(&self) -> Result<(), TcpRendezvousConfigError> {
        if self.control_queue < self.max_sessions {
            return Err(TcpRendezvousConfigError::ControlQueueBelowMaxSessions);
        }
        Ok(())
    }
}

/// Listener, fabric authentication, application admission, and TCP rendezvous limits.
#[derive(Clone)]
pub struct TcpRendezvousConfig {
    /// TCP listener bind address (multicast is rejected).
    pub bind: SocketAddr,
    /// Optional trusted-fabric key; keyed traffic never downgrades.
    pub key: Option<NetworkKey>,
    /// Fixed identity allowlist, or `None` for dynamic application admission.
    pub peers: Option<Vec<NodeId>>,
    /// Application admission policy for every registration.
    pub admission: Arc<dyn Admission>,
    /// Operational capacities and the independent control budget.
    pub limits: TcpRendezvousLimits,
    /// Optional byte-based policy for valid, admitted relay data. A paced TCP
    /// source is delayed, never dropped.
    pub relay_pacing: RelayPacing,
}

impl fmt::Debug for TcpRendezvousConfig {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("TcpRendezvousConfig")
            .field("bind", &self.bind)
            .field("key", &self.key)
            .field("peers", &self.peers)
            .field("admission", &"application policy")
            .field("limits", &self.limits)
            .field("relay_pacing", &self.relay_pacing)
            .finish()
    }
}

impl TcpRendezvousConfig {
    /// Creates keyed rendezvous configuration with a fixed identity allowlist
    /// and open application admission.
    #[must_use]
    pub fn new(bind: SocketAddr, key: NetworkKey, peers: Vec<NodeId>) -> Self {
        Self {
            peers: Some(peers),
            ..Self::with_admission(bind, Some(key), Arc::new(OpenAdmission))
        }
    }

    /// Creates explicitly keyless dynamic discovery with open application admission.
    /// Open admission proves no identity ownership.
    #[must_use]
    pub fn open(bind: SocketAddr) -> Self {
        Self::with_admission(bind, None, Arc::new(OpenAdmission))
    }

    /// Creates application-controlled admission with optional strict fabric authentication.
    #[must_use]
    pub fn with_admission(
        bind: SocketAddr,
        key: Option<NetworkKey>,
        admission: Arc<dyn Admission>,
    ) -> Self {
        Self {
            bind,
            key,
            peers: None,
            admission,
            limits: TcpRendezvousLimits::default(),
            relay_pacing: RelayPacing::Backpressure,
        }
    }

    /// Checks every rule [`TcpRendezvous::bind`](super::TcpRendezvous::bind)
    /// enforces before it binds a listener.
    ///
    /// # Errors
    /// Returns the first violated rule, naming its field.
    pub fn validate(&self) -> Result<(), TcpRendezvousConfigError> {
        self.limits.validate()?;
        if self.bind.ip().is_multicast() {
            return Err(TcpRendezvousConfigError::MulticastBind);
        }
        if let Some(peers) = &self.peers {
            check_peers(peers).map_err(|fault| match fault {
                PeerListFault::Invalid { index, error } => {
                    TcpRendezvousConfigError::Peer { index, error }
                }
                PeerListFault::Duplicate { index } => {
                    TcpRendezvousConfigError::DuplicatePeer { index }
                }
            })?;
        }
        Ok(())
    }
}

/// A [`TcpRendezvousConfig`] or [`TcpRendezvousLimits`] rule violation,
/// naming the offending field.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TcpRendezvousConfigError {
    /// `limits.control_queue` is smaller than `limits.max_sessions`.
    ControlQueueBelowMaxSessions,
    /// `bind` is a multicast address.
    MulticastBind,
    /// `peers[index]` is not a valid native identity.
    Peer {
        /// Position of the rejected entry.
        index: usize,
        /// Why the identity was rejected.
        error: NodeIdError,
    },
    /// `peers[index]` repeats an earlier entry.
    DuplicatePeer {
        /// Position of the repeated entry.
        index: usize,
    },
}

impl fmt::Display for TcpRendezvousConfigError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ControlQueueBelowMaxSessions => f.write_str(
                "limits.control_queue must hold one admission introduction per limits.max_sessions",
            ),
            Self::MulticastBind => f.write_str("bind must not be a multicast address"),
            Self::Peer { index, error } => write!(f, "peers[{index}]: {error}"),
            Self::DuplicatePeer { index } => {
                write!(f, "peers[{index}] repeats an earlier identity")
            }
        }
    }
}

impl error::Error for TcpRendezvousConfigError {}

impl From<TcpRendezvousConfigError> for io::Error {
    fn from(error: TcpRendezvousConfigError) -> Self {
        Self::new(io::ErrorKind::InvalidInput, error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pacing::Budget;
    use std::time::Duration;
    use tokio::time::Instant;

    #[test]
    fn independent_control_budget_does_not_throttle_relay_packets() {
        let start = Instant::now();
        let mut control = Budget::new(2, 2, start);
        assert_eq!(control.ready_at(start, 1), start);
        assert_eq!(control.ready_at(start, 1), start);
        assert_eq!(
            control.ready_at(start, 1),
            start + Duration::from_millis(500)
        );
        assert!(matches!(
            TcpRendezvousConfig::open(([127, 0, 0, 1], 0).into()).relay_pacing,
            RelayPacing::Backpressure
        ));
    }

    #[test]
    fn capacities_are_operational_not_fixed_peer_limits() {
        let limits = TcpRendezvousLimits {
            max_sessions: QueueCapacity::of(512),
            control_queue: QueueCapacity::of(512),
            ..Default::default()
        };
        assert_eq!(limits.validate(), Ok(()));
        let limits = TcpRendezvousLimits {
            control_queue: QueueCapacity::of(1),
            ..Default::default()
        };
        assert_eq!(
            limits.validate(),
            Err(TcpRendezvousConfigError::ControlQueueBelowMaxSessions)
        );
        let mut config = TcpRendezvousConfig::open(([127, 0, 0, 1], 0).into());
        config.limits = limits;
        assert_eq!(
            config.validate(),
            Err(TcpRendezvousConfigError::ControlQueueBelowMaxSessions)
        );
        assert!(
            TcpRendezvousConfigError::ControlQueueBelowMaxSessions
                .to_string()
                .starts_with("limits.control_queue")
        );
    }

    #[test]
    fn bind_and_allowlist_rules_name_their_field() {
        let bind = SocketAddr::from(([127, 0, 0, 1], 0));
        let key = NetworkKey::from_bytes([1; 32]);
        assert_eq!(
            TcpRendezvousConfig::open(([224, 0, 0, 1], 0).into()).validate(),
            Err(TcpRendezvousConfigError::MulticastBind)
        );
        assert_eq!(
            TcpRendezvousConfig::new(bind, key.clone(), vec!["a".into(), "a".into()]).validate(),
            Err(TcpRendezvousConfigError::DuplicatePeer { index: 1 })
        );
        assert_eq!(
            TcpRendezvousConfig::new(bind, key.clone(), vec!["x".repeat(65).into()]).validate(),
            Err(TcpRendezvousConfigError::Peer {
                index: 0,
                error: NodeIdError::TooLong { len: 65, max: 64 }
            })
        );
        TcpRendezvousConfig::new(bind, key, vec!["a".into(), "b".into()])
            .validate()
            .unwrap();
    }

    #[tokio::test]
    async fn bind_surfaces_the_typed_error_as_invalid_input() {
        let mut config = TcpRendezvousConfig::open(([127, 0, 0, 1], 0).into());
        config.limits.control_queue = QueueCapacity::of(1);
        let error = crate::TcpRendezvous::bind(config).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        assert_eq!(
            error
                .get_ref()
                .and_then(|inner| inner.downcast_ref::<TcpRendezvousConfigError>()),
            Some(&TcpRendezvousConfigError::ControlQueueBelowMaxSessions)
        );
    }
}
