//! Synchronous, permanently retireable side-effect fence.

use std::fmt;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::Instant;

use groupnet_core::replication::Operation;

use super::api::Materialized;

fn lock<T>(value: &Mutex<T>) -> MutexGuard<'_, T> {
    value.lock().unwrap_or_else(PoisonError::into_inner)
}

#[derive(Debug, Default)]
struct FenceState {
    current: Option<u64>,
    serial: u64,
    retired: bool,
}

/// Shared side-effect fence for one session incarnation. Closing a scope
/// permanently retires the old fence before admitting its successor.
#[derive(Clone, Default)]
pub struct OperationFence {
    state: Arc<Mutex<FenceState>>,
}

impl fmt::Debug for OperationFence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("OperationFence")
            .field("state", &*lock(&self.state))
            .finish()
    }
}

impl OperationFence {
    /// Invalidates permits while allowing later operations in this live session.
    pub fn invalidate(&self) {
        lock(&self.state).current = None;
    }

    /// Permanently bars old workers from reissuing side-effect permits.
    pub fn retire(&self) {
        let mut state = lock(&self.state);
        state.current = None;
        state.retired = true;
    }

    fn issue_serial(&self) -> Option<u64> {
        let mut state = lock(&self.state);
        if state.retired {
            return None;
        }
        let next = state.serial.checked_add(1)?;
        state.serial = next;
        state.current = Some(next);
        Some(next)
    }

    /// Issues an install permit for one core-issued operation.
    pub(crate) fn issue(&self, operation: Operation, deadline: Instant) -> Option<InstallPermit> {
        Some(InstallPermit {
            fence: self.clone(),
            serial: self.issue_serial()?,
            operation,
            deadline,
        })
    }

    /// Issues a revocation capability. `None` marks core's unconfirmed
    /// allocator-exhaustion path, never an invented operation token.
    pub(crate) fn issue_revocation(
        &self,
        operation: Option<Operation>,
        deadline: Instant,
    ) -> Option<RevocationPermit> {
        Some(RevocationPermit {
            fence: self.clone(),
            serial: self.issue_serial()?,
            operation,
            deadline,
        })
    }

    fn with_current<T>(
        &self,
        serial: u64,
        deadline: Instant,
        action: impl FnOnce() -> T,
    ) -> Option<T> {
        let state = lock(&self.state);
        if state.retired || state.current != Some(serial) || Instant::now() >= deadline {
            return None;
        }
        // Keep the guard across the synchronous atomic state/cursor or
        // serve-gate mutation; cancellation serializes after publication.
        Some(action())
    }
}

/// Capability for one current native state install. Stale, expired, and
/// retired permits cannot publish through [`Self::commit_sync`].
#[derive(Clone, Debug)]
pub struct InstallPermit {
    fence: OperationFence,
    serial: u64,
    operation: Operation,
    deadline: Instant,
}

impl InstallPermit {
    /// Exact core-issued operation for a conditional durable transaction.
    #[must_use]
    pub fn operation(&self) -> Operation {
        self.operation
    }

    /// Absolute monotonic deadline checked by a conditional transaction.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Runs a synchronous atomic state/cursor install only while current.
    ///
    /// # Errors
    /// Propagates the application's synchronous install failure.
    pub fn commit_sync<P, E>(
        &self,
        position: P,
        durable: bool,
        install: impl FnOnce() -> Result<(), E>,
    ) -> Result<Option<Materialized<P>>, E> {
        self.fence
            .with_current(self.serial, self.deadline, install)
            .transpose()
            .map(|result| result.map(|()| Materialized { position, durable }))
    }

    /// Confirms an async transaction only after its store atomically compared
    /// this operation and deadline with the state/cursor mutation.
    #[must_use]
    pub fn confirm_durable_transaction<P>(&self, position: P) -> Option<Materialized<P>> {
        self.fence
            .with_current(self.serial, self.deadline, || Materialized {
                position,
                durable: true,
            })
    }
}

/// Capability for one current serving revocation. The unconfirmed emergency
/// path remains scoped to this retired-on-close session fence.
#[derive(Clone, Debug)]
pub struct RevocationPermit {
    fence: OperationFence,
    serial: u64,
    operation: Option<Operation>,
    deadline: Instant,
}

impl RevocationPermit {
    /// Core operation, or `None` for allocator-exhaustion fail-closed revoke.
    #[must_use]
    pub fn operation(&self) -> Option<Operation> {
        self.operation
    }

    /// Absolute monotonic deadline for a conditional external transaction.
    #[must_use]
    pub fn deadline(&self) -> Instant {
        self.deadline
    }

    /// Atomically revokes the application serve gate only while current.
    ///
    /// # Errors
    /// Propagates the application's synchronous revocation failure.
    pub fn revoke_sync<E>(&self, revoke: impl FnOnce() -> Result<(), E>) -> Result<Option<()>, E> {
        self.fence
            .with_current(self.serial, self.deadline, revoke)
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::OperationFence;
    use groupnet_core::replication::Operation;
    use std::time::{Duration, Instant};

    #[test]
    fn retirement_blocks_reissue_and_delayed_revocation() {
        let fence = OperationFence::default();
        let deadline = Instant::now() + Duration::from_secs(1);
        let old = fence.issue_revocation(None, deadline).expect("old permit");
        fence.retire();
        assert_eq!(old.revoke_sync(|| Ok::<(), ()>(())), Ok(None));
        assert!(fence.issue_revocation(None, deadline).is_none());
        assert!(
            fence
                .issue(
                    Operation {
                        session: 1,
                        generation: 1,
                        token: 1
                    },
                    deadline
                )
                .is_none()
        );
    }
}
