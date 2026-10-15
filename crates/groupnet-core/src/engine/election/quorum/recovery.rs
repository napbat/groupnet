//! Boot-time recovery of a voter's persisted grant: the restarting engine that
//! exists only until its storage has been handed back.

use crate::{Config, ConfigError, GroupEngine, GroupId, NodeId, Time};

use super::RecoveredGrant;

/// A restarting voter's engine that has not yet been handed its storage —
/// what [`GroupEngine::recovering`] returns, and the only way a
/// [`RecoveredGrant`] reaches an engine.
///
/// It exposes nothing but [`restore`](Self::restore): it cannot be started,
/// fed a frame or ticked, and a running [`GroupEngine`] has no way back into
/// one. Recovery is therefore a construction input, applied before the first
/// event by construction rather than by convention, so it can never lower a
/// live engine's grant floor or clear a promise it has already made.
///
/// ```
/// use groupnet_core::{Config, GroupEngine, GroupId, NodeId, RecoveredGrant, Time};
///
/// # fn main() -> Result<(), groupnet_core::ConfigError> {
/// let mut engine = GroupEngine::recovering(
///     GroupId::new("g"),
///     NodeId::new("a"),
///     [NodeId::new("b")],
///     Config::default(),
/// )?
/// .restore(RecoveredGrant::none());
/// let _effects = engine.start(Time::ZERO);
/// # Ok(())
/// # }
/// ```
///
/// A built or running engine cannot be refined with a recovered grant:
///
/// ```compile_fail,E0599
/// # use groupnet_core::{Config, GroupEngine, GroupId, NodeId, RecoveredGrant, Time};
/// # fn demo() -> Result<(), groupnet_core::ConfigError> {
/// let mut engine = GroupEngine::new(GroupId::new("g"), NodeId::new("a"), [], Config::default())?;
/// engine.start(Time::ZERO);
/// let _ = engine.restore(RecoveredGrant::none());
/// # Ok(())
/// # }
/// ```
///
/// and an engine still waiting for its storage cannot run:
///
/// ```compile_fail,E0599
/// # use groupnet_core::{Config, GroupEngine, GroupId, NodeId, Time};
/// # fn demo() -> Result<(), groupnet_core::ConfigError> {
/// let mut pending =
///     GroupEngine::recovering(GroupId::new("g"), NodeId::new("a"), [], Config::default())?;
/// let _ = pending.start(Time::ZERO);
/// # Ok(())
/// # }
/// ```
#[derive(Debug)]
#[must_use = "a recovering engine does nothing until `restore` hands it its storage"]
pub struct RecoveringEngine(GroupEngine);

impl RecoveringEngine {
    /// Hands the engine exactly what the store holds and returns it, ready to
    /// [`start`](GroupEngine::start), with the ledger a floor from the first
    /// tick. Unless the group's activation is
    /// [`Quorum`](crate::Activation::Quorum) the engine comes back unchanged —
    /// a `Settle` or [`Eventual`](crate::GroupMode::Eventual) group has no
    /// voter ledger, so `recovered` is ignored rather than half-applied.
    ///
    /// See [`RecoveredGrant`] for the contract, and in particular for why
    /// recovery restores the granted **pair** and not the instant it was
    /// granted at.
    #[must_use]
    pub fn restore(self, recovered: RecoveredGrant) -> GroupEngine {
        let mut engine = self.0;
        if let Some(q) = engine.quorum_mut() {
            match recovered.0 {
                // Attested never-granted: nothing to restore, and the boot
                // blackout is what durability was standing in for.
                None => q.grant_promise_until = Time::ZERO,
                // The pair is a floor; the blackout still runs from `start`.
                Some(pair) => q.granted = Some(pair),
            }
        }
        engine
    }
}

impl GroupEngine {
    /// Builds **a restarting voter's** engine, for a driver that provides
    /// durability under [`Activation::Quorum`](crate::Activation::Quorum):
    /// `GroupEngine::recovering(group, local, seeds, config)?.restore(grant)`.
    ///
    /// Takes what [`new`](GroupEngine::new) takes and validates it the same
    /// way; the [`RecoveringEngine`] it returns becomes a [`GroupEngine`] only
    /// once [`restore`](RecoveringEngine::restore) has handed it the voter
    /// state its storage holds.
    ///
    /// # Errors
    ///
    /// The [`ConfigError`] from [`Config::validate`], exactly as
    /// [`new`](GroupEngine::new).
    pub fn recovering(
        group: GroupId,
        local: NodeId,
        seeds: impl IntoIterator<Item = NodeId>,
        config: Config,
    ) -> Result<RecoveringEngine, ConfigError> {
        Self::new(group, local, seeds, config).map(RecoveringEngine)
    }
}
