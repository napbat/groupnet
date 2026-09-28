//! One finite donor-local suffix, independent of native feed retirement.

mod followers;

use super::types::{
    BarrierReceipt, CaptureCharge, CaptureId, DeltaIdentity, Invalidation, JournalConfig,
    JournalCursor, JournalDelta, JournalError, JournalState, NativeCut, ReservationId,
    ReservationStage,
};
use crate::Time;
use crate::volatile_bootstrap::ClaimIdentity;

#[derive(Clone, Debug)]
struct OutstandingBatch {
    operation: u64,
    through: u64,
    bytes: usize,
}

#[derive(Clone, Debug)]
struct Follower {
    id: ReservationId,
    due: Time,
    stage: ReservationStage,
    attach_operation: Option<u64>,
    barrier: Option<BarrierReceipt>,
    acked: u64,
    outstanding: Option<OutstandingBatch>,
}

#[derive(Clone, Debug)]
struct AbortedFollower {
    id: ReservationId,
    outstanding: Option<OutstandingBatch>,
    notified: bool,
}

/// Sans-IO, bounded local suffix for one private index capture. Every call
/// that changes the application index must invoke `append` under the same
/// application publication lock or invalidate this candidate.
#[derive(Debug)]
pub struct DonorJournal {
    config: JournalConfig,
    id: CaptureId,
    state: JournalState,
    reason: Option<Invalidation>,
    now: Time,
    capture_started: Option<Time>,
    expires: Option<Time>,
    reserved_encoded: usize,
    reserved_decoded: usize,
    charge: Option<CaptureCharge>,
    members: Vec<ClaimIdentity>,
    cuts: Vec<NativeCut>,
    deltas: Vec<JournalDelta>,
    last_position: u64,
    suffix_bytes: usize,
    inflight_bytes: usize,
    followers: Vec<Follower>,
    aborted: Vec<AbortedFollower>,
    next_reservation: u64,
    next_operation: u64,
}

impl DonorJournal {
    /// Construct one uncaptured journal with a fresh donor capture identity.
    ///
    /// # Errors
    /// Rejects invalid identity or resource limits.
    pub fn new(config: JournalConfig, id: CaptureId) -> Result<Self, JournalError> {
        let config = config.validate()?;
        let scope_bytes = id
            .scope
            .domain
            .len()
            .checked_add(id.scope.partition.len())
            .ok_or(JournalError::InvalidConfig)?;
        if id.scope.domain.is_empty()
            || id.scope.partition.is_empty()
            || scope_bytes > config.max_scope_bytes
            || id.donor.node.as_str().is_empty()
            || id.donor.node.as_str().len() > config.max_follower_id_bytes
            || id.donor.incarnation == 0
            || id.donor.session == 0
            || id.donor.attempt == 0
            || id.recovery_generation == 0
            || id.serial == 0
        {
            return Err(JournalError::InvalidConfig);
        }
        Ok(Self {
            config,
            id,
            state: JournalState::Uncaptured,
            reason: None,
            now: Time(0),
            capture_started: None,
            expires: None,
            reserved_encoded: 0,
            reserved_decoded: 0,
            charge: None,
            members: Vec::new(),
            cuts: Vec::new(),
            deltas: Vec::new(),
            last_position: 0,
            suffix_bytes: 0,
            inflight_bytes: 0,
            followers: Vec::new(),
            aborted: Vec::new(),
            next_reservation: 1,
            next_operation: 1,
        })
    }

    /// Exact donor capture identity.
    #[must_use]
    pub fn id(&self) -> &CaptureId {
        &self.id
    }

    /// Candidate phase; no phase grants serving authority.
    #[must_use]
    pub fn state(&self) -> JournalState {
        self.state
    }

    /// Terminal invalidation reason, if one exists.
    #[must_use]
    pub fn invalidation(&self) -> Option<Invalidation> {
        self.reason
    }

    /// Current finite candidate deadline.
    #[must_use]
    pub fn next_deadline(&self) -> Option<Time> {
        self.expires
            .into_iter()
            .chain(self.followers.iter().map(|follower| follower.due))
            .min()
    }

    /// Current image charge after bounded capture completed.
    #[must_use]
    pub fn image_charge(&self) -> Option<CaptureCharge> {
        self.charge
    }

    /// Current exact bounded native writer cuts.
    #[must_use]
    pub fn covered_cuts(&self) -> &[NativeCut] {
        &self.cuts
    }

    /// Current retained local suffix bytes, excluding private image memory.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.suffix_bytes
    }

    /// Current separately reserved, returned batch bytes.
    #[must_use]
    pub fn inflight_bytes(&self) -> usize {
        self.inflight_bytes
    }

    /// Take bounded release notifications after expiry or invalidation.
    /// This does not free in-flight memory admission; the runtime must drop
    /// the exact stream/batch and call [`Self::retire_aborted`].
    pub fn take_aborted(&mut self) -> Vec<ReservationId> {
        self.aborted
            .iter_mut()
            .filter(|aborted| !aborted.notified)
            .map(|aborted| {
                aborted.notified = true;
                aborted.id.clone()
            })
            .collect()
    }

    /// Release one expired/invalidated resource only after its runtime buffer
    /// and stream are actually retired. The optional batch token must match.
    ///
    /// # Errors
    /// Rejects old reservation or wrong in-flight batch acknowledgment.
    pub fn retire_aborted(
        &mut self,
        reservation: &ReservationId,
        batch_operation: Option<u64>,
    ) -> Result<(), JournalError> {
        let index = self
            .aborted
            .iter()
            .position(|aborted| aborted.id == *reservation)
            .ok_or(JournalError::Stale)?;
        if self.aborted[index]
            .outstanding
            .as_ref()
            .map(|batch| batch.operation)
            != batch_operation
        {
            return Err(JournalError::Stale);
        }
        let aborted = self.aborted.remove(index);
        if let Some(batch) = aborted.outstanding {
            self.inflight_bytes -= batch.bytes;
        }
        Ok(())
    }

    fn invalidate_inner(&mut self, reason: Invalidation) {
        if self.state == JournalState::Invalidated {
            return;
        }
        self.reason = Some(reason);
        self.state = JournalState::Invalidated;
        self.expires = None;
        self.capture_started = None;
        self.reserved_encoded = 0;
        self.reserved_decoded = 0;
        self.charge = None;
        self.members.clear();
        self.cuts.clear();
        self.deltas.clear();
        self.suffix_bytes = 0;
        self.aborted
            .extend(self.followers.drain(..).map(|follower| AbortedFollower {
                id: follower.id,
                outstanding: follower.outstanding,
                notified: false,
            }));
    }

    fn advance(&mut self, now: Time) -> Result<(), JournalError> {
        if now < self.now {
            return Err(JournalError::BackwardTime);
        }
        self.now = now;
        if self.expires.is_some_and(|due| now >= due) {
            self.invalidate_inner(Invalidation::Expired);
            return Err(JournalError::Expired);
        }
        let mut index = 0;
        while index < self.followers.len() {
            if now >= self.followers[index].due {
                let follower = self.followers.remove(index);
                self.aborted.push(AbortedFollower {
                    id: follower.id,
                    outstanding: follower.outstanding,
                    notified: false,
                });
            } else {
                index += 1;
            }
        }
        Ok(())
    }

    /// Apply a monotone caller-supplied logical tick; expire exact holds.
    ///
    /// # Errors
    /// Rejects backward time or reports candidate expiry.
    pub fn tick(&mut self, now: Time) -> Result<(), JournalError> {
        self.advance(now)
    }

    /// End this candidate and return all exact follower releases via
    /// [`Self::take_aborted`]. The application discards its private image.
    pub fn invalidate(&mut self, reason: Invalidation) {
        self.invalidate_inner(reason);
    }

    fn valid_members(&self, members: &[ClaimIdentity]) -> Result<(), JournalError> {
        if members.is_empty() || members.len() > self.config.max_members {
            return Err(JournalError::Capacity);
        }
        let mut bytes = 0usize;
        for (index, member) in members.iter().enumerate() {
            bytes = bytes
                .checked_add(member.node.as_str().len())
                .ok_or(JournalError::Capacity)?;
            if member.node.as_str().is_empty()
                || member.incarnation == 0
                || member.session == 0
                || member.attempt == 0
                || member.node.as_str().len() > self.config.max_follower_id_bytes
                || bytes > self.config.max_membership_bytes
                || (index > 0 && members[index - 1].node >= member.node)
            {
                return Err(JournalError::Capacity);
            }
        }
        if !members.contains(&self.id.donor) {
            return Err(JournalError::Conflict);
        }
        Ok(())
    }

    fn valid_cuts(&self, cuts: &[NativeCut]) -> Result<(), JournalError> {
        if cuts.len() > self.config.max_cuts {
            return Err(JournalError::Capacity);
        }
        let mut bytes = 0usize;
        for (index, cut) in cuts.iter().enumerate() {
            bytes = bytes
                .checked_add(cut.writer.len())
                .ok_or(JournalError::Capacity)?;
            if cut.writer.is_empty()
                || cut.epoch == 0
                || bytes > self.config.max_cut_bytes
                || (index > 0 && cuts[index - 1].writer >= cut.writer)
            {
                return Err(JournalError::Capacity);
            }
        }
        Ok(())
    }

    /// Reserve image and suffix budgets before the adapter clones the index.
    /// The adapter holds the index publication lock through `finish_capture`
    /// or `invalidate`, and performs a bounded private clone under that lock.
    ///
    /// # Errors
    /// Fails closed on invalid roster/cuts, budget, or time; no image is ready.
    pub fn begin_capture(
        &mut self,
        now: Time,
        planned_encoded: usize,
        planned_decoded: usize,
        members: Vec<ClaimIdentity>,
        cuts: Vec<NativeCut>,
    ) -> Result<CaptureCharge, JournalError> {
        self.advance(now)?;
        if self.state != JournalState::Uncaptured {
            return Err(JournalError::Stage);
        }
        if planned_encoded == 0
            || planned_decoded == 0
            || planned_encoded > self.config.max_encoded_bytes
            || planned_decoded > self.config.max_decoded_bytes
        {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        }
        if let Err(error) = self
            .valid_members(&members)
            .and_then(|()| self.valid_cuts(&cuts))
        {
            self.invalidate_inner(if error == JournalError::Conflict {
                Invalidation::Membership
            } else {
                Invalidation::Capacity
            });
            return Err(error);
        }
        let Some(expires) = now.0.checked_add(self.config.max_total_ms).map(Time) else {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Exhausted);
        };
        self.expires = Some(expires);
        self.capture_started = Some(now);
        self.reserved_encoded = planned_encoded;
        self.reserved_decoded = planned_decoded;
        self.members = members;
        self.cuts = cuts;
        self.state = JournalState::Capturing;
        Ok(CaptureCharge {
            encoded_bytes: planned_encoded,
            decoded_bytes: planned_decoded,
            suffix_bytes: self.config.max_suffix_bytes,
            started: now,
        })
    }

    /// Complete the private bounded clone at the exact captured state/C cut.
    ///
    /// # Errors
    /// Rejects a clone exceeding its pre-admitted image budgets.
    pub fn finish_capture(
        &mut self,
        now: Time,
        actual_encoded: usize,
        actual_decoded: usize,
    ) -> Result<JournalCursor, JournalError> {
        self.advance(now)?;
        if self.state != JournalState::Capturing {
            return Err(JournalError::Stage);
        }
        if actual_encoded == 0
            || actual_decoded == 0
            || actual_encoded > self.reserved_encoded
            || actual_decoded > self.reserved_decoded
        {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        }
        self.charge = Some(CaptureCharge {
            encoded_bytes: actual_encoded,
            decoded_bytes: actual_decoded,
            suffix_bytes: self.config.max_suffix_bytes,
            started: self.capture_started.ok_or(JournalError::Stage)?,
        });
        self.state = JournalState::Active;
        Ok(self.cursor(0))
    }

    fn cursor(&self, position: u64) -> JournalCursor {
        JournalCursor {
            capture: self.id.clone(),
            position,
        }
    }

    /// Exact current local mutation cut. The caller must already have an
    /// attached, acknowledged reservation before using it as a barrier.
    #[must_use]
    pub fn current_cursor(&self) -> Option<JournalCursor> {
        (self.state == JournalState::Active).then(|| self.cursor(self.last_position))
    }

    /// Compare an exact bounded complete membership roster with the capture.
    /// A changed or malformed roster invalidates all transfer candidates.
    ///
    /// # Errors
    /// Fails closed on changed source membership or malformed metadata.
    pub fn observe_membership(
        &mut self,
        now: Time,
        members: &[ClaimIdentity],
    ) -> Result<(), JournalError> {
        self.advance(now)?;
        if self.state != JournalState::Active {
            return Err(JournalError::Stage);
        }
        if self.valid_members(members).is_err() || members != self.members {
            self.invalidate_inner(Invalidation::Membership);
            return Err(JournalError::Conflict);
        }
        Ok(())
    }

    /// Record one final index publication after C. The application holds its
    /// publication lock while applying the same mutation, including repairs
    /// and tombstones. A conflicting duplicate or an unknown writer closes
    /// this candidate; the live application index still proceeds normally.
    ///
    /// # Errors
    /// Rejects stale generation, noncontiguous native feed, or capacity.
    pub fn append(
        &mut self,
        now: Time,
        recovery_generation: u64,
        identity: DeltaIdentity,
        effect: Vec<u8>,
    ) -> Result<JournalCursor, JournalError> {
        self.advance(now)?;
        if self.state != JournalState::Active {
            return Err(JournalError::Stage);
        }
        if recovery_generation != self.id.recovery_generation {
            self.invalidate_inner(Invalidation::Rebuild);
            return Err(JournalError::Stale);
        }
        if let Some(existing) = self.deltas.iter().find(|delta| delta.identity == identity) {
            if existing.effect == effect {
                return Ok(self.cursor(existing.position));
            }
            self.invalidate_inner(Invalidation::Conflict);
            return Err(JournalError::Conflict);
        }
        let identity_bytes = match &identity {
            DeltaIdentity::Native(cut) => cut.writer.len(),
            DeltaIdentity::Local(id) => id.len(),
        };
        let Some(event_bytes) = identity_bytes.checked_add(effect.len()) else {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        };
        let Some(total_bytes) = self.suffix_bytes.checked_add(event_bytes) else {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        };
        if identity_bytes == 0
            || effect.is_empty()
            || identity_bytes > self.config.max_identity_bytes
            || event_bytes > self.config.max_event_bytes
            || total_bytes > self.config.max_suffix_bytes
            || self.deltas.len() >= self.config.max_events
        {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        }
        if let DeltaIdentity::Native(cut) = &identity {
            let Some(covered) = self
                .cuts
                .iter_mut()
                .find(|existing| existing.writer == cut.writer)
            else {
                self.invalidate_inner(Invalidation::Membership);
                return Err(JournalError::Conflict);
            };
            if covered.epoch != cut.epoch {
                self.invalidate_inner(Invalidation::Membership);
                return Err(JournalError::Conflict);
            }
            let Some(next) = covered.sequence.checked_add(1) else {
                self.invalidate_inner(Invalidation::Conflict);
                return Err(JournalError::Exhausted);
            };
            if cut.sequence != next {
                self.invalidate_inner(Invalidation::Gap);
                return Err(JournalError::Conflict);
            }
            covered.sequence = cut.sequence;
        }
        let Some(position) = self.last_position.checked_add(1) else {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Exhausted);
        };
        self.last_position = position;
        self.suffix_bytes = total_bytes;
        self.deltas.push(JournalDelta {
            position,
            identity,
            effect,
        });
        Ok(self.cursor(position))
    }
}
