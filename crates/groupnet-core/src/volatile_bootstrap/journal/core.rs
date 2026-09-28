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
    last_acked: Option<(u64, u64)>,
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
    image_cuts: Vec<NativeCut>,
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
    fn storage_slots(count: usize) -> Result<usize, JournalError> {
        count
            .checked_mul(2)
            .map(|slots| slots.max(4))
            .ok_or(JournalError::InvalidConfig)
    }

    fn copy_image_cuts(
        config: JournalConfig,
        cuts: &[NativeCut],
    ) -> Result<Vec<NativeCut>, JournalError> {
        let mut image_cuts = Vec::new();
        image_cuts
            .try_reserve_exact(cuts.len())
            .map_err(|_| JournalError::Capacity)?;
        if image_cuts.capacity() > Self::storage_slots(config.max_cuts)? {
            return Err(JournalError::Capacity);
        }
        for cut in cuts {
            let mut writer = Vec::new();
            writer
                .try_reserve_exact(cut.writer.len())
                .map_err(|_| JournalError::Capacity)?;
            if writer.capacity() > config.max_cut_bytes {
                return Err(JournalError::Capacity);
            }
            writer.extend_from_slice(&cut.writer);
            image_cuts.push(NativeCut {
                writer,
                epoch: cut.epoch,
                sequence: cut.sequence,
            });
        }
        Ok(image_cuts)
    }

    /// Conservative retained-heap reservation for one journal, excluding the
    /// separately admitted image and physical returned batch copies.
    ///
    /// This charges bounded vector slots (including growth headroom), exact
    /// member/cut strings, delta bodies, follower identities, and every
    /// follower's possible saved barrier. The application acquires this
    /// reservation before starting its guarded index capture.
    ///
    /// # Errors
    /// Rejects invalid limits or arithmetic overflow.
    pub fn storage_bound(config: JournalConfig) -> Result<usize, JournalError> {
        use std::mem::size_of;

        let config = config.validate()?;
        let slots = |count: usize, size: usize| {
            Self::storage_slots(count)?
                .checked_mul(size)
                .ok_or(JournalError::InvalidConfig)
        };
        let add = |total: usize, bytes: usize| {
            total.checked_add(bytes).ok_or(JournalError::InvalidConfig)
        };
        let mut total = size_of::<Self>();
        total = add(total, slots(config.max_events, size_of::<JournalDelta>())?)?;
        total = add(total, config.max_suffix_bytes)?;
        total = add(
            total,
            config
                .max_events
                .checked_mul(config.max_event_bytes)
                .ok_or(JournalError::InvalidConfig)?,
        )?;
        total = add(
            total,
            config
                .max_events
                .checked_mul(config.max_identity_bytes)
                .ok_or(JournalError::InvalidConfig)?,
        )?;
        total = add(
            total,
            slots(config.max_members, size_of::<ClaimIdentity>())?,
        )?;
        total = add(total, config.max_membership_bytes)?;
        total = add(total, slots(config.max_cuts, size_of::<NativeCut>())?)?;
        total = add(total, config.max_cut_bytes)?;
        total = add(
            total,
            config
                .max_cuts
                .checked_mul(config.max_cut_bytes)
                .ok_or(JournalError::InvalidConfig)?,
        )?;
        // C's immutable writer cuts coexist with the advancing B cuts while
        // encoding and throughout donor service.
        total = add(total, slots(config.max_cuts, size_of::<NativeCut>())?)?;
        total = add(
            total,
            config
                .max_cuts
                .checked_mul(config.max_cut_bytes)
                .ok_or(JournalError::InvalidConfig)?,
        )?;
        total = add(total, config.max_scope_bytes)?;
        total = add(total, config.max_follower_id_bytes)?;
        // Both live and aborted vectors can retain allocated slots. Each live
        // follower can additionally retain an exact B with copied roster,
        // cuts, scope, and donor/follower identities. Returned B/readback
        // values are physical clones admitted separately by the runtime.
        let per_follower = slots(1, size_of::<Follower>())?
            .checked_add(slots(1, size_of::<AbortedFollower>())?)
            .and_then(|n| n.checked_add(size_of::<BarrierReceipt>()))
            .and_then(|n| {
                n.checked_add(slots(config.max_members, size_of::<ClaimIdentity>()).ok()?)
            })
            .and_then(|n| n.checked_add(config.max_membership_bytes))
            .and_then(|n| n.checked_add(slots(config.max_cuts, size_of::<NativeCut>()).ok()?))
            .and_then(|n| n.checked_add(config.max_cut_bytes))
            .and_then(|n| n.checked_add(config.max_cuts.checked_mul(config.max_cut_bytes)?))
            .and_then(|n| n.checked_add(config.max_scope_bytes.checked_mul(8)?))
            .and_then(|n| n.checked_add(config.max_follower_id_bytes.checked_mul(8)?))
            .ok_or(JournalError::InvalidConfig)?;
        add(
            total,
            config
                .max_followers
                .checked_mul(per_follower)
                .ok_or(JournalError::InvalidConfig)?,
        )
    }

    /// Immutable finite source limits used to reject incompatible follower
    /// transfer policies before cloning source metadata or batches.
    #[must_use]
    pub const fn config(&self) -> JournalConfig {
        self.config
    }

    fn saved_barrier_fits(&self, receipt: &BarrierReceipt) -> bool {
        receipt.members.capacity() <= Self::storage_slots(self.config.max_members).unwrap_or(0)
            && receipt.covered_cuts.capacity()
                <= Self::storage_slots(self.config.max_cuts).unwrap_or(0)
            && receipt
                .covered_cuts
                .iter()
                .all(|cut| cut.writer.capacity() <= self.config.max_cut_bytes)
    }

    /// Construct one uncaptured journal with a fresh donor capture identity.
    ///
    /// # Errors
    /// Rejects invalid identity or resource limits.
    pub fn new(config: JournalConfig, id: CaptureId) -> Result<Self, JournalError> {
        let config = config.validate()?;
        Self::storage_bound(config)?;
        let scope_bytes = id
            .scope
            .domain
            .len()
            .checked_add(id.scope.partition.len())
            .ok_or(JournalError::InvalidConfig)?;
        if id.scope.domain.is_empty()
            || id.scope.partition.is_empty()
            || scope_bytes > config.max_scope_bytes
            || id
                .scope
                .domain
                .capacity()
                .checked_add(id.scope.partition.capacity())
                .is_none_or(|capacity| capacity > config.max_scope_bytes)
            || id.donor.node.as_str().is_empty()
            || id.donor.node.as_str().len() > config.max_follower_id_bytes
            || id.donor.incarnation.0 == 0
            || id.donor.session == 0
            || id.donor.attempt == 0
            || id.recovery_generation == 0
            || id.serial == 0
        {
            return Err(JournalError::InvalidConfig);
        }
        // Fix all growable core vectors at their configured ceilings. A
        // platform allocator may return more slots than requested, so verify
        // the actual capacities against the charged headroom before use.
        let mut deltas = Vec::new();
        let mut followers = Vec::new();
        let mut aborted = Vec::new();
        deltas
            .try_reserve_exact(config.max_events)
            .map_err(|_| JournalError::Capacity)?;
        followers
            .try_reserve_exact(config.max_followers)
            .map_err(|_| JournalError::Capacity)?;
        aborted
            .try_reserve_exact(config.max_followers)
            .map_err(|_| JournalError::Capacity)?;
        if deltas.capacity() > Self::storage_slots(config.max_events)?
            || followers.capacity() > Self::storage_slots(config.max_followers)?
            || aborted.capacity() > Self::storage_slots(config.max_followers)?
        {
            return Err(JournalError::Capacity);
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
            image_cuts: Vec::new(),
            cuts: Vec::new(),
            deltas,
            last_position: 0,
            suffix_bytes: 0,
            inflight_bytes: 0,
            followers,
            aborted,
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

    /// Immutable native writer cuts belonging to the private image at C.
    /// Later appended suffix events advance [`Self::covered_cuts`] only.
    #[must_use]
    pub fn image_cuts(&self) -> &[NativeCut] {
        &self.image_cuts
    }

    /// Current retained local suffix bytes, excluding private image memory.
    #[must_use]
    pub fn retained_bytes(&self) -> usize {
        self.suffix_bytes
    }

    /// Current logical outstanding batch bytes. Every physical readback copy
    /// requires separate runtime memory admission.
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
        self.image_cuts.clear();
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
                || member.incarnation.0 == 0
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
    /// The adapter holds the publication lock through the bounded clone and
    /// ingress attachment at C, then may encode off-lock while appends record
    /// later effects. It rechecks the exact candidate and finishes under the
    /// publication lock before advertising any follower availability.
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
        // A caller may pass a short Vec with an enormous retained capacity.
        // Reject that allocation before the journal takes ownership of it.
        if members.capacity() > Self::storage_slots(self.config.max_members)?
            || cuts.capacity() > Self::storage_slots(self.config.max_cuts)?
            || cuts
                .iter()
                .any(|cut| cut.writer.capacity() > self.config.max_cut_bytes)
        {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Capacity);
        }
        let image_cuts = match Self::copy_image_cuts(self.config, &cuts) {
            Ok(image_cuts) => image_cuts,
            Err(error) => {
                self.invalidate_inner(Invalidation::Capacity);
                return Err(error);
            }
        };
        let Some(expires) = now.0.checked_add(self.config.max_total_ms).map(Time) else {
            self.invalidate_inner(Invalidation::Capacity);
            return Err(JournalError::Exhausted);
        };
        self.expires = Some(expires);
        self.capture_started = Some(now);
        self.reserved_encoded = planned_encoded;
        self.reserved_decoded = planned_decoded;
        self.members = members;
        self.image_cuts = image_cuts;
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

    /// Compare an exact bounded complete membership roster with the capture,
    /// including during off-lock image encoding. This does not make a
    /// `Capturing` image available to followers. A changed or malformed
    /// roster invalidates all transfer candidates.
    ///
    /// # Errors
    /// Fails closed on changed source membership or malformed metadata.
    pub fn observe_membership(
        &mut self,
        now: Time,
        members: &[ClaimIdentity],
    ) -> Result<(), JournalError> {
        self.advance(now)?;
        if !matches!(self.state, JournalState::Capturing | JournalState::Active) {
            return Err(JournalError::Stage);
        }
        if self.valid_members(members).is_err() || members != self.members {
            self.invalidate_inner(Invalidation::Membership);
            return Err(JournalError::Conflict);
        }
        Ok(())
    }

    /// Record one final index publication after C, including while the
    /// bounded private image is still being encoded. The application holds
    /// its publication lock while applying the same mutation, including
    /// repairs and tombstones. A conflicting duplicate or unknown writer
    /// closes this candidate; the live application index still proceeds.
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
        if !matches!(self.state, JournalState::Capturing | JournalState::Active) {
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
            || effect.capacity() > self.config.max_event_bytes
            || match &identity {
                DeltaIdentity::Native(cut) => cut.writer.capacity(),
                DeltaIdentity::Local(id) => id.capacity(),
            } > self.config.max_identity_bytes
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
