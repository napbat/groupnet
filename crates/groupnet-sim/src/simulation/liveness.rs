//! The probe/liveness dispatch: the network and timer events that carry every
//! membership probe, ack and gossip frame ([`Kind::Deliver`],
//! [`Kind::Tick`]), the frame probes counted along that path, and the
//! liveness observers a failure-detection test reads.

use super::{Kind, Simulation};
use crate::rng::SplitMix64;
use std::collections::BTreeSet;

use groupnet_core::{NodeId, Status, Time, wire};

/// Whether an encoded frame's kind tag names an election frame — wire kinds
/// `8..=10` (`LeadClaim`, `LeadGrant`, `LeadState`). Read straight off the
/// byte after the version, the same cheap peek `wire::peek_group` does, so
/// counting costs nothing on the delivery path.
fn is_election_frame(wire: &[u8]) -> bool {
    matches!(wire.get(1).copied(), Some(8..=10))
}

impl Simulation {
    /// Schedules one [`Effect::Send`](groupnet_core::Effect::Send) from `from`
    /// for delivery one link latency (plus deterministic jitter) from now,
    /// after counting it against the issuance probes.
    pub(super) fn send(&mut self, from: &NodeId, to: NodeId, wire: Vec<u8>) {
        self.max_frame_bytes = self.max_frame_bytes.max(wire.len());
        self.note_lead_frame(from, &wire);
        // Deterministic per-message jitter keyed on the sequence number this
        // delivery is about to take, so links can reorder.
        let extra = if self.jitter_ms == 0 {
            0
        } else {
            SplitMix64::hash(self.seq.wrapping_add(1)) % (self.jitter_ms + 1)
        };
        let at = self
            .now
            .saturating_add(self.latency_ms)
            .saturating_add(extra);
        self.schedule(
            at,
            Kind::Deliver {
                to,
                from: from.clone(),
                wire,
            },
        );
    }

    /// One frame arriving, subject to the loss schedule and the partition set.
    pub(super) fn deliver(&mut self, to: &NodeId, from: NodeId, wire: &[u8], seq: u64) {
        if self.loss_percent != 0 && SplitMix64::hash(seq) % 100 < u64::from(self.loss_percent) {
            return; // deterministic per-message drop
        }
        if self.blocked.contains(&(from.clone(), to.clone())) {
            return; // partitioned link
        }
        let now = self.now;
        let effects = match self.engines.get_mut(to) {
            Some(engine) => engine.on_message(from, wire, now),
            None => return, // a crashed node: nothing consumed it
        };
        // Counted only once an engine has actually taken it — the probe is
        // "delivered to an engine", not "scheduled and not dropped", so a frame
        // still in flight to a node that has since crashed is not election
        // traffic anyone observed.
        if is_election_frame(wire) {
            self.election_frames += 1;
        }
        self.dispatch(to, effects);
    }

    /// One timer firing on `node` — the probe, suspicion and gossip clock. A
    /// tick armed by an engine that has since crashed fires on nothing.
    pub(super) fn tick(&mut self, node: &NodeId) {
        let now = self.now;
        if let Some(engine) = self.engines.get_mut(node) {
            let effects = engine.on_tick(now);
            self.dispatch(node, effects);
        }
    }

    /// Counts and logs one dispatched election frame, reading its body off the
    /// encoded bytes rather than off the effect: issuance is a wire fact.
    ///
    /// Every dispatched frame is decoded to find out — the codec is the only
    /// authority on what a frame is, and a byte-offset pre-filter would be a
    /// second, silently drifting copy of the wire format. The cost is test-only
    /// and lands on a path that has already allocated the encoded frame. The
    /// one decode feeds three probes: [`grant_log`](Self::grant_log),
    /// [`claim_frames_seen`](Self::claim_frames_seen) and
    /// [`grant_frames_seen`](Self::grant_frames_seen).
    fn note_lead_frame(&mut self, from: &NodeId, wire: &[u8]) {
        let Some(frame) = wire::decode(wire) else {
            return;
        };
        match frame.lead {
            Some(wire::LeadBody::Claim { .. }) => self.claim_frames += 1,
            Some(wire::LeadBody::Grant {
                epoch, claimant, ..
            }) => {
                self.grant_frames += 1;
                self.grant_log
                    .push((self.now, from.clone(), epoch, claimant));
            }
            Some(wire::LeadBody::State { .. }) | None => {}
        }
    }

    /// The largest encoded frame the sim has carried so far. A test asserts this
    /// stays within the engine's `max_delta_frame_bytes` cap across a whole run.
    #[must_use]
    pub fn max_frame_bytes(&self) -> usize {
        self.max_frame_bytes
    }

    /// How many election frames (wire kinds `LeadClaim`, `LeadGrant`,
    /// `LeadState`) the sim has actually delivered to an engine — frames lost
    /// or blocked by a partition are not counted.
    ///
    /// This is the mode-invariance probe: a run of purely
    /// [`GroupMode::Eventual`](groupnet_core::GroupMode) groups must end with
    /// zero, proving the metadata-only contract really does cost no election
    /// traffic.
    #[must_use]
    pub fn election_frames_seen(&self) -> u64 {
        self.election_frames
    }

    /// How many `LeadClaim` frames the run has **issued** — counted at
    /// dispatch, off the decoded frame.
    ///
    /// Issuance rather than delivery, unlike
    /// [`election_frames_seen`](Self::election_frames_seen), and deliberately:
    /// this is the **X-purity** probe, and the claim it pins is that an
    /// [`Activation::External`](groupnet_core::Activation::External) group
    /// never *builds* a bid. A frame that was built and then lost, dropped by
    /// the loss schedule or blocked by a partition would satisfy a delivery
    /// counter while violating the property.
    #[must_use]
    pub fn claim_frames_seen(&self) -> u64 {
        self.claim_frames
    }

    /// How many `LeadGrant` frames the run has **issued** — the other half of
    /// the X-purity probe. See [`claim_frames_seen`](Self::claim_frames_seen)
    /// for why both are counted at issuance.
    #[must_use]
    pub fn grant_frames_seen(&self) -> u64 {
        self.grant_frames
    }

    /// How many members `node` currently knows about, or `None` if `node` is
    /// not in the simulation.
    #[must_use]
    pub fn member_count(&self, node: &NodeId) -> Option<usize> {
        self.engines.get(node).map(|e| e.members().count())
    }

    /// Whether `observer` currently considers `node` a live member (present and
    /// not `Dead`), or `None` if `observer` is not in the simulation.
    #[must_use]
    pub fn is_member(&self, observer: &NodeId, node: &NodeId) -> Option<bool> {
        self.engines
            .get(observer)
            .map(|e| e.members().any(|n| n == node))
    }

    /// The live members (not `Dead`) that `observer` currently sees, or `None`
    /// if `observer` is not in the simulation.
    #[must_use]
    pub fn members_of(&self, observer: &NodeId) -> Option<BTreeSet<NodeId>> {
        self.engines
            .get(observer)
            .map(|e| e.members().cloned().collect())
    }

    /// The status `observer` holds for `node`, if any (including `Dead`).
    /// `None` too if `observer` is not in the simulation.
    #[must_use]
    pub fn status_of(&self, observer: &NodeId, node: &NodeId) -> Option<Status> {
        self.engines
            .get(observer)
            .and_then(|e| e.member_status(node))
    }

    /// The status `observer` holds for `node` **and the virtual instant it has
    /// held it continuously since** — the fencing-verdict roster, readable in
    /// exact virtual time.
    ///
    /// `None` once the observer has reaped the member's tombstone
    /// (`2×dead_timeout` after death), if it never knew the node, or if
    /// `observer` is not in the simulation.
    #[must_use]
    pub fn status_since_of(&self, observer: &NodeId, node: &NodeId) -> Option<(Status, Time)> {
        self.engines
            .get(observer)
            .and_then(|e| e.member_status_since(node))
    }
}
