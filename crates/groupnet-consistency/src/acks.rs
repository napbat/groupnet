//! Applied-watermark acknowledgements: the write-side half of strong cache
//! coherence.
//!
//! Each node publishes, as **one** gossiped entry, the highest [`WriteToken`]
//! it has *applied* from every writer it follows ([`AckLedger`]). A writer
//! that must not respond until its write is invisible-stale anywhere waits
//! for those watermarks, choosing *whom* it waits on explicitly:
//!
//! * [`applied_cluster_wide`] — every member this node currently believes
//!   `Alive` ([`AckBasis::AliveMembers`]);
//! * [`applied_by_selected`] — the `Alive` members a selector admits, e.g.
//!   those advertising [`CAP_ACKS`] ([`AckBasis::AliveSelected`]);
//! * [`applied_by_roster`] — a fixed roster the caller names, alive or not
//!   ([`AckBasis::FixedRoster`]): the only form with a known denominator.
//!
//! Every wait takes one [`AckWait`] — whose write, and how long to wait for
//! it — and resolves to an [`AckOutcome`] naming the members it selected,
//! which of them acknowledged and which are still missing, so an empty
//! selection or a selector that skipped a lagging advertiser is visible
//! rather than indistinguishable from a full acknowledgement. With eager
//! delta push the round trip is write → push → apply → ack-push ≈ two
//! network hops.
//!
//! Honesty box: membership is SWIM-derived, so "every Alive member" means
//! *every member this node currently believes alive*. A member that stops
//! acknowledging becomes `Suspect` within the probe timeouts and is then
//! excluded — bounding the wait — and a partitioned member must protect its
//! own readers (e.g. by refusing to serve cached state while its own view of
//! the membership is not fully alive). Under an *asymmetric* partition inside
//! the probe window the live-membership guarantee is bounded-time, not
//! absolute; a fixed-roster wait does not shrink, so it trades that bound for
//! a timeout, and an authoritative origin covers the absolute case.
//!
//! Traffic note: the ledger republishes its entry once per applied event
//! (same order of magnitude as the write feed itself); the entry is state,
//! so bursts coalesce exactly like the feed's ring.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use groupnet_core::{NodeId, Status};
use groupnet_runtime::{CommandRejected, Group};

use crate::publish::{Recorded, enqueue};
use crate::token::WriteToken;
use crate::wait::WaitDeadline;

/// The group entry key under which a node's applied watermarks are gossiped.
const ACK_KEY: &str = "~applied";

/// The capability name a node advertises (via
/// [`Group::advertise_capabilities`]) to declare that it runs an
/// [`AckLedger`] — i.e. that waiting on it is not waiting on a timeout.
///
/// Pair it with [`applied_by_selected`] to scope a wait to the nodes that opted
/// in, in a deployment where only some of them run a ledger:
///
/// ```no_run
/// # use std::time::Duration;
/// # use groupnet_consistency::{
/// #     AckOutcome, AckWait, CAP_ACKS, Group, WriteToken, applied_by_selected,
/// # };
/// # use groupnet_core::NodeId;
/// # async fn demo(group: &Group, me: &NodeId, token: WriteToken) -> AckOutcome {
/// group.advertise_capabilities([CAP_ACKS]).ok();
/// let wait = AckWait {
///     writer: me,
///     token,
///     timeout: Duration::from_secs(1),
/// };
/// applied_by_selected(group, wait, |peer| {
///     group.node_has_capability(peer, CAP_ACKS)
/// })
/// .await
/// # }
/// ```
pub const CAP_ACKS: &str = "acks";

/// How often an ack wait re-examines the gossiped ledgers.
const POLL_INTERVAL: Duration = Duration::from_millis(2);

/// Publisher half: this node's applied watermarks, one token per writer,
/// republished into the group whenever one advances.
pub struct AckLedger {
    group: Group,
    applied: Mutex<HashMap<NodeId, WriteToken>>,
}

impl std::fmt::Debug for AckLedger {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AckLedger")
            .field("group", &self.group.id())
            .finish_non_exhaustive()
    }
}

impl AckLedger {
    /// A ledger publishing into `group`.
    #[must_use]
    pub fn new(group: Group) -> Self {
        Self {
            group,
            applied: Mutex::new(HashMap::new()),
        }
    }

    /// Records that `writer`'s feed has been applied through `token` and, when
    /// that advances the watermark, enqueues the whole ledger for gossip
    /// (monotonic: lower or equal tokens change nothing). Call from the apply
    /// loop, after the application actually happened — typically right next
    /// to `Frontier::advance`.
    ///
    /// # Errors
    /// [`CommandRejected`] when the watermark **advanced locally** but the
    /// group actor's bounded inbox refused every enqueue attempt (or the actor
    /// has shut down). The advance is kept; the next advancing `record`
    /// re-carries it (the ledger is state, not a log), or call
    /// [`republish`](Self::republish) to retry without one.
    pub async fn record(
        &self,
        writer: &NodeId,
        token: WriteToken,
    ) -> Result<Recorded, CommandRejected> {
        let encoded = {
            let mut applied = self.lock();
            let entry = applied
                .entry(writer.clone())
                .or_insert(WriteToken { epoch: 0, seq: 0 });
            if *entry >= token {
                return Ok(Recorded::Covered);
            }
            *entry = token;
            encode(&applied)
        };
        self.publish(&encoded).await?;
        Ok(Recorded::Advanced)
    }

    /// Enqueues the current ledger for gossip without recording anything —
    /// the retry path after [`record`](Self::record) reported a rejected
    /// enqueue, and harmless at any other time.
    ///
    /// # Errors
    /// [`CommandRejected`] when the group actor's bounded inbox refused every
    /// enqueue attempt, or the actor has shut down.
    pub async fn republish(&self) -> Result<(), CommandRejected> {
        let encoded = encode(&self.lock());
        self.publish(&encoded).await
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<NodeId, WriteToken>> {
        self.applied
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn publish(&self, encoded: &[u8]) -> Result<(), CommandRejected> {
        enqueue(|| self.group.set_entry(ACK_KEY, encoded.to_vec(), None)).await
    }
}

/// The highest token of `writer`'s feed that `member` advertises having
/// applied, as gossip currently shows it (`None`: no ledger, or no entry for
/// this writer yet).
#[must_use]
pub fn applied_by(group: &Group, member: &NodeId, writer: &NodeId) -> Option<WriteToken> {
    let bytes = group.node_entry(member, ACK_KEY)?;
    decode_one(&bytes, writer.as_str())
}

/// One acknowledgement wait: `writer`'s feed applied through `token`, waited
/// for at most `timeout`.
///
/// A `timeout` longer than the clock can represent waits until the outcome
/// is complete; `Duration::ZERO` samples the gossiped ledgers once.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AckWait<'a> {
    /// The writer whose feed the acknowledgements cover; never waited on
    /// itself.
    pub writer: &'a NodeId,
    /// The token every selected member must have applied.
    pub token: WriteToken,
    /// How long to wait before reporting the members still missing.
    pub timeout: Duration,
}

/// How an ack wait chose the members it waited on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckBasis {
    /// Every member this node believes `Alive`, re-evaluated on every poll
    /// ([`applied_cluster_wide`]). A member that dies mid-wait drops out.
    AliveMembers,
    /// The `Alive` members a caller's selector admits, re-evaluated on every
    /// poll ([`applied_by_selected`]). Alive members the selector refused are
    /// reported as [`AckOutcome::skipped`].
    AliveSelected,
    /// A fixed roster the caller named ([`applied_by_roster`]). Liveness never
    /// shrinks it: a dead roster member stays missing.
    FixedRoster,
}

/// How an ack wait ended: whom it waited on, and who answered.
///
/// The lists describe the wait's **final** evaluation, in node-id order, and
/// never include the writer itself. `selected` is exactly `acknowledged` plus
/// `missing`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AckOutcome {
    /// How `selected` was chosen.
    pub basis: AckBasis,
    /// The members the wait required an acknowledgement from.
    pub selected: Vec<NodeId>,
    /// The selected members that advertise having applied the token.
    pub acknowledged: Vec<NodeId>,
    /// The selected members that did not, by the deadline.
    pub missing: Vec<NodeId>,
    /// Alive members the selector refused ([`AckBasis::AliveSelected`] only;
    /// empty otherwise). A participant whose capability advertisement has not
    /// converged here lands in this list rather than in `missing` — the
    /// advertisement-lag hazard [`applied_by_selected`] documents, made
    /// visible.
    pub skipped: Vec<NodeId>,
}

impl AckOutcome {
    /// Whether every selected member acknowledged. Vacuously true for an
    /// empty selection — check [`is_vacuous`](Self::is_vacuous) (and
    /// `skipped`) when the guarantee needs somebody to have answered.
    #[must_use]
    pub fn is_complete(&self) -> bool {
        self.missing.is_empty()
    }

    /// Whether the selection was empty: the wait resolved because there was
    /// nobody to wait on, not because anybody acknowledged.
    #[must_use]
    pub fn is_vacuous(&self) -> bool {
        self.selected.is_empty()
    }

    fn empty(basis: AckBasis) -> Self {
        Self {
            basis,
            selected: Vec::new(),
            acknowledged: Vec::new(),
            missing: Vec::new(),
            skipped: Vec::new(),
        }
    }

    fn count(&mut self, group: &Group, member: NodeId, writer: &NodeId, token: WriteToken) {
        if applied_by(group, &member, writer).is_some_and(|applied| applied >= token) {
            self.acknowledged.push(member.clone());
        } else {
            self.missing.push(member.clone());
        }
        self.selected.push(member);
    }
}

/// Waits (bounded by `wait.timeout`) until every member this node currently
/// believes `Alive`, other than `wait.writer` itself, advertises having
/// applied the writer's feed through `wait.token`
/// ([`AckBasis::AliveMembers`]).
///
/// This is live-membership unanimity, not a fixed denominator: members that
/// die mid-wait drop out as SWIM marks them non-alive, and a single-member
/// group resolves vacuously. Use [`applied_by_roster`] when the set of
/// members that must acknowledge is known in advance; in a deployment where
/// some peers do not run an [`AckLedger`] at all, prefer
/// [`applied_by_selected`] over eating a timeout per non-participant.
pub async fn applied_cluster_wide(group: &Group, wait: AckWait<'_>) -> AckOutcome {
    poll(wait.timeout, || {
        alive_outcome(group, wait, AckBasis::AliveMembers, |_| true)
    })
    .await
}

/// [`applied_cluster_wide`], scoped ([`AckBasis::AliveSelected`]): waits
/// (bounded by `wait.timeout`) only on members that are currently `Alive`,
/// are not `wait.writer` itself, **and** pass `include`.
///
/// The selection is re-evaluated on every poll, not snapshotted, so it tracks
/// both liveness churn and late-arriving advertisements: a peer that dies
/// mid-wait drops out, and a peer whose capability advertisement converges
/// mid-wait is picked up. An empty selection resolves immediately with an
/// [`AckOutcome::is_vacuous`] outcome.
///
/// # The advertisement-lag footgun
///
/// A capability selector such as `|peer| group.node_has_capability(peer,
/// CAP_ACKS)` is only as good as the advertisements that have converged. A
/// peer that *does* run an [`AckLedger`] but whose [`CAP_ACKS`] advertisement
/// has not reached this node yet — it just opted in, or just joined — is
/// **refused by the selector**, so the wait can resolve before that peer has
/// applied the write. The outcome reports it in [`AckOutcome::skipped`], so a
/// caller can refuse to treat that resolution as the guarantee it wanted.
///
/// The safe order is: advertise on every participant first, confirm the
/// advertisements have landed (e.g. every member of interest appears in
/// [`Group::members_with_capability`]), and only then narrow writers onto a
/// capability selector — or wait on a fixed roster ([`applied_by_roster`]),
/// which cannot lag.
pub async fn applied_by_selected(
    group: &Group,
    wait: AckWait<'_>,
    include: impl Fn(&NodeId) -> bool,
) -> AckOutcome {
    poll(wait.timeout, || {
        alive_outcome(group, wait, AckBasis::AliveSelected, &include)
    })
    .await
}

/// Waits (bounded by `wait.timeout`) until every member of `roster` other
/// than `wait.writer` itself advertises having applied the writer's feed
/// through `wait.token` ([`AckBasis::FixedRoster`]).
///
/// The denominator is fixed: membership churn neither adds to nor removes from
/// it, so a roster member that is dead, partitioned or not running an
/// [`AckLedger`] holds the wait until the timeout and is reported in
/// [`AckOutcome::missing`]. Duplicates in `roster` count once.
pub async fn applied_by_roster(
    group: &Group,
    wait: AckWait<'_>,
    roster: impl IntoIterator<Item = NodeId>,
) -> AckOutcome {
    let mut members: Vec<NodeId> = roster.into_iter().filter(|m| m != wait.writer).collect();
    members.sort_unstable();
    members.dedup();
    poll(wait.timeout, || {
        let mut outcome = AckOutcome::empty(AckBasis::FixedRoster);
        for member in &members {
            outcome.count(group, member.clone(), wait.writer, wait.token);
        }
        outcome
    })
    .await
}

/// One evaluation of a live-membership selection.
fn alive_outcome(
    group: &Group,
    wait: AckWait<'_>,
    basis: AckBasis,
    include: impl Fn(&NodeId) -> bool,
) -> AckOutcome {
    let mut statuses = group.statuses();
    statuses.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    let mut outcome = AckOutcome::empty(basis);
    for (member, status) in statuses {
        if member == *wait.writer || status != Status::Alive {
            continue;
        }
        if include(&member) {
            outcome.count(group, member, wait.writer, wait.token);
        } else {
            outcome.skipped.push(member);
        }
    }
    outcome
}

/// Re-evaluates until the outcome is complete or the deadline passes.
async fn poll(timeout: Duration, mut evaluate: impl FnMut() -> AckOutcome) -> AckOutcome {
    let deadline = WaitDeadline::after(timeout);
    loop {
        let outcome = evaluate();
        if outcome.is_complete() || deadline.passed() {
            return outcome;
        }
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// `(writer_len, writer_bytes, epoch, seq)*`, little-endian — dep-free.
fn encode(applied: &HashMap<NodeId, WriteToken>) -> Vec<u8> {
    let mut out = Vec::with_capacity(applied.len() * 32);
    for (writer, token) in applied {
        let name = writer.as_str().as_bytes();
        out.extend_from_slice(&u32::try_from(name.len()).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&token.epoch.to_le_bytes());
        out.extend_from_slice(&token.seq.to_le_bytes());
    }
    out
}

fn decode_one(bytes: &[u8], writer: &str) -> Option<WriteToken> {
    let mut offset = 0usize;
    while offset < bytes.len() {
        let len = usize::try_from(u32::from_le_bytes(
            bytes.get(offset..offset + 4)?.try_into().ok()?,
        ))
        .ok()?;
        offset += 4;
        let name = bytes.get(offset..offset + len)?;
        offset += len;
        let epoch = u64::from_le_bytes(bytes.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        let seq = u64::from_le_bytes(bytes.get(offset..offset + 8)?.try_into().ok()?);
        offset += 8;
        if name == writer.as_bytes() {
            return Some(WriteToken { epoch, seq });
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::{AckBasis, AckOutcome, decode_one, encode};
    use crate::token::WriteToken;
    use groupnet_core::NodeId;

    #[test]
    fn ledger_codec_round_trips_and_rejects_truncation() {
        let mut applied = HashMap::new();
        applied.insert(NodeId::new("node-a"), WriteToken { epoch: 3, seq: 41 });
        applied.insert(NodeId::new("b"), WriteToken { epoch: 1, seq: 7 });
        let bytes = encode(&applied);
        assert_eq!(
            decode_one(&bytes, "node-a"),
            Some(WriteToken { epoch: 3, seq: 41 })
        );
        assert_eq!(
            decode_one(&bytes, "b"),
            Some(WriteToken { epoch: 1, seq: 7 })
        );
        assert_eq!(decode_one(&bytes, "nobody"), None);
        for cut in 1..bytes.len() {
            let _ = decode_one(&bytes[..cut], "node-a"); // must never panic
        }
    }

    #[test]
    fn an_empty_selection_is_complete_but_vacuous() {
        let empty = AckOutcome::empty(AckBasis::AliveSelected);
        assert!(empty.is_complete());
        assert!(empty.is_vacuous());

        let answered = AckOutcome {
            selected: vec![NodeId::new("b")],
            acknowledged: vec![NodeId::new("b")],
            ..AckOutcome::empty(AckBasis::FixedRoster)
        };
        assert!(answered.is_complete());
        assert!(!answered.is_vacuous());

        let short = AckOutcome {
            selected: vec![NodeId::new("b")],
            missing: vec![NodeId::new("b")],
            ..AckOutcome::empty(AckBasis::AliveMembers)
        };
        assert!(!short.is_complete());
        assert!(!short.is_vacuous());
    }
}
