//! Applied-watermark acknowledgements: the write-side half of strong cache
//! coherence.
//!
//! Each node publishes, as **one** gossiped entry, the highest [`WriteToken`]
//! it has *applied* from every writer it follows ([`AckLedger`]). A writer
//! that must not respond until its write is invisible-stale anywhere calls
//! [`applied_cluster_wide`]: it resolves once every currently-`Alive` member
//! advertises having applied the token. With eager delta push the round trip
//! is write → push → apply → ack-push ≈ two network hops.
//!
//! Honesty box: membership is SWIM-derived, so "every Alive member" means
//! *every member this node currently believes alive*. A member that stops
//! acknowledging becomes `Suspect` within the probe timeouts and is then
//! excluded — bounding the wait — and a partitioned member must protect its
//! own readers (e.g. by refusing to serve cached state while its own view of
//! the membership is not fully alive). Under an *asymmetric* partition inside
//! the probe window the guarantee is bounded-time, not absolute; pair with an
//! authoritative origin for the absolute case.
//!
//! Traffic note: the ledger republishes its entry once per applied event
//! (same order of magnitude as the write feed itself); the entry is state,
//! so bursts coalesce exactly like the feed's ring.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_runtime::{Group, Status};

use crate::token::WriteToken;

/// The group entry key under which a node's applied watermarks are gossiped.
const ACK_KEY: &str = "~applied";

/// The capability name a node advertises (via
/// [`Group::advertise_capabilities`]) to declare that it runs an
/// [`AckLedger`] — i.e. that waiting on it is not waiting on a timeout.
///
/// Pair it with [`applied_by_selected`] to scope a wait to the participating
/// half of a mixed deployment:
///
/// ```no_run
/// # use std::time::Duration;
/// # use groupnet_consistency::{CAP_ACKS, WriteToken, applied_by_selected};
/// # use groupnet_core::NodeId;
/// # use groupnet_runtime::Group;
/// # async fn demo(group: &Group, me: &NodeId, token: WriteToken) -> bool {
/// group.advertise_capabilities([CAP_ACKS]).ok();
/// applied_by_selected(
///     group,
///     me,
///     token,
///     |peer| group.node_has_capability(peer, CAP_ACKS),
///     Duration::from_secs(1),
/// )
/// .await
/// # }
/// ```
pub const CAP_ACKS: &str = "acks";

/// Attempts before giving up on republishing under inbox backpressure (the
/// next `record` re-carries the full map; the ledger is state, not a log).
const PUBLISH_RETRIES: usize = 8;

/// How often [`applied_cluster_wide`] re-examines the gossiped ledgers.
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

    /// Records that `writer`'s feed has been applied through `token` and
    /// republishes the ledger (monotonic: lower tokens are ignored). Call
    /// from the apply loop, after the application actually happened —
    /// typically right next to `Frontier::advance`.
    pub async fn record(&self, writer: &NodeId, token: WriteToken) {
        let encoded = {
            let mut applied = self
                .applied
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            let entry = applied
                .entry(writer.clone())
                .or_insert(WriteToken { epoch: 0, seq: 0 });
            if *entry >= token {
                return;
            }
            *entry = token;
            encode(&applied)
        };
        for _ in 0..PUBLISH_RETRIES {
            if self.group.set_entry(ACK_KEY, encoded.clone(), None).is_ok() {
                return;
            }
            tokio::task::yield_now().await;
        }
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

/// Waits (bounded by `timeout`) until every currently-`Alive` member other
/// than `writer` itself advertises having applied `writer`'s feed through
/// `token`. Members that die mid-wait drop out of the wait as SWIM marks
/// them non-alive. Returns whether every such member acknowledged in time.
///
/// This is [`applied_by_selected`] with a selector that admits everyone; in a
/// deployment where some peers do not run an [`AckLedger`] at all, prefer the
/// selected form over eating a timeout per non-participant.
pub async fn applied_cluster_wide(
    group: &Group,
    writer: &NodeId,
    token: WriteToken,
    timeout: Duration,
) -> bool {
    applied_by_selected(group, writer, token, |_| true, timeout).await
}

/// [`applied_cluster_wide`], scoped: waits (bounded by `timeout`) only on
/// members that are currently `Alive`, are not `writer` itself, **and** pass
/// `include`. Returns whether every such member acknowledged in time.
///
/// The selection is re-evaluated on every poll, not snapshotted, so it tracks
/// both liveness churn and late-arriving advertisements: a peer that dies
/// mid-wait drops out, and a peer whose capability advertisement converges
/// mid-wait is picked up. An empty selection resolves `true` immediately —
/// there is nobody to wait on, and that is a real (if weak) answer, not a
/// failure.
///
/// # The rolling-upgrade footgun
///
/// A capability selector such as `|peer| group.node_has_capability(peer,
/// CAP_ACKS)` is only as good as the advertisements that have converged. A
/// peer that *does* run an [`AckLedger`] but has not advertised
/// [`CAP_ACKS`] yet — an older build, or a newer one whose advertisement is
/// still in flight — is **invisible to the selector and silently skipped**,
/// so the wait can resolve before that peer has applied the write. The
/// guarantee quietly weakens instead of failing loudly.
///
/// So the safe rollout order is: advertise fleet-wide first, confirm the
/// advertisements have landed (e.g. every member of interest appears in
/// [`Group::members_with_capability`]), and only then narrow writers onto a
/// capability selector. Selectors built from a source that cannot lag —
/// a static roster, an explicit deny-list — do not have this hazard.
pub async fn applied_by_selected(
    group: &Group,
    writer: &NodeId,
    token: WriteToken,
    include: impl Fn(&NodeId) -> bool,
    timeout: Duration,
) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        let waiting_on = group.statuses().into_iter().any(|(member, status)| {
            member != *writer
                && status == Status::Alive
                && include(&member)
                && applied_by(group, &member, writer).is_none_or(|t| t < token)
        });
        if !waiting_on {
            return true;
        }
        if tokio::time::Instant::now() >= deadline {
            return false;
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

    use super::{decode_one, encode};
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
}
