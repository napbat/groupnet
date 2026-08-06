//! The epoch-stamped commit ledger: what a voter publishes, and the vocabulary
//! the two cores read.
//!
//! # One entry, one stamp, many watermarks
//!
//! Every participant publishes a single gossiped entry, `~hosted:applied`
//! (`~hosted:applied:<name>` for a named write path), carrying **the leadership
//! epoch it has adopted** and a count, followed by the highest [`WriteToken`] it
//! has *applied* from each writer it follows:
//!
//! ```text
//! (lead_epoch: u64 LE) (records: u32 LE) (writer_len: u32, writer: utf-8, token_epoch: u64, token_seq: u64)*
//! ```
//!
//! The watermark half is the ack tier's `AckLedger`'s, byte for byte
//! and monotone for the same reason. The `lead_epoch` prefix is what makes this
//! a *different* ledger rather than a scoped reuse of `~applied`, and it is
//! load-bearing twice over:
//!
//! * **Reading it forward** (`lead_epoch == token.epoch`) is the commit rule's
//!   **view-stamp fence**: a voter that has adopted a higher epoch stops
//!   counting, so a round opened at `e` can never close once a majority has
//!   moved to `e′ > e`. Without it, an ack that lands minutes after a migration
//!   still resolves, and the write it acknowledges is lost.
//! * **Reading it backward** (`lead_epoch ≥ e′`) is the recovery rule's
//!   freshness test: it proves the reading was published *after* the new epoch
//!   was adopted, and therefore after any reading a commit at `e` could have
//!   counted. Without it a recovering host can satisfy a majority out of
//!   pre-migration views and undershoot its target.
//!
//! Both directions of that argument are in the M4 as-built subsection of
//! `docs/consistency-modes.md`, which is the contract of record.
//!
//! # The deployment contract
//!
//! **Every voter runs the follower loop**: apply the host's feed, then
//! [`CommitLedger::record`]; on an epoch change, [`CommitLedger::refresh`]. A
//! voter that votes but never publishes is invisible to both rules, and the
//! tier fails *closed* around it — commits time out and a new host stalls in
//! recovery. Loud, and never a lost write.
//!
//! # Codec honesty
//!
//! Decoding is **all-or-nothing**: anything that does not parse cleanly yields
//! `None`, never a partial reading. That direction is forced. Under-reporting a
//! watermark is conservative for the commit rule (fewer acks, a slower commit)
//! but *dangerous* for the recovery rule, where a lower maximum is a lower
//! target — so a half-read map must never masquerade as a smaller honest one.
//!
//! The record **count** is what closes that hole, and it is the whole reason
//! this layout carries one where the ack tier's does not. Without it, a
//! truncation landing exactly on a record boundary decodes to a proper *subset*
//! — well-formed, short, and silently under-reporting a recovery target. With
//! it, [`decode_ledger`] knows how many records it is owed and how many bytes
//! they must occupy: a short read runs out of input and a long one leaves a
//! tail, and both answer `None`. A count that names more distinct writers than
//! the records actually carry (a duplicated writer id) is refused for the same
//! reason — a map shorter than its own header is not a map this tier will read.
//!
//! That is defence against *hostile* input rather than against the wire: an
//! entry value crosses the fabric whole or not at all, framed and
//! length-checked by the core's codec long before it reaches here. Against a
//! peer that fabricates ledger bytes wholesale this tier's guarantee is void by
//! construction anyway — but a truncation is the one hostile input that used to
//! produce a *plausible* reading, and that is worth a `u32`.
//!
//! # The record map is a shared shape
//!
//! Everything after the stamp — the counted `(writer, token)` map — is factored
//! into [`encode_records`] / [`decode_records`], because the handoff helper's
//! wire protocol (feature `handoff`) embeds the *same* map inside its request,
//! offer and refusal frames. One encoder, one decoder, one count check: a
//! divergence between the two layouts would be a silent mis-parse of a recovery
//! target, which is the one class of bug this codec exists to make impossible.
//! [`encode_ledger`] and [`decode_ledger`] are the stamp plus a call.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

use groupnet_core::NodeId;
use groupnet_runtime::Group;

use crate::token::WriteToken;

/// The group entry key prefix the tier occupies (`~`-prefixed like the
/// runtime's other reserved entries: `~caps`, `~applied`, `~writes`,
/// `~lease`).
const LEDGER_KEY: &str = "~hosted:applied";

/// Attempts before giving up on republishing under inbox backpressure (the next
/// `record` re-carries the full map; the ledger is state, not a log).
const PUBLISH_RETRIES: usize = 8;

/// One publisher's applied watermark per writer: the highest [`WriteToken`] of
/// that writer's feed it has applied.
///
/// Ordered by writer id, so an unchanged map re-encodes byte-identically and a
/// simulation replays deterministically — the same choice the lease tier's
/// `GrantMap` makes.
pub type Watermarks = BTreeMap<NodeId, WriteToken>;

/// One member's commit-ledger reading, as the reader's own gossip view shows
/// it: the leadership epoch that member had adopted when it published, and what
/// it had applied at that point.
///
/// This is the `(lead_epoch, watermarks)` pair both cores are fed. Absence of a
/// reading — the member publishes no ledger at all, or its bytes do not
/// decode — is `None` at the [`LedgerView`] level and counts for nothing under
/// either rule.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Reading {
    /// The leadership epoch the publisher had adopted. Monotone per publisher:
    /// [`CommitLedger`] never stamps below what it has already stamped, so this
    /// number orders one member's own publications.
    pub lead_epoch: u64,
    /// The publisher's applied watermark per writer.
    pub applied: Watermarks,
}

/// One member of a commit or recovery decision, as the deciding node's own view
/// of the group shows it.
///
/// # The view must be the whole roster
///
/// Both cores derive their majority threshold from the **length of the view
/// they are handed**. A caller that omits silent voters shrinks the denominator
/// and manufactures majorities out of a minority — so a voter with no ledger
/// belongs in the view with `reading: None`, not out of it. Duplicates are
/// counted verbatim; pass a canonicalized roster
/// ([`VoterRoster`](groupnet_core::VoterRoster) already is one).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LedgerView {
    /// The member this reading belongs to.
    pub member: NodeId,
    /// Whether membership currently believes it alive.
    ///
    /// Read **only** by [`Commit::AllApplied`](super::Commit::AllApplied). The
    /// roster level is deliberately liveness-blind: a static roster is the
    /// denominator precisely so a rumour cannot move it.
    pub alive: bool,
    /// Its reading, or `None` when it publishes no decodable ledger.
    pub reading: Option<Reading>,
}

/// The entry key a commit ledger occupies: `~hosted:applied` for the default
/// write path, `~hosted:applied:<name>` for a named one.
///
/// Names must not contain `:` — the layout's own separator, so such a name
/// would merge with a neighbouring path's key space.
#[must_use]
pub fn ledger_entry_key(name: &str) -> String {
    if name.is_empty() {
        LEDGER_KEY.to_owned()
    } else {
        format!("{LEDGER_KEY}:{name}")
    }
}

/// `(lead_epoch)(records)(writer_len, writer, token_epoch, token_seq)*`,
/// little-endian — dep-free, and the same length-prefixed shape the ack ledger,
/// the grant map and the capability set use, plus the record count that makes a
/// truncation undecodable rather than merely short.
///
/// Everything after the stamp is the crate-internal `encode_records`, shared
/// verbatim with the handoff protocol's frames. The saturation posture is
/// documented there: a
/// prefix that cannot be counted becomes one [`decode_ledger`] refuses —
/// `None`, "this member publishes nothing", never something wrong.
#[must_use]
pub fn encode_ledger(lead_epoch: u64, applied: &Watermarks) -> Vec<u8> {
    let mut out = Vec::with_capacity(12 + applied.len() * 32);
    out.extend_from_slice(&lead_epoch.to_le_bytes());
    encode_records(applied, &mut out);
    out
}

/// The counted record map alone — `(records: u32)(writer_len: u32, writer,
/// token_epoch: u64, token_seq: u64)*`, little-endian — appended to `out`.
///
/// The shared half of this tier's two codecs: the ledger entry is a stamp
/// followed by one of these, and the handoff protocol embeds one inside three of
/// its five frames. Appending rather than returning is what lets a frame carry
/// a map in the middle of a body.
///
/// A writer id is assumed to fit a `u32` length, and the map to fit a `u32`
/// count: debug assertions rather than release panics, because both cases are
/// absurd (an id over 4 GiB, a roster over four billion writers) and both
/// saturate into a prefix [`decode_records`] then refuses — `None`, never
/// something wrong.
pub(crate) fn encode_records(applied: &Watermarks, out: &mut Vec<u8>) {
    debug_assert!(
        u32::try_from(applied.len()).is_ok(),
        "more watermarks than a u32 can count"
    );
    out.extend_from_slice(
        &u32::try_from(applied.len())
            .unwrap_or(u32::MAX)
            .to_le_bytes(),
    );
    for (writer, token) in applied {
        let name = writer.as_str().as_bytes();
        debug_assert!(
            u32::try_from(name.len()).is_ok(),
            "node id is longer than u32::MAX bytes"
        );
        out.extend_from_slice(&u32::try_from(name.len()).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&token.epoch.to_le_bytes());
        out.extend_from_slice(&token.seq.to_le_bytes());
    }
}

/// The record map beginning at `*offset`, advancing `*offset` past it — or
/// `None` the moment anything does not parse, leaving `*offset` untouched.
///
/// Two of the count's three checks live here: every record must be present, and
/// they must name that many **distinct** writers (a duplicated id yields a map
/// shorter than its own header, which is not a map this tier will read). The
/// third — nothing left over — belongs to the caller, because a map embedded in
/// a frame body legitimately has bytes behind it and an entry value does not.
///
/// Never panics on hostile or truncated input.
#[must_use]
pub(crate) fn decode_records(bytes: &[u8], offset: &mut usize) -> Option<Watermarks> {
    let start = *offset;
    let records = usize::try_from(u32::from_le_bytes(
        bytes.get(start..start.checked_add(4)?)?.try_into().ok()?,
    ))
    .ok()?;
    let mut at = start + 4;
    let mut applied = Watermarks::new();
    for _ in 0..records {
        let len = usize::try_from(u32::from_le_bytes(
            bytes.get(at..at.checked_add(4)?)?.try_into().ok()?,
        ))
        .ok()?;
        at += 4;
        let end = at.checked_add(len)?;
        let writer = std::str::from_utf8(bytes.get(at..end)?).ok()?;
        at = end;
        let epoch = u64::from_le_bytes(bytes.get(at..at.checked_add(8)?)?.try_into().ok()?);
        at += 8;
        let seq = u64::from_le_bytes(bytes.get(at..at.checked_add(8)?)?.try_into().ok()?);
        at += 8;
        applied.insert(NodeId::new(writer), WriteToken { epoch, seq });
    }
    // Nothing collapsed: a map shorter than its own header is a duplicated
    // writer id, and it under-reports whatever the reader is about to compute.
    if applied.len() != records {
        return None;
    }
    *offset = at;
    Some(applied)
}

/// The whole reading in `bytes`, or `None` the moment anything does not parse.
///
/// All-or-nothing by design — see the module's codec honesty note. Twelve bytes
/// exactly (a stamp and a zero count) is a legitimate reading: a member that has
/// adopted an epoch and applied nothing yet, which is what a freshly booted
/// voter publishes and what a recovery majority is allowed to be made of.
///
/// The count is checked three ways, and every failure is `None`: the records
/// must all be present, they must name that many *distinct* writers (both the
/// shared `decode_records`'s), and they must end exactly at the last byte (this
/// function's, because only an *entry* is allowed no tail). Never panics on
/// hostile or truncated input.
#[must_use]
pub fn decode_ledger(bytes: &[u8]) -> Option<Reading> {
    let lead_epoch = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
    let mut offset = 8usize;
    let applied = decode_records(bytes, &mut offset)?;
    // Nothing left over: a tail is a re-framing attempt, not a reading. (The
    // other two checks are [`decode_records`]'s own.)
    (offset == bytes.len()).then_some(Reading {
        lead_epoch,
        applied,
    })
}

/// `member`'s commit-ledger reading for the default write path, as gossip
/// currently shows it (`None`: no ledger, or bytes that do not decode).
#[must_use]
pub fn commit_reading(group: &Group, member: &NodeId) -> Option<Reading> {
    commit_reading_named("", group, member)
}

/// [`commit_reading`] for a named write path (see [`CommitLedger::named`]).
#[must_use]
pub fn commit_reading_named(name: &str, group: &Group, member: &NodeId) -> Option<Reading> {
    decode_ledger(&group.node_entry(member, &ledger_entry_key(name))?)
}

/// The leadership epoch `member` had adopted, paired with the highest token of
/// `writer`'s feed it advertises having applied — the commit rule's whole input
/// for one voter and one write.
///
/// `None` when the member publishes no decodable ledger **or** names no
/// watermark for that writer; both count for nothing, and the caller must not
/// distinguish them. Compare the epoch to the write token's own epoch: an
/// **unequal** stamp does not count, in either direction. Lower is a stale view
/// and higher is the view-stamp fence — the voter has moved on, and a round at
/// the older epoch must never close again.
#[must_use]
pub fn commit_applied_by(
    group: &Group,
    member: &NodeId,
    writer: &NodeId,
) -> Option<(u64, WriteToken)> {
    let reading = commit_reading(group, member)?;
    let token = *reading.applied.get(writer)?;
    Some((reading.lead_epoch, token))
}

/// The published half of the ledger, and the monotone fold behind it.
#[derive(Debug)]
struct State {
    /// The highest leadership epoch this ledger has ever stamped.
    ///
    /// Enforced here rather than assumed of
    /// [`Group::leadership`](groupnet_runtime::Group::leadership): the whole
    /// intersection argument rests on "a voter's stamp is monotone per
    /// publisher", and a rule that load-bearing is cheaper to make structural
    /// than to document.
    stamp: u64,
    applied: Watermarks,
}

/// Raises `writer`'s watermark to `token` and the stamp to `lead_epoch`,
/// reporting whether either moved.
///
/// A stamp advance alone is enough to republish: a voter that has adopted a new
/// epoch but applied nothing new is exactly the member a recovering host needs
/// to hear a *fresh* reading from.
fn fold(state: &mut State, writer: &NodeId, token: WriteToken, lead_epoch: u64) -> bool {
    let stamped = lead_epoch > state.stamp;
    if stamped {
        state.stamp = lead_epoch;
    }
    let entry = state
        .applied
        .entry(writer.clone())
        .or_insert(WriteToken { epoch: 0, seq: 0 });
    let advanced = token > *entry;
    if advanced {
        *entry = token;
    }
    stamped || advanced
}

/// Publisher half: this node's epoch-stamped applied watermarks, republished
/// into the group whenever the stamp or a watermark advances.
///
/// One instance per write path. Drive it from the follower loop — apply the
/// event, then [`record`](Self::record) — and [`refresh`](Self::refresh) it when
/// the group's leadership epoch changes. A durable application seeds it from its
/// own store with [`with_recovered`](Self::with_recovered) so a restart rejoins
/// the majority instead of dropping out of it for a full catch-up.
pub struct CommitLedger {
    group: Group,
    key: String,
    state: Mutex<State>,
}

impl fmt::Debug for CommitLedger {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CommitLedger")
            .field("group", &self.group.id())
            .field("key", &self.key)
            .finish_non_exhaustive()
    }
}

impl CommitLedger {
    /// A ledger for the default write path, starting from nothing applied.
    #[must_use]
    pub fn new(group: Group) -> Self {
        Self::build("", group, Watermarks::new())
    }

    /// A ledger for a named write path — independent subsystems sharing one
    /// group must name their paths so they occupy distinct entries. `name` must
    /// not contain `:`.
    #[must_use]
    pub fn named(name: &str, group: Group) -> Self {
        Self::build(name, group, Watermarks::new())
    }

    /// A ledger for the default write path, seeded with what durable storage
    /// says this node has already applied.
    ///
    /// The stamp starts at zero regardless: a store records *what* was applied,
    /// never under which leadership epoch it was published, and inventing a
    /// stamp would be inventing the one piece of evidence both rules turn on.
    /// The first [`record`](Self::record) or [`refresh`](Self::refresh) stamps
    /// it from the group's current belief — so call `refresh` once after
    /// construction to put the recovered map on the wire.
    #[must_use]
    pub fn with_recovered(
        group: Group,
        applied: impl IntoIterator<Item = (NodeId, WriteToken)>,
    ) -> Self {
        Self::build("", group, applied.into_iter().collect())
    }

    /// [`with_recovered`](Self::with_recovered) for a named write path.
    #[must_use]
    pub fn named_with_recovered(
        name: &str,
        group: Group,
        applied: impl IntoIterator<Item = (NodeId, WriteToken)>,
    ) -> Self {
        Self::build(name, group, applied.into_iter().collect())
    }

    fn build(name: &str, group: Group, applied: Watermarks) -> Self {
        Self {
            group,
            key: ledger_entry_key(name),
            state: Mutex::new(State { stamp: 0, applied }),
        }
    }

    /// The highest leadership epoch this ledger has stamped so far.
    #[must_use]
    pub fn stamp(&self) -> u64 {
        self.lock().stamp
    }

    /// The entry key this ledger publishes under — `~hosted:applied`, or
    /// `~hosted:applied:<name>` for a named write path.
    ///
    /// A [`HostedWrites`](super::HostedWrites) reads its peers' ledgers under
    /// the key derived from *its own* name, so the two must agree; the committed
    /// constructors check this rather than leaving a silently-empty roster view
    /// behind a typo'd name.
    #[must_use]
    pub fn entry_key(&self) -> &str {
        &self.key
    }

    /// The highest token of `writer`'s feed this ledger records as applied.
    #[must_use]
    pub fn applied(&self, writer: &NodeId) -> Option<WriteToken> {
        self.lock().applied.get(writer).copied()
    }

    /// This node's whole applied map, as the recovery rule consumes it.
    ///
    /// [`CompletenessCore::step`](super::CompletenessCore::step) takes the
    /// recovering host's **own** watermarks beside the roster's readings, and
    /// this is where they come from: a host is also a follower, and its own
    /// ledger *is* its applied state. Snapshotted under the lock, so a target
    /// is never computed against a half-updated map.
    #[must_use]
    pub fn watermarks(&self) -> Watermarks {
        self.lock().applied.clone()
    }

    /// Records that `writer`'s feed has been applied through `token`, stamps the
    /// group's current leadership epoch, and republishes.
    ///
    /// Both folds are monotone: a lower token is ignored, and the stamp never
    /// regresses. Call this from the apply loop **after** the application
    /// actually happened — typically right next to `Frontier::advance`. A call
    /// that moves neither the stamp nor the watermark publishes nothing.
    pub async fn record(&self, writer: &NodeId, token: WriteToken) {
        let lead_epoch = self.group.leadership().epoch;
        let encoded = {
            let mut state = self.lock();
            if !fold(&mut state, writer, token, lead_epoch) {
                return;
            }
            encode_ledger(state.stamp, &state.applied)
        };
        self.publish(encoded).await;
    }

    /// Re-stamps the ledger with the group's current leadership epoch and
    /// republishes it. Watermarks are untouched.
    ///
    /// This is the other half of the deployment contract: a voter whose apply
    /// loop is quiet still has to tell a recovering host that its view is
    /// *fresh*. Call it whenever leadership changes — the epoch `Gap` a
    /// migration surfaces is exactly the signal — and it is harmless to call at
    /// any other time.
    pub async fn refresh(&self) {
        let lead_epoch = self.group.leadership().epoch;
        let encoded = {
            let mut state = self.lock();
            state.stamp = state.stamp.max(lead_epoch);
            encode_ledger(state.stamp, &state.applied)
        };
        self.publish(encoded).await;
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    async fn publish(&self, encoded: Vec<u8>) {
        for _ in 0..PUBLISH_RETRIES {
            if self
                .group
                .set_entry(self.key.clone(), encoded.clone(), None)
                .is_ok()
            {
                return;
            }
            // Inbox backpressure: yield and retry. The ledger is state, so the
            // next record re-carries the whole map.
            tokio::task::yield_now().await;
        }
    }
}

#[cfg(test)]
mod tests {
    use groupnet_core::NodeId;

    use super::{State, Watermarks, decode_ledger, encode_ledger, fold, ledger_entry_key};
    use crate::token::WriteToken;

    fn node(name: &str) -> NodeId {
        NodeId::new(name)
    }

    fn marks(pairs: &[(&str, u64, u64)]) -> Watermarks {
        pairs
            .iter()
            .map(|(writer, epoch, seq)| {
                (
                    node(writer),
                    WriteToken {
                        epoch: *epoch,
                        seq: *seq,
                    },
                )
            })
            .collect()
    }

    fn state(stamp: u64, applied: &[(&str, u64, u64)]) -> State {
        State {
            stamp,
            applied: marks(applied),
        }
    }

    #[test]
    fn write_paths_map_to_distinct_entries() {
        assert_eq!(ledger_entry_key(""), "~hosted:applied");
        assert_eq!(ledger_entry_key("docs"), "~hosted:applied:docs");
        assert_ne!(ledger_entry_key("a"), ledger_entry_key("b"));
        // Distinct from the ack tier's ledger: the two coexist on one node and
        // mean different things.
        assert_ne!(ledger_entry_key(""), "~applied");
    }

    #[test]
    fn readings_round_trip_stamp_and_watermarks() {
        let applied = marks(&[("host-a", 3, 41), ("b", 1, 7), ("ünïcøde-node", 9, 9)]);
        let bytes = encode_ledger(12, &applied);
        let reading = decode_ledger(&bytes).expect("well-formed");
        assert_eq!(reading.lead_epoch, 12);
        assert_eq!(reading.applied, applied);
        // Ordered by writer id, so an unchanged map re-encodes byte-identically.
        let same = marks(&[("ünïcøde-node", 9, 9), ("b", 1, 7), ("host-a", 3, 41)]);
        assert_eq!(encode_ledger(12, &same), bytes);
    }

    /// A hand-built header: a stamp and a record count that need not match what
    /// follows — the shapes a hostile publisher can author and `encode_ledger`
    /// cannot.
    fn header(lead_epoch: u64, records: u32) -> Vec<u8> {
        let mut out = lead_epoch.to_le_bytes().to_vec();
        out.extend_from_slice(&records.to_le_bytes());
        out
    }

    /// One `(writer, token)` record, as the layout spells it.
    fn record(writer: &str, epoch: u64, seq: u64) -> Vec<u8> {
        let mut out = u32::try_from(writer.len())
            .expect("short name")
            .to_le_bytes()[..]
            .to_vec();
        out.extend_from_slice(writer.as_bytes());
        out.extend_from_slice(&epoch.to_le_bytes());
        out.extend_from_slice(&seq.to_le_bytes());
        out
    }

    #[test]
    fn a_bare_stamp_is_a_legitimate_reading() {
        // A freshly booted voter that has adopted an epoch and applied nothing.
        // It must be able to join a recovery majority, so this is `Some`, not a
        // decode failure.
        let bytes = encode_ledger(7, &Watermarks::new());
        assert_eq!(bytes.len(), 12, "a stamp and a zero count");
        assert_eq!(bytes, header(7, 0));
        let reading = decode_ledger(&bytes).expect("a stamp alone decodes");
        assert_eq!(reading.lead_epoch, 7);
        assert!(reading.applied.is_empty());
    }

    #[test]
    fn nothing_shorter_than_a_stamped_header_decodes() {
        for cut in 0..12 {
            assert_eq!(decode_ledger(&[0u8; 12][..cut]), None, "short at {cut}");
        }
    }

    #[test]
    fn unparseable_ledger_bytes_decode_to_nothing() {
        // A length that runs off the end.
        let mut runaway = header(1, 1);
        runaway.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0x01]);
        assert_eq!(decode_ledger(&runaway), None);
        // A well-framed writer id that is not utf-8.
        let mut bad_utf8 = header(1, 1);
        bad_utf8.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, 0xff]);
        assert_eq!(decode_ledger(&bad_utf8), None);
        // A name with no token behind it.
        let mut headless = header(1, 1);
        headless.extend_from_slice(&[0x01, 0x00, 0x00, 0x00, b'a']);
        assert_eq!(decode_ledger(&headless), None);
        // A valid record followed by trailing garbage: all-or-nothing.
        let mut mixed = encode_ledger(1, &marks(&[("host-a", 1, 1)]));
        mixed.extend_from_slice(&[0x09, 0x00, 0x00, 0x00, 0x01]);
        assert_eq!(decode_ledger(&mixed), None);
    }

    #[test]
    fn a_count_that_disagrees_with_the_records_decodes_to_nothing() {
        // Under-counted: two records claimed, three present — a tail is left.
        let mut under = header(4, 2);
        for (writer, epoch, seq) in [("a", 1, 1), ("b", 1, 2), ("c", 1, 3)] {
            under.extend_from_slice(&record(writer, epoch, seq));
        }
        assert_eq!(decode_ledger(&under), None);
        // Over-counted: three claimed, two present — the third runs off the end.
        let mut over = header(4, 3);
        for (writer, epoch, seq) in [("a", 1, 1), ("b", 1, 2)] {
            over.extend_from_slice(&record(writer, epoch, seq));
        }
        assert_eq!(decode_ledger(&over), None);
        // Collapsed: two records, one writer. The bytes frame perfectly and end
        // exactly — only the distinct-writer check catches it, and it must,
        // because the map it would yield is smaller than its own header claims.
        let mut duplicated = header(4, 2);
        duplicated.extend_from_slice(&record("a", 1, 1));
        duplicated.extend_from_slice(&record("a", 1, 2));
        assert_eq!(decode_ledger(&duplicated), None);
    }

    #[test]
    fn every_truncation_decodes_to_nothing_at_all() {
        let applied = marks(&[("host-a", 3, 41), ("host-b", 1, 7), ("host-c", 8, 2)]);
        let bytes = encode_ledger(5, &applied);
        for cut in 0..bytes.len() {
            // The record count closes the boundary case: a prefix that lands
            // exactly between two records used to decode to a well-formed
            // *subset*, which under-reports a recovery target — the unsafe
            // direction. Now every prefix is short of the count it carries.
            assert_eq!(decode_ledger(&bytes[..cut]), None, "prefix at {cut}");
        }
        assert_eq!(decode_ledger(&bytes).expect("whole").applied, applied);
    }

    #[test]
    fn the_fold_raises_a_watermark_and_never_lowers_one() {
        let mut st = state(0, &[]);
        let host = node("host");
        assert!(fold(&mut st, &host, WriteToken { epoch: 7, seq: 4 }, 7));
        assert_eq!(
            st.applied.get(&host),
            Some(&WriteToken { epoch: 7, seq: 4 })
        );
        // A lower token, and an equal one, both move nothing.
        assert!(!fold(&mut st, &host, WriteToken { epoch: 7, seq: 3 }, 7));
        assert!(!fold(&mut st, &host, WriteToken { epoch: 7, seq: 4 }, 7));
        assert_eq!(
            st.applied.get(&host),
            Some(&WriteToken { epoch: 7, seq: 4 })
        );
        // Epoch-major: a new life's first write beats every old-life token.
        assert!(fold(&mut st, &host, WriteToken { epoch: 8, seq: 1 }, 8));
        assert_eq!(
            st.applied.get(&host),
            Some(&WriteToken { epoch: 8, seq: 1 })
        );
    }

    #[test]
    fn the_stamp_is_monotone_whatever_the_watch_reports() {
        let mut st = state(9, &[]);
        let host = node("host");
        // A regressing leadership read — a stale watch borrow, a reordered
        // republish — cannot lower the stamp, because the whole intersection
        // argument rests on it never doing so.
        assert!(fold(&mut st, &host, WriteToken { epoch: 9, seq: 1 }, 3));
        assert_eq!(st.stamp, 9, "a lower epoch is ignored");
        assert!(fold(&mut st, &host, WriteToken { epoch: 9, seq: 1 }, 11));
        assert_eq!(st.stamp, 11, "a higher epoch is adopted");
    }

    #[test]
    fn a_stamp_advance_alone_is_worth_republishing() {
        let mut st = state(4, &[("host", 4, 2)]);
        let host = node("host");
        // Nothing new applied, but the voter has adopted epoch 5. A recovering
        // host at 5 needs to hear that, so the fold reports movement.
        assert!(fold(&mut st, &host, WriteToken { epoch: 4, seq: 2 }, 5));
        assert_eq!(st.stamp, 5);
        assert_eq!(
            st.applied.get(&host),
            Some(&WriteToken { epoch: 4, seq: 2 })
        );
        // …and a call that moves neither reports none, so nothing is published.
        assert!(!fold(&mut st, &host, WriteToken { epoch: 4, seq: 2 }, 5));
    }
}
