//! Capability advertisement: what a node tells the group it can *do*.
//!
//! Membership answers "is this peer alive". It does not answer "does this
//! peer speak the protocol I am about to wait on" — and in a mixed or
//! mid-upgrade deployment those are different questions. A strong-mode
//! writer that waits on every alive member eats a timeout for every peer
//! that simply does not run the acknowledging half. Capabilities let it
//! wait on the peers that actually participate instead.
//!
//! # One entry, written wholesale
//!
//! A node's entire capability set rides **one** reserved keyed entry,
//! `~caps`, rewritten in full on every advertisement — replace semantics,
//! never per-capability keys. That is load-bearing, not a compression
//! choice: the engine's restart recovery re-adopts un-authored entries from
//! peer echoes, so a node that comes back from a restart inherits its
//! previous life's entries unless it authors over them this boot. With one
//! wholesale key, a single [`advertise_capabilities`] call — *even with an
//! empty set* — authors `~caps` and out-versions the dead advertisement
//! everywhere. With per-capability keys a retired capability would have no
//! key to overwrite and would haunt the group.
//!
//! [`advertise_capabilities`]: Group::advertise_capabilities
//!
//! # Wire format
//!
//! `(u32 LE length, utf-8 bytes)*` — the same dependency-free,
//! length-prefixed shape the ack ledger uses. Names are normalized to a
//! sorted, deduplicated set at encode time, so the same set always produces
//! the same bytes regardless of iteration order. Bytes that do not decode
//! cleanly (truncation, a bogus length, invalid utf-8) yield the **empty**
//! set rather than a partial one: a half-read advertisement must never be
//! mistaken for a smaller honest one.

use groupnet_core::NodeId;

use crate::group::{CommandRejected, Group};

/// The reserved group entry key carrying a node's full capability set.
/// Reserved keys are `~`-prefixed (`~addr`, `~blob`, `~applied`, `~writes`).
const CAPS_KEY: &str = "~caps";

impl Group {
    /// Advertises this node's capabilities to the group, replacing whatever
    /// it advertised before.
    ///
    /// Call this **once at startup with the complete set** — and call it even
    /// when the set is empty. The advertisement is one wholesale entry, so
    /// the call is what stops a previous life's advertisement from being
    /// re-adopted after a restart; skipping it because "this node has no
    /// capabilities" is exactly the case where a stale set survives. Calling
    /// it again later is fine and simply replaces the set.
    ///
    /// Capability names are opaque to groupnet: any utf-8 string, compared
    /// byte-for-byte. Cross-crate names should be namespaced
    /// (`"mycrate:thing"`) to stay collision-free.
    ///
    /// The entry carries **no TTL** — a capability is a property of the
    /// running process, not a lease, and it disappears with the node itself
    /// when membership reaps it.
    ///
    /// # Errors
    /// [`CommandRejected`] if the group actor's bounded inbox is full or the
    /// actor has shut down; the advertisement was not enqueued. Retry after
    /// a beat — the set is state, so the last call wins.
    pub fn advertise_capabilities<I, S>(&self, caps: I) -> Result<(), CommandRejected>
    where
        I: IntoIterator<Item = S>,
        S: AsRef<str>,
    {
        self.set_entry(CAPS_KEY, encode(caps), None)
    }

    /// The capabilities `node` advertises, as this node currently sees them —
    /// sorted and deduplicated.
    ///
    /// Empty when `node` advertises none, has never advertised, is unknown,
    /// or its advertisement has not converged here yet. **Absence is not
    /// participation**: an empty set means "this node makes no claim", which
    /// covers both a peer that genuinely lacks the capability and an older
    /// build that does not advertise at all. Treat a missing advertisement
    /// as "do not rely on it", never as "it is not there".
    #[must_use]
    pub fn node_capabilities(&self, node: &NodeId) -> Vec<String> {
        self.node_entry(node, CAPS_KEY)
            .map(|bytes| decode(&bytes))
            .unwrap_or_default()
    }

    /// Whether `node` advertises `cap`. `false` also covers "never
    /// advertised" — see [`node_capabilities`](Self::node_capabilities) on
    /// why absence is not participation.
    #[must_use]
    pub fn node_has_capability(&self, node: &NodeId, cap: &str) -> bool {
        self.node_capabilities(node).iter().any(|c| c == cap)
    }

    /// The live members (anything not `Dead`, as [`members`](Self::members)
    /// defines it) advertising `cap`, in id order.
    #[must_use]
    pub fn members_with_capability(&self, cap: &str) -> Vec<NodeId> {
        self.members()
            .into_iter()
            .filter(|node| self.node_has_capability(node, cap))
            .collect()
    }
}

/// `(u32 len, utf-8 name)*`, little-endian — dep-free, and normalized to a
/// sorted deduplicated set so equal sets encode to equal bytes.
///
/// A capability name is assumed to fit a `u32` length. That is a debug
/// assertion rather than a release panic: the case needs a name over 4 GiB,
/// and if one ever appeared the saturated prefix would run off the end of the
/// buffer and [`decode`] would answer with the **empty** set — "makes no
/// claim", the conservative reading — never with a wrong set.
fn encode<I, S>(caps: I) -> Vec<u8>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let mut names: Vec<String> = caps.into_iter().map(|c| c.as_ref().to_owned()).collect();
    names.sort_unstable();
    names.dedup();
    let mut out = Vec::with_capacity(names.iter().map(|n| n.len() + 4).sum());
    for name in names {
        let bytes = name.as_bytes();
        debug_assert!(
            u32::try_from(bytes.len()).is_ok(),
            "capability name is longer than u32::MAX bytes"
        );
        out.extend_from_slice(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(bytes);
    }
    out
}

/// Every name in an advertisement, or the empty set if the bytes are not a
/// whole, well-formed encoding. Never panics on hostile or truncated input.
fn decode(bytes: &[u8]) -> Vec<String> {
    decode_checked(bytes).unwrap_or_default()
}

/// The strict half of [`decode`]: `None` the moment anything does not parse,
/// so a truncated advertisement can never masquerade as a shorter one.
fn decode_checked(bytes: &[u8]) -> Option<Vec<String>> {
    let mut out = Vec::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let len = usize::try_from(u32::from_le_bytes(
            bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
        ))
        .ok()?;
        offset += 4;
        let end = offset.checked_add(len)?;
        out.push(
            std::str::from_utf8(bytes.get(offset..end)?)
                .ok()?
                .to_owned(),
        );
        offset = end;
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::{decode, encode};

    #[test]
    fn empty_set_round_trips() {
        let bytes = encode(Vec::<&str>::new());
        assert!(bytes.is_empty(), "an empty set is an empty advertisement");
        assert!(decode(&bytes).is_empty());
    }

    #[test]
    fn names_round_trip_including_punctuation_and_whitespace() {
        // `:` is the namespacing convention; whitespace and non-ascii are
        // legal too — names are opaque utf-8, not identifiers.
        let caps = [
            "acks",
            "mycrate:thing",
            "two words",
            "  padded  ",
            "ünïcøde",
        ];
        let decoded = decode(&encode(caps));
        let mut expected: Vec<String> = caps.iter().map(|c| (*c).to_owned()).collect();
        expected.sort_unstable();
        assert_eq!(decoded, expected);
    }

    #[test]
    fn encoding_normalizes_to_a_sorted_deduplicated_set() {
        assert_eq!(
            decode(&encode(["b", "a", "b", "a"])),
            vec!["a".to_owned(), "b".to_owned()]
        );
        // Order-independence is what makes re-advertising the same set a
        // byte-identical write.
        assert_eq!(encode(["a", "b"]), encode(["b", "a", "a"]));
    }

    #[test]
    fn every_truncation_of_a_valid_encoding_decodes_without_panic() {
        let bytes = encode(["acks", "mycrate:thing", "two words"]);
        for cut in 0..bytes.len() {
            let prefix = decode(&bytes[..cut]);
            // A prefix may land on a record boundary and decode to a proper
            // subset; it may never be longer than the whole.
            assert!(prefix.len() <= 3, "a prefix cannot gain names");
        }
        assert_eq!(decode(&bytes).len(), 3);
    }

    #[test]
    fn unparseable_bytes_decode_to_the_empty_set() {
        // A length that runs off the end.
        assert!(decode(b"garbage").is_empty());
        assert!(decode(&[0xff, 0xff, 0xff, 0xff, 0x01]).is_empty());
        // A header shorter than the length prefix itself.
        assert!(decode(&[0x01, 0x00]).is_empty());
        // A well-framed name that is not utf-8.
        assert!(decode(&[0x01, 0x00, 0x00, 0x00, 0xff]).is_empty());
        // A valid record followed by trailing garbage: all-or-nothing.
        let mut mixed = encode(["acks"]);
        mixed.extend_from_slice(&[0x09, 0x00, 0x00, 0x00, 0x01]);
        assert!(decode(&mixed).is_empty());
    }
}
