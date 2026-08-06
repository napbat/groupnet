//! The gossiped representation of a serve-lease: which entries the tier
//! occupies, the renewal id they carry, and the two codecs.
//!
//! # Two entries, one protocol
//!
//! * **The renewal**, `~lease` (`~lease:<name>` for a named set), authored by
//!   the **reader** and carrying one [`RenewalId`] under a TTL of one lease
//!   duration. Its expiry *at each receiver* is the granter-side half of the
//!   lease: the engine arms `now + ttl_ms` against the receiver's own clock at
//!   the instant it adopts the entry, so a receiver's copy always expires no
//!   earlier than one duration after the reader recorded the publish instant.
//!   That inequality is what makes the reader's own arithmetic
//!   ([`LeaseCore`](super::LeaseCore)) conservative rather than hopeful.
//! * **The grant map**, `~lease:g` (`~lease:g:<name>`), authored by **every**
//!   member and carrying the newest renewal it has adopted from each reader it
//!   can see. Written **wholesale** on every change — replace semantics,
//!   exactly like `~caps`, and for the same reason: the engine's restart
//!   recovery re-adopts un-authored entries from peer echoes, so a granter
//!   that came back from a restart would inherit its previous life's grants
//!   unless one write authors over the whole map this boot. Per-reader keys
//!   would leave a retired grant with no key to overwrite, and it would haunt
//!   the group.
//!
//! # Wire format
//!
//! A renewal is `(u64 epoch, u64 seq)` little-endian, 16 bytes, and nothing
//! else — anything of another length is *no renewal*, never a guess.
//!
//! A grant map is `(u32 reader_len, utf-8 reader, u64 epoch, u64 seq)*`
//! little-endian: the same dependency-free, length-prefixed shape the ack
//! ledger and the capability set use. Bytes that do not decode cleanly yield
//! the **empty** map rather than a partial one, and a truncation that happens
//! to land on a record boundary yields a proper subset. Both failures point
//! the same way: fewer confirmations reach the reader, its confirmed renewal
//! freezes or vanishes, and it stops serving. A codec accident can shorten a
//! serve window; it cannot invent one.

use std::collections::BTreeMap;

use groupnet_core::NodeId;

/// The reserved group entry prefix the tier occupies (`~`-prefixed like the
/// runtime's other reserved entries: `~caps`, `~applied`, `~writes`).
const LEASE_KEY: &str = "~lease";

/// The sub-key distinguishing a granter's grant map from a reader's renewal.
/// **Reserved as a set name** — see [`validate_name`].
const GRANT_TAG: &str = "g";

/// One reader's renewal of its right to serve: the reader's lease life
/// (`epoch`) and the renewal's sequence number within it.
///
/// The derived ordering is **epoch-major**, exactly like
/// [`WriteToken`](crate::WriteToken) and for exactly the same reason: a reader
/// that restarts starts renewing at sequence 1 again, and a grant recorded
/// against its previous life must never be mistaken for confirmation of the
/// new one. A reader's epoch is its wall-clock boot time, so any renewal of a
/// new life compares above every renewal of an old one.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RenewalId {
    /// The reader's lease life — its boot epoch.
    pub epoch: u64,
    /// The renewal's sequence number within the epoch, starting at 1.
    pub seq: u64,
}

impl RenewalId {
    /// The encoded size of a renewal: two little-endian `u64`s.
    pub const ENCODED_LEN: usize = 16;
}

/// What one granter advertises: the newest [`RenewalId`] it has adopted from
/// each reader it can currently see.
///
/// Ordered by reader id so a re-advertisement of an unchanged map is a
/// byte-identical write (and so a simulation replays deterministically).
pub type GrantMap = BTreeMap<NodeId, RenewalId>;

/// The entry key a reader's renewal occupies: `~lease` for the default set,
/// `~lease:<name>` for a named one.
#[must_use]
pub fn renewal_entry_key(name: &str) -> String {
    if name.is_empty() {
        LEASE_KEY.to_owned()
    } else {
        format!("{LEASE_KEY}:{name}")
    }
}

/// The entry key a granter's grant map occupies: `~lease:g` for the default
/// set, `~lease:g:<name>` for a named one.
#[must_use]
pub fn grant_entry_key(name: &str) -> String {
    if name.is_empty() {
        format!("{LEASE_KEY}:{GRANT_TAG}")
    } else {
        format!("{LEASE_KEY}:{GRANT_TAG}:{name}")
    }
}

/// Rejects set names that would collide inside the entry layout.
///
/// # Panics
/// If `name` contains `:` (the layout's own separator, so the name would merge
/// with a neighbouring set's key space), or if it is exactly `"g"` — the
/// grant-map tag. A set named `g` would put its *renewal* entry at
/// `~lease:g`, which is the **default** set's *grant* entry: two different
/// payloads authored by the same node under one key.
pub fn validate_name(name: &str) {
    assert!(
        !name.contains(':'),
        "lease set names must not contain ':' (got {name:?})"
    );
    assert!(
        name != GRANT_TAG,
        "lease set name {GRANT_TAG:?} is reserved: it collides with the \
         default set's grant entry"
    );
}

/// `(u64 epoch, u64 seq)` little-endian, 16 bytes — dep-free.
#[must_use]
pub fn encode_renewal(id: RenewalId) -> Vec<u8> {
    let mut out = Vec::with_capacity(RenewalId::ENCODED_LEN);
    out.extend_from_slice(&id.epoch.to_le_bytes());
    out.extend_from_slice(&id.seq.to_le_bytes());
    out
}

/// The renewal in `bytes`, or `None` unless they are exactly one encoded
/// renewal. Short, long, and garbled all decode to "this node advertises no
/// lease" — the conservative answer, and the same one absence gives.
#[must_use]
pub fn decode_renewal(bytes: &[u8]) -> Option<RenewalId> {
    if bytes.len() != RenewalId::ENCODED_LEN {
        return None;
    }
    Some(RenewalId {
        epoch: u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?),
        seq: u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?),
    })
}

/// `(u32 reader_len, utf-8 reader, u64 epoch, u64 seq)*`, little-endian.
///
/// A reader id is assumed to fit a `u32` length. That is a debug assertion
/// rather than a release panic: the case needs an id over 4 GiB, and if one
/// ever appeared the saturated prefix would run off the end of the buffer and
/// [`decode_grants`] would answer with the **empty** map — "this granter
/// confirms nothing", the conservative reading — never with a wrong one.
#[must_use]
pub fn encode_grants(grants: &GrantMap) -> Vec<u8> {
    let mut out = Vec::with_capacity(grants.len() * 32);
    for (reader, id) in grants {
        let name = reader.as_str().as_bytes();
        debug_assert!(
            u32::try_from(name.len()).is_ok(),
            "node id is longer than u32::MAX bytes"
        );
        out.extend_from_slice(&u32::try_from(name.len()).unwrap_or(u32::MAX).to_le_bytes());
        out.extend_from_slice(name);
        out.extend_from_slice(&id.epoch.to_le_bytes());
        out.extend_from_slice(&id.seq.to_le_bytes());
    }
    out
}

/// Every grant in an advertisement, or the empty map if the bytes are not a
/// whole, well-formed encoding. Never panics on hostile or truncated input.
#[must_use]
pub fn decode_grants(bytes: &[u8]) -> GrantMap {
    decode_grants_checked(bytes).unwrap_or_default()
}

/// The strict half of [`decode_grants`]: `None` the moment anything does not
/// parse, so a half-read map can never masquerade as a smaller honest one.
fn decode_grants_checked(bytes: &[u8]) -> Option<GrantMap> {
    let mut out = GrantMap::new();
    let mut offset = 0usize;
    while offset < bytes.len() {
        let len = usize::try_from(u32::from_le_bytes(
            bytes.get(offset..offset.checked_add(4)?)?.try_into().ok()?,
        ))
        .ok()?;
        offset += 4;
        let end = offset.checked_add(len)?;
        let reader = std::str::from_utf8(bytes.get(offset..end)?).ok()?;
        offset = end;
        let epoch = u64::from_le_bytes(bytes.get(offset..offset.checked_add(8)?)?.try_into().ok()?);
        offset += 8;
        let seq = u64::from_le_bytes(bytes.get(offset..offset.checked_add(8)?)?.try_into().ok()?);
        offset += 8;
        out.insert(NodeId::new(reader), RenewalId { epoch, seq });
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::{
        GrantMap, RenewalId, decode_grants, decode_renewal, encode_grants, encode_renewal,
        grant_entry_key, renewal_entry_key, validate_name,
    };
    use groupnet_core::NodeId;

    fn grants(pairs: &[(&str, u64, u64)]) -> GrantMap {
        pairs
            .iter()
            .map(|(node, epoch, seq)| {
                (
                    NodeId::new(*node),
                    RenewalId {
                        epoch: *epoch,
                        seq: *seq,
                    },
                )
            })
            .collect()
    }

    #[test]
    fn renewal_ids_order_epoch_major() {
        let old_life = RenewalId {
            epoch: 1,
            seq: 5_000,
        };
        let new_life = RenewalId { epoch: 2, seq: 1 };
        assert!(new_life > old_life, "any new-life renewal beats old-life");
        assert!(RenewalId { epoch: 2, seq: 2 } > new_life);
    }

    #[test]
    fn set_names_map_to_distinct_entries() {
        assert_eq!(renewal_entry_key(""), "~lease");
        assert_eq!(renewal_entry_key("pages"), "~lease:pages");
        assert_eq!(grant_entry_key(""), "~lease:g");
        assert_eq!(grant_entry_key("pages"), "~lease:g:pages");
        // A renewal and a grant map never share an entry, and neither do two
        // sets: the reader half and the granter half are different payloads
        // authored by (potentially) the same node.
        assert_ne!(renewal_entry_key(""), grant_entry_key(""));
        assert_ne!(renewal_entry_key("pages"), grant_entry_key("pages"));
        assert_ne!(renewal_entry_key("a"), renewal_entry_key("b"));
        assert_ne!(grant_entry_key("a"), grant_entry_key("b"));
    }

    #[test]
    fn legal_set_names_validate() {
        for name in ["", "pages", "blobs-v2", "gg", "G"] {
            validate_name(name);
        }
    }

    #[test]
    #[should_panic(expected = "must not contain ':'")]
    fn a_colon_in_a_set_name_panics() {
        validate_name("pages:hot");
    }

    #[test]
    #[should_panic(expected = "is reserved")]
    fn the_grant_tag_is_a_reserved_set_name() {
        // `~lease:g` is the default set's grant entry; a set named `g` would
        // author its renewals into it.
        assert_eq!(renewal_entry_key("g"), grant_entry_key(""));
        validate_name("g");
    }

    #[test]
    fn renewals_round_trip_and_wrong_lengths_decode_to_nothing() {
        for id in [
            RenewalId { epoch: 0, seq: 0 },
            RenewalId { epoch: 1, seq: 1 },
            RenewalId {
                epoch: u64::MAX,
                seq: u64::MAX,
            },
            RenewalId {
                epoch: 1_754_000_000_000,
                seq: 42,
            },
        ] {
            let bytes = encode_renewal(id);
            assert_eq!(bytes.len(), RenewalId::ENCODED_LEN);
            assert_eq!(decode_renewal(&bytes), Some(id));
        }
        let bytes = encode_renewal(RenewalId { epoch: 3, seq: 9 });
        for cut in 0..RenewalId::ENCODED_LEN {
            assert_eq!(decode_renewal(&bytes[..cut]), None, "short at {cut}");
        }
        assert_eq!(decode_renewal(&[0u8; 17]), None, "long");
    }

    #[test]
    fn sixteen_arbitrary_bytes_are_a_renewal() {
        // The codec has no magic number and needs none: only a lease entry is
        // ever handed to it, and the ids it yields are useless to a reader
        // unless they carry that reader's own current epoch.
        assert_eq!(
            decode_renewal(b"not a renewal!!!"),
            Some(RenewalId {
                epoch: u64::from_le_bytes(*b"not a re"),
                seq: u64::from_le_bytes(*b"newal!!!"),
            })
        );
    }

    #[test]
    fn grant_maps_round_trip() {
        let map = grants(&[("node-a", 3, 41), ("b", 1, 7), ("ünïcøde-node", 9, 9)]);
        assert_eq!(decode_grants(&encode_grants(&map)), map);
        // Ordered by reader id, so an unchanged map re-encodes byte-identically.
        let same = grants(&[("ünïcøde-node", 9, 9), ("b", 1, 7), ("node-a", 3, 41)]);
        assert_eq!(encode_grants(&same), encode_grants(&map));
    }

    #[test]
    fn an_empty_grant_map_round_trips() {
        let empty = GrantMap::new();
        assert!(
            encode_grants(&empty).is_empty(),
            "granting nothing is empty"
        );
        assert!(decode_grants(&[]).is_empty());
    }

    #[test]
    fn every_truncation_of_a_grant_map_decodes_without_panic() {
        let map = grants(&[("node-a", 3, 41), ("node-b", 1, 7), ("node-c", 8, 2)]);
        let bytes = encode_grants(&map);
        for cut in 0..bytes.len() {
            let prefix = decode_grants(&bytes[..cut]);
            // A prefix may land on a record boundary and decode to a proper
            // subset; it can never gain a reader, and every grant it does
            // carry is one the whole map carries too.
            assert!(prefix.len() <= map.len(), "a prefix cannot gain readers");
            for (reader, id) in &prefix {
                assert_eq!(map.get(reader), Some(id));
            }
        }
        assert_eq!(decode_grants(&bytes).len(), 3);
    }

    #[test]
    fn unparseable_grant_bytes_decode_to_the_empty_map() {
        // A length that runs off the end.
        assert!(decode_grants(b"garbage").is_empty());
        assert!(decode_grants(&[0xff, 0xff, 0xff, 0xff, 0x01]).is_empty());
        // A header shorter than the length prefix itself.
        assert!(decode_grants(&[0x01, 0x00]).is_empty());
        // A well-framed reader id that is not utf-8.
        assert!(decode_grants(&[0x01, 0x00, 0x00, 0x00, 0xff]).is_empty());
        // A name with no renewal behind it.
        assert!(decode_grants(&[0x01, 0x00, 0x00, 0x00, b'a']).is_empty());
        // A valid record followed by trailing garbage: all-or-nothing.
        let mut mixed = encode_grants(&grants(&[("node-a", 1, 1)]));
        mixed.extend_from_slice(&[0x09, 0x00, 0x00, 0x00, 0x01]);
        assert!(decode_grants(&mixed).is_empty());
    }
}
