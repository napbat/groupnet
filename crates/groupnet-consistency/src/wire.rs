//! The gossiped representation: which group entry a feed occupies, and the
//! ring frame encoded into it.

/// The group entry key under which a node's default write feed is gossiped
/// (`~`-prefixed like the runtime's reserved entries). Named feeds append
/// `:<name>`.
const ENTRY_KEY: &str = "~writes";

/// The entry key for a feed name: the reserved default, or `~writes:<name>`.
pub(crate) fn entry_key(name: &str) -> String {
    if name.is_empty() {
        ENTRY_KEY.to_owned()
    } else {
        format!("{ENTRY_KEY}:{name}")
    }
}

/// The wire frame: the feed epoch, `first_seq`, the encoded keys of the last
/// N writes, sequential from `first_seq`, and whether the life is sealed.
///
/// A sealed frame's life ends with a seal at position [`Frame::end`], the
/// position after its last write: the writer promised no write after it.
/// The seal flag is the frame's last byte, so a decoder from before it and
/// this decoder each reject the other's frames instead of misreading them.
pub(crate) struct Frame {
    pub(crate) epoch: u64,
    pub(crate) first_seq: u64,
    pub(crate) keys: Vec<Vec<u8>>,
    pub(crate) sealed: bool,
}

impl Frame {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(21 + self.keys.iter().map(|k| 4 + k.len()).sum::<usize>());
        out.extend_from_slice(&self.epoch.to_le_bytes());
        out.extend_from_slice(&self.first_seq.to_le_bytes());
        out.extend_from_slice(
            &u32::try_from(self.keys.len())
                .unwrap_or(u32::MAX)
                .to_le_bytes(),
        );
        for key in &self.keys {
            out.extend_from_slice(&u32::try_from(key.len()).unwrap_or(u32::MAX).to_le_bytes());
            out.extend_from_slice(key);
        }
        out.push(u8::from(self.sealed));
        out
    }

    pub(crate) fn decode(bytes: &[u8]) -> Option<Self> {
        let epoch = u64::from_le_bytes(bytes.get(0..8)?.try_into().ok()?);
        let first_seq = u64::from_le_bytes(bytes.get(8..16)?.try_into().ok()?);
        let count = u32::from_le_bytes(bytes.get(16..20)?.try_into().ok()?);
        let mut offset = 20_usize;
        let mut keys = Vec::with_capacity(usize::try_from(count).ok()?.min(4096));
        for _ in 0..count {
            let len = usize::try_from(u32::from_le_bytes(
                bytes.get(offset..offset + 4)?.try_into().ok()?,
            ))
            .ok()?;
            offset += 4;
            keys.push(bytes.get(offset..offset + len)?.to_vec());
            offset += len;
        }
        let sealed = match *bytes.get(offset)? {
            0 => false,
            1 => true,
            _ => return None,
        };
        let end = first_seq.checked_add(u64::try_from(keys.len()).ok()?)?;
        if offset + 1 != bytes.len()
            || first_seq == 0
            || (keys.is_empty() && first_seq != 1)
            || (sealed && end.checked_add(1).is_none())
        {
            return None;
        }
        Some(Self {
            epoch,
            first_seq,
            keys,
            sealed,
        })
    }

    /// The position after the last write: the next write's, or the seal's.
    pub(crate) fn end(&self) -> u64 {
        self.first_seq + self.keys.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::{Frame, entry_key};

    #[test]
    fn frame_round_trips() {
        for sealed in [false, true] {
            let frame = Frame {
                epoch: 7,
                first_seq: 41,
                keys: vec![b"alpha".to_vec(), Vec::new(), b"c".to_vec()],
                sealed,
            };
            let decoded = Frame::decode(&frame.encode()).expect("decode");
            assert_eq!(decoded.epoch, 7);
            assert_eq!(decoded.first_seq, 41);
            assert_eq!(decoded.keys, frame.keys);
            assert_eq!(decoded.end(), 44);
            assert_eq!(decoded.sealed, sealed);
        }
    }

    #[test]
    fn truncated_frames_are_rejected() {
        let bytes = Frame {
            epoch: 3,
            first_seq: 1,
            keys: vec![b"key".to_vec()],
            sealed: true,
        }
        .encode();
        for cut in 0..bytes.len() {
            assert!(Frame::decode(&bytes[..cut]).is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn invalid_history_seal_and_trailing_bytes_are_rejected() {
        let frame = Frame {
            epoch: 3,
            first_seq: 1,
            keys: vec![b"key".to_vec()],
            sealed: false,
        };
        let mut trailing = frame.encode();
        trailing.push(0);
        assert!(Frame::decode(&trailing).is_none());

        let mut unknown_flag = frame.encode();
        *unknown_flag.last_mut().expect("the seal flag") = 2;
        assert!(Frame::decode(&unknown_flag).is_none());

        // A frame without the seal flag, as a mis-deployed older node
        // encodes it, does not decode.
        let mut unflagged = frame.encode();
        unflagged.pop();
        assert!(Frame::decode(&unflagged).is_none());

        let mut zero = frame.encode();
        zero[8..16].copy_from_slice(&0_u64.to_le_bytes());
        assert!(Frame::decode(&zero).is_none());

        let mut overflow = frame.encode();
        overflow[8..16].copy_from_slice(&u64::MAX.to_le_bytes());
        assert!(Frame::decode(&overflow).is_none());

        let mut last = Frame {
            first_seq: u64::MAX - 1,
            ..frame
        };
        assert!(Frame::decode(&last.encode()).is_some());
        last.sealed = true;
        assert!(
            Frame::decode(&last.encode()).is_none(),
            "a seal after the last representable position"
        );

        let empty = Frame {
            epoch: 3,
            first_seq: 4,
            keys: Vec::new(),
            sealed: false,
        };
        assert!(Frame::decode(&empty.encode()).is_none());
        assert!(
            Frame::decode(
                &Frame {
                    first_seq: 1,
                    ..empty
                }
                .encode()
            )
            .is_some()
        );
    }

    #[test]
    fn feed_names_map_to_distinct_entries() {
        assert_eq!(entry_key(""), "~writes");
        assert_eq!(entry_key("docs"), "~writes:docs");
        assert_ne!(entry_key("docs"), entry_key("index"));
    }
}
