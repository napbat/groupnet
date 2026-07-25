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

/// The wire frame: the feed epoch, `first_seq`, and the encoded keys of the
/// last N writes, sequential from `first_seq`.
pub(crate) struct Frame {
    pub(crate) epoch: u64,
    pub(crate) first_seq: u64,
    pub(crate) keys: Vec<Vec<u8>>,
}

impl Frame {
    pub(crate) fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(20 + self.keys.iter().map(|k| 4 + k.len()).sum::<usize>());
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
        Some(Self {
            epoch,
            first_seq,
            keys,
        })
    }

    pub(crate) fn end(&self) -> u64 {
        self.first_seq + self.keys.len() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::{Frame, entry_key};

    #[test]
    fn frame_round_trips() {
        let frame = Frame {
            epoch: 7,
            first_seq: 41,
            keys: vec![b"alpha".to_vec(), Vec::new(), b"c".to_vec()],
        };
        let decoded = Frame::decode(&frame.encode()).expect("decode");
        assert_eq!(decoded.epoch, 7);
        assert_eq!(decoded.first_seq, 41);
        assert_eq!(decoded.keys, frame.keys);
        assert_eq!(decoded.end(), 44);
    }

    #[test]
    fn truncated_frames_are_rejected() {
        let bytes = Frame {
            epoch: 3,
            first_seq: 1,
            keys: vec![b"key".to_vec()],
        }
        .encode();
        for cut in 0..bytes.len() {
            assert!(Frame::decode(&bytes[..cut]).is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn feed_names_map_to_distinct_entries() {
        assert_eq!(entry_key(""), "~writes");
        assert_eq!(entry_key("docs"), "~writes:docs");
        assert_ne!(entry_key("docs"), entry_key("index"));
    }
}
