//! Bounded per-origin replay windows for routed packet identities.
//!
//! A routed identity is the origin router's random 8-byte nonce followed by
//! its big-endian packet sequence. Each `(origin, nonce)` stream keeps a
//! sliding window of the last [`WINDOW`] sequences: a sequence seen inside the
//! window, or older than it, is a replay or a loop. At most
//! `ceil(capacity / WINDOW)` streams are tracked, so the retained identities
//! stay within the configured capacity; the least recently active stream is
//! evicted first and starts a fresh window if it reappears.

use std::collections::HashMap;

use groupnet_core::NodeId;

/// Sequences each stream remembers behind its newest one.
pub(super) const WINDOW: u64 = u64::BITS as u64;

/// [`WINDOW`] as a count of retained identities.
const IDENTITIES: usize = u64::BITS as usize;

#[derive(Debug)]
struct Window {
    nonce: [u8; 8],
    newest: u64,
    /// Bit `n` records `newest - n`.
    seen: u64,
    /// Activity stamp for least-recently-active eviction.
    touched: u64,
}

impl Window {
    fn accept(&mut self, sequence: u64) -> bool {
        if sequence > self.newest {
            let ahead = sequence - self.newest;
            self.seen = if ahead >= WINDOW {
                1
            } else {
                (self.seen << ahead) | 1
            };
            self.newest = sequence;
            return true;
        }
        let behind = self.newest - sequence;
        if behind >= WINDOW {
            return false;
        }
        let bit = 1 << behind;
        let first = self.seen & bit == 0;
        self.seen |= bit;
        first
    }
}

#[derive(Debug)]
pub(super) struct ReplayWindows {
    origins: HashMap<NodeId, Vec<Window>>,
    streams: usize,
    capacity: usize,
    clock: u64,
}

impl ReplayWindows {
    /// Retains about `capacity` identities as `WINDOW`-wide streams.
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            origins: HashMap::new(),
            streams: 0,
            capacity: capacity.div_ceil(IDENTITIES).max(1),
            clock: 0,
        }
    }

    /// Records a routed identity, returning `false` for a replay or a loop.
    pub(super) fn first_sighting(&mut self, from: &NodeId, id: [u8; 16]) -> bool {
        let (nonce, sequence) = id.split_at(8);
        let nonce: [u8; 8] = nonce.try_into().expect("8-byte nonce");
        let sequence = u64::from_be_bytes(sequence.try_into().expect("8-byte sequence"));
        self.clock += 1;
        if let Some(window) = self
            .origins
            .get_mut(from)
            .and_then(|windows| windows.iter_mut().find(|window| window.nonce == nonce))
        {
            window.touched = self.clock;
            return window.accept(sequence);
        }
        if self.streams == self.capacity {
            self.evict();
        }
        self.origins.entry(from.clone()).or_default().push(Window {
            nonce,
            newest: sequence,
            seen: 1,
            touched: self.clock,
        });
        self.streams += 1;
        true
    }

    /// Drops the least recently active stream; runs only when a new stream
    /// arrives at capacity.
    fn evict(&mut self) {
        let Some((origin, index)) = self
            .origins
            .iter()
            .flat_map(|(origin, windows)| {
                windows
                    .iter()
                    .enumerate()
                    .map(move |(index, window)| (window.touched, origin, index))
            })
            .min_by_key(|(touched, ..)| *touched)
            .map(|(_, origin, index)| (origin.clone(), index))
        else {
            return;
        };
        if let Some(windows) = self.origins.get_mut(&origin) {
            windows.swap_remove(index);
            if windows.is_empty() {
                self.origins.remove(&origin);
            }
            self.streams -= 1;
        }
    }

    #[cfg(test)]
    fn streams(&self) -> usize {
        self.streams
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(nonce: u8, sequence: u64) -> [u8; 16] {
        let mut id = [nonce; 16];
        id[8..].copy_from_slice(&sequence.to_be_bytes());
        id
    }

    #[test]
    fn duplicates_and_identities_behind_the_window_are_rejected() {
        let origin = NodeId::new("origin");
        let mut replay = ReplayWindows::new(4096);
        assert!(replay.first_sighting(&origin, id(1, 100)));
        assert!(!replay.first_sighting(&origin, id(1, 100)), "duplicate");
        // Reordering inside the window is accepted exactly once.
        assert!(replay.first_sighting(&origin, id(1, 100 - (WINDOW - 1))));
        assert!(!replay.first_sighting(&origin, id(1, 100 - (WINDOW - 1))));
        assert!(
            !replay.first_sighting(&origin, id(1, 100 - WINDOW)),
            "too old"
        );
        // Jumping ahead slides the window; skipped sequences stay acceptable.
        assert!(replay.first_sighting(&origin, id(1, 100 + WINDOW + 5)));
        assert!(replay.first_sighting(&origin, id(1, 100 + WINDOW + 4)));
        assert!(!replay.first_sighting(&origin, id(1, 100)), "now too old");
        assert!(!replay.first_sighting(&origin, id(1, 100 + WINDOW + 5)));
    }

    #[test]
    fn origins_and_router_lives_are_separate_streams() {
        let (a, b) = (NodeId::new("a"), NodeId::new("b"));
        let mut replay = ReplayWindows::new(4096);
        assert!(replay.first_sighting(&a, id(1, 7)));
        assert!(replay.first_sighting(&b, id(1, 7)), "another origin");
        assert!(replay.first_sighting(&a, id(2, 7)), "a restarted router");
        assert!(!replay.first_sighting(&a, id(1, 7)));
        assert!(!replay.first_sighting(&a, id(2, 7)));
        assert_eq!(replay.streams(), 3);
    }

    #[test]
    fn tracked_streams_stay_within_capacity_evicting_the_least_recent() {
        let mut replay = ReplayWindows::new(2 * IDENTITIES);
        let origins: Vec<_> = (0..3).map(|n| NodeId::new(format!("o{n}"))).collect();
        assert!(replay.first_sighting(&origins[0], id(1, 1)));
        assert!(replay.first_sighting(&origins[1], id(1, 1)));
        // Activity keeps the first stream; the second is now the oldest.
        assert!(!replay.first_sighting(&origins[0], id(1, 1)));
        assert!(replay.first_sighting(&origins[2], id(1, 1)));
        assert_eq!(replay.streams(), 2, "bounded");
        assert!(!replay.first_sighting(&origins[0], id(1, 1)), "retained");
        assert!(!replay.first_sighting(&origins[2], id(1, 1)), "retained");
        // The evicted stream starts a fresh window.
        assert!(replay.first_sighting(&origins[1], id(1, 1)));
        assert_eq!(replay.streams(), 2);
    }
}
