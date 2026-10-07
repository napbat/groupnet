//! Bounded reuse of routed packet storage.
//!
//! Packet buffers of more than one [`CLASS`] are drawn from per-size-class free
//! lists and return to them when the last [`Bytes`](bytes::Bytes) view of the
//! sent packet drops, so bulk traffic stops round-tripping the heap for every
//! segment. Idle storage is bounded by a byte limit; storage beyond it, and
//! small packets, use the heap directly.

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

/// Size-class granularity; buffers of at most one class are never retained.
const CLASS: usize = 4096;

pub(super) struct PacketPool {
    /// Free storage indexed by capacity class (`capacity / CLASS`).
    classes: Box<[Mutex<Vec<Vec<u8>>>]>,
    /// Capacity bytes currently idle in `classes`.
    retained: AtomicUsize,
    limit: usize,
}

impl std::fmt::Debug for PacketPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PacketPool")
            .field("retained", &self.retained.load(Ordering::Relaxed))
            .field("limit", &self.limit)
            .finish_non_exhaustive()
    }
}

impl PacketPool {
    /// Retains at most `limit` idle bytes of storage for packets up to
    /// `max_frame` bytes.
    pub(super) fn new(limit: usize, max_frame: usize) -> Self {
        Self {
            classes: (0..=max_frame.div_ceil(CLASS))
                .map(|_| Mutex::new(Vec::new()))
                .collect(),
            retained: AtomicUsize::new(0),
            limit,
        }
    }

    /// Empty storage with room for `length` bytes: retained storage of the
    /// same class when available, otherwise a fresh allocation.
    pub(super) fn take(&self, length: usize) -> Vec<u8> {
        let class = length.div_ceil(CLASS);
        if class < 2 {
            return Vec::with_capacity(length);
        }
        if let Some(storage) = self
            .classes
            .get(class)
            .and_then(|free| free.lock().ok()?.pop())
        {
            self.retained
                .fetch_sub(storage.capacity(), Ordering::Relaxed);
            return storage;
        }
        Vec::with_capacity(class * CLASS)
    }

    /// Keeps `storage` for reuse within the byte limit, otherwise frees it.
    pub(super) fn give(&self, mut storage: Vec<u8>) {
        let capacity = storage.capacity();
        let Some(free) = self
            .classes
            .get(capacity / CLASS)
            .filter(|_| Self::pooled(capacity))
        else {
            return;
        };
        if self.retained.fetch_add(capacity, Ordering::Relaxed) + capacity > self.limit {
            self.retained.fetch_sub(capacity, Ordering::Relaxed);
            return;
        }
        storage.clear();
        match free.lock() {
            Ok(mut free) => free.push(storage),
            Err(_) => {
                self.retained.fetch_sub(capacity, Ordering::Relaxed);
            }
        }
    }

    /// Whether storage of `capacity` bytes belongs in a size class; smaller
    /// storage is never worth a shared owner.
    pub(super) fn pooled(capacity: usize) -> bool {
        capacity >= 2 * CLASS
    }

    #[cfg(test)]
    fn retained(&self) -> usize {
        self.retained.load(Ordering::Relaxed)
    }
}

/// Sent packet storage that returns to its pool when the last view drops.
pub(super) struct Recycled {
    pub(super) storage: Vec<u8>,
    pub(super) pool: Arc<PacketPool>,
}

impl AsRef<[u8]> for Recycled {
    fn as_ref(&self) -> &[u8] {
        &self.storage
    }
}

impl Drop for Recycled {
    fn drop(&mut self) {
        self.pool.give(std::mem::take(&mut self.storage));
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn storage_is_reused_within_its_class_and_small_packets_bypass_the_pool() {
        let pool = PacketPool::new(1 << 20, 65_536);
        let mut storage = pool.take(16 * 1024 + 80);
        assert_eq!(storage.capacity(), 5 * CLASS, "rounded up to its class");
        storage.extend_from_slice(&[1; 100]);
        let address = storage.as_ptr();
        pool.give(storage);
        assert_eq!(pool.retained(), 5 * CLASS);
        let reused = pool.take(17 * 1024);
        assert_eq!(reused.as_ptr(), address, "same class reuses the storage");
        assert_eq!(reused.len(), 0, "cleared");
        assert_eq!(pool.retained(), 0);
        pool.give(pool.take(100));
        assert_eq!(pool.retained(), 0, "one class or less is never retained");
        pool.give(reused);
        let other = pool.take(9 * 1024);
        assert_ne!(other.as_ptr(), address, "other classes allocate");
    }

    #[test]
    fn idle_storage_never_exceeds_the_byte_limit() {
        let pool = PacketPool::new(3 * 5 * CLASS, 65_536);
        let buffers: Vec<_> = (0..8).map(|_| pool.take(5 * CLASS)).collect();
        for storage in buffers {
            pool.give(storage);
            assert!(pool.retained() <= 3 * 5 * CLASS);
        }
        assert_eq!(pool.retained(), 3 * 5 * CLASS);
        let disabled = PacketPool::new(0, 65_536);
        disabled.give(disabled.take(5 * CLASS));
        assert_eq!(disabled.retained(), 0);
    }
}
