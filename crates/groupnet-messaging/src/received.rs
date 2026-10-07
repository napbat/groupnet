//! Acknowledged receive records, indexed by identity and by retention deadline.
//!
//! Sweeping touches only records whose deadline has passed, never the whole
//! table: each record owns exactly one deadline entry, re-keyed lazily when a
//! receipt action or replay refreshed its retention in the meantime.

use super::Record;
use crate::codec::MessageId;
use groupnet_core::NodeId;
use std::{
    cmp::{Ordering, Reverse},
    collections::{BinaryHeap, HashMap, binary_heap::PeekMut},
    sync::{Arc, PoisonError},
};

/// A received record's sender-scoped identity.
pub(super) type Key = (NodeId, MessageId);

#[derive(Debug, Default)]
pub(super) struct Received {
    records: HashMap<Key, Arc<Record>>,
    /// Exactly one entry per record, ordered by its last observed deadline.
    deadlines: BinaryHeap<Reverse<Deadline>>,
}

/// A record's earliest possible retirement or expiry, ordered by time only.
#[derive(Debug)]
struct Deadline {
    at: u64,
    key: Key,
}

impl PartialEq for Deadline {
    fn eq(&self, other: &Self) -> bool {
        self.at == other.at
    }
}

impl Eq for Deadline {}

impl PartialOrd for Deadline {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Deadline {
    fn cmp(&self, other: &Self) -> Ordering {
        self.at.cmp(&other.at)
    }
}

impl Received {
    pub(super) fn len(&self) -> usize {
        self.records.len()
    }

    pub(super) fn get(&self, key: &Key) -> Option<&Arc<Record>> {
        self.records.get(key)
    }

    /// Admits a new record; the caller has checked capacity and absence.
    pub(super) fn insert(&mut self, key: Key, record: Arc<Record>) {
        let at = record
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .retain_until();
        self.deadlines.push(Reverse(Deadline {
            at,
            key: key.clone(),
        }));
        self.records.insert(key, record);
    }

    /// Retires abandoned pending records and evicts expired terminal ones
    /// whose deadline has passed. Records refreshed since their entry was
    /// queued are re-queued at their current deadline instead.
    pub(super) fn sweep(&mut self, now: u64) {
        while let Some(mut next) = self.deadlines.peek_mut() {
            if next.0.at > now {
                break;
            }
            let retained = self.records.get(&next.0.key).and_then(|record| {
                // Receipt state is plain data updated in place: a poisoned
                // lock still holds a consistent value.
                let mut receipt = record.state.lock().unwrap_or_else(PoisonError::into_inner);
                receipt.retire(now);
                (!receipt.expired(now)).then(|| receipt.retain_until())
            });
            // A live record's deadline always lies past `now` after
            // retirement, so re-queueing it ends this sweep's progress on it.
            if let Some(at) = retained {
                next.0.at = at;
            } else {
                let Reverse(Deadline { key, .. }) = PeekMut::pop(next);
                self.records.remove(&key);
            }
        }
    }

    pub(super) fn clear(&mut self) {
        self.records.clear();
        self.deadlines.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Delivery, Outcome, ReceiptState};
    use std::sync::Mutex;

    const RETENTION: u64 = 100;

    fn record(name: &str, id: u8, now: u64) -> (Key, Arc<Record>) {
        let key = (NodeId::new(name), MessageId([id; 16]));
        let record = Arc::new(Record {
            from: key.0.clone(),
            id: key.1,
            delivery: Delivery::Applied,
            retry_horizon_ms: 1,
            group: None,
            fingerprint: [0; 32],
            state: Mutex::new(ReceiptState::new(Delivery::Applied, now, RETENTION)),
        });
        (key, record)
    }

    fn act(received: &Received, key: &Key, outcome: Outcome, now: u64) {
        received
            .get(key)
            .unwrap()
            .state
            .lock()
            .unwrap()
            .act(outcome, now);
    }

    #[test]
    fn sweep_visits_only_due_deadlines_and_requeues_refreshed_records() {
        let mut received = Received::default();
        for (id, now) in [(1, 0), (2, 10), (3, 20)] {
            let (key, record) = record("peer", id, now);
            received.insert(key, record);
        }
        let key = |id| (NodeId::new("peer"), MessageId([id; 16]));
        act(&received, &key(1), Outcome::Applied, 5);
        act(&received, &key(2), Outcome::Applied, 50);
        received.sweep(99);
        assert_eq!(received.len(), 3, "no deadline is due yet");
        // Record 1's queued deadline (100) is stale: applied at 5, it expires at 105.
        received.sweep(104);
        assert_eq!(received.len(), 3);
        assert_eq!(received.deadlines.len(), 3, "one entry per record");
        received.sweep(105);
        assert!(received.get(&key(1)).is_none());
        // Record 2 was refreshed to 150; record 3 is pending past 120, so this
        // sweep retires it as interrupted and retains it for a new horizon.
        received.sweep(149);
        assert_eq!(received.len(), 2);
        let retired = received
            .get(&key(3))
            .unwrap()
            .state
            .lock()
            .unwrap()
            .retain_until();
        assert_eq!(
            retired, 249,
            "abandoned pending work retires, then is retained"
        );
        received.sweep(150);
        assert!(received.get(&key(2)).is_none());
        received.sweep(248);
        assert_eq!(received.len(), 1);
        received.sweep(249);
        assert_eq!(received.len(), 0);
        assert!(received.deadlines.is_empty());
    }

    #[test]
    fn pending_records_are_never_evicted_and_clear_drops_both_indexes() {
        let mut received = Received::default();
        let (key, record) = record("peer", 1, 0);
        received.insert(key.clone(), record);
        // Accepted is progress, not terminal, for Applied delivery.
        act(&received, &key, Outcome::Accepted, 90);
        received.sweep(189);
        assert_eq!(received.len(), 1);
        received.sweep(190);
        assert_eq!(received.len(), 1, "retirement records interruption first");
        received.clear();
        assert_eq!(received.len(), 0);
        assert!(received.deadlines.is_empty());
    }
}
