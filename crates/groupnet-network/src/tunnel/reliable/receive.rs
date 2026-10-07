//! Receive-side reordering, delivery buffering and reserved byte credit.
//!
//! Invariant: `memory >= delivery_cost + committed`, where `committed` is the
//! credit beyond the cumulative acknowledgement and covers every reordered
//! segment. The right edge (acknowledged position plus `committed`) only moves
//! forward, except when the peer relinquishes it with an in-order `Idle`.

use std::collections::{BTreeMap, VecDeque};

use bytes::Bytes;

use super::{
    super::{
        budget::Reservation,
        wire::{HEADER, Kind, Packet},
    },
    Segment,
};

/// Reservation growth per in-order byte received: credit grows eightfold per
/// round trip, so a receiver starting at its floor outpaces a sender's slow
/// start.
pub(super) const CREDIT_GROWTH: usize = 7;

/// The next delivery step toward TLS.
pub(super) enum Delivery {
    /// The undelivered rest of the front segment, sharing its storage.
    Data(Bytes),
    Finish,
    Empty,
}

#[derive(Debug)]
pub(super) struct Receiver {
    next: u64,
    reordered: BTreeMap<u64, Segment>,
    reordered_cost: usize,
    delivery: VecDeque<Segment>,
    delivery_cost: usize,
    offset: usize,
    committed: usize,
    memory: Reservation,
    max_window: usize,
    finished: bool,
}

impl Receiver {
    pub(super) fn new(memory: Reservation, max_window: usize) -> Self {
        Self {
            next: 0,
            reordered: BTreeMap::new(),
            reordered_cost: 0,
            delivery: VecDeque::new(),
            delivery_cost: 0,
            offset: 0,
            committed: memory.bytes(),
            memory,
            max_window,
            finished: false,
        }
    }

    /// The cumulative acknowledgement: the next in-order sequence.
    pub(super) fn next(&self) -> u64 {
        self.next
    }

    /// Advertised credit beyond [`next`](Self::next), backed by reservation.
    pub(super) fn credit(&self) -> u32 {
        u32::try_from(self.committed).expect("credit bounded by the u32 max_window")
    }

    /// Accepts a sequenced segment within credit; discards duplicates and
    /// segments beyond credit. Returns whether it arrived in order with no gap
    /// behind it, so its acknowledgement may be coalesced; duplicates,
    /// out-of-order and gap-filling segments warrant an immediate one.
    pub(super) fn accept(&mut self, packet: Packet) -> bool {
        let cost = packet.encoded.len();
        if self.finished
            || packet.sequence < self.next
            || self.reordered.contains_key(&packet.sequence)
            || self.reordered_cost + cost > self.committed
        {
            return false;
        }
        let in_order = packet.sequence == self.next && self.reordered.is_empty();
        self.reordered_cost += cost;
        self.reordered.insert(
            packet.sequence,
            Segment {
                kind: packet.kind,
                payload: packet.encoded,
                start: HEADER,
                sampled_at: None,
            },
        );
        let mut advanced = 0;
        while let Some(segment) = self.reordered.remove(&self.next) {
            self.next += 1;
            let cost = segment.cost();
            self.reordered_cost -= cost;
            self.committed -= cost;
            match segment.kind {
                Kind::Idle => {
                    self.release();
                    advanced = 0;
                }
                Kind::Fin => {
                    // FIN consumes a sequence number and terminates only this
                    // direction, never the reciprocal stream.
                    self.delivery_cost += cost;
                    self.delivery.push_back(segment);
                    self.finished = true;
                    self.discard_reordered();
                    break;
                }
                _ => {
                    advanced += cost;
                    self.delivery_cost += cost;
                    self.delivery.push_back(segment);
                }
            }
        }
        if advanced != 0 {
            // Growing by a multiple of what arrived in order keeps credit
            // ahead of the sender's slow start even from the floor.
            let target = (self.memory.bytes() + CREDIT_GROWTH * advanced).min(self.max_window);
            self.memory.grow_to(target);
            self.reopen();
        }
        in_order
    }

    pub(super) fn delivery(&self) -> Delivery {
        match self.delivery.front() {
            Some(segment) if segment.kind == Kind::Fin => Delivery::Finish,
            Some(segment) => Delivery::Data(segment.payload.slice(segment.start + self.offset..)),
            None => Delivery::Empty,
        }
    }

    /// Records bytes written to TLS; returns whether a segment was released.
    pub(super) fn delivered(&mut self, written: usize) -> bool {
        self.offset += written;
        if self
            .delivery
            .front()
            .is_some_and(|segment| segment.start + self.offset == segment.payload.len())
        {
            self.offset = 0;
            self.pop();
            return true;
        }
        false
    }

    /// Records the delivered FIN.
    pub(super) fn finish_delivered(&mut self) {
        self.pop();
    }

    fn pop(&mut self) {
        if let Some(segment) = self.delivery.pop_front() {
            self.delivery_cost -= segment.cost();
            self.reopen();
        }
    }

    /// Extends the right edge to what the reservation backs.
    fn reopen(&mut self) {
        self.committed = self.committed.max(self.memory.bytes() - self.delivery_cost);
    }

    /// The peer relinquished its credit: nothing is in flight beyond `next`, so
    /// the reservation returns to the floor plus what is still buffered.
    fn release(&mut self) {
        self.discard_reordered();
        self.memory.shrink_to(self.delivery_cost);
        self.committed = self.memory.bytes() - self.delivery_cost;
    }

    fn discard_reordered(&mut self) {
        self.reordered.clear();
        self.reordered_cost = 0;
    }

    /// Bytes reserved for receiving.
    pub(super) fn reserved(&self) -> usize {
        self.memory.bytes()
    }
}
