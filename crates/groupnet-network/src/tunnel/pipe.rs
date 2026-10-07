//! The bounded segment pipe between a session's TLS stream and its reliability
//! task.
//!
//! TLS writes ciphertext straight into routing segment buffers, which
//! reliability stamps and transmits without another copy. Received segments
//! queue as shared [`Bytes`] slices that TLS reads from directly. Each
//! direction holds at most `capacity` ciphertext bytes. A side waiting for the
//! other is woken once per wait: a consumer as soon as bytes arrive, a producer
//! only once half the capacity is free, so a bulk transfer moves several
//! segments per wake instead of ping-ponging on every read.

use std::{
    collections::VecDeque,
    fmt,
    io::{self, IoSlice},
    pin::Pin,
    sync::{Arc, Mutex, MutexGuard},
    task::{Context, Poll, Waker},
};

use bytes::{Buf, Bytes};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::wire::{HEADER, cost};
use crate::{PacketBuffer, router::TunnelBuffers};

/// Creates a connected pair holding at most `capacity` ciphertext bytes per
/// direction, cutting outbound ciphertext into segments of `payload` bytes.
pub(super) fn pair(
    buffers: TunnelBuffers,
    capacity: usize,
    payload: usize,
) -> (TlsEnd, SegmentEnd) {
    let shared = Arc::new(Shared {
        buffers,
        capacity,
        payload,
        state: Mutex::new(State::default()),
    });
    (
        TlsEnd {
            shared: shared.clone(),
        },
        SegmentEnd { shared },
    )
}

/// What reliability takes from the pipe.
#[derive(Debug)]
pub(super) enum Taken {
    /// A segment: header placeholder, then ciphertext.
    Segment(PacketBuffer),
    /// TLS closed its write direction and every byte before it was taken.
    Closed,
}

struct Shared {
    buffers: TunnelBuffers,
    capacity: usize,
    payload: usize,
    state: Mutex<State>,
}

impl Shared {
    fn lock(&self) -> io::Result<MutexGuard<'_, State>> {
        self.state
            .lock()
            .map_err(|_| io::Error::other("tunnel pipe lock poisoned"))
    }

    /// An empty segment: routing headroom, a header placeholder, and capacity
    /// for `ciphertext` bytes.
    fn segment(&self, ciphertext: usize) -> io::Result<PacketBuffer> {
        let mut buffer = self.buffers.get(cost(ciphertext))?;
        buffer.extend_from_slice(&[0; HEADER]);
        Ok(buffer)
    }

    /// The waiting producer of `direction`, once half the capacity is free.
    fn producer_due<T>(&self, direction: &mut Direction<T>) -> Option<Waker> {
        if self.capacity - direction.bytes >= self.capacity.div_ceil(2) {
            direction.producer.take()
        } else {
            None
        }
    }
}

#[derive(Default)]
struct State {
    /// TLS ciphertext toward reliability; only the last segment may be partial.
    outbound: Direction<PacketBuffer>,
    /// Received ciphertext toward TLS.
    inbound: Direction<Bytes>,
}

/// One direction's queue. Its producer closes it after the last byte; its
/// consumer abandons it by dropping its end.
struct Direction<T> {
    queue: VecDeque<T>,
    /// Ciphertext bytes queued, excluding segment header placeholders.
    bytes: usize,
    /// No further bytes will be produced.
    closed: bool,
    /// The consumer is gone: producing fails.
    abandoned: bool,
    /// The producer waiting for space.
    producer: Option<Waker>,
    /// The consumer waiting for bytes.
    consumer: Option<Waker>,
}

impl<T> Default for Direction<T> {
    fn default() -> Self {
        Self {
            queue: VecDeque::new(),
            bytes: 0,
            closed: false,
            abandoned: false,
            producer: None,
            consumer: None,
        }
    }
}

fn wake(waker: Option<Waker>) {
    if let Some(waker) = waker {
        waker.wake();
    }
}

fn broken() -> io::Error {
    io::Error::new(io::ErrorKind::BrokenPipe, "tunnel session closed")
}

/// The TLS side: ciphertext out, received ciphertext in.
pub(super) struct TlsEnd {
    shared: Arc<Shared>,
}

impl fmt::Debug for TlsEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TlsEnd").finish_non_exhaustive()
    }
}

impl TlsEnd {
    fn write(&self, cx: &mut Context<'_>, parts: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
        let total: usize = parts.iter().map(|part| part.len()).sum();
        if total == 0 {
            return Poll::Ready(Ok(0));
        }
        let shared = &*self.shared;
        let mut state = shared.lock()?;
        let outbound = &mut state.outbound;
        if outbound.abandoned || outbound.closed {
            return Poll::Ready(Err(broken()));
        }
        let budget = (shared.capacity - outbound.bytes).min(total);
        if budget == 0 {
            outbound.producer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        let mut written = 0;
        'parts: for part in parts {
            let mut part: &[u8] = part;
            while !part.is_empty() && written < budget {
                let room = outbound
                    .queue
                    .back()
                    .map_or(0, |segment| shared.payload + HEADER - segment.len());
                if room == 0 {
                    match shared.segment(shared.payload) {
                        Ok(segment) => outbound.queue.push_back(segment),
                        Err(error) if written == 0 => return Poll::Ready(Err(error)),
                        Err(_) => break 'parts,
                    }
                    continue;
                }
                let length = room.min(part.len()).min(budget - written);
                let tail = outbound.queue.back_mut().expect("tail segment has room");
                tail.extend_from_slice(&part[..length]);
                part = &part[length..];
                written += length;
            }
        }
        outbound.bytes += written;
        let taker = outbound.consumer.take();
        drop(state);
        wake(taker);
        Poll::Ready(Ok(written))
    }
}

impl AsyncWrite for TlsEnd {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.write(cx, &[IoSlice::new(bytes)])
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        parts: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.write(cx, parts)
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    /// Written ciphertext is visible to reliability immediately.
    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    /// Closes the write direction: reliability sends FIN after the last byte.
    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        let mut state = self.shared.lock()?;
        state.outbound.closed = true;
        let taker = state.outbound.consumer.take();
        drop(state);
        wake(taker);
        Poll::Ready(Ok(()))
    }
}

impl AsyncRead for TlsEnd {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let shared = &*self.shared;
        let mut state = shared.lock()?;
        let inbound = &mut state.inbound;
        if inbound.queue.is_empty() {
            if inbound.closed {
                return Poll::Ready(Ok(()));
            }
            inbound.consumer = Some(cx.waker().clone());
            return Poll::Pending;
        }
        while buffer.remaining() != 0
            && let Some(front) = inbound.queue.front_mut()
        {
            let length = front.len().min(buffer.remaining());
            buffer.put_slice(&front[..length]);
            front.advance(length);
            inbound.bytes -= length;
            if front.is_empty() {
                inbound.queue.pop_front();
            }
        }
        let deliverer = shared.producer_due(inbound);
        drop(state);
        wake(deliverer);
        Poll::Ready(Ok(()))
    }
}

impl Drop for TlsEnd {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.outbound.closed = true;
            state.inbound.abandoned = true;
            let wakers = [
                state.outbound.consumer.take(),
                state.inbound.producer.take(),
            ];
            drop(state);
            wakers.into_iter().for_each(wake);
        }
    }
}

/// The reliability side: segments out, received ciphertext in.
pub(super) struct SegmentEnd {
    shared: Arc<Shared>,
}

impl fmt::Debug for SegmentEnd {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SegmentEnd").finish_non_exhaustive()
    }
}

impl SegmentEnd {
    /// Waits for a segment of at most `limit` ciphertext bytes.
    pub(super) fn poll_take(&self, cx: &mut Context<'_>, limit: usize) -> Poll<io::Result<Taken>> {
        let mut state = self.shared.lock()?;
        let (taken, writer) = self.take(&mut state.outbound, limit)?;
        if taken.is_none() {
            state.outbound.consumer = Some(cx.waker().clone());
        }
        drop(state);
        wake(writer);
        taken.map_or(Poll::Pending, |taken| Poll::Ready(Ok(taken)))
    }

    /// Takes a segment of at most `limit` ciphertext bytes if one is ready.
    pub(super) fn try_take(&self, limit: usize) -> io::Result<Option<Taken>> {
        let mut state = self.shared.lock()?;
        let (taken, writer) = self.take(&mut state.outbound, limit)?;
        drop(state);
        wake(writer);
        Ok(taken)
    }

    /// The next segment, if any, and the writer to wake once unlocked.
    fn take(
        &self,
        outbound: &mut Direction<PacketBuffer>,
        limit: usize,
    ) -> io::Result<(Option<Taken>, Option<Waker>)> {
        let shared = &*self.shared;
        let Some(front) = outbound.queue.front_mut() else {
            return Ok((outbound.closed.then_some(Taken::Closed), None));
        };
        let segment = if front.len() - HEADER <= limit {
            outbound.queue.pop_front().expect("front segment")
        } else {
            // Only a sender with nothing unacknowledged takes less than a
            // whole segment; copying the split is rare.
            let mut head = shared.segment(limit)?;
            let mut rest = shared.segment(shared.payload)?;
            head.extend_from_slice(&front.payload()[HEADER..HEADER + limit]);
            rest.extend_from_slice(&front.payload()[HEADER + limit..]);
            *front = rest;
            head
        };
        outbound.bytes -= segment.len() - HEADER;
        let writer = shared.producer_due(outbound);
        Ok((Some(Taken::Segment(segment)), writer))
    }

    /// Waits for room, then queues a prefix of `data` toward TLS without
    /// copying it; returns the bytes queued.
    pub(super) fn poll_deliver(
        &self,
        cx: &mut Context<'_>,
        data: &Bytes,
    ) -> Poll<io::Result<usize>> {
        let mut state = self.shared.lock()?;
        let (queued, reader) = self.deliver(&mut state.inbound, data)?;
        if queued == 0 {
            state.inbound.producer = Some(cx.waker().clone());
        }
        drop(state);
        wake(reader);
        if queued == 0 {
            Poll::Pending
        } else {
            Poll::Ready(Ok(queued))
        }
    }

    /// Queues a prefix of `data` toward TLS if there is room; returns the
    /// bytes queued, zero when full.
    pub(super) fn try_deliver(&self, data: &Bytes) -> io::Result<usize> {
        let mut state = self.shared.lock()?;
        let (queued, reader) = self.deliver(&mut state.inbound, data)?;
        drop(state);
        wake(reader);
        Ok(queued)
    }

    /// Queues a prefix of `data`; returns its length and the reader to wake.
    fn deliver(
        &self,
        inbound: &mut Direction<Bytes>,
        data: &Bytes,
    ) -> io::Result<(usize, Option<Waker>)> {
        if inbound.abandoned || inbound.closed {
            return Err(broken());
        }
        let length = (self.shared.capacity - inbound.bytes).min(data.len());
        if length == 0 {
            return Ok((0, None));
        }
        inbound.queue.push_back(data.slice(..length));
        inbound.bytes += length;
        Ok((length, inbound.consumer.take()))
    }

    /// Ends the TLS read direction after the bytes already queued.
    pub(super) fn finish(&self) {
        if let Ok(mut state) = self.shared.lock() {
            state.inbound.closed = true;
            let reader = state.inbound.consumer.take();
            drop(state);
            wake(reader);
        }
    }
}

impl Drop for SegmentEnd {
    fn drop(&mut self) {
        if let Ok(mut state) = self.shared.state.lock() {
            state.outbound.abandoned = true;
            state.inbound.closed = true;
            let wakers = [
                state.outbound.producer.take(),
                state.inbound.consumer.take(),
            ];
            drop(state);
            wakers.into_iter().for_each(wake);
        }
    }
}

#[cfg(test)]
mod tests;
