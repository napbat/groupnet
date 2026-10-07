//! Boundedness, segmentation, wake hysteresis, half-close and drop contracts.

use std::{
    io,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll, Wake, Waker},
};

use bytes::Bytes;
use groupnet_core::NodeId;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

use super::{SegmentEnd, Taken, TlsEnd, pair};
use crate::{Router, RouterConfig, tunnel::wire::HEADER};

const CAPACITY: usize = 32;
const PAYLOAD: usize = 12;

#[derive(Default)]
struct Wakes(AtomicUsize);

impl Wake for Wakes {
    fn wake(self: Arc<Self>) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}

struct Fixture {
    router: Router,
    tls: TlsEnd,
    segments: SegmentEnd,
    wakes: Arc<Wakes>,
    waker: Waker,
}

impl Fixture {
    fn new() -> Self {
        let router = Router::new(NodeId::new("local"), RouterConfig::default()).unwrap();
        let (tls, segments) = pair(
            router.tunnel_buffers(&NodeId::new("peer")),
            CAPACITY,
            PAYLOAD,
        );
        let wakes = Arc::new(Wakes::default());
        Self {
            router,
            tls,
            segments,
            waker: Waker::from(wakes.clone()),
            wakes,
        }
    }

    fn wakes(&self) -> usize {
        self.wakes.0.load(Ordering::SeqCst)
    }

    fn write(&mut self, bytes: &[u8]) -> Poll<io::Result<usize>> {
        write(&mut self.tls, &self.waker, bytes)
    }

    fn read(&mut self, length: usize) -> Poll<io::Result<Vec<u8>>> {
        read(&mut self.tls, &self.waker, length)
    }

    fn take(&self, limit: usize) -> Poll<io::Result<Taken>> {
        let mut cx = Context::from_waker(&self.waker);
        self.segments.poll_take(&mut cx, limit)
    }

    fn deliver(&self, data: &Bytes) -> Poll<io::Result<usize>> {
        let mut cx = Context::from_waker(&self.waker);
        self.segments.poll_deliver(&mut cx, data)
    }
}

fn write(tls: &mut TlsEnd, waker: &Waker, bytes: &[u8]) -> Poll<io::Result<usize>> {
    let mut cx = Context::from_waker(waker);
    Pin::new(tls).poll_write(&mut cx, bytes)
}

fn read(tls: &mut TlsEnd, waker: &Waker, length: usize) -> Poll<io::Result<Vec<u8>>> {
    let mut cx = Context::from_waker(waker);
    let mut storage = vec![0; length];
    let mut buffer = ReadBuf::new(&mut storage);
    Pin::new(tls)
        .poll_read(&mut cx, &mut buffer)
        .map_ok(|()| buffer.filled().to_vec())
}

fn ciphertext(taken: Poll<io::Result<Taken>>) -> Vec<u8> {
    match taken {
        Poll::Ready(Ok(Taken::Segment(segment))) => {
            assert_eq!(
                &segment.payload()[..HEADER],
                &[0; HEADER],
                "header placeholder"
            );
            segment.payload()[HEADER..].to_vec()
        }
        other => panic!("expected a segment, got {other:?}"),
    }
}

fn is_eof(read: &Poll<io::Result<Vec<u8>>>) -> bool {
    matches!(read, Poll::Ready(Ok(read)) if read.is_empty())
}

#[tokio::test]
async fn ciphertext_is_cut_into_bounded_segments_and_the_writer_waits_for_half() {
    let mut pipe = Fixture::new();
    let bytes: Vec<u8> = (0..40).collect();
    assert!(pipe.take(PAYLOAD).is_pending());
    assert!(matches!(pipe.write(&bytes), Poll::Ready(Ok(CAPACITY))));
    assert_eq!(pipe.wakes(), 1, "the waiting taker is woken once");
    assert!(pipe.write(&bytes[CAPACITY..]).is_pending(), "full");
    // Segments are whole `PAYLOAD` cuts, the tail shorter.
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), bytes[..12]);
    assert_eq!(
        pipe.wakes(),
        1,
        "20 of 32 bytes still queued: writer not woken"
    );
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), bytes[12..24]);
    assert_eq!(pipe.wakes(), 2, "half free: the writer is woken");
    assert!(matches!(pipe.write(&bytes[CAPACITY..]), Poll::Ready(Ok(8))));
    // The partial tail keeps filling until taken.
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), bytes[24..36]);
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), bytes[36..]);
    assert!(pipe.take(PAYLOAD).is_pending());
    pipe.router.close().await;
}

#[tokio::test]
async fn a_short_limit_splits_the_front_segment_in_order() {
    let mut pipe = Fixture::new();
    assert!(matches!(pipe.write(b"abcdefghij"), Poll::Ready(Ok(10))));
    assert_eq!(ciphertext(pipe.take(4)), b"abcd");
    let taken = pipe.segments.try_take(4).unwrap().expect("ready");
    assert_eq!(ciphertext(Poll::Ready(Ok(taken))), b"efgh");
    assert!(matches!(pipe.write(b"kl"), Poll::Ready(Ok(2))));
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), b"ijkl");
    pipe.router.close().await;
}

#[tokio::test]
async fn shutdown_closes_after_the_last_byte_and_rejects_later_writes() {
    let mut pipe = Fixture::new();
    assert!(matches!(pipe.write(b"last"), Poll::Ready(Ok(4))));
    let mut cx = Context::from_waker(&pipe.waker);
    assert!(matches!(
        Pin::new(&mut pipe.tls).poll_shutdown(&mut cx),
        Poll::Ready(Ok(()))
    ));
    assert_eq!(ciphertext(pipe.take(PAYLOAD)), b"last");
    assert!(matches!(pipe.take(PAYLOAD), Poll::Ready(Ok(Taken::Closed))));
    assert!(matches!(pipe.write(b"x"), Poll::Ready(Err(_))));
    pipe.router.close().await;
}

#[tokio::test]
async fn delivery_is_zero_copy_bounded_and_finishes_after_queued_bytes() {
    let mut pipe = Fixture::new();
    assert!(pipe.read(64).is_pending());
    let segment = Bytes::from((0..40).collect::<Vec<u8>>());
    let Poll::Ready(Ok(queued)) = pipe.deliver(&segment) else {
        panic!("room for the first delivery");
    };
    assert_eq!(queued, CAPACITY, "bounded by capacity");
    assert_eq!(pipe.wakes(), 1, "the waiting reader is woken");
    {
        let state = pipe.segments.shared.state.lock().unwrap();
        assert_eq!(state.inbound.queue[0].as_ptr(), segment.as_ptr(), "no copy");
    }
    let rest = segment.slice(queued..);
    assert!(pipe.deliver(&rest).is_pending(), "full");
    assert!(matches!(pipe.read(10), Poll::Ready(Ok(read)) if read == segment[..10]));
    assert_eq!(
        pipe.wakes(),
        1,
        "22 of 32 bytes still queued: deliverer not woken"
    );
    assert!(matches!(pipe.read(10), Poll::Ready(Ok(read)) if read == segment[10..20]));
    assert_eq!(pipe.wakes(), 2, "half free: the deliverer is woken");
    assert_eq!(pipe.segments.try_deliver(&rest).unwrap(), 8);
    pipe.segments.finish();
    assert!(matches!(pipe.read(64), Poll::Ready(Ok(read)) if read == segment[20..]));
    assert!(is_eof(&pipe.read(64)));
    assert!(pipe.segments.try_deliver(&rest).is_err(), "finished");
    pipe.router.close().await;
}

#[tokio::test]
async fn dropping_the_tls_end_fails_delivery_and_closes_after_queued_ciphertext() {
    let Fixture {
        router,
        mut tls,
        segments,
        wakes,
        waker,
    } = Fixture::new();
    let mut cx = Context::from_waker(&waker);
    assert!(matches!(
        write(&mut tls, &waker, b"queued"),
        Poll::Ready(Ok(6))
    ));
    let full = Bytes::from_static(&[1; CAPACITY]);
    assert!(segments.poll_deliver(&mut cx, &full).is_ready());
    let blocked = Bytes::from_static(b"blocked");
    assert!(segments.poll_deliver(&mut cx, &blocked).is_pending());
    drop(tls);
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "the blocked deliverer is woken"
    );
    assert!(segments.try_deliver(&blocked).is_err());
    assert_eq!(
        ciphertext(segments.poll_take(&mut cx, PAYLOAD)),
        b"queued",
        "already written ciphertext still drains"
    );
    assert!(matches!(
        segments.poll_take(&mut cx, PAYLOAD),
        Poll::Ready(Ok(Taken::Closed))
    ));
    router.close().await;
}

#[tokio::test]
async fn dropping_the_segment_end_fails_writes_and_ends_reads_after_queued_bytes() {
    let Fixture {
        router,
        mut tls,
        segments,
        wakes,
        waker,
    } = Fixture::new();
    let mut cx = Context::from_waker(&waker);
    assert!(write(&mut tls, &waker, &[2; CAPACITY]).is_ready());
    assert!(write(&mut tls, &waker, b"blocked").is_pending());
    let tail = Bytes::from_static(b"tail");
    assert!(segments.poll_deliver(&mut cx, &tail).is_ready());
    drop(segments);
    assert_eq!(
        wakes.0.load(Ordering::SeqCst),
        1,
        "the blocked writer is woken"
    );
    assert!(matches!(write(&mut tls, &waker, b"x"), Poll::Ready(Err(_))));
    assert!(matches!(read(&mut tls, &waker, 64), Poll::Ready(Ok(read)) if read == b"tail"));
    assert!(is_eof(&read(&mut tls, &waker, 64)));
    router.close().await;
}
