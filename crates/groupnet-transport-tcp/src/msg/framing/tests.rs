use std::io::IoSlice;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncWrite, ReadBuf};

use super::*;

/// Accepts at most `chunk` bytes per write, across vectored slices, and counts
/// write calls.
#[derive(Default)]
struct ShortWriter {
    bytes: Vec<u8>,
    chunk: usize,
    calls: usize,
}

impl AsyncWrite for ShortWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        let n = bytes.len().min(self.chunk);
        self.bytes.extend_from_slice(&bytes[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        let mut remaining = self.chunk;
        for slice in slices {
            let n = slice.len().min(remaining);
            self.bytes.extend_from_slice(&slice[..n]);
            remaining -= n;
        }
        Poll::Ready(Ok(self.chunk - remaining))
    }

    fn is_write_vectored(&self) -> bool {
        true
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

/// Serves at most `chunk` bytes per read and counts read calls.
struct ChunkedReader<'a> {
    input: &'a [u8],
    chunk: usize,
    calls: usize,
}

impl tokio::io::AsyncRead for ChunkedReader<'_> {
    fn poll_read(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        buffer: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.calls += 1;
        let n = self.input.len().min(self.chunk).min(buffer.remaining());
        buffer.put_slice(&self.input[..n]);
        self.input = &self.input[n..];
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn short_writes_and_reads_preserve_network_endian_header_and_payload_boundaries() {
    for chunk in 1..=9 {
        let mut writer = ShortWriter {
            chunk,
            ..ShortWriter::default()
        };
        for payload in [b"".as_slice(), b"abcdefg".as_slice(), b"next".as_slice()] {
            write_frame(&mut writer, payload)
                .await
                .expect("write frame");
        }
        assert_eq!(writer.bytes, b"\0\0\0\0\0\0\0\x07abcdefg\0\0\0\x04next");
        let mut input = ChunkedReader {
            input: &writer.bytes,
            chunk,
            calls: 0,
        };
        let mut reader = FrameReader::new(7);
        for expected in [b"".as_slice(), b"abcdefg".as_slice(), b"next".as_slice()] {
            assert_eq!(
                reader.next(&mut input).await.expect("frame"),
                Some(Bytes::copy_from_slice(expected))
            );
        }
        assert_eq!(reader.next(&mut input).await.expect("EOF"), None);
    }
}

#[tokio::test]
async fn queued_frames_share_one_vectored_write_and_one_read() {
    let frames: Vec<Bytes> = (1..=WRITE_BATCH + 3)
        .map(|length| Bytes::from(vec![u8::try_from(length).expect("small"); length]))
        .collect();
    let mut writer = ShortWriter {
        chunk: usize::MAX,
        ..ShortWriter::default()
    };
    write_frames(&mut writer, &frames).await.expect("write");
    // The shared writer submits at most 16 descriptors (8 frames) per call.
    assert_eq!(writer.calls, frames.len().div_ceil(8));
    let mut input = ChunkedReader {
        input: &writer.bytes,
        chunk: usize::MAX,
        calls: 0,
    };
    let mut reader = FrameReader::new(1024);
    for frame in &frames {
        assert_eq!(
            reader.next(&mut input).await.expect("frame").as_ref(),
            Some(frame)
        );
    }
    assert_eq!(input.calls, 1, "one read carried every queued frame");
    assert_eq!(reader.next(&mut input).await.expect("EOF"), None);
}

#[tokio::test]
async fn batches_take_only_queued_frames_up_to_the_bound() {
    let (sender, mut queue) = mpsc::channel(WRITE_BATCH * 2);
    for index in 0..=WRITE_BATCH {
        sender
            .try_send(Bytes::from(vec![0; index]))
            .expect("capacity");
    }
    let mut batch = Vec::new();
    assert!(next_batch(&mut queue, &mut batch).await);
    assert_eq!(batch.len(), WRITE_BATCH);
    extend_ready(&mut queue, &mut batch);
    assert_eq!(batch.len(), WRITE_BATCH, "a full batch takes nothing more");
    batch.clear();
    extend_ready(&mut queue, &mut batch);
    assert_eq!(batch.len(), 1);
    drop(sender);
    assert!(!next_batch(&mut queue, &mut batch).await);
    assert_eq!(batch, Vec::<Bytes>::new());
}

#[tokio::test(flavor = "current_thread")]
async fn a_burst_yields_once_so_producers_extend_it_but_a_lone_frame_does_not_wait() {
    let (sender, mut queue) = mpsc::channel(WRITE_BATCH);
    let mut batch = Vec::new();
    sender
        .try_send(Bytes::from_static(b"lone"))
        .expect("capacity");
    let late = sender.clone();
    let producer = tokio::spawn(async move {
        late.try_send(Bytes::from_static(b"late"))
            .expect("capacity");
    });
    assert!(next_batch(&mut queue, &mut batch).await);
    assert_eq!(
        batch,
        [Bytes::from_static(b"lone")],
        "no yield for one frame"
    );
    producer.await.expect("producer");
    sender
        .try_send(Bytes::from_static(b"burst"))
        .expect("capacity");
    let late = sender.clone();
    let producer = tokio::spawn(async move {
        late.try_send(Bytes::from_static(b"joined"))
            .expect("capacity");
    });
    assert!(next_batch(&mut queue, &mut batch).await);
    assert_eq!(
        batch,
        [
            Bytes::from_static(b"late"),
            Bytes::from_static(b"burst"),
            Bytes::from_static(b"joined")
        ],
        "the producer scheduled during the yield joined the burst"
    );
    producer.await.expect("producer");
}

#[tokio::test]
async fn malformed_or_oversized_frames_fail_before_body_allocation() {
    for partial in [
        b"\0".as_slice(),
        b"\0\0".as_slice(),
        b"\0\0\0".as_slice(),
        b"\0\0\0\x02x".as_slice(),
    ] {
        let mut input = partial;
        assert_eq!(
            FrameReader::new(8)
                .next(&mut input)
                .await
                .expect_err("truncated")
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    let mut oversized = b"\x7f\xff\xff\xffuntouched".as_slice();
    let mut reader = FrameReader::new(8);
    assert_eq!(
        reader
            .next(&mut oversized)
            .await
            .expect_err("oversized")
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert!(
        reader.buffer.capacity() <= READ_AHEAD,
        "no storage was reserved for the declared payload"
    );
}

#[tokio::test]
async fn zero_progress_is_an_error() {
    let mut writer = ShortWriter::default();
    assert_eq!(
        write_frame(&mut writer, b"x")
            .await
            .expect_err("no progress")
            .kind(),
        io::ErrorKind::WriteZero
    );
}
