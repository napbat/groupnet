use std::pin::Pin;
use std::task::{Context, Poll};

use super::*;

#[derive(Default)]
struct ShortWriter {
    bytes: Vec<u8>,
    chunk: usize,
    vectored: bool,
    vectored_calls: usize,
}

impl AsyncWrite for ShortWriter {
    fn poll_write(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        let n = bytes.len().min(self.chunk);
        self.bytes.extend_from_slice(&bytes[..n]);
        Poll::Ready(Ok(n))
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.vectored_calls += 1;
        let mut remaining = self.chunk;
        for slice in slices {
            let n = slice.len().min(remaining);
            self.bytes.extend_from_slice(&slice[..n]);
            remaining -= n;
        }
        Poll::Ready(Ok(self.chunk - remaining))
    }

    fn is_write_vectored(&self) -> bool {
        self.vectored
    }

    fn poll_flush(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }
}

#[tokio::test]
async fn short_writes_preserve_network_endian_header_and_payload_boundaries() {
    for vectored in [false, true] {
        for chunk in 1..=9 {
            let mut writer = ShortWriter {
                chunk,
                vectored,
                ..ShortWriter::default()
            };
            for payload in [b"".as_slice(), b"abcdefg".as_slice(), b"next".as_slice()] {
                write_frame(&mut writer, payload)
                    .await
                    .expect("write frame");
            }
            assert_eq!(writer.bytes, b"\0\0\0\0\0\0\0\x07abcdefg\0\0\0\x04next");
            assert_eq!(writer.vectored_calls > 0, vectored);
            let mut input = writer.bytes.as_slice();
            assert_eq!(
                read_frame(&mut input, 7).await.expect("empty"),
                Some(Bytes::new())
            );
            assert_eq!(
                read_frame(&mut input, 7).await.expect("body"),
                Some(Bytes::from_static(b"abcdefg"))
            );
            assert_eq!(
                read_frame(&mut input, 7).await.expect("next"),
                Some(Bytes::from_static(b"next"))
            );
            assert_eq!(read_frame(&mut input, 7).await.expect("EOF"), None);
        }
    }
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
            read_frame(&mut input, 8)
                .await
                .expect_err("truncated")
                .kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    let mut oversized = b"\0\0\0\x09untouched".as_slice();
    assert_eq!(
        read_frame(&mut oversized, 8)
            .await
            .expect_err("oversized")
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(oversized, b"untouched");
}

#[tokio::test]
async fn zero_progress_is_an_error_in_both_writer_modes() {
    for vectored in [false, true] {
        let mut writer = ShortWriter {
            vectored,
            ..ShortWriter::default()
        };
        assert_eq!(
            write_frame(&mut writer, b"x")
                .await
                .expect_err("no progress")
                .kind(),
            io::ErrorKind::WriteZero
        );
    }
}
