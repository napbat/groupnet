use super::*;

/// A writer accepting at most `chunk` bytes per call, spread across the
/// offered slices in order, with scripted failures.
struct Chunked {
    written: Vec<u8>,
    chunk: usize,
    vectored: bool,
    calls: usize,
    max_slices_seen: usize,
    interrupt_first: bool,
    stall: bool,
}

impl Chunked {
    fn new(chunk: usize, vectored: bool) -> Self {
        Self {
            written: Vec::new(),
            chunk,
            vectored,
            calls: 0,
            max_slices_seen: 0,
            interrupt_first: false,
            stall: false,
        }
    }
}

impl AsyncWrite for Chunked {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write_vectored(cx, &[IoSlice::new(buf)])
    }

    fn poll_write_vectored(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
        slices: &[IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        self.calls += 1;
        self.max_slices_seen = self.max_slices_seen.max(slices.len());
        if self.interrupt_first && self.calls == 1 {
            return Poll::Ready(Err(io::ErrorKind::Interrupted.into()));
        }
        if self.stall {
            return Poll::Ready(Ok(0));
        }
        let offered = if self.vectored { slices } else { &slices[..1] };
        let mut budget = self.chunk;
        let mut written = 0;
        for slice in offered {
            let take = budget.min(slice.len());
            self.written.extend_from_slice(&slice[..take]);
            written += take;
            budget -= take;
            if budget == 0 {
                break;
            }
        }
        Poll::Ready(Ok(written))
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

fn joined(parts: &[&[u8]]) -> Vec<u8> {
    parts.concat()
}

#[test]
fn partial_vectored_writes_resume_inside_any_part() {
    let parts: [&[u8]; 4] = [b"head", b"", b"payload-bytes", b"tag"];
    for chunk in 1..=joined(&parts).len() {
        for vectored in [true, false] {
            let mut writer = Chunked::new(chunk, vectored);
            futures::executor::block_on(write_vectored(&mut writer, &parts)).unwrap();
            assert_eq!(writer.written, joined(&parts), "chunk {chunk}");
        }
    }
}

#[test]
fn whole_frame_is_one_vectored_call_when_the_writer_accepts_it() {
    let parts: [&[u8]; 3] = [b"len!", b"head", b"body"];
    let mut writer = Chunked::new(usize::MAX, true);
    futures::executor::block_on(write_vectored(&mut writer, &parts)).unwrap();
    assert_eq!(writer.calls, 1);
    assert_eq!(writer.written, joined(&parts));
}

#[test]
fn long_part_lists_are_batched_without_losing_order() {
    let bytes: Vec<[u8; 1]> = (0..=40u8).map(|byte| [byte]).collect();
    let parts: Vec<&[u8]> = bytes.iter().map(<[u8; 1]>::as_slice).collect();
    let mut writer = Chunked::new(usize::MAX, true);
    futures::executor::block_on(write_vectored(&mut writer, &parts)).unwrap();
    assert_eq!(writer.written, joined(&parts));
    assert_eq!(writer.max_slices_seen, MAX_IO_SLICES);
    assert_eq!(writer.calls, parts.len().div_ceil(MAX_IO_SLICES));
}

#[test]
fn empty_frames_write_nothing_and_interrupts_are_retried() {
    let mut writer = Chunked::new(1, true);
    futures::executor::block_on(write_vectored(&mut writer, &[&[], &[]])).unwrap();
    assert_eq!(writer.calls, 0);

    let mut writer = Chunked::new(2, true);
    writer.interrupt_first = true;
    futures::executor::block_on(write_vectored(&mut writer, &[b"abc".as_slice()])).unwrap();
    assert_eq!(writer.written, b"abc");
}

#[test]
fn a_stalled_writer_is_write_zero() {
    let mut writer = Chunked::new(1, true);
    writer.stall = true;
    let error =
        futures::executor::block_on(write_vectored(&mut writer, &[b"x".as_slice()])).unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::WriteZero);
}

#[test]
fn length_header_is_four_big_endian_bytes() {
    let header = LengthHeader::new(0x0102_0304).unwrap();
    assert_eq!(LengthHeader::SIZE, 4);
    assert_eq!(header.as_bytes(), &[1, 2, 3, 4]);
    let parsed = LengthHeader::read_from_bytes(&[1, 2, 3, 4]).unwrap();
    assert_eq!(parsed, header);
    assert_eq!(parsed.get(), 0x0102_0304);
    assert_eq!(
        LengthHeader::for_parts(&[b"ab".as_slice(), b"", b"cde"])
            .unwrap()
            .get(),
        5
    );
}

#[test]
fn length_header_bounds_encode_and_decode() {
    assert!(LengthHeader::new(MAX_FRAME_BYTES).is_ok());
    assert_eq!(
        LengthHeader::new(MAX_FRAME_BYTES + 1).unwrap_err().kind(),
        io::ErrorKind::InvalidInput
    );
    let header = LengthHeader::new(100).unwrap();
    assert_eq!(header.length_within(100).unwrap(), 100);
    assert_eq!(
        header.length_within(99).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    // A hostile prefix is capped by the ceiling even under a larger limit.
    let hostile = LengthHeader::read_from_bytes(&u32::MAX.to_be_bytes()).unwrap();
    assert_eq!(
        hostile.length_within(usize::MAX).unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
}
