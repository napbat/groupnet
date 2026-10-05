//! The futures-IO surface, with cancellation checked even for buffered TLS data.

use std::{
    fmt,
    future::Future,
    io,
    pin::Pin,
    task::{Context, Poll},
};

use futures_util::io::{AsyncRead, AsyncWrite};
use tokio::io::DuplexStream;
use tokio_rustls::TlsStream;
use tokio_util::{
    compat::{Compat, TokioAsyncReadCompatExt},
    sync::{CancellationToken, WaitForCancellationFutureOwned},
};

/// An authenticated, reliable end-to-end TLS duplex stream.
///
/// Closing the write direction sends TLS `close_notify` and waits for reliable
/// acknowledgement without closing the read direction. Dropping the stream
/// cancels its session. Revocation interrupts even already-buffered TLS data.
pub struct TunneledStream {
    inner: Compat<TlsStream<DuplexStream>>,
    cancel: CancellationToken,
    cancelled: Pin<Box<WaitForCancellationFutureOwned>>,
    sent: Pin<Box<WaitForCancellationFutureOwned>>,
}

impl fmt::Debug for TunneledStream {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TunneledStream").finish_non_exhaustive()
    }
}

impl TunneledStream {
    pub(super) fn new(
        inner: TlsStream<DuplexStream>,
        cancel: CancellationToken,
        sent: CancellationToken,
    ) -> Self {
        Self {
            inner: inner.compat(),
            cancelled: Box::pin(cancel.clone().cancelled_owned()),
            sent: Box::pin(sent.cancelled_owned()),
            cancel,
        }
    }

    fn check(&mut self, cx: &mut Context<'_>) -> io::Result<()> {
        if self.cancelled.as_mut().poll(cx).is_ready() {
            Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "tunnel cancelled or peer revoked",
            ))
        } else {
            Ok(())
        }
    }
}

impl Drop for TunneledStream {
    fn drop(&mut self) {
        self.cancel.cancel();
    }
}

impl AsyncRead for TunneledStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &mut [u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_read(cx, bytes)
    }
}

impl AsyncWrite for TunneledStream {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bytes: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_write(cx, bytes)
    }

    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        self.check(cx)?;
        match Pin::new(&mut self.inner).poll_close(cx) {
            Poll::Ready(Ok(())) => self.sent.as_mut().poll(cx).map(|()| Ok(())),
            other => other,
        }
    }
}
