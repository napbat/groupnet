//! Windows byte-mode local-only named pipes with reserved first instances.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::Duration;

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::windows::named_pipe::{
    ClientOptions, NamedPipeClient, NamedPipeServer, ServerOptions,
};

use super::super::{IpcAddress, MAX_SESSIONS};

#[derive(Debug)]
pub(in crate::ipc) enum Stream {
    Client(NamedPipeClient),
    Server(NamedPipeServer),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Client(pipe) => Pin::new(pipe).poll_read(cx, buf),
            Self::Server(pipe) => Pin::new(pipe).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Self::Client(pipe) => Pin::new(pipe).poll_write(cx, buf),
            Self::Server(pipe) => Pin::new(pipe).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Client(pipe) => Pin::new(pipe).poll_flush(cx),
            Self::Server(pipe) => Pin::new(pipe).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Self::Client(pipe) => Pin::new(pipe).poll_shutdown(cx),
            Self::Server(pipe) => Pin::new(pipe).poll_shutdown(cx),
        }
    }
}

#[derive(Debug)]
pub(in crate::ipc) struct Listener {
    name: String,
    pending: NamedPipeServer,
}

impl Listener {
    pub(in crate::ipc) fn bind(address: &IpcAddress) -> io::Result<Self> {
        validate_address(address)?;
        let IpcAddress::NamedPipe(name) = address;
        Ok(Self {
            name: name.clone(),
            pending: create(name, true)?,
        })
    }

    pub(in crate::ipc) async fn accept(&mut self) -> io::Result<Stream> {
        self.pending.connect().await?;
        // Reserve the next instance before releasing the connected instance,
        // leaving no namespace gap in which a second listener can take over.
        let next = create(&self.name, false)?;
        Ok(Stream::Server(std::mem::replace(&mut self.pending, next)))
    }
}

fn create(name: &str, first: bool) -> io::Result<NamedPipeServer> {
    ServerOptions::new()
        .first_pipe_instance(first)
        .reject_remote_clients(true)
        // One pending instance and one transient replacement in addition to
        // the session cap. Buffers and the total number of handles are bounded.
        .max_instances(MAX_SESSIONS + 2)
        .in_buffer_size(65_000)
        .out_buffer_size(65_000)
        .create(name)
}

pub(in crate::ipc) fn validate_address(address: &IpcAddress) -> io::Result<()> {
    let IpcAddress::NamedPipe(name) = address;
    let suffix = name.strip_prefix(r"\\.\pipe\");
    if name.len() > 256
        || suffix.is_none_or(|suffix| {
            suffix.is_empty()
                || suffix
                    .bytes()
                    .any(|byte| byte == 0 || byte == b'\\' || byte == b'/')
        })
    {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "IPC requires a local \\\\.\\pipe\\name address",
        ));
    }
    Ok(())
}

pub(in crate::ipc) async fn connect(address: &IpcAddress) -> io::Result<Stream> {
    validate_address(address)?;
    let IpcAddress::NamedPipe(name) = address;
    loop {
        match ClientOptions::new().open(name) {
            Ok(pipe) => return Ok(Stream::Client(pipe)),
            // ERROR_PIPE_BUSY: the listener is replacing its pending instance.
            // The caller bounds this wait together with the handshake to 5s.
            Err(error) if error.raw_os_error() == Some(231) => {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(error) => return Err(error),
        }
    }
}
