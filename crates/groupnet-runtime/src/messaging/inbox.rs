//! Exclusive manual/callback receive ownership over a bounded application inbox.

use std::{future::Future, io, sync::Arc};

use groupnet_messaging::Frame;
use tokio::{
    sync::{Mutex, Semaphore, mpsc},
    task::JoinHandle,
};
use tokio_util::sync::CancellationToken;

const CAPACITY: usize = 64;

#[derive(Debug)]
struct Shared {
    cancel: CancellationToken,
    sender: mpsc::Sender<Frame>,
    receiver: Mutex<mpsc::Receiver<Frame>>,
    owner: Arc<Semaphore>,
}

/// One node or group inbox, shared by all clones of its public handle.
#[derive(Clone, Debug)]
pub(super) struct Inbox {
    shared: Arc<Shared>,
}

impl Inbox {
    pub(super) fn new(cancel: CancellationToken) -> Self {
        let (sender, receiver) = mpsc::channel(CAPACITY);
        Self {
            shared: Arc::new(Shared {
                cancel,
                sender,
                receiver: Mutex::new(receiver),
                owner: Arc::new(Semaphore::new(1)),
            }),
        }
    }

    pub(super) fn ensure_open(&self) -> io::Result<()> {
        if self.shared.cancel.is_cancelled() {
            Err(closed())
        } else {
            Ok(())
        }
    }

    pub(super) fn deliver(&self, frame: Frame) {
        if self.shared.cancel.is_cancelled() {
            let _ = frame.receipt().reject(io::ErrorKind::NotConnected);
            return;
        }
        match self.shared.sender.try_reserve() {
            Ok(slot) => {
                // Reserve before recording acceptance. Enqueue even if the
                // immediate ACK cannot be sent: retries use the recorded state.
                let _ = frame.receipt().accepted();
                slot.send(frame);
            }
            Err(error) => {
                let kind = match error {
                    mpsc::error::TrySendError::Full(()) => io::ErrorKind::WouldBlock,
                    mpsc::error::TrySendError::Closed(()) => io::ErrorKind::NotConnected,
                };
                let _ = frame.receipt().reject(kind);
            }
        }
    }

    pub(super) async fn recv(&self) -> io::Result<Frame> {
        self.ensure_open()?;
        let _owner = self
            .shared
            .owner
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        receive(&self.shared).await
    }

    pub(super) fn on_recv<F, Fut>(&self, mut callback: F) -> io::Result<ReceiveHandle>
    where
        F: FnMut(Frame) -> Fut + Send + 'static,
        Fut: Future<Output = io::Result<()>> + Send + 'static,
    {
        self.ensure_open()?;
        let owner = self
            .shared
            .owner
            .clone()
            .try_acquire_owned()
            .map_err(|_| busy())?;
        let shared = self.shared.clone();
        let task = tokio::spawn(async move {
            // The permit spans the entire worker, including an in-flight
            // callback. Other receivers must fail rather than steal its work.
            let _owner = owner;
            loop {
                let frame = receive(&shared).await?;
                let receipt = frame.receipt();
                let outcome = tokio::select! {
                    biased;
                    () = shared.cancel.cancelled() => return Err(closed()),
                    result = callback(frame) => result,
                };
                match outcome {
                    Ok(()) => receipt.applied()?,
                    Err(error) => {
                        let _ = receipt.reject(error.kind());
                        return Err(error);
                    }
                }
            }
        });
        Ok(ReceiveHandle { task })
    }
}

async fn receive(shared: &Shared) -> io::Result<Frame> {
    tokio::select! {
        biased;
        () = shared.cancel.cancelled() => Err(closed()),
        frame = async { shared.receiver.lock().await.recv().await } => {
            frame.ok_or_else(closed)
        }
    }
}

fn busy() -> io::Error {
    io::Error::new(
        io::ErrorKind::WouldBlock,
        "application inbox already has a receiver",
    )
}

fn closed() -> io::Error {
    io::Error::new(io::ErrorKind::NotConnected, "application inbox closed")
}

/// Owns one serial asynchronous receive callback.
///
/// Keep this handle alive to keep receiving. Dropping it cancels the worker,
/// including an in-flight callback, without acknowledging unfinished work.
/// Cancellation cannot undo application side effects already performed. Use
/// [`Self::close`] to wait until receive ownership is released before installing
/// another handler or calling manual `recv`.
#[derive(Debug)]
#[must_use = "dropping the handle cancels the receive callback"]
pub struct ReceiveHandle {
    task: JoinHandle<io::Result<()>>,
}

impl ReceiveHandle {
    /// Cancels and drains the callback, releasing exclusive receive ownership.
    ///
    /// # Errors
    /// Returns an already-completed callback failure or a callback panic.
    pub async fn close(mut self) -> io::Result<()> {
        self.task.abort();
        let result = (&mut self.task).await;
        match result {
            Ok(result) => result,
            Err(error) if error.is_cancelled() => Ok(()),
            Err(error) => Err(io::Error::other(error)),
        }
    }

    /// Waits for a callback failure or messaging shutdown and drains the worker.
    ///
    /// Successful callbacks keep receiving; they do not finish this future.
    /// Cancelling this future drops the handle and cancels the worker.
    ///
    /// # Errors
    /// Returns the callback's error, messaging shutdown, or callback panic.
    pub async fn wait(mut self) -> io::Result<()> {
        (&mut self.task).await.map_err(io::Error::other)?
    }
}

impl Drop for ReceiveHandle {
    fn drop(&mut self) {
        self.task.abort();
    }
}
