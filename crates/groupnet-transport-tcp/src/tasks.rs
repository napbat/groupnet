//! Task ownership shared by the endpoint and its optional link lifecycle.

use std::future::{Future, poll_fn};
use std::sync::Mutex;
use std::task::Poll;

use tokio::sync::Mutex as AsyncMutex;
use tokio::sync::watch;
use tokio::task::JoinSet;

#[derive(Debug)]
pub(crate) struct Tasks {
    running: Mutex<JoinSet<()>>,
    // Sticky stop flag. Written only while `running` is locked so spawn's check
    // and shutdown's abort stay atomic; the watch makes the stop observable.
    stopped: watch::Sender<bool>,
    // JoinSet stores one join waker: serialize concurrent drain callers.
    drain: AsyncMutex<()>,
}

impl Default for Tasks {
    fn default() -> Self {
        Self {
            running: Mutex::default(),
            stopped: watch::Sender::new(false),
            drain: AsyncMutex::default(),
        }
    }
}

impl Tasks {
    pub(crate) fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut tasks = self.running.lock().expect("task lock poisoned");
        if self.stopped() {
            return false;
        }
        // Reap completed sessions so long-lived endpoints retain only live tasks.
        while tasks.try_join_next().is_some() {}
        tasks.spawn(task);
        true
    }

    pub(crate) fn stopped(&self) -> bool {
        *self.stopped.borrow()
    }

    /// Resolves once shutdown has begun; sticky for every later caller.
    pub(crate) async fn stopping(&self) {
        let mut stopped = self.stopped.subscribe();
        // The sender lives in `self`, so the wait can only end by stopping.
        let _ = stopped.wait_for(|stopped| *stopped).await;
    }

    pub(crate) fn shutdown(&self) {
        let mut tasks = self.running.lock().expect("task lock poisoned");
        self.stopped.send_replace(true);
        tasks.abort_all();
    }

    pub(crate) async fn close(&self) {
        self.shutdown();
        let _drain = self.drain.lock().await;
        poll_fn(|cx| {
            let mut tasks = self.running.lock().expect("task lock poisoned");
            loop {
                match tasks.poll_join_next(cx) {
                    Poll::Ready(Some(_)) => {}
                    Poll::Ready(None) => return Poll::Ready(()),
                    Poll::Pending => return Poll::Pending,
                }
            }
        })
        .await;
    }
}

#[cfg(feature = "link")]
impl groupnet_transport::link::LinkLifecycle for Tasks {
    fn shutdown(&self) {
        Self::shutdown(self);
    }

    fn close(&self) -> groupnet_transport::link::LinkFuture<'_, ()> {
        Box::pin(Self::close(self))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::time::Duration;

    use super::Tasks;

    struct Dropped(Arc<AtomicBool>);

    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn concurrent_close_drains_cancelled_tasks_and_prevents_new_tasks() {
        let tasks = Tasks::default();
        let dropped = Arc::new(AtomicBool::new(false));
        let guard = Dropped(dropped.clone());
        let (started, ready) = tokio::sync::oneshot::channel();
        assert!(tasks.spawn(async move {
            let _guard = guard;
            started.send(()).expect("notify start");
            std::future::pending::<()>().await;
        }));
        ready.await.expect("task started");
        tokio::time::timeout(Duration::from_secs(5), async {
            tokio::join!(tasks.close(), tasks.close());
        })
        .await
        .expect("concurrent close must not lose a drain waker");
        assert!(dropped.load(Ordering::SeqCst));
        assert!(!tasks.spawn(async { panic!("closed registry spawned a task") }));
        assert!(tasks.running.lock().expect("tasks").is_empty());
    }

    #[tokio::test]
    async fn stopping_is_sticky_and_observes_shutdown() {
        let tasks = Arc::new(Tasks::default());
        let waiter = tokio::spawn({
            let tasks = tasks.clone();
            async move { tasks.stopping().await }
        });
        tokio::task::yield_now().await;
        assert!(!waiter.is_finished());
        tasks.shutdown();
        tokio::time::timeout(Duration::from_secs(5), waiter)
            .await
            .expect("stop observed")
            .expect("waiter");
        tokio::time::timeout(Duration::from_secs(5), tasks.stopping())
            .await
            .expect("a later caller sees the sticky stop");
    }
}
