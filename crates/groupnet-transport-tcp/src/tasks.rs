//! Task ownership shared by the endpoint and its optional link lifecycle.

use std::future::{Future, poll_fn};
use std::sync::Mutex;
use std::task::Poll;

use tokio::sync::Mutex as AsyncMutex;
use tokio::task::JoinSet;

#[derive(Debug, Default)]
struct State {
    stopped: bool,
    tasks: JoinSet<()>,
}

#[derive(Debug, Default)]
pub(crate) struct Tasks {
    state: Mutex<State>,
    // JoinSet stores one join waker: serialize concurrent drain callers.
    drain: AsyncMutex<()>,
}

impl Tasks {
    pub(crate) fn spawn(&self, task: impl Future<Output = ()> + Send + 'static) -> bool {
        let mut state = self.state.lock().expect("task lock poisoned");
        if state.stopped {
            return false;
        }
        // Reap completed sessions so long-lived endpoints retain only live tasks.
        while state.tasks.try_join_next().is_some() {}
        state.tasks.spawn(task);
        true
    }

    pub(crate) fn stopped(&self) -> bool {
        self.state.lock().expect("task lock poisoned").stopped
    }

    pub(crate) fn shutdown(&self) {
        let mut state = self.state.lock().expect("task lock poisoned");
        state.stopped = true;
        state.tasks.abort_all();
    }

    pub(crate) async fn close(&self) {
        self.shutdown();
        let _drain = self.drain.lock().await;
        poll_fn(|cx| {
            let mut state = self.state.lock().expect("task lock poisoned");
            loop {
                match state.tasks.poll_join_next(cx) {
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
        assert!(tasks.state.lock().expect("state").tasks.is_empty());
    }
}
