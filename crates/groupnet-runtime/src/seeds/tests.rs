//! The resolver task's schedule and teaching rules, on a paused clock with a
//! scripted resolver and a transport that records what it is taught.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use groupnet_core::NodeId;
use groupnet_transport::{Inbound, Transport};

use super::{NamedSeeds, ResolveFuture, SeedEvent, SeedResolver, resolve_named_seeds};

const RETRY: Duration = Duration::from_secs(1);
const REFRESH: Duration = Duration::from_secs(5);
const ATTEMPTS: u32 = 3;

/// A transport that only records `learn_peer` calls.
#[derive(Default)]
struct Recording {
    taught: Mutex<Vec<(NodeId, String)>>,
}

impl Recording {
    fn taught(&self) -> Vec<(NodeId, String)> {
        self.taught.lock().expect("taught lock").clone()
    }
}

impl Transport for Recording {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> + Send {
        std::future::ready(Ok(()))
    }

    fn recv(&self) -> impl Future<Output = io::Result<Inbound>> + Send {
        std::future::pending()
    }

    fn learn_peer(&self, node: &NodeId, addr: &str) {
        self.taught
            .lock()
            .expect("taught lock")
            .push((node.clone(), addr.to_owned()));
    }
}

/// A resolver whose answers the test edits; it counts lookups per name.
#[derive(Clone, Default)]
struct Scripted {
    answers: Arc<Mutex<HashMap<String, SocketAddr>>>,
    lookups: Arc<Mutex<HashMap<String, u32>>>,
}

impl Scripted {
    fn set(&self, name: &str, addr: &str) {
        self.answers
            .lock()
            .expect("answers lock")
            .insert(name.to_owned(), addr.parse().expect("addr"));
    }

    fn clear(&self, name: &str) {
        self.answers.lock().expect("answers lock").remove(name);
    }

    fn lookups(&self, name: &str) -> u32 {
        self.lookups
            .lock()
            .expect("lookups lock")
            .get(name)
            .copied()
            .unwrap_or(0)
    }
}

impl SeedResolver for Scripted {
    fn resolve<'a>(&'a self, name: &'a str) -> ResolveFuture<'a> {
        *self
            .lookups
            .lock()
            .expect("lookups lock")
            .entry(name.to_owned())
            .or_default() += 1;
        let answer = self
            .answers
            .lock()
            .expect("answers lock")
            .get(name)
            .copied();
        Box::pin(async move { answer.ok_or_else(|| io::Error::from(io::ErrorKind::NotFound)) })
    }
}

/// Events as `(node, addr, previous)` for resolutions and `node` alone for
/// give-ups, in emission order.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Seen {
    Resolved(String, SocketAddr, Option<SocketAddr>),
    Unresolved(String),
}

struct Harness {
    transport: Arc<Recording>,
    resolver: Scripted,
    seen: Arc<Mutex<Vec<Seen>>>,
    task: tokio::task::JoinHandle<()>,
}

impl Harness {
    fn spawn(seeds: &[(&str, &str)], resolver: Scripted) -> Self {
        let transport = Arc::new(Recording::default());
        let seen = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&seen);
        let mut config = NamedSeeds::new(resolver.clone())
            .retry_interval(RETRY)
            .startup_attempts(ATTEMPTS)
            .refresh_interval(REFRESH)
            .on_event(move |event| {
                let seen = match event {
                    SeedEvent::Resolved {
                        node,
                        addr,
                        previous,
                        ..
                    } => Seen::Resolved(node.as_str().to_owned(), *addr, *previous),
                    SeedEvent::Unresolved { node, .. } => {
                        Seen::Unresolved(node.as_str().to_owned())
                    }
                };
                sink.lock().expect("seen lock").push(seen);
            });
        for (node, name) in seeds {
            config = config.seed(NodeId::new(*node), *name);
        }
        let task = tokio::spawn(resolve_named_seeds(
            Arc::downgrade(&transport),
            NodeId::new("local"),
            config,
        ));
        Self {
            transport,
            resolver,
            seen,
            task,
        }
    }

    fn seen(&self) -> Vec<Seen> {
        self.seen.lock().expect("seen lock").clone()
    }
}

/// A step past a due instant, so an attempt scheduled exactly at a boundary has
/// run before the test looks.
const SETTLE: Duration = Duration::from_millis(1);

/// Lets the paused clock run `by` (plus [`SETTLE`]) and every task due in it
/// run to idle.
async fn advance(by: Duration) {
    tokio::time::sleep(by + SETTLE).await;
    tokio::task::yield_now().await;
}

fn addr(s: &str) -> SocketAddr {
    s.parse().expect("addr")
}

#[tokio::test(start_paused = true)]
async fn a_resolvable_seed_is_taught_at_once_and_not_retaught_while_unchanged() {
    let resolver = Scripted::default();
    resolver.set("b.svc:7000", "10.0.0.2:7000");
    let h = Harness::spawn(&[("b", "b.svc:7000")], resolver);

    advance(Duration::ZERO).await;
    assert_eq!(
        h.transport.taught(),
        vec![(NodeId::new("b"), "10.0.0.2:7000".to_owned())]
    );

    advance(REFRESH * 3).await;
    assert_eq!(
        h.resolver.lookups("b.svc:7000"),
        4,
        "one lookup per refresh"
    );
    assert_eq!(h.transport.taught().len(), 1, "unchanged address re-taught");
    assert_eq!(
        h.seen(),
        vec![Seen::Resolved("b".into(), addr("10.0.0.2:7000"), None)]
    );
}

#[tokio::test(start_paused = true)]
async fn a_moved_seed_is_retaught_within_one_refresh() {
    let resolver = Scripted::default();
    resolver.set("b.svc:7000", "10.0.0.2:7000");
    let h = Harness::spawn(&[("b", "b.svc:7000")], resolver);
    advance(Duration::ZERO).await;

    h.resolver.set("b.svc:7000", "10.0.0.9:7000");
    advance(REFRESH).await;
    assert_eq!(
        h.transport.taught().last(),
        Some(&(NodeId::new("b"), "10.0.0.9:7000".to_owned()))
    );
    assert_eq!(
        h.seen().last(),
        Some(&Seen::Resolved(
            "b".into(),
            addr("10.0.0.9:7000"),
            Some(addr("10.0.0.2:7000"))
        ))
    );
}

#[tokio::test(start_paused = true)]
async fn a_lookup_failure_keeps_the_last_address() {
    let resolver = Scripted::default();
    resolver.set("b.svc:7000", "10.0.0.2:7000");
    let h = Harness::spawn(&[("b", "b.svc:7000")], resolver);
    advance(Duration::ZERO).await;

    h.resolver.clear("b.svc:7000");
    advance(REFRESH * 2).await;
    assert_eq!(h.transport.taught().len(), 1, "a DNS blip churned the book");
    assert_eq!(
        h.resolver.lookups("b.svc:7000"),
        3,
        "a failed refresh waits a refresh, not a retry"
    );
    assert_eq!(
        h.seen().len(),
        1,
        "a resolved seed is never reported unresolved"
    );
}

#[tokio::test(start_paused = true)]
async fn a_late_seed_is_retried_at_the_retry_cadence_until_it_resolves() {
    let h = Harness::spawn(&[("b", "b.svc:7000")], Scripted::default());
    advance(Duration::ZERO).await;
    advance(RETRY).await;
    assert_eq!(h.resolver.lookups("b.svc:7000"), 2);
    assert_eq!(h.transport.taught(), Vec::new());

    h.resolver.set("b.svc:7000", "10.0.0.2:7000");
    advance(RETRY).await;
    assert_eq!(
        h.transport.taught(),
        vec![(NodeId::new("b"), "10.0.0.2:7000".to_owned())]
    );
    assert_eq!(
        h.seen(),
        vec![Seen::Resolved("b".into(), addr("10.0.0.2:7000"), None)],
        "resolving inside the window reports no give-up"
    );
}

#[tokio::test(start_paused = true)]
async fn a_seed_outliving_its_window_is_reported_once_and_kept_at_the_refresh_cadence() {
    let h = Harness::spawn(&[("b", "b.svc:7000")], Scripted::default());
    advance(Duration::ZERO).await;
    advance(RETRY * (ATTEMPTS - 1)).await;
    assert_eq!(h.resolver.lookups("b.svc:7000"), ATTEMPTS);
    assert_eq!(h.seen(), vec![Seen::Unresolved("b".into())]);

    advance(RETRY).await;
    assert_eq!(
        h.resolver.lookups("b.svc:7000"),
        ATTEMPTS,
        "past its window a seed waits a refresh"
    );
    advance(REFRESH * 2).await;
    assert_eq!(
        h.seen(),
        vec![Seen::Unresolved("b".into())],
        "reported once"
    );

    h.resolver.set("b.svc:7000", "10.0.0.2:7000");
    advance(REFRESH).await;
    assert_eq!(
        h.transport.taught(),
        vec![(NodeId::new("b"), "10.0.0.2:7000".to_owned())],
        "a seed is never abandoned"
    );
}

#[tokio::test(start_paused = true)]
async fn the_local_node_is_never_its_own_seed() {
    let resolver = Scripted::default();
    resolver.set("local.svc:7000", "10.0.0.1:7000");
    resolver.set("b.svc:7000", "10.0.0.2:7000");
    let h = Harness::spawn(
        &[("local", "local.svc:7000"), ("b", "b.svc:7000")],
        resolver,
    );
    advance(REFRESH).await;
    assert_eq!(h.resolver.lookups("local.svc:7000"), 0);
    assert_eq!(
        h.transport.taught(),
        vec![(NodeId::new("b"), "10.0.0.2:7000".to_owned())]
    );
}

#[tokio::test(start_paused = true)]
async fn the_task_ends_with_its_transport() {
    let resolver = Scripted::default();
    resolver.set("b.svc:7000", "10.0.0.2:7000");
    let Harness {
        transport, task, ..
    } = Harness::spawn(&[("b", "b.svc:7000")], resolver);
    advance(Duration::ZERO).await;
    drop(transport);
    advance(REFRESH).await;
    assert!(task.is_finished());
}
