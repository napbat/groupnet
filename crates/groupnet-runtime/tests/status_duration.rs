//! Integration test: the **continuous-status duration** surface and the
//! **effective-config** surface, over the real async runtime and a real
//! `Transport`.
//!
//! Four things get pinned here that only the runtime layer can prove:
//!
//! * [`Group::config`] hands back the config the node was *built* with, so a
//!   consumer sizing a trust window off `detection_window_ms` tracks
//!   configuration drift instead of silently reading `Config::default()`.
//! * [`Group::status_held_for`] advances on the same clock the driver feeds
//!   the engine, so a peer that stays Alive reads as a monotonically growing
//!   duration.
//! * [`Group::statuses_held`] — the roster-shaped form the same consumer
//!   iterates — agrees with `status_held_for` for every member it lists, so
//!   the cheap one-snapshot sweep and the per-node lookup can never tell a
//!   fence maintainer different stories.
//! * The **reap horizon** really does end the reading: past
//!   `2 × dead_timeout_ms` the member is gone and the answer is `None`, not a
//!   stale `Dead`. That caveat is documented, so it is tested.
//!
//! All waiting is a bounded poll on a predicate (`eventually*`), never a bare
//! sleep.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use groupnet_core::{Config, NodeId, Status};
use groupnet_runtime::{Group, Node};
use groupnet_testkit::cluster::{MemCluster, converged_within, eventually_within};
use groupnet_transport::{Inbound, Transport};
use groupnet_transport_mem::{MemTransport, Network};

/// The poll budget for these assertions: a genuine regression reports in a few
/// seconds rather than riding the harness default out.
const SETTLE: Duration = Duration::from_secs(5);

/// The gossip cadence the cluster is *built* with — deliberately unlike
/// [`Config::default`]'s, so an accessor returning the defaults fails loudly.
const OVERRIDDEN_GOSSIP_MS: u64 = 20;

#[tokio::test]
async fn effective_config_and_alive_duration_are_readable_from_a_group() {
    let cluster = MemCluster::builder(&["node-a", "node-b", "node-c"])
        .group("shard-7")
        .gossip_interval_ms(OVERRIDDEN_GOSSIP_MS)
        .spawn();
    let groups = &cluster.groups;
    let ids = &cluster.ids;

    // --- Item 1: the built node's effective config, not the defaults. ---
    for group in groups {
        let cfg = group.config();
        assert_eq!(
            cfg.gossip_interval_ms, OVERRIDDEN_GOSSIP_MS,
            "config() must report the builder override, not Config::default()"
        );
        assert_ne!(
            cfg.gossip_interval_ms,
            Config::default().gossip_interval_ms,
            "the fixture is only meaningful if the override differs from the default"
        );
        // The knob sets the anti-entropy cadence in step, and the detector
        // timings are untouched — so the window is computable from here.
        assert_eq!(cfg.anti_entropy_interval_ms, OVERRIDDEN_GOSSIP_MS);
        assert_eq!(
            cfg.detection_window_ms(3),
            2 * (cfg.probe_interval_ms + 2 * cfg.probe_timeout_ms) + cfg.suspect_timeout_ms
        );
    }

    converged_within(&groups.iter().collect::<Vec<_>>(), SETTLE).await;

    // --- Item 3: a peer that stays Alive reads as a growing duration. ---
    let observer = &groups[0];
    let peer = &ids[1];
    eventually_within("node-a to hold node-b Alive", SETTLE, || {
        matches!(observer.status_held_for(peer), Some((Status::Alive, _)))
    })
    .await;

    let mut readings = Vec::new();
    let (_, first) = observer.status_held_for(peer).expect("Alive above");
    readings.push(first);
    // Spaced reads, spaced by a *predicate* rather than a guessed sleep: wait
    // until the duration has demonstrably advanced, twice.
    for _ in 0..2 {
        let previous = *readings.last().expect("seeded above");
        eventually_within("the Alive duration to advance", SETTLE, || {
            observer
                .status_held_for(peer)
                .is_some_and(|(_, held)| held > previous)
        })
        .await;
        let (status, held) = observer.status_held_for(peer).expect("still known");
        assert_eq!(status, Status::Alive, "node-b never stopped being alive");
        readings.push(held);
    }
    assert!(
        readings.windows(2).all(|w| w[1] >= w[0]),
        "the held-for duration must never go backwards: {readings:?}"
    );

    // Self-observation works the same way: a node has been alive since boot.
    assert!(
        matches!(observer.status_held_for(&ids[0]), Some((Status::Alive, _))),
        "a node should be able to read its own liveness duration"
    );

    // --- A departure: the survivor's verdict flips, freshly stamped. ---
    let leaver = ids[2].clone();
    groups[2].leave();
    eventually_within("the survivors to hold node-c Dead", SETTLE, || {
        groups[..2]
            .iter()
            .all(|g| matches!(g.status_held_for(&leaver), Some((Status::Dead, _))))
    })
    .await;
    for group in &groups[..2] {
        let (status, held) = group.status_held_for(&leaver).expect("Dead above");
        assert_eq!(status, Status::Dead);
        assert!(
            held < Duration::from_secs(1),
            "the Dead stamp must date from the departure just observed, not from boot: {held:?}"
        );
    }

    // --- The roster-shaped read agrees with the per-node one. ---
    // `statuses_held` exists so a fence maintainer can sweep the membership in
    // one snapshot instead of N lookups; it must say exactly what those N
    // lookups would say, for every member — including the departed one, and
    // including this node itself.
    for group in &groups[..2] {
        let roster = group.statuses_held();
        assert_eq!(
            roster.iter().map(|(n, _, _)| n).collect::<Vec<_>>(),
            group.statuses().iter().map(|(n, _)| n).collect::<Vec<_>>(),
            "the roster must list the same members, in the same id order, as statuses()"
        );
        assert!(
            roster
                .iter()
                .any(|(n, s, _)| *n == leaver && *s == Status::Dead),
            "the departed member must appear in the roster, held Dead"
        );
        for (node, status, held) in &roster {
            let (per_node_status, per_node_held) = group
                .status_held_for(node)
                .expect("a member the roster lists is a member status_held_for knows");
            assert_eq!(
                *status, per_node_status,
                "roster and per-node reads disagree about {node}"
            );
            // The per-node read is taken later, so its duration is the larger
            // of the two — but only by the wall-clock gap between the reads,
            // never by a whole different stamp.
            let drift = per_node_held
                .checked_sub(*held)
                .expect("the later read of a running stopwatch cannot be smaller");
            assert!(
                drift < Duration::from_secs(1),
                "roster held {held:?} for {node} but the per-node read said {per_node_held:?}"
            );
        }
    }
}

/// A crashed node — detected by the failure detector, not announced — is held
/// `Dead` with a fresh stamp, and stays readable for as long as the tombstone
/// lives.
#[tokio::test]
async fn a_silent_node_is_eventually_held_dead_with_a_fresh_stamp() {
    // A reap horizon (2 × 4s) far beyond the test, so the Dead reading is
    // stable to assert against.
    let cluster = CrashCluster::spawn(&["a", "b", "c"], &detector_config(4_000));
    cluster.converge().await;

    let victim = cluster.ids[2].clone();
    cluster.unplug(2);

    eventually_within("the survivors to detect the silent node", SETTLE, || {
        cluster.groups[..2]
            .iter()
            .all(|g| matches!(g.status_held_for(&victim), Some((Status::Dead, _))))
    })
    .await;

    for group in &cluster.groups[..2] {
        let (status, held) = group.status_held_for(&victim).expect("Dead above");
        assert_eq!(status, Status::Dead);
        assert!(
            held < Duration::from_secs(2),
            "the Dead stamp must date from detection, not from boot: {held:?}"
        );
    }
}

/// The documented reap-horizon caveat, pinned: with a short
/// `dead_timeout_ms`, the tombstone is reaped `2 ×` later and the reading
/// becomes `None` — the duration is only legible *inside* the horizon.
///
/// This is the sharp edge consumers must handle, and the reason
/// [`Group::status_held_for`]'s docs name both remedies (raise
/// `dead_timeout_ms`, or read post-registration `None` as "dead for at least
/// the horizon").
#[tokio::test]
async fn a_reaped_tombstone_stops_reporting_a_duration() {
    const DEAD_TIMEOUT_MS: u64 = 150; // reaped 300ms after death
    let cluster = CrashCluster::spawn(&["a", "b", "c"], &detector_config(DEAD_TIMEOUT_MS));
    cluster.converge().await;

    let victim = cluster.ids[2].clone();
    cluster.unplug(2);

    eventually_within(
        "the tombstone to be reaped past the horizon",
        SETTLE,
        || {
            cluster.groups[..2]
                .iter()
                .all(|g| g.status_held_for(&victim).is_none())
        },
    )
    .await;

    for group in &cluster.groups[..2] {
        assert_eq!(
            group.status_held_for(&victim),
            None,
            "past the reap horizon the member is forgotten, not reported stale"
        );
        assert_eq!(group.member_status(&victim), None, "and so is its status");
        assert!(!group.members().contains(&victim));
    }
}

/// Tight detector timings with a caller-chosen reap horizon, so a crash is
/// detected in well under a second of wall-clock time.
fn detector_config(dead_timeout_ms: u64) -> Config {
    Config {
        gossip_interval_ms: 20,
        anti_entropy_interval_ms: 20,
        probe_interval_ms: 20,
        probe_timeout_ms: 20,
        suspect_timeout_ms: 60,
        dead_timeout_ms,
        ..Config::default()
    }
}

/// A [`MemTransport`] endpoint with a kill switch.
///
/// [`MemCluster`] cannot express either half of what the crash tests need — a
/// full [`Config`] override, and a node that goes *silent* rather than
/// announcing its departure — so these two tests build their nodes directly.
/// Unplugged, the endpoint neither sends nor delivers: to its peers that is
/// indistinguishable from the process having died, which is exactly the input
/// the failure detector is supposed to act on.
#[derive(Debug)]
struct Unpluggable {
    inner: MemTransport,
    plugged: Arc<AtomicBool>,
}

impl Transport for Unpluggable {
    type Error = <MemTransport as Transport>::Error;

    async fn send(&self, to: &NodeId, msg: &[u8]) -> Result<(), Self::Error> {
        if self.plugged.load(Ordering::Relaxed) {
            self.inner.send(to, msg).await
        } else {
            Ok(()) // best-effort transports drop, they do not error
        }
    }

    async fn recv(&self) -> Result<Inbound, Self::Error> {
        loop {
            let inbound = self.inner.recv().await?;
            if self.plugged.load(Ordering::Relaxed) {
                return Ok(inbound);
            }
            // Unplugged: the datagram is swallowed, as a dead process would.
        }
    }
}

/// An all-to-all in-memory cluster whose nodes can be individually unplugged.
struct CrashCluster {
    ids: Vec<NodeId>,
    groups: Vec<Group>,
    plugs: Vec<Arc<AtomicBool>>,
    /// Kept alive for the duration of the test; dropping it stops the nodes.
    _nodes: Vec<Node<Unpluggable>>,
}

impl CrashCluster {
    const GROUP: &'static str = "crash";

    fn spawn(names: &[&str], config: &Config) -> Self {
        let net = Network::new();
        let ids: Vec<NodeId> = names.iter().map(|n| NodeId::new(*n)).collect();
        let mut nodes = Vec::with_capacity(ids.len());
        let mut plugs = Vec::with_capacity(ids.len());
        for id in &ids {
            let plugged = Arc::new(AtomicBool::new(true));
            let transport = Unpluggable {
                inner: net.endpoint(id.clone()),
                plugged: plugged.clone(),
            };
            let mut builder = Node::builder(id.clone(), transport).config(config.clone());
            for seed in ids.iter().filter(|o| *o != id) {
                builder = builder.seed(seed.clone());
            }
            nodes.push(builder.spawn());
            plugs.push(plugged);
        }
        let groups = nodes.iter().map(|n| n.join_group(Self::GROUP)).collect();
        Self {
            ids,
            groups,
            plugs,
            _nodes: nodes,
        }
    }

    async fn converge(&self) {
        converged_within(&self.groups.iter().collect::<Vec<_>>(), SETTLE).await;
    }

    /// Silences node `index` in both directions.
    fn unplug(&self, index: usize) {
        self.plugs[index].store(false, Ordering::Relaxed);
    }
}
