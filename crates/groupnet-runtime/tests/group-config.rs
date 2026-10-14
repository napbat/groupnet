//! Integration test: the node builder's group [`Config`] contract.
//!
//! [`NodeBuilder::start`](groupnet_runtime::NodeBuilder::start) validates the
//! config before binding anything and reports a violated rule as an
//! `InvalidInput` I/O error wrapping the field-specific [`ConfigError`]; a
//! valid config reaches every group exactly as it was configured, with no
//! clamping or correction on the way.

use std::io;
use std::num::{NonZeroU64, NonZeroUsize};
use std::time::Duration;

use groupnet_core::{Config, ConfigError, NodeId};
use groupnet_runtime::{Node, NodeBuilder};
use groupnet_transport_mem::{MemLink, Network};

/// A builder for `id` on `net` with one memory link and no peers.
fn builder(net: &Network, id: &str) -> NodeBuilder {
    let id = NodeId::new(id);
    Node::builder(id.clone()).link(MemLink::new(net.endpoint(id), Vec::new()))
}

/// Starts `builder`, expecting it to be refused, and returns the typed cause.
async fn rejection(builder: NodeBuilder) -> ConfigError {
    let Err(error) = builder.start().await else {
        panic!("an invalid group config must not start a node");
    };
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    *error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<ConfigError>())
        .expect("the io error wraps the typed ConfigError")
}

#[tokio::test]
async fn start_rejects_a_zero_gossip_interval() {
    let net = Network::new();
    let cause = rejection(builder(&net, "zero-gossip").gossip_interval(Duration::ZERO)).await;
    assert_eq!(cause, ConfigError::ZeroGossipInterval);
}

#[tokio::test]
async fn start_rejects_a_zero_probe_timeout() {
    let net = Network::new();
    let config = Config {
        probe_timeout_ms: 0,
        ..Config::default()
    };
    let cause = rejection(builder(&net, "zero-timeout").config(config)).await;
    assert_eq!(cause, ConfigError::ZeroProbeTimeout);
}

#[tokio::test]
async fn configured_values_reach_the_group_as_given() {
    let net = Network::new();
    let gossip_fanout = NonZeroUsize::new(5).expect("nonzero");
    let frame = NonZeroUsize::new(1).expect("nonzero");
    let indirect = NonZeroUsize::new(7).expect("nonzero");
    let node = builder(&net, "as-given")
        .config(Config {
            probe_interval_ms: 1,
            probe_timeout_ms: 1,
            indirect_probes: indirect,
            ..Config::default()
        })
        .gossip_interval(Duration::from_millis(7))
        .gossip_fanout(gossip_fanout)
        .max_delta_frame_bytes(frame)
        .full_digest_every(NonZeroU64::MIN)
        .start()
        .await
        .expect("a valid config starts");
    let group = node.join_group("g");
    let config = group.config();
    assert_eq!(config.gossip_interval_ms, 7);
    assert_eq!(config.probe_interval_ms, 1);
    assert_eq!(config.probe_timeout_ms, 1);
    assert_eq!(config.indirect_probes, indirect);
    assert_eq!(config.gossip_fanout, gossip_fanout);
    assert_eq!(config.max_delta_frame_bytes, frame);
    assert_eq!(config.full_digest_every, NonZeroU64::MIN);
    assert_eq!(
        config.suspect_timeout_ms,
        Config::default().suspect_timeout_ms
    );
}

#[tokio::test]
async fn the_gossip_interval_reaches_the_core_in_whole_milliseconds() {
    let net = Network::new();
    for (interval, ms) in [
        (Duration::from_millis(7), 7),
        (Duration::from_micros(7_001), 8),
        (Duration::MAX, u64::MAX),
    ] {
        let node = builder(&net, &format!("interval-{ms}"))
            .gossip_interval(interval)
            .start()
            .await
            .expect("a nonzero interval starts");
        assert_eq!(node.join_group("g").config().gossip_interval_ms, ms);
        node.close().await;
    }
}
