//! The coherence-lease tier reached through the umbrella facade (feature
//! `consistency-leases`).
//!
//! A smoke test, deliberately: the tier's behaviour is proved next door in
//! `groupnet-consistency`. What this pins is that the feature wires up — that
//! `consistency-leases` really does turn on the underlying crate's `leases`
//! (and the `acks` tier its fast path is built on), and that the shell runs
//! when reached only through `groupnet::`.

#![cfg(all(feature = "consistency-leases", feature = "mem"))]

use std::time::Duration;

use groupnet::consistency::CAP_ACKS;
use groupnet::consistency::lease::{CAP_LEASE, LeaseConfig, LeaseState, Leases};
use groupnet::core::NodeId;
use groupnet::runtime::Node;
use groupnet::transport::mem::{MemLink, Network};
use groupnet_testkit::cluster::eventually;

/// …and the shell actually runs on it: a solo reader has nobody who must
/// confirm, so its own renewal is the confirmed one and it serves as soon as
/// the consumer affirms catch-up.
#[tokio::test]
async fn the_lease_shell_runs_through_the_facade() {
    let net = Network::new();
    let me = NodeId::new("facade-a");
    let node = Node::builder(me.clone())
        .link(MemLink::new(net.endpoint(me.clone()), Vec::new()))
        .start()
        .await
        .expect("start node");
    let group = node.join_group("stores");
    group
        .advertise_capabilities([CAP_ACKS, CAP_LEASE])
        .expect("the advertisement is enqueued");

    let leases = Leases::new(
        group,
        me,
        LeaseConfig::for_duration(Duration::from_millis(300)),
    );
    let view = leases.view();
    assert!(
        !view.valid(),
        "a booting reader serves nothing until it affirms catch-up"
    );
    eventually("the solo reader confirms its own renewal", || {
        view.mark_caught_up()
    })
    .await;
    assert!(view.valid());
    assert_eq!(view.state(), LeaseState::Serving);
    assert!(
        view.remaining().is_some(),
        "and it reports how much is left"
    );
}
