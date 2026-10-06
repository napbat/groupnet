//! Reject namespaces not recognized by this deployment inside authenticated TLS.

#[path = "../../tests/tunnels/fixtures.rs"]
mod fixtures;

use std::time::Duration;

use groupnet_transport::bulk::BulkTransport;
use tokio::time::timeout;

use crate as network;

#[tokio::test]
async fn authenticated_unknown_namespace_fails_closed_without_entering_any_accept_queue() {
    let fabric = fixtures::Fabric::new(false, false).await;
    let (a, c, _) = fixtures::credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let result = timeout(
        Duration::from_secs(12),
        sender.connect_namespace(fabric.c.local_id(), u16::MAX),
    )
    .await
    .unwrap();
    assert!(result.is_err());
    let (ordered, control) = tokio::join!(
        timeout(Duration::from_millis(100), receiver.accept()),
        timeout(Duration::from_millis(100), receiver.accept_control()),
    );
    assert!(ordered.is_err());
    assert!(control.is_err());
    // Rejection is session-local, not transport shutdown.
    let (connected, accepted) = timeout(Duration::from_secs(30), async {
        tokio::join!(sender.connect(fabric.c.local_id()), receiver.accept(),)
    })
    .await
    .unwrap();
    assert!(connected.is_ok());
    assert!(accepted.is_ok());
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
    assert_eq!(
        fabric
            .faults
            .dropped
            .load(std::sync::atomic::Ordering::Relaxed),
        0,
    );
}
