//! Managed message settings reach node/group queues and descriptor validation.

use std::{io, time::Duration};

use groupnet_core::NodeId;
use groupnet_network::RouterConfig;
use groupnet_runtime::messaging::{Bytes, Delivery, MessagingConfig, SendOptions};
use groupnet_runtime::{Messages, Node};
use groupnet_transport::QueueCapacity;

#[tokio::test]
async fn managed_configuration_controls_payload_deadlines_and_node_inbox() -> io::Result<()> {
    let id = NodeId::new("configured");
    let config = MessagingConfig {
        max_payload: 70_000,
        inbox_capacity: QueueCapacity::of(1),
        max_timeout: Duration::from_secs(90),
        dedup_retention: Duration::from_secs(91),
        fanout_concurrency: QueueCapacity::of(1),
        ..MessagingConfig::default()
    };
    let node = Node::builder(id.clone())
        .routing(RouterConfig {
            max_frame: 100_000,
            ..RouterConfig::default()
        })
        .messaging(config)
        .start()
        .await?;
    // Binding and empty fanout must not retain the former thirty-second cap.
    let _endpoint = node.endpoint(Messages::delivered(Duration::from_secs(90)))?;
    let empty = node.join_group("empty");
    let options = SendOptions {
        delivery: Delivery::Delivered,
        timeout: Duration::from_secs(90),
    };
    let payload = Bytes::from(vec![7; 70_000]);
    assert!(
        empty
            .send_frame(payload.clone(), options)
            .await?
            .outcomes
            .is_empty()
    );
    let message = tokio::time::timeout(
        Duration::from_secs(5),
        node.send_frame(&id, payload.clone(), options),
    )
    .await
    .map_err(io::Error::other)??;
    assert_eq!(payload.as_ref(), vec![7; 70_000]);
    // The managed node inbox uses the selected capacity, not a separate fixed 64.
    let rejected = tokio::time::timeout(
        Duration::from_secs(5),
        node.send_frame(&id, Bytes::new(), options),
    )
    .await
    .map_err(io::Error::other)?
    .unwrap_err();
    assert_eq!(rejected.kind(), io::ErrorKind::WouldBlock);
    let frame = tokio::time::timeout(Duration::from_secs(5), node.recv_frame())
        .await
        .map_err(io::Error::other)??;
    assert_eq!(frame.id, message);
    assert_eq!(frame.payload, payload);
    assert_eq!(
        empty
            .send_frame(Bytes::from(vec![0; 70_001]), options)
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    assert_eq!(
        node.endpoint(Messages::delivered(Duration::from_secs(91)))
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidInput
    );
    node.router().close().await;
    Ok(())
}

#[tokio::test]
async fn managed_start_rejects_retention_that_cannot_cover_send_horizon() {
    let result = Node::builder(NodeId::new("invalid"))
        .messaging(MessagingConfig {
            max_timeout: Duration::from_secs(90),
            dedup_retention: Duration::from_secs(90),
            ..MessagingConfig::default()
        })
        .start()
        .await;
    assert_eq!(result.unwrap_err().kind(), io::ErrorKind::InvalidInput);
}
