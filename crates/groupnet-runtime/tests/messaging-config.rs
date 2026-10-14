//! Managed message settings reach node/group queues and descriptor validation.

use std::{io, num::NonZeroUsize, time::Duration};

use groupnet_core::NodeId;
use groupnet_runtime::messaging::{
    Bytes, MessagingConfig, MessagingConfigError, SendOptions, SendValidationError,
};
use groupnet_runtime::{Messages, Node, RouterConfig};
use groupnet_transport::QueueCapacity;

#[tokio::test]
async fn managed_configuration_controls_payload_deadlines_and_node_inbox() -> io::Result<()> {
    let id = NodeId::new("configured");
    let config = MessagingConfig {
        max_payload: NonZeroUsize::new(70_000).unwrap(),
        inbox_capacity: QueueCapacity::of(1),
        max_timeout: Duration::from_secs(90),
        dedup_retention: Duration::from_secs(91),
        fanout_concurrency: QueueCapacity::of(1),
        ..MessagingConfig::default()
    };
    let node = Node::builder(id.clone())
        .routing(RouterConfig {
            max_frame: NonZeroUsize::new(100_000).unwrap(),
            ..RouterConfig::default()
        })
        .messaging(config)
        .start()
        .await?;
    // Binding and empty fanout must not retain the former thirty-second cap.
    let _endpoint = node.endpoint(Messages::delivered(Duration::from_secs(90)))?;
    let empty = node.join_group("empty");
    let options = SendOptions::Delivered {
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
    let oversized = empty
        .send_frame(Bytes::from(vec![0; 70_001]), options)
        .await
        .unwrap_err();
    assert_eq!(oversized.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        oversized
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<SendValidationError>()),
        Some(&SendValidationError::PayloadTooLarge {
            len: 70_001,
            max: NonZeroUsize::new(70_000).unwrap(),
        })
    );
    let too_long = node
        .endpoint(Messages::delivered(Duration::from_secs(91)))
        .unwrap_err();
    assert_eq!(too_long.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        too_long
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<SendValidationError>()),
        Some(&SendValidationError::TimeoutAboveMax {
            timeout: Duration::from_secs(91),
            max: Duration::from_secs(90),
        })
    );
    node.router().close().await;
    Ok(())
}

#[tokio::test]
async fn managed_start_rejects_retention_that_cannot_cover_send_horizon() {
    let error = Node::builder(NodeId::new("invalid"))
        .messaging(MessagingConfig {
            max_timeout: Duration::from_secs(90),
            dedup_retention: Duration::from_secs(90),
            ..MessagingConfig::default()
        })
        .start()
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    assert_eq!(
        error
            .get_ref()
            .and_then(|inner| inner.downcast_ref::<MessagingConfigError>()),
        Some(&MessagingConfigError::RetentionNotAboveMaxTimeout)
    );
}
