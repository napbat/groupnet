use super::*;
use groupnet_transport::NodeIdError;
use std::num::NonZeroUsize;

/// A link that never delivers a frame; only its registration is under test.
#[derive(Debug)]
struct Idle;

impl Transport for Idle {
    type Error = io::Error;

    fn send(&self, _to: &NodeId, _msg: &[u8]) -> impl Future<Output = io::Result<()>> {
        std::future::ready(Ok(()))
    }

    async fn recv(&self) -> io::Result<Inbound> {
        std::future::pending().await
    }
}

fn typed<E: std::error::Error + Copy + 'static>(error: &io::Error) -> E {
    assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    *error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<E>())
        .expect("typed inner error")
}

#[tokio::test]
async fn invalid_local_identity_is_the_typed_node_id_error() {
    assert_eq!(
        typed::<NodeIdError>(&Router::new(NodeId::new("")).unwrap_err()),
        NodeIdError::Empty
    );
    let long = NodeId::new("n".repeat(MAX_NODE_ID_BYTES + 1));
    assert_eq!(
        typed::<NodeIdError>(&Router::new(long).unwrap_err()),
        NodeIdError::TooLong {
            len: MAX_NODE_ID_BYTES + 1,
            max: MAX_NODE_ID_BYTES,
        }
    );
}

#[tokio::test]
async fn link_outside_router_bounds_is_a_typed_registration_error() -> io::Result<()> {
    let config = RouterConfig {
        max_routes: NonZeroUsize::new(1).unwrap(),
        ..RouterConfig::default()
    };
    let max_frame = config.max_frame.get();
    let max_fragments = usize::from(config.reassembly.max_fragments.get());
    let router = Router::with_config(NodeId::new("local"), config)?;
    let rejected = |config: LinkConfig| {
        typed::<LinkRegistrationError>(&router.add_transport(Idle, config).unwrap_err())
    };
    let peers = vec![NodeId::new("a"), NodeId::new("b")];
    assert_eq!(
        rejected(LinkConfig::new(peers)),
        LinkRegistrationError::PeersAboveMaxRoutes {
            peers: 2,
            max_routes: 1,
        }
    );
    let with_mtu = |mtu| LinkConfig {
        mtu: NonZeroUsize::new(mtu).unwrap(),
        ..LinkConfig::new(vec![NodeId::new("a")])
    };
    let min = wire::FRAGMENT + 1;
    for mtu in [min - 1, max_frame + 1] {
        assert_eq!(
            rejected(with_mtu(mtu)),
            LinkRegistrationError::MtuOutOfRange {
                mtu,
                min,
                max: max_frame,
            }
        );
    }
    assert_eq!(
        rejected(with_mtu(min)),
        LinkRegistrationError::FragmentsAboveReassembly {
            fragments: max_frame,
            max_fragments,
        }
    );
    // The smallest MTU whose fragments fit is accepted.
    let fits = wire::FRAGMENT + max_frame.div_ceil(max_fragments);
    router.add_transport(Idle, with_mtu(fits))?;
    router.close().await;
    Ok(())
}

#[tokio::test]
async fn routed_sends_reject_invalid_destinations_and_payloads_with_typed_errors() -> io::Result<()>
{
    let router = Router::new(NodeId::new("local"))?;
    let application = router.bind_protocol(42)?;
    let to = router.local_id().clone();
    assert_eq!(
        typed::<RoutedSendError>(&application.packet_buffer(&NodeId::new(""), 0).unwrap_err()),
        RoutedSendError::Destination(NodeIdError::Empty)
    );
    let long = NodeId::new("n".repeat(MAX_NODE_ID_BYTES + 1));
    assert_eq!(
        typed::<RoutedSendError>(&application.send_owned(&long, Bytes::new()).unwrap_err()),
        RoutedSendError::Destination(NodeIdError::TooLong {
            len: MAX_NODE_ID_BYTES + 1,
            max: MAX_NODE_ID_BYTES,
        })
    );
    let max = application.max_payload(&to);
    application.packet_buffer(&to, max)?;
    let too_large = RoutedSendError::PayloadTooLarge { len: max + 1, max };
    assert_eq!(
        typed::<RoutedSendError>(&application.packet_buffer(&to, max + 1).unwrap_err()),
        too_large
    );
    assert_eq!(
        typed::<RoutedSendError>(&application.send(&to, &vec![0; max + 1]).unwrap_err()),
        too_large
    );
    assert_eq!(
        typed::<RoutedSendError>(
            &application
                .send_owned(&to, Bytes::from(vec![0; max + 1]))
                .unwrap_err()
        ),
        too_large
    );
    let mut outgrown = application.packet_buffer(&to, 0)?;
    outgrown.extend_from_slice(&vec![0; max + 1]);
    assert_eq!(
        typed::<RoutedSendError>(&application.send_packet(&to, outgrown).unwrap_err()),
        too_large
    );
    let foreign = application.packet_buffer(&to, 0)?;
    assert_eq!(
        typed::<RoutedSendError>(
            &application
                .send_packet(&NodeId::new("other"), foreign)
                .unwrap_err()
        ),
        RoutedSendError::PacketMismatch
    );
    let other = router.bind_protocol(43)?;
    let foreign = other.packet_buffer(&to, 0)?;
    assert_eq!(
        typed::<RoutedSendError>(&application.send_packet(&to, foreign).unwrap_err()),
        RoutedSendError::PacketMismatch
    );
    let error = RoutedSendError::Destination(NodeIdError::Empty);
    assert_eq!(
        std::error::Error::source(&error).and_then(|source| source.downcast_ref::<NodeIdError>()),
        Some(&NodeIdError::Empty)
    );
    router.close().await;
    Ok(())
}
