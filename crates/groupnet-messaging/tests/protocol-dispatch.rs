//! Public custom implementations and lossless generic receipt/context ownership.

use groupnet_core::{GroupId, NodeId};
use groupnet_messaging::{
    Bytes, Delivery, Frame, MessageId, MessageProtocol, Messaging, SendOptions, codec,
};
use groupnet_network::{ProtocolId, ProtocolIo, Router, RouterConfig};
use std::io;

#[derive(Debug)]
struct CustomReceipt(u8);

#[derive(Debug)]
struct CustomProtocol(ProtocolIo);

impl MessageProtocol for CustomProtocol {
    const ID: ProtocolId = 41;
    type SendOptions = u8;
    type Receipt = CustomReceipt;

    fn send(
        &self,
        to: &NodeId,
        group: Option<&GroupId>,
        payload: Bytes,
        options: u8,
    ) -> impl Future<Output = io::Result<MessageId>> + Send {
        let id = MessageId([options; 16]);
        std::future::ready(
            codec::data(id, Delivery::BestEffort, group, &payload)
                .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "custom payload bound"))
                .and_then(|packet| self.0.send(to, &packet))
                .map(|()| id),
        )
    }

    async fn recv(&self) -> io::Result<Frame<CustomReceipt>> {
        let packet = self.0.recv().await?;
        let codec::Packet::Data {
            id, group, payload, ..
        } = codec::decode(&packet.payload)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "custom packet"))?
        else {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "unexpected ack"));
        };
        let offset = packet.payload.len() - payload.len();
        Ok(Frame {
            id,
            from: packet.from,
            group,
            payload: packet.payload.slice(offset..),
            receipt: CustomReceipt(id.0[0]),
        })
    }

    fn shutdown(&self) {
        self.0.shutdown();
    }

    async fn closed(&self) {
        self.0.closed().await;
    }
}

async fn typed_send<P: MessageProtocol>(
    endpoint: &P,
    to: &NodeId,
    group: &GroupId,
    bytes: Bytes,
    options: P::SendOptions,
) -> io::Result<MessageId> {
    endpoint.send(to, Some(group), bytes, options).await
}

#[tokio::test]
async fn custom_and_default_protocols_share_routes_without_inbox_stealing() -> io::Result<()> {
    let local = NodeId::new("custom-local");
    let group = GroupId::new("custom-group");
    let router = Router::new(local.clone(), RouterConfig::default())?;
    let messages = Messaging::new(&router)?;
    let custom = CustomProtocol(router.bind_protocol(CustomProtocol::ID)?);
    let ordinary = typed_send(
        &messages,
        &local,
        &group,
        Bytes::from_static(b"ordinary"),
        SendOptions::default(),
    )
    .await?;
    let custom_id = typed_send(&custom, &local, &group, Bytes::from_static(b"custom"), 9).await?;
    let custom_frame = custom.recv().await?;
    assert_eq!(custom_frame.id, custom_id);
    let pointer = custom_frame.payload.as_ptr();
    let (context, payload) = custom_frame.into_parts();
    assert_eq!(payload.as_ptr(), pointer);
    assert_eq!(context.id, custom_id);
    assert_eq!(context.from, local);
    assert_eq!(context.group, Some(group.clone()));
    assert_eq!(context.receipt.0, 9);
    let ordinary_frame = MessageProtocol::recv(&messages).await?;
    assert_eq!(ordinary_frame.id, ordinary);
    assert_eq!(ordinary_frame.group, Some(group));
    assert_eq!(ordinary_frame.payload.as_ref(), b"ordinary");
    custom.shutdown();
    custom.closed().await;
    assert_eq!(
        custom.recv().await.unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
    assert!(!router.is_closed());
    messages.close().await;
    router.close().await;
    Ok(())
}

#[test]
fn generic_context_moves_nonclone_receipt_without_losing_metadata() {
    let payload = Bytes::from(vec![1, 2, 3]);
    let pointer = payload.as_ptr();
    let id = MessageId([3; 16]);
    let frame = Frame {
        id,
        from: NodeId::new("origin"),
        group: Some(GroupId::new("group")),
        payload,
        receipt: CustomReceipt(5),
    };
    let (context, bytes) = frame.into_parts();
    assert_eq!(bytes.as_ptr(), pointer);
    drop(bytes);
    assert_eq!(context.id, id);
    assert_eq!(context.from, NodeId::new("origin"));
    assert_eq!(context.group, Some(GroupId::new("group")));
    assert_eq!(context.receipt.0, 5);
}
