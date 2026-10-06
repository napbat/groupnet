//! Concrete transport futures behind one erased worker lifetime.
use super::{LinkFuture, LinkIo};
use crate::Transport;
use futures_util::{SinkExt, StreamExt};
use groupnet_core::NodeId;
use std::sync::Arc;

pub(super) trait Worker: Send + Sync {
    fn learn_peer(&self, peer: &NodeId, address: &str);

    fn run(self: Arc<Self>, io: LinkIo) -> LinkFuture<'static, ()>;
}

pub(super) struct Typed<T>(pub(super) T);

impl<T: Transport> Worker for Typed<T> {
    fn learn_peer(&self, peer: &NodeId, address: &str) {
        self.0.learn_peer(peer, address);
    }

    fn run(self: Arc<Self>, mut io: LinkIo) -> LinkFuture<'static, ()> {
        Box::pin(async move {
            let send = async {
                while let Some(packet) = io.outgoing.next().await {
                    let _ = tokio::time::timeout_at(
                        packet.deadline,
                        self.0
                            .send_admitted(&packet.peer, packet.bytes(), packet.session),
                    )
                    .await;
                }
            };
            let receive = async {
                loop {
                    match self.0.recv_admitted().await {
                        Ok(packet) if packet.packet.msg.len() <= io.mtu => {
                            if io.incoming.send(Some(packet)).await.is_err() {
                                break;
                            }
                        }
                        Ok(_) => {}
                        Err(_) => {
                            let _ = io.incoming.send(None).await;
                            break;
                        }
                    }
                }
            };
            tokio::select! {
                biased;
                () = io.cancel.cancelled() => {},
                () = send => {},
                () = receive => {},
            }
        })
    }
}
