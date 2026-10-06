//! Concrete transport futures behind one erased worker lifetime.
use super::{LinkFuture, LinkIo};
use crate::Transport;
use futures_util::{SinkExt, StreamExt};

pub(super) trait Worker: Send {
    fn run(self: Box<Self>, io: LinkIo) -> LinkFuture<'static, ()>;
}

pub(super) struct Typed<T>(pub(super) T);

impl<T: Transport> Worker for Typed<T> {
    fn run(self: Box<Self>, mut io: LinkIo) -> LinkFuture<'static, ()> {
        Box::pin(async move {
            let send = async {
                while let Some(packet) = io.outgoing.next().await {
                    let _ = tokio::time::timeout_at(
                        packet.deadline,
                        self.0.send(&packet.peer, packet.bytes()),
                    )
                    .await;
                }
            };
            let receive = async {
                loop {
                    match self.0.recv().await {
                        Ok(packet) if packet.msg.len() <= io.mtu => {
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
