//! Broken bulk peers cannot turn an incomplete exchange into an admitted reply.

#![cfg(feature = "volatile-bootstrap-bulk")]

use std::time::{Duration, Instant};

use futures_util::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionLimits, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_wire::{
    BootstrapBulkClient, BulkError, BulkLimits, Correlation, Envelope, ExchangeKind, Message,
    PhaseLimits, WireLimits, encode,
};
use groupnet_consistency::volatile_recovery::bootstrap::ports::DonorRequest;
use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::journal::{CaptureId, JournalCursor, ReservationId};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapOperation, BootstrapScope, ClaimIdentity,
};
use groupnet_transport::bulk::{DataPlane, DataStream};
use groupnet_transport_mem::MemBulkNet;
use tokio::sync::oneshot;

fn budget() -> ByteAdmission {
    ByteAdmission::new(AdmissionLimits {
        max_total_bytes: 100_000,
        max_encoded_bytes: 100_000,
        max_decoded_bytes: 100_000,
        max_suffix_bytes: 100_000,
        max_native_overlap_bytes: 100_000,
        max_inflight_bytes: 100_000,
        max_reservations: 100,
    })
    .unwrap()
}

fn limits() -> BulkLimits {
    BulkLimits {
        wire: WireLimits {
            max_frame_bytes: 4096,
            max_scope_bytes: 64,
            max_node_bytes: 32,
            max_payload_bytes: 2048,
        },
        phase: PhaseLimits {
            max_body_bytes: 2048,
            max_scope_bytes: 64,
            max_node_bytes: 32,
            max_writer_bytes: 32,
            max_cuts: 4,
            max_members: 4,
            max_events: 4,
            max_identity_bytes: 32,
            max_effect_bytes: 128,
        },
        server_request_ms: 1000,
    }
}

fn exchange() -> (Correlation, DonorRequest) {
    let scope = BootstrapScope {
        domain: "origin".into(),
        partition: "bucket".into(),
    };
    let donor = ClaimIdentity {
        node: NodeId::from("donor"),
        incarnation: BootId(11),
        session: 12,
        attempt: 13,
    };
    let follower = ClaimIdentity {
        node: NodeId::from("follower"),
        incarnation: BootId(21),
        session: 22,
        attempt: 23,
    };
    let capture = CaptureId {
        scope: scope.clone(),
        donor: donor.clone(),
        recovery_generation: 2,
        serial: 3,
    };
    (
        Correlation {
            scope,
            donor,
            follower: follower.clone(),
            parent: BootstrapOperation {
                incarnation: BootId(21),
                session: 22,
                generation: 1,
                token: 31,
            },
            child: BootstrapOperation {
                incarnation: BootId(21),
                session: 22,
                generation: 1,
                token: 32,
            },
        },
        DonorRequest::Release {
            reservation: ReservationId {
                capture,
                follower,
                serial: 4,
            },
        },
    )
}

#[derive(Clone, Copy, Debug)]
enum Fault {
    WrongCorrelation,
    TrailingReply,
    DuplicateTerminal,
    MissingTerminal,
    StalledEof,
    PartialHeader,
    PartialPayload,
    LostReserveResponse,
}

async fn serve_fault<S: AsyncRead + AsyncWrite + Unpin>(
    mut stream: DataStream<S>,
    correlation: Correlation,
    fault: Fault,
    released: oneshot::Receiver<()>,
) {
    assert!(stream.recv_bounded(4096).await.unwrap().is_some());
    assert!(stream.recv_bounded(4096).await.unwrap().is_none());
    let reply = Envelope {
        exchange: ExchangeKind::Release,
        correlation,
        message: Message::Reply(Vec::new()),
    };
    let terminal = Envelope {
        message: Message::Terminator {
            frames: 1,
            bytes: 0,
        },
        ..reply.clone()
    };
    match fault {
        Fault::WrongCorrelation => {
            let mut wrong = reply;
            wrong.correlation.child.token += 1;
            stream
                .send_bounded(encode(&wrong, limits().wire).unwrap().into(), 4096)
                .await
                .unwrap();
        }
        Fault::TrailingReply | Fault::DuplicateTerminal | Fault::StalledEof => {
            for frame in [&reply, &terminal] {
                stream
                    .send_bounded(encode(frame, limits().wire).unwrap().into(), 4096)
                    .await
                    .unwrap();
            }
            if matches!(fault, Fault::StalledEof) {
                let _ = released.await;
            } else {
                let extra = if matches!(fault, Fault::TrailingReply) {
                    &reply
                } else {
                    &terminal
                };
                stream
                    .send_bounded(encode(extra, limits().wire).unwrap().into(), 4096)
                    .await
                    .unwrap();
            }
        }
        Fault::MissingTerminal => {
            stream
                .send_bounded(encode(&reply, limits().wire).unwrap().into(), 4096)
                .await
                .unwrap();
        }
        Fault::PartialHeader | Fault::PartialPayload => {
            let mut raw = stream.into_inner();
            if matches!(fault, Fault::PartialHeader) {
                raw.write_all(&[0, 0, 0]).await.unwrap();
            } else {
                raw.write_all(&[0, 0, 0, 10, 0, 0, 0, 0, 1, 2])
                    .await
                    .unwrap();
            }
            raw.flush().await.unwrap();
        }
        Fault::LostReserveResponse => {}
    }
}

#[tokio::test]
async fn malformed_lost_and_stalled_peers_never_yield_a_typed_reply() {
    for fault in [
        Fault::WrongCorrelation,
        Fault::TrailingReply,
        Fault::DuplicateTerminal,
        Fault::MissingTerminal,
        Fault::StalledEof,
        Fault::PartialHeader,
        Fault::PartialPayload,
        Fault::LostReserveResponse,
    ] {
        let net = MemBulkNet::new();
        let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
        let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
        let charge = budget();
        let (correlation, mut request) = exchange();
        if matches!(fault, Fault::LostReserveResponse) {
            let DonorRequest::Release { reservation } = &request else {
                unreachable!();
            };
            request = DonorRequest::Reserve {
                follower: correlation.follower.clone(),
                cut: JournalCursor {
                    capture: reservation.capture.clone(),
                    position: 0,
                },
            };
        }
        let server_correlation = correlation.clone();
        let (release_server, released) = oneshot::channel::<()>();
        let server = tokio::spawn(async move {
            let (_, stream) = donor.accept().await.unwrap();
            serve_fault(stream, server_correlation, fault, released).await;
        });
        let client = BootstrapBulkClient::new(follower, charge.clone(), limits()).unwrap();
        let result = client
            .request(
                correlation,
                &request,
                Instant::now() + Duration::from_millis(80),
            )
            .await;
        assert!(result.is_err(), "{fault:?} admitted an incomplete exchange");
        if matches!(fault, Fault::StalledEof) {
            assert!(matches!(result, Err(BulkError::Expired)));
        }
        let _ = release_server.send(());
        server.await.unwrap();
        assert_eq!(charge.usage().0, 0, "{fault:?} retained bytes");
    }
}
