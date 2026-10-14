//! Exact bounded donor request/reply over the real in-memory bulk binding.

#![cfg(feature = "volatile-bootstrap-bulk")]

use std::time::{Duration, Instant};

use groupnet_consistency::volatile_recovery::bootstrap::admission::{
    AdmissionClass, AdmissionLimits, ByteAdmission,
};
use groupnet_consistency::volatile_recovery::bootstrap::bulk_wire::{
    BootstrapBulkClient, BootstrapBulkListener, BulkError, BulkLimits, Correlation, PhaseLimits,
    Refusal, WireLimits, WireReply,
};
use groupnet_consistency::volatile_recovery::bootstrap::inbox::DonorInbox;
use groupnet_consistency::volatile_recovery::bootstrap::ports::{DonorReply, DonorRequest};
use groupnet_core::NodeId;
use groupnet_core::volatile_bootstrap::journal::{CaptureId, ReservationId};
use groupnet_core::volatile_bootstrap::{
    BootId, BootstrapOperation, BootstrapScope, ClaimIdentity,
};
use groupnet_transport::bulk::DataPlane;
use groupnet_transport_mem::MemBulkNet;
use tokio::sync::{oneshot, watch};

fn admission() -> ByteAdmission {
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
    let request = DonorRequest::Release {
        reservation: ReservationId {
            capture,
            follower: follower.clone(),
            serial: 4,
        },
    };
    (
        Correlation {
            scope,
            donor,
            follower,
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
        request,
    )
}

#[tokio::test]
async fn real_bulk_binding_delivers_exact_typed_reply_and_releases_all_charges() {
    let net = MemBulkNet::new();
    let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let server_budget = admission();
    let client_budget = admission();
    let (correlation, request) = exchange();
    let (sender, mut inbox) = DonorInbox::new(4).unwrap();
    inbox.set_identity(Some(correlation.donor.clone()));
    let listener =
        BootstrapBulkListener::new(donor, sender, server_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let expected = request.clone();
    let worker = tokio::spawn(async move {
        let incoming = inbox.recv().await.expect("one admitted request");
        assert_eq!(incoming.request(), &expected);
        incoming.respond(Ok(DonorReply::Released));
    });
    let client = BootstrapBulkClient::new(follower, client_budget.clone(), limits()).unwrap();
    let reply = client
        .request(
            correlation,
            &request,
            Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
    assert!(matches!(reply.get(), WireReply::Released));
    drop(reply);
    worker.await.unwrap();
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    assert_eq!(client_budget.usage().0, 0);
    assert_eq!(server_budget.usage().0, 0);
}

#[tokio::test]
async fn timed_out_caller_and_cancelled_listener_retire_queued_work() {
    let net = MemBulkNet::new();
    let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let server_budget = admission();
    let client_budget = admission();
    let (correlation, request) = exchange();
    let (sender, inbox) = DonorInbox::new(1).unwrap();
    inbox.set_identity(Some(correlation.donor.clone()));
    let listener =
        BootstrapBulkListener::new(donor, sender, server_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let client = BootstrapBulkClient::new(follower, client_budget.clone(), limits()).unwrap();
    let result = client
        .request(
            correlation,
            &request,
            Instant::now() + Duration::from_millis(30),
        )
        .await;
    assert!(matches!(result, Err(BulkError::Expired)));
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    drop(inbox);
    assert_eq!(client_budget.usage().0, 0);
    assert_eq!(server_budget.usage().0, 0);
}

#[tokio::test]
async fn full_donor_inbox_returns_exact_capacity_refusal_and_retires_charge() {
    let net = MemBulkNet::new();
    let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let server_budget = admission();
    let client_budget = admission();
    let (correlation, request) = exchange();
    let (sender, inbox) = DonorInbox::new(1).unwrap();
    inbox.set_identity(Some(correlation.donor.clone()));
    let occupied = server_budget
        .reserve(AdmissionClass::Inflight, 512)
        .unwrap()
        .hold(request.clone());
    let pending = sender.try_submit(occupied).unwrap();
    let listener =
        BootstrapBulkListener::new(donor, sender, server_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let client = BootstrapBulkClient::new(follower, client_budget.clone(), limits()).unwrap();
    assert!(matches!(
        client
            .request(
                correlation,
                &request,
                Instant::now() + Duration::from_secs(2)
            )
            .await,
        Err(BulkError::Refused(Refusal::Capacity))
    ));
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    drop(pending);
    drop(inbox);
    assert_eq!(client_budget.usage().0, 0);
    assert_eq!(server_budget.usage().0, 0);
}

#[tokio::test]
async fn closed_donor_inbox_rejects_without_retaining_bytes() {
    let net = MemBulkNet::new();
    let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let server_budget = admission();
    let client_budget = admission();
    let (correlation, request) = exchange();
    let (sender, inbox) = DonorInbox::new(1).unwrap();
    inbox.set_identity(Some(correlation.donor.clone()));
    drop(inbox);
    let listener =
        BootstrapBulkListener::new(donor, sender, server_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let client = BootstrapBulkClient::new(follower, client_budget.clone(), limits()).unwrap();
    assert!(matches!(
        client
            .request(
                correlation,
                &request,
                Instant::now() + Duration::from_secs(2)
            )
            .await,
        Err(BulkError::Protocol)
    ));
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    assert_eq!(client_budget.usage().0, 0);
    assert_eq!(server_budget.usage().0, 0);
}

#[tokio::test]
async fn listener_follows_worker_capture_identity_across_session_rotation() {
    let net = MemBulkNet::new();
    let donor = DataPlane::new(net.endpoint(NodeId::from("donor")));
    let follower = DataPlane::new(net.endpoint(NodeId::from("follower")));
    let server_budget = admission();
    let client_budget = admission();
    let (old, old_request) = exchange();
    let mut renewed = old.clone();
    renewed.donor.session += 1;
    renewed.donor.attempt = 1;
    let mut next_request = old_request.clone();
    let DonorRequest::Release { reservation } = &mut next_request else {
        unreachable!();
    };
    reservation.capture.donor = renewed.donor.clone();
    let (sender, mut inbox) = DonorInbox::new(4).unwrap();
    inbox.set_identity(Some(old.donor.clone()));
    let listener =
        BootstrapBulkListener::new(donor, sender, server_budget.clone(), limits()).unwrap();
    let (stop, stopped) = watch::channel(false);
    let listening = tokio::spawn(listener.run(stopped));
    let (rotate, rotated) = oneshot::channel::<()>();
    let (ready, ready_to_use) = oneshot::channel::<()>();
    let next_identity = renewed.donor.clone();
    let worker = tokio::spawn(async move {
        inbox
            .recv()
            .await
            .unwrap()
            .respond(Ok(DonorReply::Released));
        rotated.await.unwrap();
        inbox.set_identity(Some(next_identity));
        ready.send(()).unwrap();
        inbox
            .recv()
            .await
            .unwrap()
            .respond(Ok(DonorReply::Released));
    });
    let client = BootstrapBulkClient::new(follower, client_budget.clone(), limits()).unwrap();
    let deadline = || Instant::now() + Duration::from_secs(2);
    let old_reply = client
        .request(old.clone(), &old_request, deadline())
        .await
        .unwrap();
    assert!(matches!(old_reply.get(), WireReply::Released));
    drop(old_reply);
    rotate.send(()).unwrap();
    ready_to_use.await.unwrap();
    assert!(client.request(old, &old_request, deadline()).await.is_err());
    let next_reply = client
        .request(renewed, &next_request, deadline())
        .await
        .unwrap();
    assert!(matches!(next_reply.get(), WireReply::Released));
    drop(next_reply);
    worker.await.unwrap();
    stop.send(true).unwrap();
    listening.await.unwrap().unwrap();
    assert_eq!(client_budget.usage().0, 0);
    assert_eq!(server_budget.usage().0, 0);
}
