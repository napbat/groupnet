//! Authenticated setup namespaces must not leak into the ordered bulk queue.

use futures_util::io::{AsyncReadExt, AsyncWriteExt};
use groupnet_transport::bulk::BulkTransport;
use tokio::time::timeout;

use super::{
    DEADLINE,
    fixtures::{Fabric, credentials},
};

#[tokio::test]
async fn concurrent_ordered_and_control_accepts_never_steal_setup_and_exporters_bind_session() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    timeout(DEADLINE, async {
        let (ordered, control, accepted_ordered, accepted_control) = tokio::join!(
            sender.connect(fabric.c.local_id()),
            sender.connect_control(fabric.c.local_id()),
            receiver.accept(),
            receiver.accept_control(),
        );
        let mut ordered = ordered.unwrap();
        let mut control = control.unwrap();
        let (ordered_peer, mut accepted_ordered) = accepted_ordered.unwrap();
        let (control_peer, mut accepted_control) = accepted_control.unwrap();
        assert_eq!(ordered_peer, *fabric.a.local_id());
        assert_eq!(control_peer, *fabric.a.local_id());
        ordered.write_all(b"ordered").await.unwrap();
        control.write_all(b"control").await.unwrap();
        let mut ordered_bytes = [0; 7];
        let mut control_bytes = [0; 7];
        accepted_ordered
            .read_exact(&mut ordered_bytes)
            .await
            .unwrap();
        accepted_control
            .read_exact(&mut control_bytes)
            .await
            .unwrap();
        assert_eq!(&ordered_bytes, b"ordered");
        assert_eq!(&control_bytes, b"control");
        accepted_ordered.write_all(b"bulk reply").await.unwrap();
        accepted_control.write_all(b"setup reply").await.unwrap();
        let mut bulk_reply = [0; 10];
        let mut setup_reply = [0; 11];
        ordered.read_exact(&mut bulk_reply).await.unwrap();
        control.read_exact(&mut setup_reply).await.unwrap();
        assert_eq!(&bulk_reply, b"bulk reply");
        assert_eq!(&setup_reply, b"setup reply");

        let mut client_key = [0; 32];
        let mut server_key = [0; 32];
        let mut reverse_key = [0; 32];
        let mut ordered_key = [0; 32];
        let label = b"EXPORTER-groupnet-test-session";
        control
            .export_keying_material(&mut client_key, label, Some(b"forward"))
            .unwrap();
        accepted_control
            .export_keying_material(&mut server_key, label, Some(b"forward"))
            .unwrap();
        control
            .export_keying_material(&mut reverse_key, label, Some(b"reverse"))
            .unwrap();
        ordered
            .export_keying_material(&mut ordered_key, label, Some(b"forward"))
            .unwrap();
        assert_eq!(client_key, server_key);
        assert_ne!(client_key, reverse_key);
        assert_ne!(client_key, ordered_key);
        assert!(receiver.revoke_peer(fabric.a.local_id()));
        assert!(accepted_control.cancellation().is_cancelled());
        assert!(
            accepted_control
                .export_keying_material(&mut server_key, label, None)
                .is_err()
        );
        assert!(accepted_ordered.read(&mut [0]).await.is_err());
    })
    .await
    .unwrap();
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn queued_control_from_revoked_admission_is_not_restored_by_readmission() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let pin =
        groupnet_network::tunnel::PeerIdentity::new(fabric.a.local_id().clone(), &a.leaf).unwrap();
    let (sender, receiver) = fabric.tunnels(a, c);
    let mut old = timeout(DEADLINE, sender.connect_control(fabric.c.local_id()))
        .await
        .unwrap()
        .unwrap();
    assert!(receiver.revoke_peer(fabric.a.local_id()));
    receiver.admit_peer(pin).unwrap();
    timeout(DEADLINE, async {
        let (new, accepted) = tokio::join!(
            sender.connect_control(fabric.c.local_id()),
            receiver.accept_control(),
        );
        let mut new = new.unwrap();
        let mut accepted = accepted.unwrap().1;
        new.write_all(b"fresh").await.unwrap();
        let mut bytes = [0; 5];
        accepted.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"fresh");
        assert!(old.read(&mut [0]).await.is_err());
    })
    .await
    .unwrap();
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn transport_shutdown_wakes_both_namespace_accepts() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    timeout(DEADLINE, async {
        let (ordered, control, ()) = tokio::join!(
            receiver.accept(),
            receiver.accept_control(),
            receiver.close()
        );
        assert!(ordered.is_err());
        assert!(control.is_err());
    })
    .await
    .unwrap();
    sender.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn last_transport_handle_drop_cancels_live_control_and_ordered_streams() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let (ordered, control, accepted_ordered, accepted_control) = timeout(DEADLINE, async {
        tokio::join!(
            sender.connect(fabric.c.local_id()),
            sender.connect_control(fabric.c.local_id()),
            receiver.accept(),
            receiver.accept_control()
        )
    })
    .await
    .unwrap();
    let mut ordered = ordered.unwrap();
    let mut control = control.unwrap();
    let _accepted_ordered = accepted_ordered.unwrap();
    let _accepted_control = accepted_control.unwrap();
    drop(sender);
    assert!(ordered.cancellation().is_cancelled());
    assert!(control.cancellation().is_cancelled());
    assert!(ordered.write_all(b"closed").await.is_err());
    assert!(control.write_all(b"closed").await.is_err());
    receiver.close().await;
    fabric.close().await;
}

#[tokio::test]
async fn retained_idle_control_keeps_exporter_admission_alive() {
    let fabric = Fabric::new(false, false).await;
    let (a, c, _) = credentials();
    let (sender, receiver) = fabric.tunnels(a, c);
    let (client, server) = timeout(DEADLINE, async {
        tokio::join!(
            sender.connect_control(fabric.c.local_id()),
            receiver.accept_control(),
        )
    })
    .await
    .unwrap();
    let mut client = client.unwrap();
    let mut server = server.unwrap().1;
    let mut clock = tokio::time::interval(std::time::Duration::from_secs(1));
    for _ in 0..23 {
        clock.tick().await;
    }
    assert!(!client.cancellation().is_cancelled());
    assert!(!server.cancellation().is_cancelled());
    timeout(DEADLINE, async {
        client.write_all(b"alive").await.unwrap();
        let mut bytes = [0; 5];
        server.read_exact(&mut bytes).await.unwrap();
        assert_eq!(&bytes, b"alive");
        let mut client_key = [0; 32];
        let mut server_key = [0; 32];
        client
            .export_keying_material(&mut client_key, b"EXPORTER-idle-control", None)
            .unwrap();
        server
            .export_keying_material(&mut server_key, b"EXPORTER-idle-control", None)
            .unwrap();
        assert_eq!(client_key, server_key);
    })
    .await
    .unwrap();
    sender.close().await;
    receiver.close().await;
    fabric.close().await;
}
