use super::*;

#[tokio::test(start_paused = true)]
async fn missing_authenticated_heartbeats_still_expire_the_established_read_leg() {
    let (mut reader, _blackholed_server) = tokio::io::duplex(64);
    let (events, _incoming) = mpsc::channel(1);
    let reading = tokio::spawn(async move {
        let auth = Some(wire::keyed(&[3; 32]));
        read_control(&mut reader, &auth, &events).await
    });
    tokio::task::yield_now().await;
    tokio::time::advance(IDLE + Duration::from_secs(1)).await;
    assert_eq!(
        reading.await.unwrap().unwrap_err().kind(),
        io::ErrorKind::NotConnected
    );
}
