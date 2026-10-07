use super::*;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn captured(auth: &Auth, message: &Message) -> Vec<u8> {
    let (mut writer, mut reader) = tokio::io::duplex(4096);
    write(&mut writer, auth, message).await.unwrap();
    drop(writer);
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();
    bytes
}

pub(in crate::tcp) async fn rejects_reflection_and_replay(
    sender: &Duplex,
    recipient: &Duplex,
    message: &Message,
) {
    let bytes = captured(&sender.tx, message).await;
    let (mut writer, mut reader) = tokio::io::duplex(8192);
    writer.write_all(&bytes).await.unwrap();
    assert!(
        read(&mut reader, &sender.rx).await.is_err(),
        "reflected authentic outbound bytes"
    );
    let (mut writer, mut reader) = tokio::io::duplex(8192);
    writer.write_all(&bytes).await.unwrap();
    writer.write_all(&bytes).await.unwrap();
    assert!(read(&mut reader, &recipient.rx).await.is_ok());
    assert!(
        read(&mut reader, &recipient.rx).await.is_err(),
        "replayed authentic inbound bytes"
    );
}

#[tokio::test]
async fn control_relay_bytes_are_direction_bound_and_monotonic() {
    let key = crate::NetworkKey::from_bytes([8; 32]);
    let master = auth(Some(&key));
    let client = control_auth(&master, &[1; 32], &[2; 32], Role::Client);
    let server = control_auth(&master, &[1; 32], &[2; 32], Role::Server);
    rejects_reflection_and_replay(
        &client,
        &server,
        &Message::Relay {
            node: NodeId::from("peer"),
            session: [3; 32],
            data: Bytes::from_static(b"cannot reflect as peer traffic"),
        },
    )
    .await;
}
