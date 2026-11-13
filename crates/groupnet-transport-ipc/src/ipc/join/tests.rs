use super::*;
use tokio::io::{AsyncWriteExt, duplex};

#[tokio::test]
async fn hello_and_identity_round_trip() {
    let (mut dialer, mut acceptor) = duplex(4096);
    let credential = vec![7; MAX_CREDENTIAL_BYTES];
    write_hello(&mut dialer, &NodeId::new("client"), &credential)
        .await
        .unwrap();
    write_id(&mut dialer, &NodeId::new("n\u{153}ud"))
        .await
        .unwrap();
    let hello = read_hello(&mut acceptor).await.unwrap();
    assert_eq!(hello.node, NodeId::new("client"));
    assert_eq!(hello.credential, credential);
    assert_eq!(
        read_id(&mut acceptor).await.unwrap(),
        NodeId::new("n\u{153}ud")
    );
}

#[tokio::test]
async fn a_static_introduction_is_refused_on_its_magic() {
    let (mut dialer, mut acceptor) = duplex(64);
    // Only the static magic: the reader must not wait for more bytes.
    dialer.write_all(b"GNI1").await.unwrap();
    let error = read_hello(&mut acceptor).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn oversized_fields_are_refused_before_their_bodies_arrive() {
    let too_long_id = u32::try_from(MAX_ID + 1).unwrap();
    let too_long_credential = u32::try_from(MAX_CREDENTIAL_BYTES + 1).unwrap();
    for (id, credential) in [(too_long_id, 0), (1, too_long_credential)] {
        let (mut dialer, mut acceptor) = duplex(64);
        dialer.write_all(&MAGIC).await.unwrap();
        dialer.write_all(&id.to_be_bytes()).await.unwrap();
        if id == 1 {
            dialer.write_all(b"a").await.unwrap();
            dialer.write_all(&credential.to_be_bytes()).await.unwrap();
        }
        // No body follows: a reader that allocated and waited would hang.
        let error = read_hello(&mut acceptor).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData, "{error}");
    }
}

#[tokio::test]
async fn empty_and_non_utf8_identities_are_refused() {
    for body in [&[][..], &[0xff, 0xfe][..]] {
        let (mut dialer, mut acceptor) = duplex(64);
        let length = u32::try_from(body.len()).unwrap();
        dialer.write_all(&length.to_be_bytes()).await.unwrap();
        dialer.write_all(body).await.unwrap();
        let error = read_id(&mut acceptor).await.unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    }
}

#[tokio::test]
async fn identities_outside_the_bound_are_never_written() {
    let mut sink = Vec::new();
    for id in [String::new(), "x".repeat(MAX_ID + 1)] {
        let error = write_id(&mut sink, &NodeId::new(id.clone()))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
        let error = write_hello(&mut sink, &NodeId::new(id), &[])
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::InvalidInput);
    }
    assert_eq!(sink, Vec::<u8>::new());
}
