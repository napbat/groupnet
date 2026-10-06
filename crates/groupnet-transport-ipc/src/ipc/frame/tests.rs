use super::*;

#[test]
fn typed_headers_preserve_existing_layout_and_endianness() {
    let intro = IntroductionHeader {
        magic: MAGIC,
        id_len: 64,
    };
    assert_eq!(intro.as_bytes(), b"GNI1\x40");
    assert_eq!(std::mem::size_of::<IntroductionHeader>(), 5);
    let frame = FrameHeader {
        length: U32::new(0x1234_5678),
    };
    assert_eq!(frame.as_bytes(), &[0x78, 0x56, 0x34, 0x12]);
    assert_eq!(std::mem::size_of::<FrameHeader>(), 4);
    let parsed = FrameHeader::ref_from_bytes(frame.as_bytes()).unwrap();
    assert_eq!(parsed.length.get(), 0x1234_5678);
}

#[tokio::test]
async fn frame_round_trip_includes_empty_and_maximum_payloads() {
    for payload in [Vec::new(), b"hello".to_vec(), vec![0x5a; MAX_FRAME]] {
        let mut encoded = Vec::new();
        write(&mut encoded, &payload).await.unwrap();
        assert_eq!(
            &encoded[..4],
            &u32::try_from(payload.len()).unwrap().to_le_bytes()
        );
        let mut input = encoded.as_slice();
        assert_eq!(read(&mut input).await.unwrap(), payload);
        assert_eq!(input, b"");
    }
}

#[tokio::test]
async fn malformed_and_truncated_frames_fail_closed() {
    let oversized = u32::try_from(MAX_FRAME + 1).unwrap().to_le_bytes();
    assert_eq!(
        read(&mut oversized.as_slice()).await.unwrap_err().kind(),
        io::ErrorKind::InvalidData
    );
    for bytes in [&[1, 0, 0][..], &[2, 0, 0, 0, 1][..]] {
        let mut input = bytes;
        assert_eq!(
            read(&mut input).await.unwrap_err().kind(),
            io::ErrorKind::UnexpectedEof
        );
    }
    let mut output = Vec::new();
    assert_eq!(
        write(&mut output, &vec![0; MAX_FRAME + 1])
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::InvalidData
    );
    assert_eq!(output.as_slice(), b"");
}

#[tokio::test]
async fn introduction_round_trip_accepts_protocol_id_boundary() {
    let (mut left, mut right) = tokio::io::duplex(256);
    let local = NodeId::new("local");
    let remote = NodeId::new("r".repeat(MAX_ID));
    let (left_id, right_id) =
        tokio::join!(introduce(&mut left, &local), introduce(&mut right, &remote));
    assert_eq!(left_id.unwrap(), remote);
    assert_eq!(right_id.unwrap(), local);
}

#[tokio::test]
async fn introduction_rejects_magic_lengths_utf8_and_truncation() {
    for bytes in [
        &b"BAD!\x01x"[..],
        &b"GNI1\x00"[..],
        &b"GNI1\x41"[..],
        &b"GNI1\x01\xff"[..],
    ] {
        let (mut local, mut remote) = tokio::io::duplex(256);
        remote.write_all(bytes).await.unwrap();
        assert_eq!(
            introduce(&mut local, &NodeId::new("local"))
                .await
                .unwrap_err()
                .kind(),
            io::ErrorKind::InvalidData
        );
    }
    let (mut local, mut remote) = tokio::io::duplex(256);
    remote.write_all(b"GNI1\x02x").await.unwrap();
    remote.shutdown().await.unwrap();
    assert_eq!(
        introduce(&mut local, &NodeId::new("local"))
            .await
            .unwrap_err()
            .kind(),
        io::ErrorKind::UnexpectedEof
    );
    assert!(validate_id(&NodeId::new("")).is_err());
    assert!(validate_id(&NodeId::new("x".repeat(MAX_ID + 1))).is_err());
}
