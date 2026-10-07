use super::codec::{Cursor, decode, encode};
use super::frame::{DataHeader, FrameHeader, RelayTarget, VERSION};
use super::*;
use crate::tcp::MAX_TCP_MESSAGE;
use groupnet_transport::framing::LengthHeader;
use ring::hmac;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use zerocopy::byteorder::big_endian::{U16, U64};
use zerocopy::{FromBytes, IntoBytes};

#[test]
fn typed_headers_preserve_network_layout_and_unaligned_views() {
    let length = LengthHeader::new(0x0102_0304).unwrap();
    assert_eq!(length.as_bytes(), &[1, 2, 3, 4]);
    let header = FrameHeader {
        version: VERSION,
        authenticated: 1,
        sequence: U64::new(0x0102_0304_0506_0708),
    };
    assert_eq!(header.as_bytes(), &[2, 1, 1, 2, 3, 4, 5, 6, 7, 8]);
    let mut unaligned = [0; 11];
    unaligned[1..].copy_from_slice(header.as_bytes());
    assert_eq!(
        FrameHeader::ref_from_bytes(&unaligned[1..])
            .unwrap()
            .sequence
            .get(),
        header.sequence.get()
    );
    let data = DataHeader {
        kind: 11,
        length: U16::new(0x0102),
    };
    assert_eq!(data.as_bytes(), &[11, 1, 2]);
    assert_eq!(core::mem::size_of::<RelayTarget>(), 34);
}

#[test]
fn decoding_data_slices_the_original_owned_frame() {
    let frame = Bytes::from(vec![11, 0, 3, 4, 5, 6]);
    let pointer = frame[3..].as_ptr();
    let mut cursor = Cursor(&frame);
    let Message::Data(payload) = decode(&mut cursor, &frame, frame.len()).unwrap() else {
        panic!("data frame")
    };
    assert_eq!(payload.as_ptr(), pointer);
    assert_eq!(payload.as_ref(), &[4, 5, 6]);
    assert_eq!(cursor.0, b"");
    drop(frame);
    assert_eq!(payload.as_ref(), &[4, 5, 6]);
}

#[tokio::test]
async fn split_data_and_relay_writes_match_the_existing_authenticated_layout() {
    for message in [
        Message::Data(Bytes::from_static(b"payload")),
        Message::Relay {
            node: NodeId::new("peer"),
            session: [4; 32],
            data: Bytes::from_static(b"payload"),
        },
    ] {
        let auth = Some(keyed(&[9; 32]));
        let mut expected = FrameHeader {
            version: VERSION,
            authenticated: 1,
            sequence: U64::new(1),
        }
        .as_bytes()
        .to_vec();
        encode(&message, &mut expected).unwrap();
        expected.extend_from_slice(hmac::sign(&auth.as_ref().unwrap().key, &expected).as_ref());
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let mut actual = Vec::new();
        let mut scratch = Vec::new();
        let writing = async {
            write_buffered(&mut writer, &auth, &message, &mut scratch)
                .await
                .unwrap();
            drop(writer);
        };
        let reading = reader.read_to_end(&mut actual);
        let ((), read_result) = tokio::join!(writing, reading);
        read_result.unwrap();
        assert!(
            scratch.is_empty(),
            "hot data writes must not concatenate into scratch"
        );
        assert_eq!(
            actual[..4],
            LengthHeader::new(expected.len()).unwrap().as_bytes()[..]
        );
        assert_eq!(&actual[4..], expected.as_slice());
        let (mut writer, mut reader) = tokio::io::duplex(1);
        let receive_auth = Some(keyed(&[9; 32]));
        let reading = read(&mut reader, &receive_auth);
        let writing = async {
            writer.write_all(&actual).await.unwrap();
        };
        let (decoded, ()) = tokio::join!(reading, writing);
        let mut reencoded = Vec::new();
        encode(&decoded.unwrap(), &mut reencoded).unwrap();
        let mut original = Vec::new();
        encode(&message, &mut original).unwrap();
        assert_eq!(original, reencoded);
    }
}

#[tokio::test]
async fn control_writer_reuses_its_scratch_storage() {
    let (mut writer, mut reader) = tokio::io::duplex(256);
    let mut scratch = Vec::new();
    write_buffered(&mut writer, &None, &Message::Ping, &mut scratch)
        .await
        .unwrap();
    let pointer = scratch.as_ptr();
    read(&mut reader, &None).await.unwrap();
    write_buffered(&mut writer, &None, &Message::Ping, &mut scratch)
        .await
        .unwrap();
    assert_eq!(scratch.as_ptr(), pointer);
    read(&mut reader, &None).await.unwrap();
}

#[tokio::test]
async fn keyed_frames_do_not_downgrade() {
    let key = crate::NetworkKey::from_bytes([9; 32]);
    let (mut writer, mut reader) = tokio::io::duplex(256);
    write(&mut writer, &None, &Message::Ping).await.unwrap();
    assert!(read(&mut reader, &auth(Some(&key))).await.is_err());
}

#[test]
fn proofs_bind_every_session_and_challenge() {
    let secret = [4; 32];
    let proof = proof(&secret, b"hello", &[&[1; 32], &[2; 32]]);
    assert!(!matches(
        &proof,
        &super::proof(&secret, b"hello", &[&[1; 32], &[3; 32]])
    ));
    assert!(!matches(
        &proof,
        &super::proof(&[5; 32], b"hello", &[&[1; 32], &[2; 32]])
    ));
}

#[tokio::test]
async fn recorded_keyed_control_frames_cannot_cross_registration_sessions() {
    let master = auth(Some(&crate::NetworkKey::from_bytes([7; 32])));
    let old = control_auth(&master, &[1; 32], &[2; 32], Role::Server);
    let new = control_auth(&master, &[1; 32], &[3; 32], Role::Client);
    let (mut writer, mut reader) = tokio::io::duplex(256);
    write(
        &mut writer,
        &old.tx,
        &Message::Intro {
            node: NodeId::from("peer"),
            session: [4; 32],
            secret: [5; 32],
            candidates: Vec::new(),
        },
    )
    .await
    .unwrap();
    assert!(read(&mut reader, &new.rx).await.is_err());
}

#[test]
fn every_message_round_trips_with_bounded_ipv4_ipv6_candidates() {
    let v4: SocketAddr = "127.0.0.1:1234".parse().unwrap();
    let v6: SocketAddr = "[::1]:4321".parse().unwrap();
    let messages = vec![
        Message::Challenge([1; 32]),
        Message::Register {
            node: NodeId::from("local"),
            session: [2; 32],
            challenge: [3; 32],
            credential: vec![4; 1024],
            peers: vec![NodeId::from("remote")],
            dynamic: true,
            relay_only: false,
            candidates: vec![v4, v6],
        },
        Message::Welcome { observed: v4 },
        Message::Denied,
        Message::Intro {
            node: NodeId::from("remote"),
            session: [5; 32],
            secret: [6; 32],
            candidates: vec![v6],
        },
        Message::Gone {
            node: NodeId::from("remote"),
            session: [5; 32],
        },
        Message::Relay {
            node: NodeId::from("remote"),
            session: [5; 32],
            data: vec![7; MAX_TCP_MESSAGE].into(),
        },
        Message::Ping,
        Message::Hello {
            node: NodeId::from("remote"),
            session: [8; 32],
            target: [9; 32],
            nonce: [10; 32],
            proof: [11; 32],
        },
        Message::Answer {
            nonce: [12; 32],
            proof: [13; 32],
        },
        Message::Finish([14; 32]),
        Message::Data(Bytes::new()),
    ];
    for message in messages {
        let mut bytes = Vec::new();
        encode(&message, &mut bytes).unwrap();
        let bytes = Bytes::from(bytes);
        let mut cursor = Cursor(&bytes);
        let decoded = decode(&mut cursor, &bytes, bytes.len()).unwrap();
        assert_eq!(cursor.0, []);
        let mut encoded = Vec::new();
        encode(&decoded, &mut encoded).unwrap();
        assert_eq!(bytes, encoded);
    }
    let unknown = Bytes::from_static(&[255]);
    let truncated = Bytes::from_static(&[0, 1]);
    assert!(decode(&mut Cursor(&unknown), &unknown, unknown.len()).is_err());
    assert!(decode(&mut Cursor(&truncated), &truncated, truncated.len()).is_err());
}
