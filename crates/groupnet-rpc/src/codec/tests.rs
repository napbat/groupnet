use super::*;

/// Encodes `frame` into the contiguous bytes a data-plane frame carries.
fn wire(frame: Frame) -> Bytes {
    let encoded = frame.encode();
    let mut bytes = encoded.head().to_vec();
    bytes.extend_from_slice(encoded.body());
    assert_eq!(bytes.len(), encoded.len());
    Bytes::from(bytes)
}

fn samples() -> Vec<Frame> {
    vec![
        Frame::Request {
            id: 0,
            deadline_ms: 1,
            payload: Bytes::new(),
        },
        Frame::Request {
            id: u64::MAX,
            deadline_ms: u32::MAX,
            payload: Bytes::from_static(b"request body"),
        },
        Frame::Response {
            id: 7,
            payload: Bytes::from_static(b"\x00\xffbinary"),
        },
        Frame::Response {
            id: 8,
            payload: Bytes::new(),
        },
        Frame::Error {
            id: 9,
            status: RpcStatus::new(404, "no such key: caf\u{e9}"),
        },
        Frame::Error {
            id: 10,
            status: RpcStatus::new(RpcStatus::DEADLINE_EXCEEDED, ""),
        },
        Frame::Subscribe {
            id: 11,
            window: NonZeroU32::MIN,
            payload: Bytes::new(),
        },
        Frame::Subscribe {
            id: u64::MAX,
            window: NonZeroU32::MAX,
            payload: Bytes::from_static(b"topic"),
        },
        Frame::Item {
            id: 12,
            payload: Bytes::from_static(b"\x00item"),
        },
        Frame::Item {
            id: 13,
            payload: Bytes::new(),
        },
        Frame::End { id: 14 },
        Frame::Credit {
            id: 15,
            grant: NonZeroU32::MIN,
        },
        Frame::Credit {
            id: 16,
            grant: NonZeroU32::MAX,
        },
        Frame::Cancel { id: 17 },
    ]
}

/// The head length of `frame`'s kind: every shorter prefix is malformed.
fn head_len(frame: &Frame) -> usize {
    match frame {
        Frame::Request { .. } => REQUEST_HEAD,
        Frame::Response { .. } => RESPONSE_HEAD,
        Frame::Error { .. } => ERROR_HEAD,
        Frame::Subscribe { .. } => SUBSCRIBE_HEAD,
        Frame::Item { .. } => ITEM_HEAD,
        Frame::End { .. } | Frame::Cancel { .. } => BARE_FRAME,
        Frame::Credit { .. } => CREDIT_FRAME,
    }
}

#[test]
fn every_kind_round_trips() {
    for frame in samples() {
        assert_eq!(Frame::decode(&wire(frame.clone())), Ok(frame));
    }
}

#[test]
fn the_layout_is_the_documented_one() {
    // The typed heads are unaligned and padding-free: 1 + 1 + 8 (+ 4 | + 2).
    assert_eq!((REQUEST_HEAD, RESPONSE_HEAD, ERROR_HEAD), (14, 10, 12));
    let bytes = wire(Frame::Request {
        id: 0x0102_0304_0506_0708,
        deadline_ms: 0x0A0B_0C0D,
        payload: Bytes::from_static(b"p"),
    });
    assert_eq!(
        &bytes[..],
        &[
            VERSION, 0, 1, 2, 3, 4, 5, 6, 7, 8, 0x0A, 0x0B, 0x0C, 0x0D, b'p'
        ]
    );
    let bytes = wire(Frame::Error {
        id: 1,
        status: RpcStatus::new(0x0203, "m"),
    });
    assert_eq!(
        &bytes[..],
        &[VERSION, 2, 0, 0, 0, 0, 0, 0, 0, 1, 2, 3, b'm']
    );
}

#[test]
fn the_subscription_layouts_are_the_documented_ones() {
    assert_eq!(
        (SUBSCRIBE_HEAD, ITEM_HEAD, BARE_FRAME, CREDIT_FRAME),
        (14, 10, 10, 14)
    );
    let id = [1, 2, 3, 4, 5, 6, 7, 8];
    let cases = [
        (
            Frame::Subscribe {
                id: 0x0102_0304_0506_0708,
                window: NonZeroU32::new(0x0A0B_0C0D).unwrap(),
                payload: Bytes::from_static(b"s"),
            },
            [&[VERSION, 3][..], &id, &[0x0A, 0x0B, 0x0C, 0x0D, b's']].concat(),
        ),
        (
            Frame::Item {
                id: 0x0102_0304_0506_0708,
                payload: Bytes::from_static(b"i"),
            },
            [&[VERSION, 4][..], &id, b"i"].concat(),
        ),
        (
            Frame::End {
                id: 0x0102_0304_0506_0708,
            },
            [&[VERSION, 5][..], &id].concat(),
        ),
        (
            Frame::Credit {
                id: 0x0102_0304_0506_0708,
                grant: NonZeroU32::new(0x0A0B_0C0D).unwrap(),
            },
            [&[VERSION, 6][..], &id, &[0x0A, 0x0B, 0x0C, 0x0D]].concat(),
        ),
        (
            Frame::Cancel {
                id: 0x0102_0304_0506_0708,
            },
            [&[VERSION, 7][..], &id].concat(),
        ),
    ];
    for (frame, expected) in cases {
        assert_eq!(&wire(frame.clone())[..], &expected[..], "{frame:?}");
    }
}

#[test]
fn a_zero_window_or_grant_is_rejected() {
    let mut subscribe = wire(Frame::Subscribe {
        id: 1,
        window: NonZeroU32::MIN,
        payload: Bytes::new(),
    })
    .to_vec();
    subscribe[13] = 0;
    assert_eq!(
        Frame::decode(&Bytes::from(subscribe)),
        Err(Malformed("zero window"))
    );
    let mut credit = wire(Frame::Credit {
        id: 1,
        grant: NonZeroU32::MIN,
    })
    .to_vec();
    credit[13] = 0;
    assert_eq!(
        Frame::decode(&Bytes::from(credit)),
        Err(Malformed("zero grant"))
    );
}

#[test]
fn bytes_after_an_end_credit_or_cancel_are_rejected() {
    for frame in [
        Frame::End { id: 1 },
        Frame::Credit {
            id: 1,
            grant: NonZeroU32::MIN,
        },
        Frame::Cancel { id: 1 },
    ] {
        let mut bytes = wire(frame.clone()).to_vec();
        bytes.push(0);
        assert_eq!(
            Frame::decode(&Bytes::from(bytes)),
            Err(Malformed("bytes after a fixed-size frame")),
            "{frame:?}"
        );
    }
}

#[test]
fn every_truncated_head_is_rejected() {
    for frame in samples() {
        let bytes = wire(frame.clone());
        for cut in 0..head_len(&frame) {
            assert!(
                Frame::decode(&bytes.slice(..cut)).is_err(),
                "{frame:?} cut at {cut} decoded"
            );
        }
    }
}

#[test]
fn a_foreign_version_or_kind_is_rejected() {
    let good = wire(Frame::Response {
        id: 1,
        payload: Bytes::from_static(b"x"),
    });
    for (index, value) in [(0, VERSION + 1), (0, 0), (1, 8), (1, u8::MAX)] {
        let mut bad = good.to_vec();
        bad[index] = value;
        assert!(
            Frame::decode(&Bytes::from(bad)).is_err(),
            "byte {index} = {value}"
        );
    }
}

#[test]
fn a_zero_deadline_is_rejected() {
    let mut bytes = wire(Frame::Request {
        id: 1,
        deadline_ms: 1,
        payload: Bytes::new(),
    })
    .to_vec();
    bytes[13] = 0;
    assert_eq!(
        Frame::decode(&Bytes::from(bytes)),
        Err(Malformed("zero deadline"))
    );
}

#[test]
fn a_non_utf8_error_message_is_rejected() {
    let mut bytes = wire(Frame::Error {
        id: 1,
        status: RpcStatus::new(1, "ok"),
    })
    .to_vec();
    bytes.push(0xFF);
    assert!(Frame::decode(&Bytes::from(bytes)).is_err());
}

#[test]
fn a_decoded_payload_shares_the_frame_buffer() {
    let bytes = wire(Frame::Response {
        id: 1,
        payload: Bytes::from_static(b"shared"),
    });
    let Ok(Frame::Response { payload, .. }) = Frame::decode(&bytes) else {
        panic!("decodes");
    };
    assert_eq!(payload.as_ptr(), bytes[RESPONSE_HEAD..].as_ptr());
}
