use tokio::io::{AsyncReadExt, AsyncWriteExt, DuplexStream, duplex};

use super::*;

fn secret(byte: u8) -> ClusterSecret {
    ClusterSecret::new([byte; ClusterSecret::MIN_LEN]).expect("valid secret")
}

fn node(id: &str) -> NodeId {
    NodeId::new(id)
}

/// Runs one side of a handshake and then drops its end of the pipe, as a
/// failed endpoint closes its socket.
async fn dialing(
    mut sock: DuplexStream,
    local: NodeId,
    peer: NodeId,
    secret: Option<ClusterSecret>,
) -> io::Result<DuplexStream> {
    dial(&mut sock, &local, &peer, secret.as_ref()).await?;
    Ok(sock)
}

async fn accepting(
    mut sock: DuplexStream,
    local: NodeId,
    secret: Option<ClusterSecret>,
) -> io::Result<(NodeId, DuplexStream)> {
    let from = accept(&mut sock, &local, secret.as_ref()).await?;
    Ok((from, sock))
}

async fn handshake(
    dialer: Option<ClusterSecret>,
    acceptor: Option<ClusterSecret>,
) -> (io::Result<DuplexStream>, io::Result<(NodeId, DuplexStream)>) {
    let (near, far) = duplex(1024);
    tokio::join!(
        dialing(near, node("a"), node("b"), dialer),
        accepting(far, node("b"), acceptor),
    )
}

/// Moves exactly `len` bytes from `from` to `to`, returning them.
async fn shuttle(from: &mut DuplexStream, to: &mut DuplexStream, len: usize) -> Vec<u8> {
    let mut bytes = vec![0; len];
    from.read_exact(&mut bytes).await.expect("read");
    to.write_all(&bytes).await.expect("write");
    bytes
}

const HELLO: usize = size_of::<Hello>();
const ID_FIELD: usize = 4 + 1;

#[tokio::test]
async fn peers_with_the_same_secret_connect_and_exchange_bytes() {
    let (dialer, acceptor) = handshake(Some(secret(1)), Some(secret(1))).await;
    let mut dialer = dialer.expect("dialer authenticated");
    let (from, mut acceptor) = acceptor.expect("acceptor authenticated");
    assert_eq!(from, node("a"));

    dialer.write_all(b"frame").await.expect("write");
    let mut frame = [0; 5];
    acceptor.read_exact(&mut frame).await.expect("read");
    assert_eq!(&frame, b"frame");
}

#[tokio::test]
async fn open_peers_keep_the_unauthenticated_handshake() {
    let (dialer, acceptor) = handshake(None, None).await;
    dialer.expect("open dial");
    assert_eq!(acceptor.expect("open accept").0, node("a"));
}

#[tokio::test]
async fn a_peer_with_another_secret_is_refused_on_both_sides() {
    let (dialer, acceptor) = handshake(Some(secret(1)), Some(secret(2))).await;
    assert_eq!(
        dialer.expect_err("acceptor tag refused").kind(),
        io::ErrorKind::PermissionDenied
    );
    // The dialer closed without answering: the acceptor never hands it on.
    assert_eq!(
        acceptor.expect_err("no answer").kind(),
        io::ErrorKind::UnexpectedEof
    );
}

#[tokio::test]
async fn a_peer_without_a_secret_is_refused_by_a_keyed_acceptor() {
    let (_, acceptor) = handshake(None, Some(secret(1))).await;
    assert_eq!(
        acceptor.expect_err("open hello refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

#[tokio::test]
async fn a_keyed_peer_is_refused_by_an_open_acceptor() {
    let (dialer, acceptor) = handshake(Some(secret(1)), None).await;
    assert_eq!(
        acceptor.expect_err("keyed hello refused").kind(),
        io::ErrorKind::PermissionDenied
    );
    assert_eq!(
        dialer.expect_err("no challenge").kind(),
        io::ErrorKind::UnexpectedEof
    );
}

/// A recorded dialer transcript does not authenticate a later connection:
/// the acceptor's fresh nonce changes the tag it expects.
#[tokio::test]
async fn a_replayed_handshake_is_refused() {
    let (dialer_near, mut dialer_far) = duplex(1024);
    let (acceptor_near, mut acceptor_far) = duplex(1024);
    let relay = async {
        let hello = shuttle(
            &mut dialer_far,
            &mut acceptor_far,
            HELLO + ID_FIELD + NONCE_LEN,
        )
        .await;
        shuttle(
            &mut acceptor_far,
            &mut dialer_far,
            ID_FIELD + NONCE_LEN + TAG_LEN,
        )
        .await;
        let answer = shuttle(&mut dialer_far, &mut acceptor_far, TAG_LEN).await;
        (hello, answer)
    };
    let (dialer, acceptor, (hello, answer)) = tokio::join!(
        dialing(dialer_near, node("a"), node("b"), Some(secret(1))),
        accepting(acceptor_near, node("b"), Some(secret(1))),
        relay,
    );
    dialer.expect("genuine dial");
    assert_eq!(acceptor.expect("genuine accept").0, node("a"));

    let (mut attacker, far) = duplex(1024);
    let attack = async {
        attacker.write_all(&hello).await.expect("replay hello");
        let mut challenge = [0; ID_FIELD + NONCE_LEN + TAG_LEN];
        attacker
            .read_exact(&mut challenge)
            .await
            .expect("challenge");
        attacker.write_all(&answer).await.expect("replay answer");
        attacker
    };
    let (_attacker, refused) = tokio::join!(attack, accepting(far, node("b"), Some(secret(1))));
    assert_eq!(
        refused.expect_err("replay refused").kind(),
        io::ErrorKind::PermissionDenied
    );
}

/// An acceptor that holds the secret but answers for another node id is not
/// the peer that was dialed.
#[tokio::test]
async fn an_acceptor_answering_for_another_id_is_refused() {
    let (near, far) = duplex(1024);
    let (dialer, _) = tokio::join!(
        dialing(near, node("a"), node("b"), Some(secret(1))),
        accepting(far, node("c"), Some(secret(1))),
    );
    assert_eq!(
        dialer.expect_err("wrong acceptor id").kind(),
        io::ErrorKind::PermissionDenied
    );
}

/// Rewriting the dialer's claimed id in flight breaks both tags: the
/// acceptor signs the forged id, which the dialer never claimed.
#[tokio::test]
async fn a_forged_dialer_id_is_refused() {
    let (dialer_near, mut dialer_far) = duplex(1024);
    let (acceptor_near, mut acceptor_far) = duplex(1024);
    let forge = async {
        let mut hello = [0; HELLO + ID_FIELD + NONCE_LEN];
        dialer_far.read_exact(&mut hello).await.expect("hello");
        assert_eq!(hello[HELLO + 4], b'a');
        hello[HELLO + 4] = b'z';
        acceptor_far.write_all(&hello).await.expect("forged hello");
        shuttle(
            &mut acceptor_far,
            &mut dialer_far,
            ID_FIELD + NONCE_LEN + TAG_LEN,
        )
        .await;
        drop(dialer_far);
        drop(acceptor_far);
    };
    let (dialer, acceptor, ()) = tokio::join!(
        dialing(dialer_near, node("a"), node("b"), Some(secret(1))),
        accepting(acceptor_near, node("b"), Some(secret(1))),
        forge,
    );
    assert_eq!(
        dialer.expect_err("forged transcript refused").kind(),
        io::ErrorKind::PermissionDenied
    );
    acceptor.expect_err("forged dialer never accepted");
}

/// The unversioned handshake, a bare id field, is refused.
#[tokio::test]
async fn the_unversioned_handshake_is_refused() {
    let (mut near, far) = duplex(1024);
    near.write_all(&[0, 0, 0, 1, b'a', 0]).await.expect("write");
    let error = accepting(far, node("b"), None)
        .await
        .expect_err("old version refused");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn the_hello_keeps_its_wire_layout() {
    let (near, mut far) = duplex(1024);
    dialing(near, node("ab"), node("b"), None)
        .await
        .expect("open dial");
    let mut wire = Vec::new();
    far.read_to_end(&mut wire).await.expect("read");
    assert_eq!(
        wire,
        [
            b'G', b'N', b'B', b'H', VERSION, OPEN, 0, 0, 0, 2, b'a', b'b'
        ]
    );
}
