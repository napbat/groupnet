//! Independent bounded TCP framing and challenge-bound proofs.
//!
//! [`frame`] owns the length-prefixed, optionally authenticated and sequenced
//! frame I/O (length prefix and vectored writes from
//! `groupnet_transport::framing`); [`codec`] owns message bodies; [`auth`] owns
//! frame keys and handshake proofs.
use bytes::Bytes;
use groupnet_core::NodeId;
use std::net::SocketAddr;

mod auth;
mod codec;
mod frame;

pub(super) use auth::{Auth, Duplex, Role, auth, control_auth, fresh, keyed, matches, proof};
pub(super) use frame::{read, write, write_buffered};

pub(super) type Token = [u8; 32];

pub(super) enum Message {
    Challenge(Token),
    Register {
        node: NodeId,
        session: Token,
        challenge: Token,
        credential: Vec<u8>,
        peers: Vec<NodeId>,
        dynamic: bool,
        relay_only: bool,
        candidates: Vec<SocketAddr>,
    },
    Welcome {
        observed: SocketAddr,
    },
    Denied,
    Intro {
        node: NodeId,
        session: Token,
        secret: Token,
        candidates: Vec<SocketAddr>,
    },
    Gone {
        node: NodeId,
        session: Token,
    },
    Relay {
        node: NodeId,
        session: Token,
        data: Bytes,
    },
    Ping,
    Hello {
        node: NodeId,
        session: Token,
        target: Token,
        nonce: Token,
        proof: Token,
    },
    Answer {
        nonce: Token,
        proof: Token,
    },
    Finish(Token),
    Data(Bytes),
}

#[cfg(test)]
mod tests;

#[cfg(test)]
pub(super) mod directional_tests;
