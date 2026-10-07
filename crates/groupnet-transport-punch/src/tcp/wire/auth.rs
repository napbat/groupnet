//! Frame-authentication keys, direction-separated control keys, and proofs.
use ring::hmac;
use std::sync::{Arc, atomic::AtomicU64};

use super::Token;

pub(in crate::tcp) type Auth = Option<Arc<Authentication>>;

/// One direction's frame MAC key and its monotonic frame sequence.
pub(in crate::tcp) struct Authentication {
    pub(super) key: hmac::Key,
    pub(super) sequence: AtomicU64,
}

#[derive(Clone)]
pub(in crate::tcp) struct Duplex {
    pub(in crate::tcp) tx: Auth,
    pub(in crate::tcp) rx: Auth,
}

#[cfg(test)]
impl Duplex {
    pub(in crate::tcp) const fn plain() -> Self {
        Self { tx: None, rx: None }
    }
}

#[derive(Clone, Copy)]
pub(in crate::tcp) enum Role {
    Client,
    Server,
}

pub(in crate::tcp) fn keyed(secret: &Token) -> Arc<Authentication> {
    Arc::new(Authentication {
        key: hmac::Key::new(hmac::HMAC_SHA256, secret),
        sequence: AtomicU64::new(0),
    })
}

pub(in crate::tcp) fn auth(key: Option<&crate::NetworkKey>) -> Auth {
    key.map(|key| keyed(&key.to_bytes()))
}

pub(in crate::tcp) fn fresh(auth: &Auth) -> Auth {
    auth.as_ref().map(|auth| {
        Arc::new(Authentication {
            key: auth.key.clone(),
            sequence: AtomicU64::new(0),
        })
    })
}

pub(in crate::tcp) fn control_auth(
    auth: &Auth,
    challenge: &Token,
    session: &Token,
    role: Role,
) -> Duplex {
    let derive = |direction: &[u8]| {
        auth.as_ref().map(|auth| {
            let mut context = hmac::Context::with_key(&auth.key);
            context.update(b"groupnet TCP control session\0");
            context.update(challenge);
            context.update(session);
            context.update(direction);
            Arc::new(Authentication {
                key: hmac::Key::new(hmac::HMAC_SHA256, context.sign().as_ref()),
                sequence: AtomicU64::new(0),
            })
        })
    };
    let client = derive(b"client to server");
    let server = derive(b"server to client");
    match role {
        Role::Client => Duplex {
            tx: client,
            rx: server,
        },
        Role::Server => Duplex {
            tx: server,
            rx: client,
        },
    }
}

pub(in crate::tcp) fn proof(secret: &Token, domain: &[u8], parts: &[&[u8]]) -> Token {
    let key = hmac::Key::new(hmac::HMAC_SHA256, secret);
    let mut context = hmac::Context::with_key(&key);
    context.update(b"groupnet native TCP proof\0");
    context.update(domain);
    for part in parts {
        context.update(part);
    }
    let mut output = [0; 32];
    output.copy_from_slice(context.sign().as_ref());
    output
}

pub(in crate::tcp) fn matches(expected: &Token, actual: &Token) -> bool {
    // HMAC verification supplies a constant-time comparison without exposing
    // the introduction secret in the handshake.
    let key = hmac::Key::new(hmac::HMAC_SHA256, expected);
    hmac::verify(
        &key,
        b"TCP proof equality",
        hmac::sign(
            &hmac::Key::new(hmac::HMAC_SHA256, actual),
            b"TCP proof equality",
        )
        .as_ref(),
    )
    .is_ok()
}
