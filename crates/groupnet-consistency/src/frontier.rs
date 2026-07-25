//! Applied-write watermarks and the read-your-writes barrier they enable.

use std::collections::HashMap;

use groupnet_core::NodeId;

use crate::token::WriteToken;

/// Applied-write watermarks per peer, advanced by the application's apply
/// loop — see [`Frontier`].
type Applied = HashMap<NodeId, WriteToken>;

/// The writer half of the applied-write frontier.
///
/// The apply loop calls [`Frontier::advance`] after each peer write has
/// actually been applied (the stale copy dropped, or the gap remediation
/// finished). Barriers on the matching [`FrontierView`] then mean *applied*,
/// not merely delivered.
#[derive(Debug)]
pub struct Frontier {
    tx: tokio::sync::watch::Sender<Applied>,
}

/// The reader half: cheap to clone, held wherever reads need a
/// read-your-writes barrier.
#[derive(Debug, Clone)]
pub struct FrontierView {
    rx: tokio::sync::watch::Receiver<Applied>,
}

impl Frontier {
    /// A fresh frontier (nothing applied) and its reader view.
    #[must_use]
    pub fn new() -> (Self, FrontierView) {
        let (tx, rx) = tokio::sync::watch::channel(Applied::new());
        (Self { tx }, FrontierView { rx })
    }

    /// Marks `peer`'s writes as applied through `token` (monotonic in
    /// epoch-major order: lower tokens are ignored).
    pub fn advance(&self, peer: &NodeId, token: WriteToken) {
        self.tx.send_modify(|applied| {
            let entry = applied
                .entry(peer.clone())
                .or_insert(WriteToken { epoch: 0, seq: 0 });
            if *entry < token {
                *entry = token;
            }
        });
    }
}

impl FrontierView {
    /// Waits until `peer`'s writes through `token` have been applied
    /// locally. A watermark from a newer epoch also satisfies older-epoch
    /// tokens: the frontier only enters a new epoch through gap
    /// remediation, which covered the previous life.
    ///
    /// Returns `false` if the [`Frontier`] was dropped first (the apply
    /// loop is gone — do not serve reads assuming freshness). Combine with
    /// a caller-side timeout for bounded waiting.
    pub async fn reached(&self, peer: &NodeId, token: WriteToken) -> bool {
        let mut rx = self.rx.clone();
        rx.wait_for(|applied| applied.get(peer).is_some_and(|&t| t >= token))
            .await
            .is_ok()
    }
}
