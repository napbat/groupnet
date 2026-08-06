//! The three background tasks the shell spawns: renew, grant, ingest.
//!
//! Each one is a loop around a step that lives on
//! [`Shared`] next door — the tasks decide *when*, the
//! steps decide *what*. They hold an [`Arc`] of that shared state and never a
//! [`Leases`](super::Leases): a task that held the handle would keep its
//! [`Drop`] (and so its own abort) from ever running.
//!
//! All three are abort-driven. There is no shutdown channel and no join: the
//! handle's [`Drop`] aborts them, and every step is idempotent, so being
//! cancelled part-way through one costs at most a turn.

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use groupnet_runtime::GroupEvent;
use tokio::sync::broadcast::Receiver;
use tokio::sync::broadcast::error::RecvError;
use tokio::task::JoinHandle;
use tokio::time::{Interval, MissedTickBehavior};

use super::LeaseConfig;
use super::shell::Shared;
use super::wire::encode_grants;

/// Spawns the set's three tasks onto the current runtime.
///
/// # Panics
/// If there is no current Tokio runtime.
pub(super) fn spawn(shared: &Arc<Shared>) -> Vec<JoinHandle<()>> {
    vec![
        tokio::spawn(renewal_task(Arc::clone(shared))),
        tokio::spawn(granter_task(Arc::clone(shared))),
        tokio::spawn(view_task(Arc::clone(shared))),
    ]
}

/// A backstop ticker: the cadence at which a task re-folds even though no event
/// asked it to. One renewal interval, which is inside `duration / 2`, so a
/// whole class of missed edges costs latency rather than a lapse.
///
/// Reset immediately, because [`tokio::time::interval`]'s first tick completes
/// at once and every task here does its first turn before it waits.
fn backstop_ticker(cfg: &LeaseConfig) -> Interval {
    let mut ticker = tokio::time::interval(Duration::from_millis(cfg.renew_every_ms()));
    ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
    ticker.reset();
    ticker
}

/// The renewal ticker (a). The first turn fires at construction, so a reader is
/// renewing before [`Leases::new`](super::Leases::new) returns.
async fn renewal_task(shared: Arc<Shared>) {
    let mut ticker = tokio::time::interval(Duration::from_millis(shared.cfg.renew_every_ms()));
    // A stalled process re-spaces its renewals from *now* rather than firing
    // the burst it slept through: those turns are gone, and pretending
    // otherwise would record publish instants nobody's TTL was armed against.
    ticker.set_missed_tick_behavior(MissedTickBehavior::Delay);
    loop {
        ticker.tick().await;
        if shared.left.load(Ordering::Relaxed) {
            return;
        }
        let _ = shared.renew();
    }
}

/// The granter (b): re-fold and republish this node's wholesale grant map
/// whenever something that could have changed it happened.
async fn granter_task(shared: Arc<Shared>) {
    let mut events = shared.group.events();
    let mut ticker = backstop_ticker(&shared.cfg);
    // The last bytes actually published. Re-authoring an identical map would
    // bump the entry's version and re-disseminate it for nothing — and the
    // grant map is byte-stable by construction (a `BTreeMap`), so the
    // comparison is exact rather than a heuristic.
    let mut published: Option<Vec<u8>> = None;
    loop {
        if shared.left.load(Ordering::Relaxed) {
            return;
        }
        let encoded = encode_grants(&shared.grant_map());
        if published.as_deref() != Some(encoded.as_slice())
            && shared.publish_grants(encoded.clone()).is_ok()
        {
            published = Some(encoded);
        }
        if !await_grant_trigger(&shared, &mut events, &mut ticker).await {
            return;
        }
    }
}

/// Blocks until something that could change this node's grant map happens.
/// `false` once the group is gone.
async fn await_grant_trigger(
    shared: &Shared,
    events: &mut Receiver<GroupEvent>,
    ticker: &mut Interval,
) -> bool {
    loop {
        tokio::select! {
            _ = ticker.tick() => return true,
            event = events.recv() => match event {
                Ok(GroupEvent::NodeStateChanged { node, key }) => {
                    if key == shared.renewal_key && node != shared.me {
                        return true;
                    }
                }
                // Lag is a missed *edge*, never missed state: the fold above
                // re-reads every member's renewal entry wholesale, so one
                // re-fold is the whole resync.
                Ok(GroupEvent::MembershipChanged) | Err(RecvError::Lagged(_)) => return true,
                Ok(_) => {}
                Err(RecvError::Closed) => return false,
            },
        }
    }
}

/// The view (c): keep the reader's state machine and its published serve
/// deadline current.
///
/// Unlike the granter this reacts to **every** node-state change rather than
/// filtering by key. The roster is derived from the capability advertisement,
/// whose entry key belongs to the runtime layer and is deliberately not part of
/// this crate's vocabulary; guessing at it to save a fold would trade a
/// fail-*open* window (an advertiser missing from the roster confirms
/// vacuously) for CPU. The fold is read-only and its only output is a
/// deduplicated `watch` publish — but it is not free, and the cost lands where
/// a reader would not look for it: an [`AckLedger`](crate::AckLedger)
/// republishing a watermark per applied write wakes this task per write per
/// peer, and each turn re-decodes every granter's `O(roster)` map. The turn
/// count scales with the group's **write** rate, not its lease rate (the tier's
/// honesty box prices this).
async fn view_task(shared: Arc<Shared>) {
    let mut events = shared.group.events();
    let mut ticker = backstop_ticker(&shared.cfg);
    loop {
        if shared.left.load(Ordering::Relaxed) {
            return;
        }
        shared.refresh_view();
        tokio::select! {
            _ = ticker.tick() => {}
            event = events.recv() => {
                if matches!(event, Err(RecvError::Closed)) {
                    return;
                }
            }
        }
    }
}
