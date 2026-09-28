//! Per-scope worker event loop.

use super::{Command, Driver, Manager, Published, SessionShared};
use crate::replication::api::{ApplicationAdapter, SourceAdapter};
use crate::replication::snapshot_runtime::SnapshotMode;
use groupnet_core::replication::{Event, SessionEngine};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;
use tokio::sync::{mpsc, watch};

pub(in crate::replication::shell) async fn worker<S, A, M>(
    manager: Arc<Manager<S, A, M>>,
    shared: Arc<SessionShared>,
    engine: SessionEngine,
    mut receiver: mpsc::Receiver<Command>,
    updates: watch::Sender<Published>,
) where
    S: SourceAdapter,
    A: ApplicationAdapter<S::Position, S::Batch>,
    M: SnapshotMode<S, A>,
{
    let mut driver = Driver {
        manager,
        shared,
        engine,
        updates,
        effects: VecDeque::new(),
        deadlines: HashMap::new(),
        payloads: HashMap::new(),
        checkpoints: HashMap::new(),
        deferred_floors: VecDeque::new(),
        started: Instant::now(),
        proof: None,
        tail_checked_at: None,
        tail_request_started: None,
        tail_authority_epoch: 0,
        failure: None,
        cancel_reply: None,
        cancel_processed: false,
        snapshot_hold: None,
        snapshot_read: None,
        snapshot_attachment: None,
        snapshot_stage: None,
        snapshot_candidate_permit: None,
        snapshot_chunk: None,
        snapshot_proof: None,
        snapshot_payload_id: None,
        snapshot_candidate_id: None,
        snapshot_attempt: None,
    };
    driver.step(Event::StartBootstrap);
    loop {
        if driver.shared.cancelled.load(Ordering::Acquire) && !driver.cancel_processed {
            driver.shared.local_gate.store(false, Ordering::Release);
            driver.shared.fence.invalidate();
            driver.effects.clear();
            driver.payloads.clear();
            driver.checkpoints.clear();
            driver.snapshot_attachment = None;
            driver.snapshot_stage = None;
            driver.snapshot_chunk = None;
            driver.snapshot_candidate_permit = None;
            driver.snapshot_read = None;
            driver.step(Event::Cancel);
            driver.cancel_processed = true;
        }
        if driver
            .engine
            .next_deadline()
            .is_some_and(|due| due <= driver.logical_now())
        {
            driver.tick();
        }
        if let Ok(command) = receiver.try_recv() {
            driver.command(command);
        }
        if let Some(effect) = driver.effects.pop_front() {
            driver.effect(effect).await;
            continue;
        }
        if let Some(reply) = driver.cancel_reply.take() {
            let _ = reply.send(driver.failure);
        }
        let next_timer = driver.next_timer();
        let shared = Arc::clone(&driver.shared);
        tokio::select! {
            command = receiver.recv() => {
                let Some(command) = command else { break; };
                driver.command(command);
            }
            () = shared.hints.notified() => {
                if shared.hinted.swap(false, Ordering::AcqRel) {
                    driver.step(Event::Hint);
                }
            }
            () = async {
                if let Some(instant) = next_timer {
                    tokio::time::sleep_until(instant).await;
                } else {
                    std::future::pending::<()>().await;
                }
            } => driver.tick(),
        }
    }
    driver.shared.alive.store(false, Ordering::Release);
    driver.shared.local_gate.store(false, Ordering::Release);
    driver.shared.fence.invalidate();
}
