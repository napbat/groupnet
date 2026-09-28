//! One bounded asynchronous worker for volatile recovery effects.

use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::Time;
use groupnet_core::volatile_recovery::{
    RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryOperation,
};

use super::{BoxRecoveryFuture, RecoveryAdapter, Shared, lock, pending_event, signal_effects};
use crate::volatile_recovery::bootstrap::driver::{BootstrapDriver, BootstrapOutcome};

fn logical_now(start: Instant) -> Time {
    Time(u64::try_from(start.elapsed().as_millis()).unwrap_or(u64::MAX))
}

struct WorkerExitGuard<A: RecoveryAdapter>(Arc<Shared<A>>);

impl<A: RecoveryAdapter> Drop for WorkerExitGuard<A> {
    fn drop(&mut self) {
        self.0.force_close(true);
    }
}

pub(super) fn absolute_deadline(started: Instant, due: Time) -> Option<Instant> {
    started.checked_add(Duration::from_millis(due.0))
}

pub(super) fn terminal_deadline<A: RecoveryAdapter>(
    shared: &Shared<A>,
    deadline: Option<Instant>,
) -> Option<Instant> {
    if deadline.is_none() {
        shared.force_close(true);
    }
    deadline
}

pub(super) fn affirm_effect<A: RecoveryAdapter>(
    shared: &Shared<A>,
    engine: &mut RecoveryEngine,
    started: Instant,
    active_version: u64,
    op: RecoveryOperation,
    now: impl Fn() -> Instant,
) -> Vec<RecoveryEffect> {
    if !engine.accepts_operation(op) {
        return Vec::new();
    }
    let Some(due) = engine.next_deadline() else {
        return Vec::new();
    };
    let Some(deadline) = terminal_deadline(shared, absolute_deadline(started, due)) else {
        return Vec::new();
    };
    let mut control = lock(&shared.control);
    if now() >= deadline {
        drop(control);
        return engine.step(RecoveryEvent::Tick(due)).effects;
    }
    let accepted =
        !control.terminal && control.version == active_version && shared.adapter.affirm(op);
    if accepted && now() >= deadline {
        control.open = false;
        shared.adapter.revoke_serving();
        drop(control);
        return engine.step(RecoveryEvent::Tick(due)).effects;
    }
    let result = engine.step(RecoveryEvent::Affirmed { op, accepted });
    control.state = engine.state();
    control.open = accepted && control.state.recovered;
    result.effects
}

#[expect(
    clippy::too_many_lines,
    reason = "one auditable shell effect dispatch; all protocol decisions remain in the sans-IO engine"
)]
pub(super) async fn run<A: RecoveryAdapter>(
    shared: Arc<Shared<A>>,
    mut engine: RecoveryEngine,
    config: RecoveryConfig,
    mut bootstrap: Option<Box<dyn BootstrapDriver>>,
) {
    let _exit_guard = WorkerExitGuard(Arc::clone(&shared));
    let started = Instant::now();
    let mut effects = std::collections::VecDeque::new();
    let mut active_version = 1;
    let mut active_baseline = None;
    loop {
        let (pending, version) = {
            let mut control = lock(&shared.control);
            let pending = std::mem::take(&mut control.pending);
            (pending, control.version)
        };
        if pending.start || pending.gap || pending.lapse > 0 || pending.cancel {
            active_version = version;
            // External signals can arrive after an arbitrarily long ready
            // interval. Start their finite deadlines at this actual time.
            let tick = engine.step(RecoveryEvent::Tick(logical_now(started)));
            let transition = engine.step(pending_event(pending));
            effects.clear();
            effects.extend(signal_effects(tick, transition));
            lock(&shared.control).state = engine.state();
            if pending.cancel {
                lock(&shared.control).pending.cancel = true;
                if let (Some(child), Some(op)) = (bootstrap.as_mut(), active_baseline) {
                    child.cancel(op).await;
                }
                return;
            }
        }

        if let Some(effect) = effects.pop_front() {
            let elapsed = engine.step(RecoveryEvent::Tick(logical_now(started)));
            if !elapsed.effects.is_empty() {
                effects.clear();
                effects.extend(elapsed.effects);
                lock(&shared.control).state = engine.state();
                continue;
            }
            match effect {
                RecoveryEffect::CloseGate { .. } => {
                    let Some(version) = shared.close(active_version) else {
                        continue;
                    };
                    active_version = version;
                }
                RecoveryEffect::ArmTimer(_) => {}
                RecoveryEffect::CancelBaseline { op } => {
                    if let Some(child) = bootstrap.as_mut() {
                        child.cancel(op).await;
                    }
                    if active_baseline == Some(op) {
                        active_baseline = None;
                    }
                }
                RecoveryEffect::AcquireBaseline { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let Some(child) = bootstrap.as_mut() else {
                        effects
                            .extend(engine.step(RecoveryEvent::BootstrapDeclined { op }).effects);
                        continue;
                    };
                    active_baseline = Some(op);
                    let Some(due) = terminal_deadline(
                        &shared,
                        engine
                            .next_deadline()
                            .and_then(|due| absolute_deadline(started, due)),
                    ) else {
                        return;
                    };
                    let Some(permit) = shared.permit(op, active_version, due) else {
                        continue;
                    };
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        child.acquire(op, permit, due),
                    )
                    .await;
                    shared.disarm(op);
                    effects.extend(
                        engine
                            .step(RecoveryEvent::Tick(logical_now(started)))
                            .effects,
                    );
                    if !engine.accepts_operation(op) {
                        child.cancel(op).await;
                        continue;
                    }
                    let result = match response {
                        Some(BootstrapOutcome::LocalBuilt) => {
                            engine.step(RecoveryEvent::LocalBaselineBuilt { op })
                        }
                        Some(BootstrapOutcome::PeerInstalled(handoff)) => {
                            handoff.consume(|handoff| {
                                engine.step(RecoveryEvent::PeerBaselineInstalled { op, handoff })
                            })
                        }
                        Some(BootstrapOutcome::Declined) | None => {
                            child.cancel(op).await;
                            engine.step(RecoveryEvent::BootstrapDeclined { op })
                        }
                    };
                    effects.extend(result.effects);
                }
                RecoveryEffect::ObservePeerHeads { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.observe_peer_heads(op, config),
                    )
                    .await;
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok((peers, identities)) => {
                                            RecoveryEvent::PeerHeadsObserved {
                                                op,
                                                peers,
                                                identities,
                                            }
                                        }
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::Invalidate {
                    op,
                    distrust_bodies,
                } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let Some(due) = terminal_deadline(
                        &shared,
                        engine
                            .next_deadline()
                            .and_then(|due| absolute_deadline(started, due)),
                    ) else {
                        return;
                    };
                    let Some(permit) = shared.permit(op, active_version, due) else {
                        continue;
                    };
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.invalidate(op, distrust_bodies, permit),
                    )
                    .await;
                    shared.disarm(op);
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::Invalidated { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::RebuildOrigin { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let Some(due) = terminal_deadline(
                        &shared,
                        engine
                            .next_deadline()
                            .and_then(|due| absolute_deadline(started, due)),
                    ) else {
                        return;
                    };
                    let Some(permit) = shared.permit(op, active_version, due) else {
                        continue;
                    };
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.rebuild_origin(op, permit),
                    )
                    .await;
                    shared.disarm(op);
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::Materialized { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::ObservePeers { op } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.observe_peers(op, config),
                    )
                    .await;
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok((peers, confirmed)) => RecoveryEvent::PeersObserved {
                                            op,
                                            peers,
                                            confirmed,
                                        },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::WaitFrontiers { op, heads } => {
                    if !engine.accepts_operation(op) {
                        continue;
                    }
                    let response = await_operation(
                        &shared,
                        &engine,
                        started,
                        active_version,
                        shared.adapter.wait_frontiers(op, heads),
                    )
                    .await;
                    if let Some(result) = response {
                        effects.extend(
                            engine
                                .step(RecoveryEvent::Tick(logical_now(started)))
                                .effects,
                        );
                        if engine.accepts_operation(op) {
                            effects.extend(
                                engine
                                    .step(match result {
                                        Ok(()) => RecoveryEvent::FrontiersReached { op },
                                        Err(_) => RecoveryEvent::Failed { op },
                                    })
                                    .effects,
                            );
                        }
                    }
                }
                RecoveryEffect::Affirm { op } => {
                    effects.extend(affirm_effect(
                        &shared,
                        &mut engine,
                        started,
                        active_version,
                        op,
                        Instant::now,
                    ));
                }
            }
            lock(&shared.control).state = engine.state();
            continue;
        }

        if let Some(child) = bootstrap.as_mut()
            && child
                .next_deadline()
                .is_some_and(|due| due <= Instant::now())
        {
            child.maintain(Instant::now()).await;
            continue;
        }
        let child_wake = bootstrap.as_ref().map(|child| child.wake());
        let Some(due) = engine.next_deadline() else {
            if let Some(child) = bootstrap.as_mut() {
                if let Some(due) = child.next_deadline() {
                    tokio::select! {
                        () = shared.notify.notified() => {},
                        () = bootstrap_wake(child_wake.as_ref()) => {
                            child.maintain(Instant::now()).await;
                        }
                        () = tokio::time::sleep_until(tokio::time::Instant::from_std(due)) => {
                            child.maintain(Instant::now()).await;
                        }
                    }
                } else {
                    tokio::select! {
                        () = shared.notify.notified() => {},
                        () = bootstrap_wake(child_wake.as_ref()) => {
                            child.maintain(Instant::now()).await;
                        }
                    }
                }
            } else {
                shared.notify.notified().await;
            }
            continue;
        };
        let Some(deadline) = terminal_deadline(&shared, absolute_deadline(started, due)) else {
            return;
        };
        let deadline = bootstrap
            .as_ref()
            .and_then(|child| child.next_deadline())
            .map_or(deadline, |child_due| deadline.min(child_due));
        tokio::select! {
            () = shared.notify.notified() => {}
            () = bootstrap_wake(child_wake.as_ref()) => {
                if let Some(child) = bootstrap.as_mut() {
                    child.maintain(Instant::now()).await;
                }
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                effects.extend(engine.step(RecoveryEvent::Tick(logical_now(started))).effects);
                lock(&shared.control).state = engine.state();
            }
        }
    }
}

async fn bootstrap_wake(wake: Option<&Arc<tokio::sync::Notify>>) {
    if let Some(wake) = wake {
        wake.notified().await;
    } else {
        std::future::pending::<()>().await;
    }
}

async fn await_operation<A: RecoveryAdapter, T>(
    shared: &Arc<Shared<A>>,
    engine: &RecoveryEngine,
    started: Instant,
    active_version: u64,
    future: BoxRecoveryFuture<'_, T>,
) -> Option<T> {
    let due = engine.next_deadline()?;
    let deadline = terminal_deadline(shared, absolute_deadline(started, due))?;
    tokio::pin!(future);
    loop {
        let delay = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            biased;
            () = shared.notify.notified() => {
                let control = lock(&shared.control);
                if control.version != active_version || control.pending.cancel {
                    return None;
                }
            }
            () = tokio::time::sleep(delay) => return None,
            value = &mut future => return Some(value),
        }
    }
}
