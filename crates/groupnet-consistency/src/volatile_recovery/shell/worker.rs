//! One bounded asynchronous worker for volatile recovery effects.

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::{Duration, Instant};

use groupnet_core::Time;
use groupnet_core::volatile_recovery::{
    RecoveryConfig, RecoveryEffect, RecoveryEngine, RecoveryEvent, RecoveryOperation,
};

use super::{
    AdapterError, BoxRecoveryFuture, RecoveryAdapter, Shared, lock, pending_event, signal_effects,
};
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
    let mut effects = VecDeque::new();
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
                if let Some(child) = bootstrap.as_mut() {
                    child.shutdown().await;
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
                RecoveryEffect::SuspendLocalBaseline { op } => {
                    if let Some(child) = bootstrap.as_mut() {
                        child.suspend_local(op).await;
                    }
                    // Keep the exact old cleanup binding until Resume or
                    // CancelBaseline. A terminal signal can interrupt the
                    // lapse before the core's queued cleanup effects run.
                }
                RecoveryEffect::ResumeLocalBaseline { previous, current } => {
                    if let Some(child) = bootstrap.as_mut() {
                        let ready = Instant::now()
                            .checked_add(Duration::from_millis(config.total_ms))
                            .and_then(|deadline| shared.ready_capture(deadline));
                        if ready
                            .as_ref()
                            .and_then(|guard| guard.capture(|generation| generation))
                            == Some(current.generation)
                            && child.resume_local(previous, current)
                        {
                            active_baseline = Some(current);
                            maintain_child(&shared, &config, child).await;
                        } else {
                            child.cancel(previous).await;
                        }
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
                        &mut engine,
                        started,
                        (active_version, op),
                        &mut effects,
                        child.acquire(op, permit),
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
                    let Some(due) = terminal_deadline(
                        &shared,
                        engine
                            .next_deadline()
                            .and_then(|due| absolute_deadline(started, due)),
                    ) else {
                        return;
                    };
                    // The child owns native participation; the adapter adds
                    // only its domain feed heads through the existing peer
                    // observation. Without a participation roster the check
                    // fails closed and the core recovers from origin.
                    let identities = match bootstrap.as_mut() {
                        Some(child) => {
                            await_operation(
                                &shared,
                                &mut engine,
                                started,
                                (active_version, op),
                                &mut effects,
                                child.peer_roster(due),
                            )
                            .await
                        }
                        None => Some(None),
                    };
                    let response = match identities {
                        Some(Some(identities)) => await_operation(
                            &shared,
                            &mut engine,
                            started,
                            (active_version, op),
                            &mut effects,
                            shared.adapter.observe_peers(op, config),
                        )
                        .await
                        .map(|observed| observed.map(|(peers, _)| (peers, identities))),
                        Some(None) => Some(Err(AdapterError)),
                        None => None,
                    };
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
                        &mut engine,
                        started,
                        (active_version, op),
                        &mut effects,
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
                        &mut engine,
                        started,
                        (active_version, op),
                        &mut effects,
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
                        &mut engine,
                        started,
                        (active_version, op),
                        &mut effects,
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
                        &mut engine,
                        started,
                        (active_version, op),
                        &mut effects,
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
                    // LocalOnly may become donor-eligible at this exact
                    // affirmation. Do not wait for the next presence timer;
                    // derive a fresh Ready guard after the control lock is
                    // released and let the child decide from its source cut.
                    if let Some(child) = bootstrap.as_mut() {
                        maintain_child(&shared, &config, child).await;
                    }
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
            maintain_child(&shared, &config, child).await;
            continue;
        }
        let child_wake = bootstrap.as_ref().map(|child| child.wake());
        let Some(due) = engine.next_deadline() else {
            if let Some(child) = bootstrap.as_mut() {
                if let Some(due) = child.next_deadline() {
                    tokio::select! {
                        () = shared.notify.notified() => {},
                        () = bootstrap_wake(child_wake.as_ref()) => {
                            maintain_child(&shared, &config, child).await;
                        }
                        () = tokio::time::sleep_until(tokio::time::Instant::from_std(due)) => {
                            maintain_child(&shared, &config, child).await;
                        }
                    }
                } else {
                    tokio::select! {
                        () = shared.notify.notified() => {},
                        () = bootstrap_wake(child_wake.as_ref()) => {
                            maintain_child(&shared, &config, child).await;
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
                    maintain_child(&shared, &config, child).await;
                }
            }
            () = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {
                effects.extend(engine.step(RecoveryEvent::Tick(logical_now(started))).effects);
                lock(&shared.control).state = engine.state();
            }
        }
    }
}

async fn maintain_child<A: RecoveryAdapter>(
    shared: &Shared<A>,
    config: &RecoveryConfig,
    child: &mut Box<dyn BootstrapDriver>,
) {
    let now = Instant::now();
    let ready = now
        .checked_add(Duration::from_millis(config.total_ms))
        .and_then(|deadline| shared.ready_capture(deadline));
    child.maintain(now, ready).await;
}

async fn bootstrap_wake(wake: Option<&Arc<tokio::sync::Notify>>) {
    if let Some(wake) = wake {
        wake.notified().await;
    } else {
        std::future::pending::<()>().await;
    }
}

/// Await one adapter or child operation until the core's current deadline for
/// it. A progress report on the operation's permit wakes this loop: the core
/// renews the operation and the permit's deadline follows, so a scan that
/// keeps committing work outlives any fixed attempt or episode bound. If the
/// operation expired before the report, the core's fallback is queued instead.
async fn await_operation<A: RecoveryAdapter, T>(
    shared: &Arc<Shared<A>>,
    engine: &mut RecoveryEngine,
    started: Instant,
    (active_version, op): (u64, RecoveryOperation),
    effects: &mut VecDeque<RecoveryEffect>,
    future: BoxRecoveryFuture<'_, T>,
) -> Option<T> {
    let due = engine.next_deadline()?;
    let mut deadline = terminal_deadline(shared, absolute_deadline(started, due))?;
    let mut reported = 0;
    tokio::pin!(future);
    loop {
        let delay = deadline.saturating_duration_since(Instant::now());
        tokio::select! {
            biased;
            () = shared.notify.notified() => {
                {
                    let control = lock(&shared.control);
                    if control.version != active_version || control.pending.cancel {
                        return None;
                    }
                }
                let Some(progress) = shared.progress(op, active_version) else {
                    continue;
                };
                if progress == reported {
                    continue;
                }
                reported = progress;
                let elapsed = engine.step(RecoveryEvent::Tick(logical_now(started)));
                effects.extend(elapsed.effects);
                if !engine.accepts_operation(op) {
                    return None;
                }
                if engine.step(RecoveryEvent::Progressed { op }).rejection.is_some() {
                    continue;
                }
                let due = engine.next_deadline()?;
                deadline = terminal_deadline(shared, absolute_deadline(started, due))?;
                shared.extend(op, active_version, deadline);
            }
            () = tokio::time::sleep(delay) => return None,
            value = &mut future => return Some(value),
        }
    }
}
