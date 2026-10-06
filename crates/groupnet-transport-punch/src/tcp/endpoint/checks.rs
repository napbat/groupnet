//! Fair rotating candidate checks with per-tuple ownership and burst cooldowns.
use super::{DEADLINE, Event, ProofPeer, Runtime, direct};
use std::{net::SocketAddr, sync::Arc, time::Duration};
use tokio::{
    sync::{Semaphore, mpsc},
    task::JoinSet,
    time::Instant,
};

const BURST: u8 = 3;
const COOLDOWN: Duration = Duration::from_secs(10);

pub(super) struct Check {
    pub(super) tuple: (SocketAddr, SocketAddr),
    attempts: u8,
    next: Instant,
    pending: bool,
}

impl Check {
    pub(super) fn new(tuple: (SocketAddr, SocketAddr)) -> Self {
        Self {
            tuple,
            attempts: 0,
            next: Instant::now(),
            pending: false,
        }
    }

    pub(super) fn finished(&mut self) {
        self.pending = false;
        if self.attempts == BURST {
            self.next = Instant::now() + COOLDOWN;
        }
    }

    pub(super) fn rearm(&mut self) {
        self.attempts = 0;
        self.next = Instant::now();
    }

    fn ready(&mut self, now: Instant) -> bool {
        if self.pending || now < self.next {
            return false;
        }
        if self.attempts == BURST {
            self.attempts = 0;
        }
        true
    }

    fn started(&mut self, now: Instant) {
        self.pending = true;
        self.attempts += 1;
        if self.attempts == BURST {
            self.next = now + DEADLINE + COOLDOWN;
        }
    }
}

impl Runtime {
    pub(super) fn check(
        &mut self,
        events: &mpsc::Sender<Event>,
        dials: &Arc<Semaphore>,
        tasks: &mut JoinSet<()>,
    ) {
        if self.config.policy == crate::PathPolicy::RelayOnly {
            return;
        }
        let now = Instant::now();
        let visits = self.schedule.len() * 4;
        let mut scheduled = 0;
        for _ in 0..visits {
            if scheduled == 16 {
                break;
            }
            let Some(node) = self.schedule.pop_front() else {
                break;
            };
            self.schedule.push_back(node.clone());
            let Some(peer) = self.peers.get_mut(&node) else {
                continue;
            };
            if peer.direct.is_some() || peer.checks.is_empty() {
                continue;
            }
            let mut selected = None;
            for _ in 0..peer.checks.len() {
                let index = peer.next_check;
                peer.next_check = (index + 1) % peer.checks.len();
                let check = &mut peer.checks[index];
                if !self.pending_checks.contains(&check.tuple) && check.ready(now) {
                    selected = Some(index);
                    break;
                }
            }
            let Some(index) = selected else {
                continue;
            };
            let Ok(permit) = dials.clone().try_acquire_owned() else {
                break;
            };
            let check = &mut peer.checks[index];
            check.started(now);
            let tuple = check.tuple;
            self.pending_checks.insert(tuple);
            scheduled += 1;
            let local = self.config.local.clone();
            let session = self.session;
            let proof = ProofPeer {
                session: peer.session,
                secret: peer.secret,
            };
            let target_session = peer.session;
            let events = events.clone();
            tasks.spawn(async move {
                let _permit = permit;
                if let Ok(Ok(event)) = tokio::time::timeout(
                    DEADLINE,
                    direct::dial(local, session, node.clone(), proof, tuple.0, tuple.1),
                )
                .await
                {
                    let _ = events.send(event).await;
                }
                // Completion is emitted even for timeout/refusal. The owner
                // releases the exact tuple, never another replacement's slot.
                let _ = events
                    .send(Event::Checked {
                        node,
                        session: target_session,
                        tuple,
                    })
                    .await;
            });
        }
    }
}
