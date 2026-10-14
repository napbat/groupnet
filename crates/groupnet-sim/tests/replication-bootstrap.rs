//! Seeded delayed checkpoint replies over the sans-IO bootstrap core.

use groupnet_core::Time;
use groupnet_core::replication::{
    ApplyReceipt, Config, Cursor, Effect, Event, Mode, Operation, ReadDecision, Refusal, Scope,
    SessionEngine, SourceHistory, Step, Stream,
};
use groupnet_sim::SplitMix64;

fn scope() -> Scope {
    Scope {
        stream: Stream {
            group: "g".into(),
            topic: "state".into(),
            kind: "v1".into(),
        },
        partition: "p".into(),
    }
}

fn cursor() -> Cursor {
    Cursor {
        scope: scope(),
        history: SourceHistory {
            source: "cas".into(),
            generation: 1,
        },
        position: vec![7],
    }
}

fn load(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::LoadCheckpoint { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap()
}

fn install(step: &Step) -> Operation {
    step.effects
        .iter()
        .find_map(|effect| match effect {
            Effect::InstallCheckpoint { op, .. } => Some(*op),
            _ => None,
        })
        .unwrap()
}

#[test]
fn seeded_delayed_load_install_and_cancel_never_publish_stale_checkpoint() {
    for seed in 0..128 {
        let mut rng = SplitMix64::new(seed);
        let load_delay = u64::from(rng.below(18));
        let install_delay = u64::from(rng.below(18));
        let cancel_install = rng.below(4) == 0;
        let mut engine = SessionEngine::new(
            scope(),
            Mode::StateSync,
            Config {
                retry_ms: 5,
                attempt_timeout_ms: 10,
                max_retries: 3,
                ..Config::default()
            },
            seed + 1,
        )
        .unwrap();
        let first = load(&engine.step(Event::StartBootstrap));
        engine.step(Event::Tick(Time(load_delay)));
        let mut now = load_delay;
        let accepted_load = if engine.accepts_operation(first) {
            first
        } else {
            assert!(
                engine
                    .step(Event::CheckpointLoaded {
                        op: first,
                        cursor: Some(cursor()),
                        payload_id: Some(first.token),
                    })
                    .rejection
                    .is_some()
            );
            now = load_delay + 5;
            load(&engine.step(Event::Tick(Time(now))))
        };
        let installing = engine.step(Event::CheckpointLoaded {
            op: accepted_load,
            cursor: Some(cursor()),
            payload_id: Some(accepted_load.token),
        });
        let install_op = install(&installing);
        assert_ne!(install_op, accepted_load);
        assert_eq!(
            engine.read_decision(),
            ReadDecision::Refuse(Refusal::Unready)
        );
        assert!(engine.state().checkpoint.is_none());
        if cancel_install {
            engine.step(Event::Cancel);
            assert!(!engine.accepts_operation(install_op));
        } else {
            now += install_delay;
            engine.step(Event::Tick(Time(now)));
        }
        let accepted = !cancel_install && engine.accepts_operation(install_op);
        let installed = engine.step(Event::CheckpointInstalled {
            op: install_op,
            receipt: ApplyReceipt {
                through: cursor(),
                durable: true,
            },
        });
        if accepted {
            assert!(installed.rejection.is_none(), "seed {seed}");
            assert_eq!(engine.state().checkpoint, Some(cursor()), "seed {seed}");
        } else {
            assert!(installed.rejection.is_some(), "seed {seed}");
            assert!(engine.state().checkpoint.is_none(), "seed {seed}");
        }
        assert_eq!(
            engine.read_decision(),
            ReadDecision::Refuse(Refusal::Unready)
        );
    }
}
