//! Congestion window in segments: slow start and loss halving as in Reno, with
//! Vegas-style delay control so a large receive window never turns into a
//! standing queue.
//!
//! The base round trip is the windowed minimum sample of the last
//! [`BASE_WINDOW`]. A round lasts until everything sent when it began is
//! acknowledged, or twice the base round trip, whichever comes first; its round
//! trip is the smallest sample it collected, so a single noisy sample never
//! counts, while the time bound follows a queue that builds within a long
//! burst. Queued segments are estimated as `in_flight × (rtt − base) / rtt`.
//! The tolerated queue is the segments worth a target delay — the larger of
//! [`DELAY_FLOOR`] and an eighth of the base round trip — or [`ALPHA`]
//! segments: the target keeps round-trip jitter (timer granularity, scheduler
//! wakes of tens of microseconds per hop) from being read as a queue, while
//! bounding a WAN queue to a fraction of its round trip.
//!
//! Slow start grows the window by two segments per acknowledged segment
//! (tripling it per round trip, near BBR's startup gain of 2.89) and ends, once
//! a round has [`EXIT_SAMPLES`]
//! samples, as soon as more than the tolerated queue builds, cutting the window
//! to the path's capacity (in flight minus queued) plus the tolerated queue.
//! Each round of congestion avoidance then grows a used window by a quarter
//! while less than half the tolerated queue builds, by one segment while less
//! than all of it does, and above twice it (at least [`BETA`] segments) cuts it
//! the same way, by at most half. Segments acknowledged in the round after a
//! cut were sent before it, so that round is not measured. Loss still halves
//! the window.

use std::time::Duration;

use tokio::time::Instant;

/// Queued segments always tolerated.
const ALPHA: usize = 2;

/// Queued segments always tolerated before congestion avoidance shrinks.
const BETA: usize = 4;

/// Smallest tolerated queueing delay, above host scheduling jitter.
const DELAY_FLOOR: Duration = Duration::from_micros(250);

/// Samples a round needs before its minimum may end slow start.
const EXIT_SAMPLES: usize = 8;

/// Smallest delay-controlled window; loss may still halve to one segment.
const MIN_WINDOW: usize = 2;

/// Span of the base round trip's windowed minimum: a path whose delay rose
/// for longer than this is re-measured instead of read as a standing queue.
const BASE_WINDOW: Duration = Duration::from_secs(10);

/// Running minimum over a sliding time window (Kathleen Nichols' algorithm,
/// as in Linux `win_minmax`): the best, second and third candidates of
/// successively later sub-windows.
#[derive(Clone, Copy, Debug)]
struct WindowedMin {
    candidates: [(Duration, Instant); 3],
}

impl WindowedMin {
    fn new(sample: Duration, now: Instant) -> Self {
        Self {
            candidates: [(sample, now); 3],
        }
    }

    fn get(&self) -> Duration {
        self.candidates[0].0
    }

    fn update(&mut self, sample: Duration, now: Instant) {
        let value = (sample, now);
        if sample <= self.candidates[0].0 || now.duration_since(self.candidates[2].1) > BASE_WINDOW
        {
            *self = Self::new(sample, now);
            return;
        }
        let [best, second, third] = &mut self.candidates;
        if sample <= second.0 {
            *second = value;
            *third = value;
        } else if sample <= third.0 {
            *third = value;
        }
        let age = now.duration_since(best.1);
        if age > BASE_WINDOW {
            *best = *second;
            *second = *third;
            *third = value;
            if now.duration_since(best.1) > BASE_WINDOW {
                *best = *second;
                *second = *third;
            }
        } else if second.1 == best.1 && age > BASE_WINDOW / 4 {
            *second = value;
            *third = value;
        } else if third.1 == second.1 && age > BASE_WINDOW / 2 {
            *third = value;
        }
    }
}

/// One round: segments sent before `end` acknowledged, or twice the base
/// round trip elapsed.
#[derive(Clone, Copy, Debug, Default)]
struct Round {
    end: u64,
    /// First acknowledgement of the round.
    started: Option<Instant>,
    min: Option<Duration>,
    samples: usize,
    acknowledged: usize,
    /// Carries segments sent before the latest cut: not measured.
    hold: bool,
}

/// What one cumulative acknowledgement reports.
#[derive(Clone, Copy, Debug)]
pub(super) struct Acknowledgement {
    /// Newly acknowledged segments.
    pub acknowledged: usize,
    /// Round trip of the first newly acknowledged, never retransmitted segment.
    pub sample: Option<Duration>,
    /// Oldest sequence still unacknowledged.
    pub unacknowledged: u64,
    /// Next sequence to send.
    pub next_send: u64,
    /// Segments still unacknowledged.
    pub in_flight: usize,
    /// Loss recovery is in progress: the window neither grows nor shrinks.
    pub recovering: bool,
}

#[derive(Debug)]
pub(super) struct Congestion {
    window: usize,
    threshold: usize,
    ceiling: usize,
    base: Option<WindowedMin>,
    round: Round,
}

impl Congestion {
    /// A window of `initial` segments that never exceeds `ceiling`.
    pub(super) fn new(initial: usize, ceiling: usize) -> Self {
        Self {
            window: initial,
            threshold: ceiling,
            ceiling,
            base: None,
            round: Round::default(),
        }
    }

    /// Segments that may be unacknowledged.
    pub(super) fn window(&self) -> usize {
        self.window
    }

    #[cfg(test)]
    pub(super) fn threshold(&self) -> usize {
        self.threshold
    }

    /// Loss: halve into the slow-start threshold.
    pub(super) fn reduce(&mut self) {
        self.threshold = (self.window / 2).max(1);
        self.window = self.threshold;
    }

    pub(super) fn acknowledge(&mut self, ack: Acknowledgement, now: Instant) {
        let started = *self.round.started.get_or_insert(now);
        if let Some(sample) = ack.sample {
            self.sample(sample, now);
        }
        self.round.acknowledged += ack.acknowledged;
        if !ack.recovering && self.window < self.threshold {
            self.window = (self.window + 2 * ack.acknowledged).min(self.threshold);
            if self.round.samples >= EXIT_SAMPLES
                && let Some(queued) = self.queued(ack.in_flight)
                && let Some(tolerated) = self.tolerated(ack.in_flight)
                && queued > tolerated
            {
                // The path is full: keep its capacity plus the tolerated queue.
                self.window = (ack.in_flight - queued + tolerated)
                    .clamp(MIN_WINDOW, self.window.max(MIN_WINDOW));
                self.threshold = self.window;
                self.next_round(ack.next_send, true);
                return;
            }
        }
        let expired = self
            .base
            .is_some_and(|base| now.duration_since(started) >= base.get() * 2);
        if ack.unacknowledged >= self.round.end || expired {
            let cut = !ack.recovering
                && !self.round.hold
                && self.window >= self.threshold
                && self.avoid();
            self.next_round(ack.next_send, cut);
        }
    }

    fn next_round(&mut self, end: u64, hold: bool) {
        self.round = Round {
            end,
            hold,
            ..Round::default()
        };
    }

    /// Congestion avoidance at the end of a round; true if the window shrank.
    fn avoid(&mut self) -> bool {
        let in_flight = self.round.acknowledged;
        let queued = self.queued(in_flight).unwrap_or(0);
        let tolerated = self.tolerated(in_flight).unwrap_or(ALPHA);
        let high = BETA.max(2 * tolerated);
        if queued > high {
            let target = (in_flight - queued + tolerated).max(self.window / 2);
            self.window = target.min(self.window).max(MIN_WINDOW);
            self.threshold = self.window;
            return true;
        }
        // Grow only a window the round actually used.
        if queued < tolerated && 2 * in_flight >= self.window {
            let step = if 2 * queued < tolerated {
                (self.window / 4).max(1)
            } else {
                1
            };
            self.window = (self.window + step).min(self.ceiling);
        }
        false
    }

    fn sample(&mut self, sample: Duration, now: Instant) {
        match &mut self.base {
            Some(base) => base.update(sample, now),
            None => self.base = Some(WindowedMin::new(sample, now)),
        }
        self.round.min = Some(self.round.min.map_or(sample, |min| min.min(sample)));
        self.round.samples += 1;
    }

    /// Segments of `in_flight` queued along the path this round, if measured.
    fn queued(&self, in_flight: usize) -> Option<usize> {
        let rtt = self.round.min?;
        let base = self.base?.get();
        Some(delay_segments(in_flight, rtt.saturating_sub(base), rtt))
    }

    /// Queued segments tolerated this round: `in_flight` worth the target
    /// delay, at least [`ALPHA`].
    fn tolerated(&self, in_flight: usize) -> Option<usize> {
        let rtt = self.round.min?;
        let target = DELAY_FLOOR.max(self.base?.get() / 8);
        Some(ALPHA.max(delay_segments(in_flight, target, rtt)))
    }
}

/// `in_flight × delay / rtt`.
fn delay_segments(in_flight: usize, delay: Duration, rtt: Duration) -> usize {
    let rtt = rtt.as_nanos();
    if rtt == 0 {
        return 0;
    }
    let segments = (in_flight as u128) * delay.as_nanos() / rtt;
    usize::try_from(segments).unwrap_or(usize::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    const RTT: Duration = Duration::from_millis(80);

    /// An ACK-clocked sender: every acknowledgement refills the window.
    struct Path {
        congestion: Congestion,
        now: Instant,
        sent: u64,
        acknowledged: u64,
    }

    impl Path {
        fn new(initial: usize) -> Self {
            Self {
                congestion: Congestion::new(initial, 1024),
                now: Instant::now(),
                sent: 0,
                acknowledged: 0,
            }
        }

        fn in_flight(&self) -> usize {
            usize::try_from(self.sent - self.acknowledged).unwrap()
        }

        fn fill(&mut self) {
            let window = self.congestion.window();
            if self.in_flight() < window {
                self.sent = self.acknowledged + window as u64;
            }
        }

        /// Acknowledges the oldest segment, sampled at `rtt`.
        fn acknowledge_one(&mut self, rtt: Duration) {
            self.acknowledged += 1;
            self.congestion.acknowledge(
                Acknowledgement {
                    acknowledged: 1,
                    sample: Some(rtt),
                    unacknowledged: self.acknowledged,
                    next_send: self.sent,
                    in_flight: self.in_flight(),
                    recovering: false,
                },
                self.now,
            );
        }

        /// One round trip at `rtt`: acknowledges everything in flight at its
        /// start, refilling the window after every acknowledgement.
        fn round(&mut self, rtt: Duration) {
            self.fill();
            let end = self.sent;
            self.now += rtt;
            while self.acknowledged < end {
                self.acknowledge_one(rtt);
                self.fill();
            }
        }
    }

    #[test]
    fn slow_start_triples_without_queueing_delay() {
        let mut path = Path::new(4);
        for expected in [12, 36, 108, 324] {
            path.round(RTT);
            assert_eq!(path.congestion.window(), expected);
        }
    }

    #[test]
    fn round_trip_jitter_below_the_delay_target_never_ends_slow_start() {
        // An 80 ms path whose rounds alternate with 2 ms more delay: at a few
        // hundred segments that once read as more than two queued segments and
        // ended slow start far below a gigabit path's capacity.
        let mut path = Path::new(4);
        for round in 0..8 {
            let jitter = Duration::from_millis(2) * (round % 2);
            path.round(RTT + jitter);
        }
        assert_eq!(
            (path.congestion.window(), path.congestion.threshold()),
            (1024, 1024)
        );
    }

    #[test]
    fn queueing_delay_ends_slow_start_near_path_capacity() {
        let mut path = Path::new(64);
        path.round(RTT);
        // Half of the segments in flight wait in a queue: RTT doubles.
        path.now += RTT;
        let mut in_flight = 0;
        while path.congestion.threshold() == 1024 {
            path.acknowledge_one(RTT * 2);
            in_flight = path.in_flight();
            path.fill();
        }
        let window = path.congestion.window();
        // Capacity (in flight minus queued) plus the 10 ms target's worth.
        assert_eq!(window, in_flight - in_flight / 2 + in_flight / 16);
        // The segments sent before the cut drain without a further cut.
        while path.acknowledged < path.sent {
            path.acknowledge_one(RTT * 2);
        }
        assert_eq!(path.congestion.window(), window);
    }

    #[test]
    fn avoidance_grows_without_queue_and_drains_a_standing_queue() {
        let mut path = Path::new(64);
        path.round(RTT);
        path.congestion.reduce();
        let reduced = path.congestion.window();
        // No queue: grow by a quarter per round.
        path.round(RTT);
        path.round(RTT);
        let grown = path.congestion.window();
        assert!(grown >= reduced + reduced / 4, "{reduced} -> {grown}");
        // A third of the round trip queued: cut toward capacity (two thirds)
        // plus the tolerated queue, by at most half per measured round. The
        // first acknowledgement at the new delay closes the previous round.
        path.round(RTT * 3 / 2);
        path.round(RTT * 3 / 2);
        let cut = path.congestion.window();
        assert!((grown / 2..grown).contains(&cut), "{grown} -> {cut}");
        // At base delay plus less than the target, the window holds or grows.
        path.round(RTT + RTT / 16);
        assert!(path.congestion.window() >= cut);
    }

    #[test]
    fn a_queue_building_within_a_long_burst_is_measured_in_time() {
        // A stale window of 1024 segments bursts into a path that holds 64:
        // the queue grows for the whole burst, longer than any round of
        // acknowledgements. Rounds bounded by twice the base round trip still
        // see it and cut the window.
        let mut path = Path::new(4);
        path.round(RTT);
        path.congestion.reduce();
        path.congestion.window = 1024;
        path.congestion.threshold = 1024;
        path.sent = path.acknowledged + 1024;
        let start = path.now;
        // Acknowledgements arrive at the path's rate, 64 per base round trip;
        // segment `k` waited behind the `k - 64` ahead of it.
        for k in 0..1024_u32 {
            path.now = start + RTT * k / 64 + RTT;
            let queued = RTT * k.saturating_sub(64) / 64;
            path.acknowledge_one(RTT + queued);
            if path.congestion.window() < 1024 {
                break;
            }
        }
        assert!(
            path.congestion.window() <= 512,
            "{}",
            path.congestion.window()
        );
    }

    #[test]
    fn host_pipelining_within_the_delay_floor_keeps_growing() {
        let base = Duration::from_micros(50);
        let mut path = Path::new(4);
        path.round(base);
        path.congestion.reduce();
        // Each segment in flight adds five microseconds of pipeline delay: the
        // window settles where it holds about DELAY_FLOOR of it, near fifty
        // segments, instead of reading the pipeline as a queue.
        for _ in 0..200 {
            let window = u32::try_from(path.congestion.window()).unwrap();
            path.round(base + Duration::from_micros(5) * window);
        }
        let window = path.congestion.window();
        assert!((45..=100).contains(&window), "window {window}");
    }

    #[test]
    fn base_round_trip_follows_a_longer_path_after_its_window() {
        let mut now = Instant::now();
        let mut base = WindowedMin::new(RTT, now);
        for _ in 0..200 {
            now += RTT;
            base.update(RTT * 2, now);
        }
        assert_eq!(base.get(), RTT * 2);
        base.update(RTT, now);
        assert_eq!(base.get(), RTT);
        // Within the window, a higher sample never replaces the minimum.
        now += BASE_WINDOW / 2;
        base.update(RTT * 3, now);
        assert_eq!(base.get(), RTT);
    }

    #[test]
    fn loss_halves_and_recovery_freezes_the_window() {
        let mut path = Path::new(32);
        path.congestion.reduce();
        assert_eq!(
            (path.congestion.window(), path.congestion.threshold()),
            (16, 16)
        );
        path.congestion.acknowledge(
            Acknowledgement {
                acknowledged: 16,
                sample: Some(RTT * 4),
                unacknowledged: 100,
                next_send: 100,
                in_flight: 0,
                recovering: true,
            },
            path.now,
        );
        assert_eq!(path.congestion.window(), 16);
    }
}
