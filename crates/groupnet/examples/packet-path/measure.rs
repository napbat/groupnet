//! Batched delivery accounting and echo latency; no send-success delivery assumptions.

use std::{
    io,
    time::{Duration, Instant},
};

use futures_util::{StreamExt, stream::FuturesUnordered};
use groupnet::messaging::Bytes;
use groupnet::runtime::Unordered;
use groupnet::streams::{UnorderedDelivery, UnorderedOptions, UnorderedSession};

use super::setup::{Pair, Path, SETUP_TIMEOUT};

#[derive(Default)]
pub(super) struct Stats {
    pub(super) forward: usize,
    pub(super) roundtrips: usize,
    pub(super) send_errors: usize,
    pub(super) echo_send_errors: usize,
    send_ok: usize,
    send_would_block: usize,
    send_timeouts: usize,
    duplicates: usize,
    expired_batches: usize,
    forward_bytes: usize,
    roundtrip_bytes: usize,
    elapsed: Duration,
    latency: Vec<Duration>,
}

impl Stats {
    fn sent(&mut self, result: io::Result<()>) {
        match result {
            Ok(()) => self.send_ok += 1,
            Err(error) => {
                self.send_errors += 1;
                match error.kind() {
                    io::ErrorKind::WouldBlock => self.send_would_block += 1,
                    io::ErrorKind::TimedOut => self.send_timeouts += 1,
                    _ => {}
                }
            }
        }
    }

    #[expect(
        clippy::cast_precision_loss,
        reason = "human-readable performance rates use floating point counters"
    )]
    pub(super) fn print(
        &mut self,
        path: Path,
        mode: UnorderedDelivery,
        size: usize,
        attempted: usize,
    ) {
        self.latency.sort_unstable();
        let seconds = self.elapsed.as_secs_f64();
        let mode = match mode {
            UnorderedDelivery::Reliable => "reliable",
            UnorderedDelivery::Unreliable => "unreliable",
        };
        let quantile = |percent: usize| -> String {
            if self.latency.is_empty() {
                return "NA".to_owned();
            }
            // Nearest-rank quantiles, computed without a counter multiplication overflow.
            let n = self.latency.len();
            let rank = (n / 100) * percent + ((n % 100) * percent).div_ceil(100);
            format!("{}", self.latency[rank.saturating_sub(1)].as_micros())
        };
        println!(
            "{path},{mode},{size},{attempted},{},{},{},{},{},{},{},{},{},{},{},{},{},{:.3},{:.3},{:.3},{:.3},{},{}",
            self.send_ok,
            self.send_errors,
            self.send_would_block,
            self.send_timeouts,
            self.echo_send_errors,
            self.forward,
            self.forward_bytes,
            self.roundtrips,
            self.roundtrip_bytes,
            attempted - self.forward,
            attempted - self.roundtrips,
            self.duplicates,
            self.expired_batches,
            seconds * 1000.0,
            self.forward as f64 / seconds,
            self.forward_bytes as f64 / seconds / 1_048_576.0,
            self.roundtrips as f64 / seconds,
            quantile(50),
            quantile(99)
        );
    }
}

async fn connect(
    pair: &Pair,
    delivery: UnorderedDelivery,
) -> io::Result<(UnorderedSession, UnorderedSession)> {
    let descriptor = Unordered::new(UnorderedOptions {
        delivery,
        timeout: SETUP_TIMEOUT,
    });
    let endpoint = pair.right.endpoint(descriptor.clone())?;
    let peer = pair.left.peer(pair.right.id().clone(), descriptor)?;
    let (outgoing, (origin, incoming)) = tokio::time::timeout(SETUP_TIMEOUT, async {
        tokio::try_join!(peer.connect(), endpoint.accept())
    })
    .await
    .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "session setup timed out"))??;
    if origin != *pair.left.id()
        || outgoing.delivery() != delivery
        || incoming.delivery() != delivery
    {
        return Err(corrupt("session peer identity or delivery policy changed"));
    }
    Ok((outgoing, incoming))
}

pub(super) async fn run(
    pair: &Pair,
    delivery: UnorderedDelivery,
    size: usize,
    messages: usize,
    batch: usize,
    timeout: Duration,
) -> io::Result<Stats> {
    pair.check_path()?;
    let (outgoing, incoming) = connect(pair, delivery).await?;
    // Allocate and fill measurement bookkeeping/payloads before starting the clock.
    messages
        .checked_mul(size)
        .ok_or_else(|| corrupt("payload byte count overflow"))?;
    let payloads = (0..messages)
        .map(|sequence| packet(sequence, size))
        .collect::<io::Result<Vec<_>>>()?;
    let mut forwarded = vec![false; messages];
    let mut echoed = vec![false; messages];
    let mut sent_at = vec![None; messages];
    let mut stats = Stats {
        latency: Vec::with_capacity(messages),
        ..Stats::default()
    };
    let started = Instant::now();
    for start in (0..messages).step_by(batch) {
        let end = start.saturating_add(batch).min(messages);
        let mut sends = FuturesUnordered::new();
        let mut replies = FuturesUnordered::new();
        for sequence in start..end {
            sent_at[sequence] = Some(Instant::now());
            sends.push(outgoing.send(payloads[sequence].clone()));
        }
        let deadline = tokio::time::Instant::now()
            .checked_add(timeout)
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "batch timeout exceeds clock range",
                )
            })?;
        // Every batch submits the configured count. WouldBlock is accounted as a
        // local error, not mistaken for end-of-input or successful delivery.
        loop {
            if sends.is_empty() && replies.is_empty() && echoed[start..end].iter().all(|seen| *seen)
            {
                break;
            }
            tokio::select! {
                result = sends.next(), if !sends.is_empty() => {
                    stats.sent(result.expect("guarded nonempty send set"));
                }
                result = replies.next(), if !replies.is_empty() => {
                    let result: io::Result<()> = result.expect("guarded nonempty echo set");
                    if result.is_err() {
                        stats.echo_send_errors += 1;
                    }
                }
                packet = incoming.recv() => {
                    let packet = packet?;
                    let sequence = validate(&packet, &payloads, end)?;
                    if forwarded[sequence] {
                        stats.duplicates += 1;
                    } else {
                        forwarded[sequence] = true;
                        stats.forward += 1;
                        stats.forward_bytes += packet.len();
                        replies.push(incoming.send(packet));
                    }
                }
                packet = outgoing.recv() => {
                    let packet = packet?;
                    let sequence = validate(&packet, &payloads, end)?;
                    if echoed[sequence] {
                        stats.duplicates += 1;
                    } else {
                        echoed[sequence] = true;
                        stats.roundtrips += 1;
                        stats.roundtrip_bytes += packet.len();
                        stats.latency.push(sent_at[sequence].ok_or_else(|| corrupt("echo was never submitted"))?.elapsed());
                    }
                }
                () = tokio::time::sleep_until(deadline) => {
                    stats.expired_batches += 1;
                    stats.send_errors += sends.len();
                    stats.send_timeouts += sends.len();
                    stats.echo_send_errors += replies.len();
                    break;
                }
            }
        }
        // Remaining reliable futures are cancelled at the batch deadline; their
        // delivery is unknown. Late receives from earlier batches still count by
        // sequence, and duplicates never inflate throughput or RTT samples.
    }
    stats.elapsed = started.elapsed();
    pair.check_path()?;
    // Fresh sessions isolate warmup and measured phases; dropping both handles
    // cancels their workers and never leaves a background echo task running.
    Ok(stats)
}

fn packet(sequence: usize, size: usize) -> io::Result<Bytes> {
    let sequence = u64::try_from(sequence)
        .map_err(io::Error::other)?
        .to_be_bytes();
    let mut bytes = Vec::with_capacity(size);
    bytes.extend_from_slice(&sequence);
    for offset in 8..size {
        bytes.push(
            sequence[offset % sequence.len()]
                .wrapping_add(u8::try_from(offset % 251).expect("remainder fits u8")),
        );
    }
    Ok(Bytes::from(bytes))
}

fn validate(packet: &[u8], payloads: &[Bytes], submitted: usize) -> io::Result<usize> {
    let header: [u8; 8] = packet
        .get(..8)
        .ok_or_else(|| corrupt("packet missing sequence"))?
        .try_into()
        .map_err(io::Error::other)?;
    let sequence = usize::try_from(u64::from_be_bytes(header)).map_err(io::Error::other)?;
    if sequence >= submitted
        || payloads
            .get(sequence)
            .is_none_or(|expected| expected.as_ref() != packet)
    {
        return Err(corrupt(
            "packet sequence, length or payload integrity changed",
        ));
    }
    Ok(sequence)
}

fn corrupt(reason: &'static str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, reason)
}
