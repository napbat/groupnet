//! Bulk unidirectional and request/response workloads over one stream pair.

use std::{
    io,
    time::{Duration, Instant},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use super::setup::Stream;

/// Period of the verifiable byte pattern; prime, so chunk boundaries drift.
const PATTERN: usize = 251;

fn pattern_byte(offset: u64) -> u8 {
    u8::try_from(offset % PATTERN as u64).expect("pattern period fits a byte")
}

/// Sends `total` bytes in `chunk`-sized writes; the receiver acknowledges the
/// final byte with one byte back. Time runs from the first write to that
/// acknowledgement, so it covers delivery rather than local buffering.
pub(super) async fn bulk(
    mut client: Stream,
    mut server: Stream,
    total: u64,
    chunk: usize,
    verify: bool,
) -> io::Result<Duration> {
    let receiver = tokio::spawn(async move {
        let mut buffer = vec![0; chunk];
        let mut received = 0_u64;
        while received < total {
            let length = server.read(&mut buffer).await?;
            if length == 0 {
                return Err(io::Error::from(io::ErrorKind::UnexpectedEof));
            }
            if verify
                && buffer[..length]
                    .iter()
                    .zip(received..)
                    .any(|(byte, offset)| *byte != pattern_byte(offset))
            {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "bulk payload corrupted",
                ));
            }
            received += length as u64;
        }
        server.write_all(&[1]).await?;
        server.flush().await?;
        Ok::<_, io::Error>(server)
    });
    let payload: Vec<u8> = (0..(chunk + PATTERN) as u64).map(pattern_byte).collect();
    let started = Instant::now();
    let mut sent = 0_u64;
    while sent < total {
        let length = usize::try_from(total - sent).map_or(chunk, |left| left.min(chunk));
        let start = usize::try_from(sent % PATTERN as u64).expect("below the period");
        client.write_all(&payload[start..start + length]).await?;
        sent += length as u64;
    }
    client.flush().await?;
    let mut acknowledgement = [0];
    client.read_exact(&mut acknowledgement).await?;
    let elapsed = started.elapsed();
    drop(receiver.await.map_err(io::Error::other)??);
    Ok(elapsed)
}

/// Completed request/response latencies.
pub(super) struct RoundTrips {
    pub(super) latencies: Vec<Duration>,
    pub(super) elapsed: Duration,
}

impl RoundTrips {
    pub(super) fn quantile(&self, fraction: f64) -> Duration {
        let mut sorted = self.latencies.clone();
        sorted.sort_unstable();
        let last = sorted.len().saturating_sub(1);
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            clippy::cast_precision_loss,
            reason = "a rounded rank within an in-memory sample"
        )]
        let rank = ((last as f64) * fraction).round() as usize;
        sorted.get(rank).copied().unwrap_or_default()
    }

    pub(super) fn mean(&self) -> Duration {
        let count = u32::try_from(self.latencies.len().max(1)).unwrap_or(u32::MAX);
        self.latencies.iter().sum::<Duration>() / count
    }
}

/// Sequential request/response exchanges after untimed warmup exchanges.
pub(super) async fn round_trips(
    mut client: Stream,
    mut server: Stream,
    warmup: usize,
    rounds: usize,
    request: usize,
    response: usize,
) -> io::Result<RoundTrips> {
    let responder = tokio::spawn(async move {
        let mut inbound = vec![0; request];
        let outbound = vec![0x5A; response];
        for _ in 0..warmup + rounds {
            server.read_exact(&mut inbound).await?;
            server.write_all(&outbound).await?;
            server.flush().await?;
        }
        Ok::<_, io::Error>(server)
    });
    let outbound = vec![0xA5; request];
    let mut inbound = vec![0; response];
    let mut latencies = Vec::with_capacity(rounds);
    let mut started = Instant::now();
    for round in 0..warmup + rounds {
        if round == warmup {
            started = Instant::now();
        }
        let sent = Instant::now();
        client.write_all(&outbound).await?;
        client.flush().await?;
        client.read_exact(&mut inbound).await?;
        if round >= warmup {
            latencies.push(sent.elapsed());
        }
    }
    let elapsed = started.elapsed();
    drop(responder.await.map_err(io::Error::other)??);
    Ok(RoundTrips { latencies, elapsed })
}
