//! Loopback throughput and latency of pinned Groupnet TLS tunnel streams over
//! admitted TCP links, beside TLS 1.3 over plain TCP and plain TCP baselines.
//!
//! Run: `cargo run --release -p groupnet --example tunnel-throughput --features tcp-msg`.
//! Options (each takes a value): `--path tcp|tls|tunnel|all`,
//! `--workload bulk|rr|all`, `--bytes <MiB>` (default 256), `--chunk <KiB>`
//! application write/read size (default 64), `--runs <n>` (default 3),
//! `--rounds <n>` and `--warmup <n>` request/response exchanges (default
//! 2000/200), `--request <bytes>`/`--response <bytes>` (default 64/64),
//! `--segment <bytes>` tunnel send segment (default the `TunnelLimits`
//! default), `--threads <n>` Tokio workers (default all cores) and
//! `--verify 1` to check every bulk byte.
//!
//! Every path uses the same credentials, chunk sizes and workload code, so the
//! TLS row isolates TLS cost and the tunnel row adds Groupnet's routing,
//! reliability and link layers on top. Every endpoint runs as a Tokio worker
//! task. Each run opens a fresh stream pair; paths interleave per run to share
//! thermal and scheduler drift. Bulk time runs from the first write until the
//! receiver acknowledges the last byte; request/response latency covers one
//! write, flush and complete reply. Loopback numbers expose per-byte and
//! per-packet CPU cost, not WAN bandwidth-delay behaviour.

mod setup;
mod workload;

use std::{io, time::Duration};

use groupnet::network::tunnel::{SegmentSize, TunnelLimits};
use setup::{Fixture, Path};

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Workload {
    Bulk,
    RoundTrip,
}

#[derive(Debug)]
struct Options {
    paths: Vec<Path>,
    workloads: Vec<Workload>,
    bytes: u64,
    chunk: usize,
    runs: usize,
    rounds: usize,
    warmup: usize,
    request: usize,
    response: usize,
    segment: SegmentSize,
    threads: usize,
    verify: bool,
}

impl Options {
    fn parse() -> io::Result<Self> {
        let mut options = Self {
            paths: Path::ALL.to_vec(),
            workloads: vec![Workload::Bulk, Workload::RoundTrip],
            bytes: 256 << 20,
            chunk: 64 << 10,
            runs: 3,
            rounds: 2000,
            warmup: 200,
            request: 64,
            response: 64,
            segment: TunnelLimits::default().payload,
            threads: std::thread::available_parallelism().map_or(4, usize::from),
            verify: false,
        };
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| input("each option needs a value"))?;
            match argument.as_str() {
                "--path" => {
                    options.paths = match value.as_str() {
                        "all" => Path::ALL.to_vec(),
                        "tcp" => vec![Path::Tcp],
                        "tls" => vec![Path::Tls],
                        "tunnel" => vec![Path::Tunnel],
                        _ => return Err(input("path must be tcp, tls, tunnel or all")),
                    };
                }
                "--workload" => {
                    options.workloads = match value.as_str() {
                        "all" => vec![Workload::Bulk, Workload::RoundTrip],
                        "bulk" => vec![Workload::Bulk],
                        "rr" => vec![Workload::RoundTrip],
                        _ => return Err(input("workload must be bulk, rr or all")),
                    };
                }
                "--bytes" => options.bytes = (number(&value)? as u64) << 20,
                "--chunk" => options.chunk = number(&value)? << 10,
                "--runs" => options.runs = number(&value)?,
                "--rounds" => options.rounds = number(&value)?,
                "--warmup" => options.warmup = number(&value)?,
                "--request" => options.request = number(&value)?,
                "--response" => options.response = number(&value)?,
                "--segment" => options.segment = SegmentSize::try_from(number(&value)?)?,
                "--threads" => options.threads = number(&value)?,
                "--verify" => options.verify = number(&value)? != 0,
                _ => return Err(input(format!("unknown option {argument}"))),
            }
        }
        if options.bytes == 0
            || options.chunk == 0
            || options.runs == 0
            || options.rounds == 0
            || options.request == 0
            || options.response == 0
            || options.threads == 0
        {
            return Err(input("sizes, runs, rounds and threads must be positive"));
        }
        Ok(options)
    }
}

fn number(value: &str) -> io::Result<usize> {
    value.parse().map_err(input)
}

fn input(reason: impl std::fmt::Display) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, reason.to_string())
}

#[expect(
    clippy::cast_precision_loss,
    reason = "throughput is reported, not accumulated"
)]
fn megabytes_per_second(bytes: u64, elapsed: Duration) -> f64 {
    bytes as f64 / elapsed.as_secs_f64() / 1e6
}

fn micros(duration: Duration) -> f64 {
    duration.as_secs_f64() * 1e6
}

/// Median bulk MB/s per path, collected for the closing summary.
#[derive(Default)]
struct Summary {
    bulk: Vec<(Path, f64)>,
    round_trip: Vec<(Path, f64)>,
}

impl Summary {
    fn median(samples: &[(Path, f64)], path: Path) -> Option<f64> {
        let mut values: Vec<f64> = samples
            .iter()
            .filter(|(sample, _)| *sample == path)
            .map(|(_, value)| *value)
            .collect();
        values.sort_by(f64::total_cmp);
        values.get(values.len() / 2).copied()
    }

    fn print(&self, paths: &[Path]) {
        let tls = Self::median(&self.bulk, Path::Tls);
        for &path in paths {
            let bulk = Self::median(&self.bulk, path);
            let latency = Self::median(&self.round_trip, path);
            let share = bulk.zip(tls).map_or_else(String::new, |(bulk, tls)| {
                format!(" ({:.0}% of tls-tcp)", 100.0 * bulk / tls)
            });
            println!(
                "summary path={path} bulk_median_mb_s={}{share} rr_median_p50_us={}",
                bulk.map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
                latency.map_or_else(|| "-".to_owned(), |value| format!("{value:.1}")),
            );
        }
    }
}

async fn measure(options: &Options) -> io::Result<()> {
    let mut fixtures = Vec::with_capacity(options.paths.len());
    for &path in &options.paths {
        fixtures.push((path, Fixture::start(path, options.segment).await?));
    }
    println!("path,workload,run,bytes,wall_ms,mb_s,ops_s,p50_us,p99_us,mean_us");
    let mut summary = Summary::default();
    let result = async {
        for run in 0..options.runs {
            for (path, fixture) in &fixtures {
                for &workload in &options.workloads {
                    let (client, server) = fixture.pair().await?;
                    match workload {
                        Workload::Bulk => {
                            let elapsed = workload::bulk(
                                client,
                                server,
                                options.bytes,
                                options.chunk,
                                options.verify,
                            )
                            .await?;
                            let rate = megabytes_per_second(options.bytes, elapsed);
                            summary.bulk.push((*path, rate));
                            println!(
                                "{path},bulk,{run},{},{:.1},{rate:.1},,,,",
                                options.bytes,
                                elapsed.as_secs_f64() * 1e3
                            );
                        }
                        Workload::RoundTrip => {
                            let trips = workload::round_trips(
                                client,
                                server,
                                options.warmup,
                                options.rounds,
                                options.request,
                                options.response,
                            )
                            .await?;
                            let p50 = micros(trips.quantile(0.5));
                            summary.round_trip.push((*path, p50));
                            #[expect(clippy::cast_precision_loss, reason = "a reported rate")]
                            let ops = options.rounds as f64 / trips.elapsed.as_secs_f64();
                            println!(
                                "{path},rr,{run},{},{:.1},,{ops:.0},{p50:.1},{:.1},{:.1}",
                                options.rounds * (options.request + options.response),
                                trips.elapsed.as_secs_f64() * 1e3,
                                micros(trips.quantile(0.99)),
                                micros(trips.mean()),
                            );
                        }
                    }
                }
            }
        }
        Ok::<(), io::Error>(())
    }
    .await;
    for (_, fixture) in fixtures {
        fixture.close().await;
    }
    result?;
    summary.print(&options.paths);
    Ok(())
}

fn main() -> io::Result<()> {
    let options = Options::parse()?;
    println!(
        "tunnel-throughput threads={} bytes={} chunk={} segment={} runs={} rounds={} warmup={} request={} response={} verify={}",
        options.threads,
        options.bytes,
        options.chunk,
        options.segment.get(),
        options.runs,
        options.rounds,
        options.warmup,
        options.request,
        options.response,
        options.verify,
    );
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(options.threads)
        .enable_all()
        .build()?;
    // Both endpoints of every path run as worker tasks, as services do; the
    // blocking main thread only waits for the result.
    runtime.block_on(async move {
        tokio::spawn(async move { measure(&options).await })
            .await
            .map_err(io::Error::other)?
    })
}
