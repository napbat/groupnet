//! Repeatable, integrity-checked unordered packet-path measurements on loopback.
//!
//! Smoke: `cargo run -p groupnet --release --example packet-path --features udp,tcp-msg,connectivity`.
//! Moderate: append `-- --messages 2000 --warmup 128 --batch 16 --timeout-ms 3000`.
//! Select `--path memory|udp|tcp|relay|all`, `--mode reliable|unreliable|all`,
//! or `--size 64|1200|all`. Setup, payload preparation and warmup are not timed.
//! Rates count receiver-observed unique payloads, never unreliable send success.
//! RTT is measured from batch enqueue to integrity-checked echo at the origin;
//! quantiles describe completed roundtrips only. Losses and local send failures
//! are reported separately. This is local path evidence, not Internet/NAT evidence.

mod measure;
mod setup;

use std::{io, time::Duration};

use groupnet::streams::UnorderedDelivery;

#[derive(Debug)]
struct Options {
    messages: usize,
    warmup: usize,
    batch: usize,
    timeout: Duration,
    paths: Vec<setup::Path>,
    modes: Vec<UnorderedDelivery>,
    sizes: Vec<usize>,
}

impl Options {
    fn parse() -> io::Result<Self> {
        let mut options = Self {
            messages: 64,
            warmup: 16,
            batch: 16,
            timeout: Duration::from_millis(3000),
            paths: setup::Path::ALL.to_vec(),
            modes: vec![UnorderedDelivery::Unreliable, UnorderedDelivery::Reliable],
            sizes: vec![64, 1200],
        };
        let mut arguments = std::env::args().skip(1);
        while let Some(argument) = arguments.next() {
            let value = arguments
                .next()
                .ok_or_else(|| input("each option needs a value"))?;
            match argument.as_str() {
                "--messages" => options.messages = number(&value)?,
                "--warmup" => options.warmup = number(&value)?,
                "--batch" => options.batch = number(&value)?,
                "--timeout-ms" => {
                    options.timeout = Duration::from_millis(value.parse().map_err(input)?);
                }
                "--path" => {
                    options.paths = match value.as_str() {
                        "all" => setup::Path::ALL.to_vec(),
                        "memory" => vec![setup::Path::Memory],
                        "udp" => vec![setup::Path::Udp],
                        "tcp" => vec![setup::Path::Tcp],
                        "relay" => vec![setup::Path::Relay],
                        _ => return Err(input("path must be memory, udp, tcp, relay or all")),
                    };
                }
                "--mode" => {
                    options.modes = match value.as_str() {
                        "all" => vec![UnorderedDelivery::Unreliable, UnorderedDelivery::Reliable],
                        "reliable" => vec![UnorderedDelivery::Reliable],
                        "unreliable" => vec![UnorderedDelivery::Unreliable],
                        _ => return Err(input("mode must be reliable, unreliable or all")),
                    };
                }
                "--size" => {
                    options.sizes = match value.as_str() {
                        "all" => vec![64, 1200],
                        "64" => vec![64],
                        "1200" => vec![1200],
                        _ => return Err(input("size must be 64, 1200 or all")),
                    };
                }
                _ => return Err(input(format!("unknown option {argument}"))),
            }
        }
        if options.messages == 0 || options.batch == 0 || options.timeout.is_zero() {
            return Err(input("messages, batch and timeout must be positive"));
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

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> io::Result<()> {
    let options = Options::parse()?;
    println!(
        "packet-path threads=2 messages={} warmup={} batch={} batch_timeout_ms={}",
        options.messages,
        options.warmup,
        options.batch,
        options.timeout.as_millis()
    );
    println!(
        "path,mode,payload_bytes,attempted,send_ok,send_errors,send_would_block,send_timeouts,echo_send_errors,forward_unique,forward_bytes,roundtrips,roundtrip_bytes,forward_missing,roundtrip_missing,duplicates,expired_batches,wall_ms,forward_msg_s,forward_mib_s,roundtrip_msg_s,rtt_p50_us,rtt_p99_us"
    );
    let mut reliable_failed = false;
    for path in options.paths.iter().copied() {
        let network = setup::Pair::new(path).await?;
        let result = async {
            for &mode in &options.modes {
                for &size in &options.sizes {
                    if options.warmup != 0 {
                        let warmup = measure::run(&network, mode, size, options.warmup,
                            options.batch, options.timeout).await?;
                        if warmup.forward != options.warmup || warmup.roundtrips != options.warmup {
                            eprintln!("warmup path={path} mode={mode:?} size={size}: forward={}/{} roundtrips={}/{} send_errors={} echo_send_errors={}",
                                warmup.forward, options.warmup, warmup.roundtrips, options.warmup,
                                warmup.send_errors, warmup.echo_send_errors);
                        }
                    }
                    let mut stats = measure::run(&network, mode, size, options.messages,
                        options.batch, options.timeout).await?;
                    stats.print(path, mode, size, options.messages);
                    if mode == UnorderedDelivery::Reliable
                        && (stats.forward != options.messages || stats.roundtrips != options.messages
                            || stats.send_errors != 0 || stats.echo_send_errors != 0)
                    {
                        reliable_failed = true;
                    }
                }
            }
            Ok::<(), io::Error>(())
        }.await;
        network.close().await;
        result?;
    }
    if reliable_failed {
        return Err(io::Error::other(
            "reliable cases had missing deliveries or send errors; see rows",
        ));
    }
    Ok(())
}
