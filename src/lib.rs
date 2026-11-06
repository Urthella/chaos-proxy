//! A TCP proxy that makes the network worse on purpose: latency, jitter, a bandwidth limit,
//! dropped connections and partitions, all changeable while it runs. For testing how a client
//! or service behaves when the network does.

use std::io::{self, Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc;
use std::sync::{Arc, RwLock};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Faults {
    /// Added to every chunk of data, in each direction.
    pub latency: Duration,
    /// Up to this much more per chunk, at random. Data still arrives in order, as on TCP.
    pub jitter: Duration,
    /// Bytes per second in each direction of each connection. `None` is unlimited.
    pub bandwidth: Option<u64>,
    /// The chance, from 0 to 1, that a new connection is closed as soon as it's accepted.
    pub drop: f64,
    /// While set, no data moves and new connections wait, as if the network were cut. What was
    /// sent meanwhile arrives when it's lifted, as TCP would deliver it after retransmitting.
    pub partition: bool,
    /// The chance, from 0 to 1, that a chunk cuts its connection instead of going through,
    /// closing both directions at once, as when a load balancer or NAT forgets the connection.
    /// Std can't send a TCP reset, so both sides see the connection end rather than a reset error.
    pub cut: f64,
}

/// Counts of what the proxy did, since it started.
#[derive(Debug, Default)]
pub struct Stats {
    pub accepted: AtomicU64,
    pub dropped: AtomicU64,
    pub bytes: AtomicU64,
    pub cuts: AtomicU64,
}

pub struct Proxy {
    addr: SocketAddr,
    faults: Arc<RwLock<Faults>>,
    pub stats: Arc<Stats>,
}

impl Proxy {
    /// Listens on `listen` and forwards each connection to `upstream`, in background threads that
    /// live as long as the process.
    pub fn start(
        listen: impl ToSocketAddrs,
        upstream: SocketAddr,
        faults: Faults,
    ) -> io::Result<Proxy> {
        let listener = TcpListener::bind(listen)?;
        let proxy = Proxy {
            addr: listener.local_addr()?,
            faults: Arc::new(RwLock::new(faults)),
            stats: Arc::default(),
        };
        let (faults, stats) = (proxy.faults.clone(), proxy.stats.clone());
        thread::spawn(move || {
            for (n, client) in listener.incoming().flatten().enumerate() {
                let (faults, stats) = (faults.clone(), stats.clone());
                thread::spawn(move || connection(client, upstream, &faults, &stats, n as u64));
            }
        });
        Ok(proxy)
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    pub fn faults(&self) -> Faults {
        self.faults.read().unwrap().clone()
    }

    /// Changes the faults. Connections already open pick up the change with their next chunk.
    pub fn set(&self, faults: Faults) {
        *self.faults.write().unwrap() = faults;
    }
}

/// A small xorshift generator; the faults don't need more.
struct Rng(u64);

impl Rng {
    fn new(salt: u64) -> Rng {
        let nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(1, |d| d.as_nanos() as u64);
        Rng((nanos ^ salt.wrapping_mul(0x9e3779b97f4a7c15)) | 1)
    }

    /// Uniform in [0, 1).
    fn unit(&mut self) -> f64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        (self.0 >> 11) as f64 / (1u64 << 53) as f64
    }
}

fn read_faults(faults: &RwLock<Faults>) -> Faults {
    faults.read().unwrap().clone()
}

fn connection(
    client: TcpStream,
    upstream: SocketAddr,
    faults: &Arc<RwLock<Faults>>,
    stats: &Arc<Stats>,
    n: u64,
) {
    stats.accepted.fetch_add(1, Ordering::Relaxed);
    let mut rng = Rng::new(n);
    if rng.unit() < read_faults(faults).drop {
        stats.dropped.fetch_add(1, Ordering::Relaxed);
        // Returning drops the stream, which closes it.
        return;
    }
    // During a partition, the upstream can't be reached yet: wait.
    while read_faults(faults).partition {
        thread::sleep(Duration::from_millis(10));
    }
    let Ok(server) = TcpStream::connect(upstream) else {
        return;
    };
    let _ = (client.set_nodelay(true), server.set_nodelay(true));
    let (Ok(client2), Ok(server2)) = (client.try_clone(), server.try_clone()) else {
        return;
    };
    let (f1, s1) = (faults.clone(), stats.clone());
    let upstream_bound =
        thread::spawn(move || pipe(client, server2, &f1, &s1, Rng::new(n * 2 + 1)));
    pipe(server, client2, faults, stats, Rng::new(n * 2 + 2));
    let _ = upstream_bound.join();
}

/// One direction of a connection. A reader stamps each chunk with when it's due; a writer
/// delivers them in the order they were read, so jitter delays chunks but never reorders them. It
/// also waits out partitions and paces to the bandwidth.
fn pipe(
    mut from: TcpStream,
    mut to: TcpStream,
    faults: &Arc<RwLock<Faults>>,
    stats: &Arc<Stats>,
    mut rng: Rng,
) {
    // Bounded, so a sender that keeps writing during a partition is held back, as TCP's flow
    // control would hold it, instead of filling memory: 64 chunks of up to 16 KB.
    let (tx, rx) = mpsc::sync_channel::<(Instant, Vec<u8>)>(64);
    let reader_faults = faults.clone();
    let mut cut = Rng(rng.0.rotate_left(32) | 1);
    let reader = thread::spawn(move || {
        let mut buf = vec![0u8; 16 * 1024];
        loop {
            let n = match from.read(&mut buf) {
                Ok(0) | Err(_) => break,
                Ok(n) => n,
            };
            let f = read_faults(&reader_faults);
            let due = Instant::now() + f.latency + f.jitter.mul_f64(rng.unit());
            if tx.send((due, buf[..n].to_vec())).is_err() {
                break;
            }
        }
        // An empty chunk means the sender is done.
        let _ = tx.send((Instant::now(), Vec::new()));
    });

    let mut next_free = Instant::now();
    for (due, chunk) in rx {
        sleep_until(due);
        while read_faults(faults).partition {
            thread::sleep(Duration::from_millis(10));
        }
        if chunk.is_empty() {
            break;
        }
        if cut.unit() < read_faults(faults).cut {
            stats.cuts.fetch_add(1, Ordering::Relaxed);
            // Closing this side ends the other direction too, and once this loop stops, the
            // reader's next send fails and its socket is dropped.
            let _ = to.shutdown(Shutdown::Both);
            break;
        }
        if let Some(rate) = read_faults(faults).bandwidth {
            // Each chunk takes its share of the second before the next may go.
            sleep_until(next_free);
            next_free = Instant::now().max(next_free)
                + Duration::from_secs_f64(chunk.len() as f64 / rate.max(1) as f64);
        }
        if to.write_all(&chunk).is_err() {
            break;
        }
        stats.bytes.fetch_add(chunk.len() as u64, Ordering::Relaxed);
    }
    // Pass on the half-close, so the other side sees the end of the data.
    let _ = to.shutdown(Shutdown::Write);
    let _ = reader.join();
}

fn sleep_until(at: Instant) {
    let now = Instant::now();
    if at > now {
        thread::sleep(at - now);
    }
}

/// Parses `250ms`, `1.5s` or `2m`.
pub fn parse_duration(text: &str) -> Result<Duration, String> {
    let (number, unit) = text.split_at(
        text.find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(text.len()),
    );
    let value: f64 = number
        .parse()
        .map_err(|_| format!("not a duration: {text}"))?;
    let seconds = match unit {
        "ms" => value / 1000.0,
        "s" => value,
        "m" => value * 60.0,
        _ => return Err(format!("not a duration: {text} (use ms, s or m)")),
    };
    if !(seconds >= 0.0 && seconds.is_finite()) {
        return Err(format!("not a duration: {text}"));
    }
    Ok(Duration::from_secs_f64(seconds))
}

/// Parses a rate like `500kb` or `2mb`, in bytes per second.
pub fn parse_rate(text: &str) -> Result<u64, String> {
    let lower = text.to_ascii_lowercase();
    let (number, unit) = lower.split_at(
        lower
            .find(|c: char| c.is_ascii_alphabetic())
            .unwrap_or(lower.len()),
    );
    let value: f64 = number.parse().map_err(|_| format!("not a rate: {text}"))?;
    let scale = match unit {
        "" | "b" => 1.0,
        "kb" => 1024.0,
        "mb" => 1024.0 * 1024.0,
        _ => return Err(format!("not a rate: {text} (use b, kb or mb a second)")),
    };
    if !(value > 0.0 && value.is_finite()) {
        return Err(format!("not a rate: {text}"));
    }
    Ok((value * scale) as u64)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_and_rates() {
        assert_eq!(parse_duration("250ms"), Ok(Duration::from_millis(250)));
        assert_eq!(parse_duration("1.5s"), Ok(Duration::from_millis(1500)));
        assert_eq!(parse_duration("2m"), Ok(Duration::from_secs(120)));
        assert_eq!(parse_duration("0s"), Ok(Duration::ZERO));
        for bad in ["", "5", "5h", "-1s", "s", "1.2.3s"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
        assert_eq!(parse_rate("500kb"), Ok(512_000));
        assert_eq!(parse_rate("2MB"), Ok(2 * 1024 * 1024));
        assert_eq!(parse_rate("100"), Ok(100));
        for bad in ["", "0kb", "-1mb", "1gb", "kb"] {
            assert!(parse_rate(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn the_random_numbers_stay_in_range() {
        let mut rng = Rng::new(7);
        let samples: Vec<f64> = (0..10_000).map(|_| rng.unit()).collect();
        assert!(samples.iter().all(|x| (0.0..1.0).contains(x)));
        let mean = samples.iter().sum::<f64>() / samples.len() as f64;
        assert!((0.45..0.55).contains(&mean), "{mean}");
    }
}
