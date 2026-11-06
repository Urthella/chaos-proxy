//! Each fault, measured through a real proxy in front of a real echo server.

use std::io::{Read, Write};
use std::net::{Shutdown, SocketAddr, TcpListener, TcpStream};
use std::sync::{Mutex, MutexGuard};
use std::thread;
use std::time::{Duration, Instant};

use chaos_proxy::{Faults, Proxy};

/// These tests measure time, so they take turns rather than slow each other down.
fn one_at_a_time() -> MutexGuard<'static, ()> {
    static TURN: Mutex<()> = Mutex::new(());
    TURN.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// A server that sends back whatever it gets, until the client stops sending.
fn echo_server() -> SocketAddr {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            thread::spawn(move || {
                let _ = stream.set_nodelay(true);
                let mut reader = stream.try_clone().unwrap();
                let mut writer = stream;
                let _ = std::io::copy(&mut reader, &mut writer);
                let _ = writer.shutdown(Shutdown::Write);
            });
        }
    });
    addr
}

fn proxy(faults: Faults) -> Proxy {
    Proxy::start("127.0.0.1:0", echo_server(), faults).unwrap()
}

fn connect(proxy: &Proxy) -> TcpStream {
    let stream = TcpStream::connect(proxy.addr()).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .unwrap();
    stream
}

/// Sends a byte and waits for it to come back.
fn round_trip(stream: &mut TcpStream) -> Duration {
    let started = Instant::now();
    stream.write_all(b"x").unwrap();
    let mut byte = [0u8];
    stream.read_exact(&mut byte).unwrap();
    started.elapsed()
}

fn median(mut times: Vec<Duration>) -> Duration {
    times.sort();
    times[times.len() / 2]
}

#[test]
fn data_comes_through_unchanged_and_whole() {
    let _turn = one_at_a_time();
    let p = proxy(Faults::default());
    let mut stream = connect(&p);
    let data: Vec<u8> = (0..4_000_000u32)
        .map(|i| (i.wrapping_mul(2654435761) >> 13) as u8)
        .collect();
    let sent = data.clone();
    let mut writer = stream.try_clone().unwrap();
    let sender = thread::spawn(move || {
        writer.write_all(&sent).unwrap();
        // The half-close has to pass through for the echo server to finish.
        writer.shutdown(Shutdown::Write).unwrap();
    });
    let mut back = Vec::new();
    stream.read_to_end(&mut back).unwrap();
    sender.join().unwrap();
    assert!(
        back == data,
        "got {} bytes back of {}",
        back.len(),
        data.len()
    );
    let bytes = p.stats.bytes.load(std::sync::atomic::Ordering::Relaxed);
    assert_eq!(bytes, 2 * data.len() as u64, "bytes counted both ways");
}

#[test]
#[cfg_attr(
    target_os = "macos",
    ignore = "GitHub's macOS runners delay threads by tens of milliseconds, past these upper bounds"
)]
fn latency_is_added_each_way() {
    let _turn = one_at_a_time();
    let p = proxy(Faults {
        latency: Duration::from_millis(50),
        ..Faults::default()
    });
    let mut stream = connect(&p);
    let rtt = median((0..15).map(|_| round_trip(&mut stream)).collect());
    assert!(
        rtt >= Duration::from_millis(100) && rtt < Duration::from_millis(180),
        "round trip {rtt:?}"
    );
}

#[test]
fn jitter_never_reorders_the_stream() {
    let _turn = one_at_a_time();
    let p = proxy(Faults {
        latency: Duration::from_millis(2),
        jitter: Duration::from_millis(30),
        ..Faults::default()
    });
    let mut stream = connect(&p);
    let mut writer = stream.try_clone().unwrap();
    let sender = thread::spawn(move || {
        for i in 0..300u16 {
            writer.write_all(&i.to_be_bytes()).unwrap();
            thread::sleep(Duration::from_micros(500));
        }
        writer.shutdown(Shutdown::Write).unwrap();
    });
    let mut back = Vec::new();
    stream.read_to_end(&mut back).unwrap();
    sender.join().unwrap();
    let numbers: Vec<u16> = back
        .chunks(2)
        .map(|c| u16::from_be_bytes([c[0], c[1]]))
        .collect();
    assert_eq!(numbers, (0..300).collect::<Vec<_>>());
}

#[test]
#[cfg_attr(
    target_os = "macos",
    ignore = "GitHub's macOS runners delay threads by tens of milliseconds, past these upper bounds"
)]
fn bandwidth_is_capped() {
    let _turn = one_at_a_time();
    let p = proxy(Faults {
        bandwidth: Some(200 * 1024),
        ..Faults::default()
    });
    let mut stream = connect(&p);
    let mut writer = stream.try_clone().unwrap();
    let started = Instant::now();
    // 300 KB each way at 200 KB a second: about 1.5 s, since both directions run at once.
    let sender = thread::spawn(move || {
        writer.write_all(&vec![7u8; 300 * 1024]).unwrap();
        writer.shutdown(Shutdown::Write).unwrap();
    });
    let mut back = Vec::new();
    stream.read_to_end(&mut back).unwrap();
    sender.join().unwrap();
    let took = started.elapsed();
    assert_eq!(back.len(), 300 * 1024);
    assert!(
        took >= Duration::from_millis(1300) && took < Duration::from_millis(2600),
        "took {took:?}"
    );
}

#[test]
fn dropped_connections_are_closed_at_once() {
    let _turn = one_at_a_time();
    let p = proxy(Faults {
        drop: 1.0,
        ..Faults::default()
    });
    let mut stream = connect(&p);
    let mut buf = [0u8; 1];
    // Closed by the proxy: the read ends (or fails) right away instead of timing out.
    let started = Instant::now();
    let result = stream.write_all(b"x").and_then(|_| stream.read(&mut buf));
    assert!(matches!(result, Ok(0) | Err(_)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(2));

    p.set(Faults {
        drop: 0.5,
        ..Faults::default()
    });
    for _ in 0..200 {
        drop(TcpStream::connect(p.addr()).unwrap());
    }
    // Every connect is counted, though the proxy may still be handling the last few.
    thread::sleep(Duration::from_millis(300));
    let dropped = p.stats.dropped.load(std::sync::atomic::Ordering::Relaxed) - 1;
    assert!(
        (60..=140).contains(&dropped),
        "{dropped} of 200 dropped at a chance of 0.5"
    );
}

#[test]
fn a_partition_holds_data_and_new_connections_until_it_ends() {
    let _turn = one_at_a_time();
    // An echo server that counts the connections it gets.
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let upstream = listener.local_addr().unwrap();
    let arrived = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let counter = arrived.clone();
    thread::spawn(move || {
        for stream in listener.incoming().flatten() {
            counter.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            thread::spawn(move || {
                let mut reader = stream.try_clone().unwrap();
                let mut writer = stream;
                let _ = std::io::copy(&mut reader, &mut writer);
            });
        }
    });
    let count = || arrived.load(std::sync::atomic::Ordering::SeqCst);
    let p = Proxy::start("127.0.0.1:0", upstream, Faults::default()).unwrap();
    let mut stream = connect(&p);
    round_trip(&mut stream);
    assert_eq!(count(), 1);

    p.set(Faults {
        partition: true,
        ..Faults::default()
    });
    stream.write_all(b"held").unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(400)))
        .unwrap();
    let mut buf = [0u8; 4];
    assert!(
        stream.read(&mut buf).is_err(),
        "data came through a partition"
    );

    // A new connection doesn't reach the upstream until the partition ends.
    let mut late = connect(&p);
    late.write_all(b"late").unwrap();
    thread::sleep(Duration::from_millis(200));
    assert_eq!(
        count(),
        1,
        "a connection reached the upstream during the partition"
    );

    p.set(Faults::default());
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"held");
    late.read_exact(&mut buf).unwrap();
    assert_eq!(&buf, b"late");
    assert_eq!(count(), 2);
}

#[test]
#[cfg_attr(
    target_os = "macos",
    ignore = "GitHub's macOS runners delay threads by tens of milliseconds, past these upper bounds"
)]
fn faults_change_while_a_connection_is_open() {
    let _turn = one_at_a_time();
    let p = proxy(Faults::default());
    let mut stream = connect(&p);
    let before = median((0..9).map(|_| round_trip(&mut stream)).collect());
    p.set(Faults {
        latency: Duration::from_millis(40),
        ..Faults::default()
    });
    let after = median((0..9).map(|_| round_trip(&mut stream)).collect());
    assert!(before < Duration::from_millis(30), "before {before:?}");
    assert!(after >= Duration::from_millis(80), "after {after:?}");
}

#[test]
fn an_unreachable_upstream_closes_the_client() {
    let _turn = one_at_a_time();
    let port = TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap()
        .port();
    let p = Proxy::start(
        "127.0.0.1:0",
        format!("127.0.0.1:{port}").parse().unwrap(),
        Faults::default(),
    )
    .unwrap();
    let mut stream = connect(&p);
    let mut buf = [0u8; 1];
    let result = stream.write_all(b"x").and_then(|_| stream.read(&mut buf));
    assert!(matches!(result, Ok(0) | Err(_)), "{result:?}");
}

#[test]
fn a_cut_ends_the_connection_both_ways() {
    let _turn = one_at_a_time();
    let p = proxy(Faults {
        cut: 1.0,
        ..Faults::default()
    });
    let mut stream = connect(&p);
    stream.write_all(b"x").unwrap();
    let mut buf = [0u8; 1];
    let started = Instant::now();
    let result = stream.read(&mut buf);
    assert!(matches!(result, Ok(0) | Err(_)), "{result:?}");
    assert!(started.elapsed() < Duration::from_secs(2));
    assert_eq!(p.stats.cuts.load(std::sync::atomic::Ordering::Relaxed), 1);

    // At a chance of 0.2 a chunk, a run of round trips gets cut before long.
    p.set(Faults {
        cut: 0.2,
        ..Faults::default()
    });
    let mut stream = connect(&p);
    let mut answered = 0;
    for _ in 0..100 {
        let mut byte = [0u8];
        if stream
            .write_all(b"x")
            .and_then(|_| stream.read_exact(&mut byte))
            .is_err()
        {
            break;
        }
        answered += 1;
    }
    assert!(answered < 50, "{answered} round trips before a cut");
}
