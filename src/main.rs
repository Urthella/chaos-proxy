use std::io::{self, BufRead};
use std::net::ToSocketAddrs;
use std::process::ExitCode;
use std::sync::atomic::Ordering;

use chaos_proxy::{Faults, Proxy, parse_duration, parse_rate};

const USAGE: &str = "usage: chaos-proxy LISTEN UPSTREAM [--latency 50ms] [--jitter 10ms] [--bandwidth 500kb] [--drop 0.1]

  chaos-proxy 127.0.0.1:9000 127.0.0.1:8080 --latency 50ms

Then type commands to change the faults while it runs:
  latency 100ms   jitter 20ms   bandwidth 1mb   bandwidth off   drop 0.2
  partition on    partition off   show   stats";

/// Applies one `name value` setting to the faults.
fn apply(faults: &mut Faults, name: &str, value: &str) -> Result<(), String> {
    match name {
        "latency" => faults.latency = parse_duration(value)?,
        "jitter" => faults.jitter = parse_duration(value)?,
        "bandwidth" => {
            faults.bandwidth = if value == "off" {
                None
            } else {
                Some(parse_rate(value)?)
            }
        }
        "drop" => {
            faults.drop = value
                .parse()
                .ok()
                .filter(|p| (0.0..=1.0).contains(p))
                .ok_or(format!("drop is a chance from 0 to 1, not {value}"))?
        }
        "partition" => {
            faults.partition = match value {
                "on" => true,
                "off" => false,
                _ => return Err(format!("partition is on or off, not {value}")),
            }
        }
        _ => return Err(format!("unknown setting: {name}")),
    }
    Ok(())
}

fn parse(args: &[String]) -> Result<(String, String, Faults), String> {
    let [listen, upstream, flags @ ..] = args else {
        return Err("give LISTEN and UPSTREAM addresses".into());
    };
    let mut faults = Faults::default();
    let mut rest = flags.iter();
    while let Some(flag) = rest.next() {
        let name = flag
            .strip_prefix("--")
            .ok_or(format!("unknown option: {flag}"))?;
        let value = rest.next().ok_or(format!("{flag} needs a value"))?;
        apply(&mut faults, name, value)?;
    }
    Ok((listen.clone(), upstream.clone(), faults))
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args[0] == "-h" || args[0] == "--help" {
        println!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let (listen, upstream, faults) = match parse(&args) {
        Ok(parsed) => parsed,
        Err(e) => {
            eprintln!("{e}\n\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let Some(upstream) = upstream.to_socket_addrs().ok().and_then(|mut a| a.next()) else {
        eprintln!("can't resolve {upstream}");
        return ExitCode::from(2);
    };
    let proxy = match Proxy::start(&listen, upstream, faults) {
        Ok(proxy) => proxy,
        Err(e) => {
            eprintln!("can't listen on {listen}: {e}");
            return ExitCode::FAILURE;
        }
    };
    println!("{} -> {upstream}  {:?}", proxy.addr(), proxy.faults());
    for line in io::stdin().lock().lines().map_while(Result::ok) {
        let words: Vec<&str> = line.split_whitespace().collect();
        match words[..] {
            [] => {}
            ["show"] => println!("{:?}", proxy.faults()),
            ["stats"] => println!(
                "{} connections, {} dropped, {} bytes",
                proxy.stats.accepted.load(Ordering::Relaxed),
                proxy.stats.dropped.load(Ordering::Relaxed),
                proxy.stats.bytes.load(Ordering::Relaxed)
            ),
            [name, value] => {
                let mut faults = proxy.faults();
                match apply(&mut faults, name, value) {
                    Ok(()) => {
                        proxy.set(faults);
                        println!("{:?}", proxy.faults());
                    }
                    Err(e) => println!("{e}"),
                }
            }
            _ => println!("commands: latency, jitter, bandwidth, drop, partition, show, stats"),
        }
    }
    // Without a terminal, keep running until stopped.
    std::thread::park();
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    fn args(line: &str) -> Vec<String> {
        line.split_whitespace().map(String::from).collect()
    }

    #[test]
    fn options() {
        let (listen, upstream, f) = parse(&args(
            "127.0.0.1:9000 127.0.0.1:8080 --latency 50ms --jitter 1s --bandwidth 2mb --drop 0.25",
        ))
        .unwrap();
        assert_eq!(
            (listen.as_str(), upstream.as_str()),
            ("127.0.0.1:9000", "127.0.0.1:8080")
        );
        assert_eq!(f.latency, std::time::Duration::from_millis(50));
        assert_eq!(
            (f.bandwidth, f.drop, f.partition),
            (Some(2 * 1024 * 1024), 0.25, false)
        );
        for bad in [
            "a",
            "a b --latency",
            "a b --latency fast",
            "a b --drop 2",
            "a b --speed 1",
            "a b latency 1s",
            "a b --partition maybe",
        ] {
            assert!(parse(&args(bad)).is_err(), "{bad}");
        }
    }

    #[test]
    fn commands_change_one_setting() {
        let mut f = Faults::default();
        apply(&mut f, "partition", "on").unwrap();
        apply(&mut f, "bandwidth", "1kb").unwrap();
        assert!(f.partition);
        apply(&mut f, "bandwidth", "off").unwrap();
        apply(&mut f, "partition", "off").unwrap();
        assert_eq!(f, Faults::default());
    }
}
