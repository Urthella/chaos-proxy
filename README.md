# chaos-proxy

[![ci](https://github.com/Urthella/chaos-proxy/actions/workflows/ci.yml/badge.svg)](https://github.com/Urthella/chaos-proxy/actions/workflows/ci.yml)

A TCP proxy that makes the network worse on purpose: latency, jitter, a bandwidth limit, dropped
connections, cut connections and partitions, all changeable while it runs. Put it between a client and a service
to see how they cope. Plain Rust, no dependencies.

```bash
cargo run --release -- 127.0.0.1:9000 127.0.0.1:8080 --latency 50ms --jitter 10ms
```

Then type to change the faults while it runs:

```
latency 100ms   jitter 20ms   bandwidth 1mb   bandwidth off   drop 0.2   cut 0.01
partition on    partition off   show   stats
```

## A partition, measured

Here it's in front of a small HTTP server with 5 ms of latency each way. Partway through,
`partition on` and, a second later, `partition off` were typed into it, while
[rs-loadgen](https://github.com/Urthella/rs-loadgen) sent 200 requests a second through it:

```
1000 requests to 127.0.0.1:4403/ at 200/s over 20 connections, open model
second   0:    200 requests  p50 16.4 ms  p90 17.3 ms  p99 18.7 ms  p99.9 25.4 ms  max 25.4 ms
second   1:    200 requests  p50 624.4 ms  p90 971.7 ms  p99 1057.9 ms  p99.9 1059.5 ms  max 1059.5 ms
second   2:    200 requests  p50 16.6 ms  p90 189.4 ms  p99 268.5 ms  p99.9 276.0 ms  max 276.0 ms
second   3:    200 requests  p50 16.3 ms  p90 16.8 ms  p99 17.3 ms  p99.9 17.6 ms  max 17.6 ms
second   4:    200 requests  p50 16.2 ms  p90 16.8 ms  p99 17.3 ms  p99.9 18.1 ms  max 18.1 ms
1000 of 1000 scheduled requests answered, 0 errors, 5.0 s
```

The second with the partition is a second of waiting, the backlog clears in the next, and nothing
was lost: data sent during a partition is held and arrives when it ends, as TCP would deliver it
after retransmitting.

## Faults

| Fault | What it does |
| --- | --- |
| `--latency 50ms` | Delays every chunk of data by that much, in each direction. |
| `--jitter 10ms` | Adds up to that much more per chunk, at random. Data still arrives in order. |
| `--bandwidth 500kb` | Limits each direction of each connection to that many bytes a second. |
| `--drop 0.1` | Closes that fraction of new connections as soon as they're accepted. |
| `--cut 0.01` | Gives each chunk that chance of cutting its connection instead of going through, both ways at once, as when a load balancer or NAT forgets a connection. Std can't send a TCP reset, so both sides see the connection end rather than a reset error. |
| `partition on` | Stops all data, and new connections wait without reaching the upstream, until `partition off`. |

As a library, `Proxy::start(listen, upstream, faults)` returns a handle whose `set` changes the
faults for connections that are already open, so a test can cut the network at the moment it wants.

## How it's tested

The tests run the proxy in front of a real echo server and measure each fault: 4 MB comes back
unchanged, 50 ms of latency adds 100 ms to a round trip, 300 numbered messages with heavy jitter
come back in order, 300 KB at 200 KB a second takes about 1.5 s, half the connections are dropped
at a chance of 0.5, a cut ends a connection for the client within a moment, and during a partition
neither data nor new connections reach the upstream.

To check that the tests can fail, 12 bugs were planted one at a time, and every one was caught:
among them, data flowing during a partition, new connections reaching the upstream during one, and
the half-close not being passed on, which left the other side waiting for data that would never come.

Writing the tests also showed four pieces of logic that never decided anything, so they were
deleted. The main one kept each chunk's due time after the one before it so jitter couldn't
reorder data, but the writer delivers chunks in the order they were read anyway.

## Limits

- **Faults act on chunks of up to 16 KB as they're read, not on packets.** Latency and jitter
  are per chunk, and there's no packet loss, only the partition and dropped connections.
- **Four threads per connection**, which is fine for tests, not for thousands of connections.
- **During a partition, data is held, not lost.** Up to 1 MB per direction waits in the proxy, and
  then the sender is held back too, as TCP's flow control would hold it.
- **The tests with upper time limits are skipped on GitHub's macOS runners**, where the machine's
  own scheduling delays reached tens of milliseconds. They run on Linux and Windows.

The Shopify team's [Toxiproxy](https://github.com/Shopify/toxiproxy) is the full-featured tool
for this, with an HTTP API and many more faults. This one is small enough to read in one sitting.

Claude, Anthropic's AI model, wrote this with Halil Utku Demirtaş directing the work.

## License

MIT
